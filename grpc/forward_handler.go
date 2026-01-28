package grpc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// ForwardHandler handles write forwarding from read-only replicas
type ForwardHandler struct {
	nodeID       uint64
	clock        *hlc.Clock
	sessionMgr   *ForwardSessionManager
	coordHandler *coordinator.CoordinatorHandler
	dbManager    ForwardDBManager
}

// ForwardDBManager interface for database operations
type ForwardDBManager interface {
	GetDatabaseConnection(name string) (*sql.DB, error)
	DatabaseExists(name string) bool
}

// NewForwardHandler creates a new forward handler
func NewForwardHandler(nodeID uint64, clock *hlc.Clock, sessionMgr *ForwardSessionManager,
	coordHandler *coordinator.CoordinatorHandler, dbManager ForwardDBManager) *ForwardHandler {
	return &ForwardHandler{
		nodeID:       nodeID,
		clock:        clock,
		sessionMgr:   sessionMgr,
		coordHandler: coordHandler,
		dbManager:    dbManager,
	}
}

// HandleForwardQuery handles a forwarded query from a read-only replica
func (h *ForwardHandler) HandleForwardQuery(ctx context.Context, req *ForwardQueryRequest) (*ForwardQueryResponse, error) {
	if req.ReplicaNodeId == 0 || req.SessionId == 0 {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "replica_node_id and session_id are required",
		}, nil
	}

	// Handle transaction control first - doesn't need parsing or database validation
	switch req.TxnControl {
	case ForwardTxnControl_FWD_TXN_BEGIN, ForwardTxnControl_FWD_TXN_COMMIT, ForwardTxnControl_FWD_TXN_ROLLBACK:
		// Transaction control requires database context
		if req.Database == "" {
			return &ForwardQueryResponse{
				Success:      false,
				ErrorMessage: "database is required",
			}, nil
		}
		if !h.dbManager.DatabaseExists(req.Database) {
			return &ForwardQueryResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("database %s does not exist", req.Database),
			}, nil
		}
		key := ForwardSessionKey{
			ReplicaNodeID: req.ReplicaNodeId,
			SessionID:     req.SessionId,
		}
		session := h.sessionMgr.GetOrCreateSession(key, req.Database)
		session.Touch()

		switch req.TxnControl {
		case ForwardTxnControl_FWD_TXN_BEGIN:
			return h.handleBegin(ctx, session, req)
		case ForwardTxnControl_FWD_TXN_COMMIT:
			return h.handleCommit(ctx, session, req)
		case ForwardTxnControl_FWD_TXN_ROLLBACK:
			return h.handleRollback(ctx, session, req)
		}
	}

	// For regular statements, database is required (unless it's CREATE/DROP DATABASE)
	// Check database first if SQL is empty or not a database operation
	if req.Sql == "" {
		if req.Database == "" {
			return &ForwardQueryResponse{
				Success:      false,
				ErrorMessage: "database is required",
			}, nil
		}
	} else {
		// Parse to classify statement type - need transpilation for proper classification
		stmt := protocol.ParseStatement(req.Sql)

		// CREATE/DROP DATABASE don't need database context - handle via coordinator directly
		if stmt.Type == protocol.StatementCreateDatabase || stmt.Type == protocol.StatementDropDatabase {
			return h.handleDatabaseOp(ctx, req, stmt)
		}
	}

	// For all other statements, database is required
	if req.Database == "" {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "database is required",
		}, nil
	}

	if !h.dbManager.DatabaseExists(req.Database) {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("database %s does not exist", req.Database),
		}, nil
	}

	key := ForwardSessionKey{
		ReplicaNodeID: req.ReplicaNodeId,
		SessionID:     req.SessionId,
	}
	session := h.sessionMgr.GetOrCreateSession(key, req.Database)
	session.Touch()

	return h.handleStatement(ctx, session, req)
}

// handleDatabaseOp handles CREATE/DROP DATABASE via coordinator
func (h *ForwardHandler) handleDatabaseOp(_ context.Context, req *ForwardQueryRequest, stmt protocol.Statement) (*ForwardQueryResponse, error) {
	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Int("stmt_type", int(stmt.Type)).
		Str("database", stmt.Database).
		Msg("Database operation via forward")

	// Use coordinator with transpilation enabled - it handles CREATE/DROP DATABASE properly
	connSession := &protocol.ConnectionSession{
		ConnID:               req.SessionId,
		TranspilationEnabled: true, // Need this for proper CREATE/DROP DATABASE handling
	}

	rs, err := h.coordHandler.HandleQuery(connSession, req.Sql, nil)
	if err != nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	var rowsAffected int64
	if rs != nil {
		rowsAffected = rs.RowsAffected
	}

	return &ForwardQueryResponse{
		Success:      true,
		RowsAffected: rowsAffected,
	}, nil
}

// handleBegin starts a new transaction
func (h *ForwardHandler) handleBegin(_ context.Context, session *ForwardSession, req *ForwardQueryRequest) (*ForwardQueryResponse, error) {
	startTS := h.clock.Now()
	txnID := startTS.ToTxnID()

	if err := session.BeginTransaction(txnID, startTS, req.Database); err != nil {
		log.Debug().
			Err(err).
			Uint64("replica_node_id", req.ReplicaNodeId).
			Uint64("session_id", req.SessionId).
			Msg("BEGIN failed")
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("txn_id", txnID).
		Str("database", req.Database).
		Msg("BEGIN transaction on leader")

	return &ForwardQueryResponse{
		Success: true,
	}, nil
}

// handleCommit commits the transaction
func (h *ForwardHandler) handleCommit(_ context.Context, session *ForwardSession, req *ForwardQueryRequest) (*ForwardQueryResponse, error) {
	txn := session.GetTransaction()
	if txn == nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "no active transaction",
		}, nil
	}

	if len(txn.Statements) == 0 {
		session.ClearTransaction()
		log.Debug().
			Uint64("replica_node_id", req.ReplicaNodeId).
			Uint64("session_id", req.SessionId).
			Msg("COMMIT empty transaction")
		return &ForwardQueryResponse{
			Success: true,
		}, nil
	}

	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("txn_id", txn.TxnID).
		Int("stmt_count", len(txn.Statements)).
		Msg("COMMIT transaction via coordinator")

	connSession := &protocol.ConnectionSession{
		ConnID:          req.SessionId,
		CurrentDatabase: txn.Database,
	}

	var totalRowsAffected int64
	var lastInsertId int64

	for _, stmt := range txn.Statements {
		rs, err := h.coordHandler.HandleQuery(connSession, stmt.SQL, stmt.Params)
		if err != nil {
			session.ClearTransaction()
			log.Debug().
				Err(err).
				Uint64("replica_node_id", req.ReplicaNodeId).
				Uint64("session_id", req.SessionId).
				Uint64("txn_id", txn.TxnID).
				Msg("COMMIT failed during statement execution")
			return &ForwardQueryResponse{
				Success:      false,
				ErrorMessage: err.Error(),
			}, nil
		}

		if rs != nil {
			totalRowsAffected += rs.RowsAffected
			if rs.LastInsertId > 0 {
				lastInsertId = rs.LastInsertId
			}
		}
	}

	committedTxnID := txn.TxnID
	session.ClearTransaction()

	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("txn_id", committedTxnID).
		Int64("rows_affected", totalRowsAffected).
		Msg("COMMIT transaction successful")

	return &ForwardQueryResponse{
		Success:        true,
		RowsAffected:   totalRowsAffected,
		LastInsertId:   lastInsertId,
		CommittedTxnId: committedTxnID,
	}, nil
}

// handleRollback rolls back the transaction
func (h *ForwardHandler) handleRollback(_ context.Context, session *ForwardSession, req *ForwardQueryRequest) (*ForwardQueryResponse, error) {
	txn := session.GetTransaction()
	stmtCount := 0
	var txnID uint64
	if txn != nil {
		stmtCount = len(txn.Statements)
		txnID = txn.TxnID
	}

	session.ClearTransaction()

	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("txn_id", txnID).
		Int("discarded_stmts", stmtCount).
		Msg("ROLLBACK transaction")

	return &ForwardQueryResponse{
		Success: true,
	}, nil
}

// handleStatement handles a regular statement (auto-commit or buffered)
func (h *ForwardHandler) handleStatement(ctx context.Context, session *ForwardSession, req *ForwardQueryRequest) (*ForwardQueryResponse, error) {
	params, err := DeserializeParams(req.Params)
	if err != nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to deserialize params: %v", err),
		}, nil
	}

	txn := session.GetTransaction()
	if txn != nil {
		if err := session.AddStatement(req.Sql, params); err != nil {
			return &ForwardQueryResponse{
				Success:      false,
				ErrorMessage: err.Error(),
			}, nil
		}

		log.Debug().
			Uint64("replica_node_id", req.ReplicaNodeId).
			Uint64("session_id", req.SessionId).
			Uint64("txn_id", txn.TxnID).
			Str("sql", req.Sql).
			Msg("Buffered statement in transaction")

		return &ForwardQueryResponse{
			Success:       true,
			RowsAffected:  1,
			InTransaction: true,
		}, nil
	}

	timeout := time.Duration(cfg.Config.Replica.ForwardWriteTimeoutSec) * time.Second
	if req.TimeoutMs > 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// SQL from replica is already transpiled - skip re-transpilation
	connSession := &protocol.ConnectionSession{
		ConnID:               req.SessionId,
		CurrentDatabase:      req.Database,
		TranspilationEnabled: false,
	}

	done := make(chan struct{})
	var totalRowsAffected int64
	var lastInsertId int64
	var committedTxnId uint64
	var execErr error

	go func() {
		rs, err := h.coordHandler.HandleQuery(connSession, req.Sql, params)
		if err != nil {
			execErr = err
		} else if rs != nil {
			totalRowsAffected = rs.RowsAffected
			lastInsertId = rs.LastInsertId
			committedTxnId = rs.CommittedTxnId
		}
		close(done)
	}()

	select {
	case <-done:
		if execErr != nil {
			log.Debug().
				Err(execErr).
				Uint64("replica_node_id", req.ReplicaNodeId).
				Uint64("session_id", req.SessionId).
				Str("sql", req.Sql).
				Msg("Statement execution failed")
			return &ForwardQueryResponse{
				Success:      false,
				ErrorMessage: execErr.Error(),
			}, nil
		}

		log.Debug().
			Uint64("replica_node_id", req.ReplicaNodeId).
			Uint64("session_id", req.SessionId).
			Int64("rows_affected", totalRowsAffected).
			Msg("Statement executed successfully")

		return &ForwardQueryResponse{
			Success:        true,
			CommittedTxnId: committedTxnId,
			RowsAffected:   totalRowsAffected,
			LastInsertId:   lastInsertId,
		}, nil

	case <-execCtx.Done():
		log.Debug().
			Uint64("replica_node_id", req.ReplicaNodeId).
			Uint64("session_id", req.SessionId).
			Str("sql", req.Sql).
			Msg("Statement execution timeout")
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "execution timeout",
		}, nil
	}
}
