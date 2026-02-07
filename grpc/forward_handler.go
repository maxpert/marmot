package grpc

import (
	"context"
	"database/sql"
	"fmt"

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
	if req.RequestId == 0 {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "request_id is required",
		}, nil
	}

	key := ForwardSessionKey{
		ReplicaNodeID: req.ReplicaNodeId,
		SessionID:     req.SessionId,
	}
	session := h.sessionMgr.GetOrCreateSession(key, req.Database)
	session.Touch()

	// Handle transaction control first.
	switch req.TxnControl {
	case ForwardTxnControl_FWD_TXN_BEGIN, ForwardTxnControl_FWD_TXN_COMMIT, ForwardTxnControl_FWD_TXN_ROLLBACK:
		// Database is optional for COMMIT/ROLLBACK and BEGIN without pre-selected DB.
		if req.Database != "" && !h.dbManager.DatabaseExists(req.Database) {
			return &ForwardQueryResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("database %s does not exist", req.Database),
			}, nil
		}

		return h.executeIdempotent(ctx, session, req, func(connSession *protocol.ConnectionSession) *ForwardQueryResponse {
			return h.handleTxnControl(connSession, req)
		})
	}

	if req.Sql == "" {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "sql is required",
		}, nil
	}

	// Parse to classify statement type.
	stmt := protocol.ParseStatement(req.Sql)

	// CREATE/DROP DATABASE don't need database context.
	if stmt.Type == protocol.StatementCreateDatabase || stmt.Type == protocol.StatementDropDatabase {
		return h.executeIdempotent(ctx, session, req, func(connSession *protocol.ConnectionSession) *ForwardQueryResponse {
			return h.handleDatabaseOp(connSession, req, stmt)
		})
	}

	// For all other statements, database is required.
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

	return h.executeIdempotent(ctx, session, req, func(connSession *protocol.ConnectionSession) *ForwardQueryResponse {
		return h.handleStatement(connSession, req)
	})
}

func (h *ForwardHandler) executeIdempotent(
	ctx context.Context,
	session *ForwardSession,
	req *ForwardQueryRequest,
	execFn func(connSession *protocol.ConnectionSession) *ForwardQueryResponse,
) (*ForwardQueryResponse, error) {
	reqState, isNew := session.BeginRequest(req.RequestId)
	if !isNew {
		return session.WaitForRequest(ctx, reqState)
	}

	resp, execErr := session.Execute(req.Database, func(connSession *protocol.ConnectionSession) (*ForwardQueryResponse, error) {
		return execFn(connSession), nil
	})
	if execErr != nil {
		resp = &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: execErr.Error(),
		}
	}
	if resp == nil {
		resp = &ForwardQueryResponse{Success: true}
	}
	resp.InTransaction = session.HasActiveTransaction()

	session.CompleteRequest(req.RequestId, reqState, resp)
	return cloneForwardResponse(resp), nil
}

// handleDatabaseOp handles CREATE/DROP DATABASE via coordinator.
func (h *ForwardHandler) handleDatabaseOp(connSession *protocol.ConnectionSession, req *ForwardQueryRequest, stmt protocol.Statement) *ForwardQueryResponse {
	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("request_id", req.RequestId).
		Int("stmt_type", int(stmt.Type)).
		Str("database", stmt.Database).
		Msg("Database operation via forward")

	rs, err := h.coordHandler.HandleQuery(connSession, req.Sql, nil)
	if err != nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}
	}

	var rowsAffected int64
	var committedTxnID uint64
	if rs != nil {
		rowsAffected = rs.RowsAffected
		committedTxnID = rs.CommittedTxnId
	}

	return &ForwardQueryResponse{
		Success:        true,
		RowsAffected:   rowsAffected,
		CommittedTxnId: committedTxnID,
	}
}

func (h *ForwardHandler) handleTxnControl(connSession *protocol.ConnectionSession, req *ForwardQueryRequest) *ForwardQueryResponse {
	var sql string
	switch req.TxnControl {
	case ForwardTxnControl_FWD_TXN_BEGIN:
		sql = "BEGIN"
	case ForwardTxnControl_FWD_TXN_COMMIT:
		sql = "COMMIT"
	case ForwardTxnControl_FWD_TXN_ROLLBACK:
		sql = "ROLLBACK"
	default:
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "invalid transaction control",
		}
	}

	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("request_id", req.RequestId).
		Str("sql", sql).
		Msg("Transaction control forwarded")

	prevTranspilation := connSession.TranspilationEnabled
	connSession.TranspilationEnabled = true
	rs, err := h.coordHandler.HandleQuery(connSession, sql, nil)
	connSession.TranspilationEnabled = prevTranspilation
	if err != nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}
	}

	resp := &ForwardQueryResponse{Success: true}
	if rs != nil {
		resp.RowsAffected = rs.RowsAffected
		resp.LastInsertId = rs.LastInsertId
		resp.CommittedTxnId = rs.CommittedTxnId
	}
	return resp
}

// handleStatement handles a regular statement (auto-commit or within transaction).
func (h *ForwardHandler) handleStatement(connSession *protocol.ConnectionSession, req *ForwardQueryRequest) *ForwardQueryResponse {
	params, err := DeserializeParams(req.Params)
	if err != nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to deserialize params: %v", err),
		}
	}

	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("request_id", req.RequestId).
		Bool("in_transaction", connSession.InTransaction()).
		Str("sql", req.Sql).
		Msg("Forwarded statement")

	rs, execErr := h.coordHandler.HandleQuery(connSession, req.Sql, params)
	if execErr != nil {
		log.Debug().
			Err(execErr).
			Uint64("replica_node_id", req.ReplicaNodeId).
			Uint64("session_id", req.SessionId).
			Uint64("request_id", req.RequestId).
			Str("sql", req.Sql).
			Msg("Statement execution failed")
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: execErr.Error(),
		}
	}

	var rowsAffected int64
	var lastInsertID int64
	var committedTxnID uint64
	if rs != nil {
		rowsAffected = rs.RowsAffected
		lastInsertID = rs.LastInsertId
		committedTxnID = rs.CommittedTxnId
	}

	log.Debug().
		Uint64("replica_node_id", req.ReplicaNodeId).
		Uint64("session_id", req.SessionId).
		Uint64("request_id", req.RequestId).
		Int64("rows_affected", rowsAffected).
		Uint64("committed_txn_id", committedTxnID).
		Msg("Statement executed successfully")

	return &ForwardQueryResponse{
		Success:        true,
		CommittedTxnId: committedTxnID,
		RowsAffected:   rowsAffected,
		LastInsertId:   lastInsertID,
	}
}
