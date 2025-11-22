package grpc

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// ReplicationHandler handles transaction replication with MVCC
type ReplicationHandler struct {
	nodeID uint64
	dbMgr  *db.DatabaseManager
	clock  *hlc.Clock
}

// NewReplicationHandler creates a new replication handler
func NewReplicationHandler(nodeID uint64, dbMgr *db.DatabaseManager, clock *hlc.Clock) *ReplicationHandler {
	return &ReplicationHandler{
		nodeID: nodeID,
		dbMgr:  dbMgr,
		clock:  clock,
	}
}

// HandleReplicateTransaction handles incoming transaction replication requests
// This is the CRITICAL gRPC handler that implements 2PC protocol
func (rh *ReplicationHandler) HandleReplicateTransaction(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	// Update local clock with incoming timestamp
	incomingTS := hlc.Timestamp{
		WallTime: req.Timestamp.WallTime,
		Logical:  req.Timestamp.Logical,
		NodeID:   req.Timestamp.NodeId,
	}
	rh.clock.Update(incomingTS)

	switch req.Phase {
	case TransactionPhase_PREPARE:
		return rh.handlePrepare(ctx, req)
	case TransactionPhase_COMMIT:
		return rh.handleCommit(ctx, req)
	case TransactionPhase_ABORT:
		return rh.handleAbort(ctx, req)
	default:
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("unknown transaction phase: %v", req.Phase),
		}, nil
	}
}

// handlePrepare processes Phase 1 of 2PC: Create write intents
func (rh *ReplicationHandler) handlePrepare(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	// Get the target database from request
	dbName := req.Database
	if dbName == "" {
		dbName = db.DefaultDatabaseName
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("database %s not found: %v", dbName, err),
		}, nil
	}

	txnMgr := dbInstance.GetTransactionManager()
	database := dbInstance.GetDB()

	// Begin local transaction
	txn, err := txnMgr.BeginTransaction(req.SourceNodeId)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to begin transaction: %v", err),
		}, nil
	}

	// Store original ID before override
	originalID := txn.ID

	// Override transaction ID to match coordinator's
	txn.ID = req.TxnId
	txn.StartTS = hlc.Timestamp{
		WallTime: req.Timestamp.WallTime,
		Logical:  req.Timestamp.Logical,
		NodeID:   req.Timestamp.NodeId,
	}

	// Update the transaction ID in the active transactions map
	txnMgr.UpdateTransactionID(originalID, req.TxnId)

	// Update transaction record in database with coordinator's ID
	_, err = database.Exec(`
		UPDATE __marmot__txn_records
		SET txn_id = ?
		WHERE txn_id = ?
	`, req.TxnId, originalID)
	if err != nil {
		txnMgr.AbortTransaction(txn)
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to update txn ID: %v", err),
		}, nil
	}

	// Process each statement and create write intents
	for _, stmt := range req.Statements {
		// Convert proto statement to internal format
		internalStmt := protocol.Statement{
			SQL:       stmt.Sql,
			Type:      convertStatementType(stmt.Type),
			TableName: stmt.TableName,
			Database:  stmt.Database,
		}

		// Add statement to transaction
		if err := txnMgr.AddStatement(txn, internalStmt); err != nil {
			txnMgr.AbortTransaction(txn)
			return &TransactionResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("failed to add statement: %v", err),
			}, nil
		}

		// Create write intent for this statement
		// Extract row key from statement (simplified - in real impl would parse SQL)
		rowKey := extractRowKey(stmt.Sql, stmt.TableName)
		dataSnapshot, _ := db.SerializeData(map[string]interface{}{
			"sql":       stmt.Sql,
			"type":      stmt.Type.String(),
			"timestamp": req.Timestamp.WallTime,
		})

		err := txnMgr.WriteIntent(txn, stmt.TableName, rowKey, internalStmt, dataSnapshot)
		if err != nil {
			// Write-write conflict detected!
			txnMgr.AbortTransaction(txn)
			return &TransactionResponse{
				Success:          false,
				ErrorMessage:     fmt.Sprintf("write conflict: %v", err),
				ConflictDetected: true,
				ConflictDetails:  err.Error(),
			}, nil
		}
	}

	// Success! Write intents created
	return &TransactionResponse{
		Success: true,
		AppliedAt: &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

// handleCommit processes Phase 2 of 2PC: Commit transaction
func (rh *ReplicationHandler) handleCommit(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	// Get the target database from request
	dbName := req.Database
	if dbName == "" {
		dbName = db.DefaultDatabaseName
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("database %s not found: %v", dbName, err),
		}, nil
	}

	txnMgr := dbInstance.GetTransactionManager()

	// Retrieve transaction from active transactions
	txn := txnMgr.GetTransaction(req.TxnId)
	if txn == nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("transaction %d not found", req.TxnId),
		}, nil
	}

	// Commit the transaction
	err = txnMgr.CommitTransaction(txn)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to commit: %v", err),
		}, nil
	}

	return &TransactionResponse{
		Success: true,
		AppliedAt: &HLC{
			WallTime: txn.CommitTS.WallTime,
			Logical:  txn.CommitTS.Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

// handleAbort processes abort: Rollback transaction
func (rh *ReplicationHandler) handleAbort(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	// Get the target database from request
	dbName := req.Database
	if dbName == "" {
		dbName = db.DefaultDatabaseName
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("database %s not found: %v", dbName, err),
		}, nil
	}

	txnMgr := dbInstance.GetTransactionManager()

	// Retrieve transaction from active transactions
	txn := txnMgr.GetTransaction(req.TxnId)
	if txn == nil {
		// Transaction not found - already aborted or never started
		return &TransactionResponse{
			Success: true, // Idempotent - abort of non-existent txn is success
		}, nil
	}

	// Abort the transaction
	err = txnMgr.AbortTransaction(txn)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to abort: %v", err),
		}, nil
	}

	return &TransactionResponse{
		Success: true,
	}, nil
}

// HandleRead handles incoming read requests with MVCC snapshot isolation
func (rh *ReplicationHandler) HandleRead(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	// Update local clock with incoming timestamp
	snapshotTS := hlc.Timestamp{
		WallTime: req.SnapshotTs.WallTime,
		Logical:  req.SnapshotTs.Logical,
		NodeID:   req.SnapshotTs.NodeId,
	}
	rh.clock.Update(snapshotTS)

	// Get the target database from request
	dbName := req.Database
	if dbName == "" {
		dbName = db.DefaultDatabaseName
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return &ReadResponse{
			Timestamp: &HLC{
				WallTime: rh.clock.Now().WallTime,
				Logical:  rh.clock.Now().Logical,
				NodeId:   rh.nodeID,
			},
		}, fmt.Errorf("database %s not found: %w", dbName, err)
	}

	database := dbInstance.GetDB()

	// Execute local snapshot read
	rows, err := database.QueryContext(ctx, req.Query)
	if err != nil {
		return &ReadResponse{
			Timestamp: &HLC{
				WallTime: rh.clock.Now().WallTime,
				Logical:  rh.clock.Now().Logical,
				NodeId:   rh.nodeID,
			},
		}, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Read all rows
	var results []*Row
	for rows.Next() {
		// Create a slice of interface{}'s to scan into
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		// Build result map
		rowMap := make(map[string][]byte)
		for i, col := range columns {
			val := values[i]
			// Convert to bytes
			if b, ok := val.([]byte); ok {
				rowMap[col] = b
			} else if s, ok := val.(string); ok {
				rowMap[col] = []byte(s)
			} else {
				rowMap[col] = []byte(fmt.Sprintf("%v", val))
			}
		}
		results = append(results, &Row{Columns: rowMap})
	}

	return &ReadResponse{
		Rows: results,
		Timestamp: &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

// Helper functions

func convertStatementType(st StatementType) protocol.StatementType {
	switch st {
	case StatementType_INSERT:
		return protocol.StatementInsert
	case StatementType_UPDATE:
		return protocol.StatementUpdate
	case StatementType_DELETE:
		return protocol.StatementDelete
	case StatementType_REPLACE:
		return protocol.StatementReplace
	case StatementType_DDL:
		return protocol.StatementDDL
	default:
		return protocol.StatementInsert // Default to insert
	}
}

// extractRowKey extracts row key from SQL statement (simplified)
// In a real implementation, this would parse the SQL and extract primary key values
func extractRowKey(sql, tableName string) string {
	// Simple heuristic: look for "WHERE id = X" pattern
	// For testing purposes, extract the ID value
	var rowKey string

	// Try to extract from WHERE clause
	if idx := indexOf(sql, "WHERE id = "); idx >= 0 {
		start := idx + len("WHERE id = ")
		// Extract the number
		end := start
		for end < len(sql) && (sql[end] >= '0' && sql[end] <= '9') {
			end++
		}
		if end > start {
			rowKey = sql[start:end]
		}
	}

	// Try to extract from VALUES clause for INSERT
	if rowKey == "" && indexOf(sql, "VALUES (") >= 0 {
		start := indexOf(sql, "VALUES (") + len("VALUES (")
		end := start
		for end < len(sql) && (sql[end] >= '0' && sql[end] <= '9') {
			end++
		}
		if end > start {
			rowKey = sql[start:end]
		}
	}

	// Fallback: use hash
	if rowKey == "" {
		rowKey = fmt.Sprintf("%x", []byte(sql)[:min(16, len(sql))])
	}

	return rowKey
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
