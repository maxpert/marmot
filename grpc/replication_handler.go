package grpc

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
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
	log.Debug().
		Uint64("node_id", rh.nodeID).
		Str("database", req.Database).
		Int("num_statements", len(req.Statements)).
		Msg("handlePrepare called")

	// Handle CREATE DATABASE and DROP DATABASE using system database for transaction tracking
	// These operations need 2PC but don't have a user database to track the transaction in
	if len(req.Statements) == 1 {
		stmt := req.Statements[0]
		stmtType := convertStatementType(stmt.Type)

		if stmtType == protocol.StatementCreateDatabase || stmtType == protocol.StatementDropDatabase {
			// Use system database for transaction management
			systemDB, err := rh.dbMgr.GetDatabase("__marmot_system")
			if err != nil {
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("system database not found: %v", err),
				}, nil
			}

			txnMgr := systemDB.GetTransactionManager()
			txn, err := txnMgr.BeginTransaction(req.SourceNodeId)
			if err != nil {
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to begin transaction: %v", err),
				}, nil
			}

			// Override transaction ID to match coordinator's
			originalID := txn.ID
			txn.ID = req.TxnId
			txn.StartTS = hlc.Timestamp{
				WallTime: req.Timestamp.WallTime,
				Logical:  req.Timestamp.Logical,
				NodeID:   req.Timestamp.NodeId,
			}

			txnMgr.UpdateTransactionID(originalID, req.TxnId)

			// Update transaction record in system database
			_, err = systemDB.GetDB().Exec(`
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

			// Convert proto statement to internal format
			internalStmt := protocol.Statement{
				SQL:       stmt.GetSQL(),
				Type:      stmtType,
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

			// Create write intent for database operation
			rowKey := stmt.Database
			opName := "CREATE_DATABASE"
			if stmtType == protocol.StatementDropDatabase {
				opName = "DROP_DATABASE"
			}
			snapshotData := map[string]interface{}{
				"type":          int(stmt.Type),
				"timestamp":     req.Timestamp.WallTime,
				"database_name": stmt.Database,
				"operation":     opName,
			}
			dataSnapshot, _ := db.SerializeData(snapshotData)

			err = txnMgr.WriteIntent(txn, "__marmot__database_operations", rowKey, internalStmt, dataSnapshot)
			if err != nil {
				txnMgr.AbortTransaction(txn)
				return &TransactionResponse{
					Success:          false,
					ErrorMessage:     fmt.Sprintf("write conflict: %v", err),
					ConflictDetected: true,
					ConflictDetails:  err.Error(),
				}, nil
			}

			log.Info().
				Str("database", stmt.Database).
				Str("operation", opName).
				Uint64("node_id", rh.nodeID).
				Uint64("txn_id", req.TxnId).
				Msg("Database operation prepared (intent created)")

			return &TransactionResponse{
				Success: true,
				AppliedAt: &HLC{
					WallTime: rh.clock.Now().WallTime,
					Logical:  rh.clock.Now().Logical,
					NodeId:   rh.nodeID,
				},
			}, nil
		}
	}

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
			SQL:       stmt.GetSQL(),
			Type:      convertStatementType(stmt.Type),
			TableName: stmt.TableName,
			Database:  stmt.Database,
			RowKey:    stmt.GetRowKey(),
		}

		// If CDC data is present, populate it
		if rowChange := stmt.GetRowChange(); rowChange != nil {
			internalStmt.OldValues = rowChange.OldValues
			internalStmt.NewValues = rowChange.NewValues
		}

		// Add statement to transaction
		if err := txnMgr.AddStatement(txn, internalStmt); err != nil {
			txnMgr.AbortTransaction(txn)
			return &TransactionResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("failed to add statement: %v", err),
			}, nil
		}

		// Use pre-extracted row key from protobuf message
		// The row key was extracted from the original MySQL AST during parsing
		rowKey := stmt.GetRowKey()
		if rowKey == "" {
			// Fallback to hash if no row key was extracted
			sql := stmt.GetSQL()
			if sql == "" {
				sql = "unknown"
			}
			log.Warn().
				Str("sql", sql).
				Msg("Empty RowKey received - using fallback hex hash")
			rowKey = fmt.Sprintf("%x", []byte(sql)[:min(16, len(sql))])
		}

		// Serialize data snapshot (CDC data if available, otherwise SQL)
		snapshotData := map[string]interface{}{
			"type":      stmt.Type.String(),
			"timestamp": req.Timestamp.WallTime,
		}
		if stmt.HasCDCData() {
			snapshotData["cdc_data"] = true
			snapshotData["old_values_count"] = len(stmt.GetRowChange().OldValues)
			snapshotData["new_values_count"] = len(stmt.GetRowChange().NewValues)
		} else {
			snapshotData["sql"] = stmt.GetSQL()
		}
		dataSnapshot, _ := db.SerializeData(snapshotData)

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
	// Check if this is a database operation (CREATE/DROP DATABASE)
	// These are tracked in the system database
	systemDB, err := rh.dbMgr.GetDatabase("__marmot_system")
	if err == nil {
		// Try to find transaction in system database first
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnId)

		if systemTxn != nil && len(systemTxn.Statements) > 0 {
			stmt := systemTxn.Statements[0]

			// Check if it's a database operation
			if stmt.Type == protocol.StatementCreateDatabase || stmt.Type == protocol.StatementDropDatabase {
				// Execute the database operation BEFORE committing the transaction
				var dbOpErr error
				if stmt.Type == protocol.StatementCreateDatabase {
					log.Info().Str("database", stmt.Database).Uint64("node_id", rh.nodeID).Msg("Executing CREATE DATABASE in commit phase")
					dbOpErr = rh.dbMgr.CreateDatabase(stmt.Database)
				} else {
					log.Info().Str("database", stmt.Database).Uint64("node_id", rh.nodeID).Msg("Executing DROP DATABASE in commit phase")
					dbOpErr = rh.dbMgr.DropDatabase(stmt.Database)
				}

				if dbOpErr != nil {
					opName := "CREATE_DATABASE"
					if stmt.Type == protocol.StatementDropDatabase {
						opName = "DROP_DATABASE"
					}
					log.Error().Err(dbOpErr).Str("database", stmt.Database).Str("operation", opName).Msg("Database operation failed in commit phase")
					systemTxnMgr.AbortTransaction(systemTxn)
					return &TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("database operation failed: %v", dbOpErr),
					}, nil
				}

				// Now commit the transaction to mark it as completed
				if err := systemTxnMgr.CommitTransaction(systemTxn); err != nil {
					// Database operation succeeded but transaction commit failed
					// This is not ideal but the operation is done
					log.Warn().Err(err).Str("database", stmt.Database).Msg("Database operation succeeded but transaction commit failed")
				}

				opName := "CREATE_DATABASE"
				if stmt.Type == protocol.StatementDropDatabase {
					opName = "DROP_DATABASE"
				}
				log.Info().
					Str("database", stmt.Database).
					Str("operation", opName).
					Uint64("node_id", rh.nodeID).
					Msg("Database operation committed successfully")

				return &TransactionResponse{
					Success: true,
					AppliedAt: &HLC{
						WallTime: rh.clock.Now().WallTime,
						Logical:  rh.clock.Now().Logical,
						NodeId:   rh.nodeID,
					},
				}, nil
			}
		}
	}

	// Regular operation - get user database
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
	// Check system database first for database operations
	systemDB, err := rh.dbMgr.GetDatabase("__marmot_system")
	if err == nil {
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnId)
		if systemTxn != nil {
			if err := systemTxnMgr.AbortTransaction(systemTxn); err != nil {
				log.Warn().Err(err).Uint64("txn_id", req.TxnId).Msg("Failed to abort system database transaction")
			}
			return &TransactionResponse{Success: true}, nil
		}
	}

	// Try user database
	dbName := req.Database
	if dbName == "" {
		dbName = db.DefaultDatabaseName
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		// Database not found - transaction likely never started or already aborted
		return &TransactionResponse{Success: true}, nil
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
	case StatementType_CREATE_DATABASE:
		return protocol.StatementCreateDatabase
	case StatementType_DROP_DATABASE:
		return protocol.StatementDropDatabase
	default:
		return protocol.StatementInsert // Default to insert
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
