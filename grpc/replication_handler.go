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
	nodeID        uint64
	dbMgr         *db.DatabaseManager
	clock         *hlc.Clock
	guardRegistry *GuardRegistry
}

// NewReplicationHandler creates a new replication handler
func NewReplicationHandler(nodeID uint64, dbMgr *db.DatabaseManager, clock *hlc.Clock, guardRegistry *GuardRegistry) *ReplicationHandler {
	return &ReplicationHandler{
		nodeID:        nodeID,
		dbMgr:         dbMgr,
		clock:         clock,
		guardRegistry: guardRegistry,
	}
}

// HandleReplicateTransaction handles incoming transaction replication requests.
// Entry point for all remote 2PC phases (PREPARE, COMMIT, ABORT).
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
		Int("num_guards", len(req.MutationGuards)).
		Msg("handlePrepare called")

	// Check MutationGuards for conflicts before processing statements
	if len(req.MutationGuards) > 0 && rh.guardRegistry != nil {
		conflictResult := rh.checkMutationGuardConflicts(req)
		if conflictResult.HasConflict {
			return &TransactionResponse{
				Success:          false,
				ErrorMessage:     conflictResult.Details,
				ConflictDetected: true,
				ConflictDetails:  conflictResult.Details,
			}, nil
		}
	}

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
			// Use coordinator's txn_id directly to avoid ID collision race conditions
			startTS := hlc.Timestamp{
				WallTime: req.Timestamp.WallTime,
				Logical:  req.Timestamp.Logical,
				NodeID:   req.Timestamp.NodeId,
			}
			txn, err := txnMgr.BeginTransactionWithID(req.TxnId, req.SourceNodeId, startTS)
			if err != nil {
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to begin transaction: %v", err),
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

	// Use coordinator's txn_id directly to avoid ID collision race conditions
	startTS := hlc.Timestamp{
		WallTime: req.Timestamp.WallTime,
		Logical:  req.Timestamp.Logical,
		NodeID:   req.Timestamp.NodeId,
	}
	txn, err := txnMgr.BeginTransactionWithID(req.TxnId, req.SourceNodeId, startTS)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to begin transaction: %v", err),
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
			// For INSERT statements without explicit PK (auto-increment),
			// skip write intent creation - no row-level conflict possible
			if internalStmt.Type == protocol.StatementInsert {
				log.Trace().
					Str("table", stmt.TableName).
					Msg("GRPC: INSERT with auto-increment PK - skipping write intent")
				continue
			}

			// For UPDATE/DELETE without rowKey, use SQL hash as fallback
			sql := stmt.GetSQL()
			if sql == "" {
				sql = "unknown"
			}
			log.Warn().
				Str("sql", sql).
				Msg("Empty RowKey for non-INSERT - using SQL-derived identifier")
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

	// Register MutationGuards after successful prepare
	if len(req.MutationGuards) > 0 {
		rh.registerMutationGuards(req)
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

				// Remove MutationGuards after successful database operation commit
				if rh.guardRegistry != nil {
					rh.guardRegistry.Remove(req.TxnId)
				}

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
		// Transaction not found in memory - clean up any orphaned intents
		// This can happen if:
		// 1. Prepare succeeded but transaction was removed from memory (timeout/GC)
		// 2. Different node is receiving the commit request
		// 3. Race condition between prepare and commit
		metaStore := dbInstance.GetMetaStore()
		if metaStore != nil {
			if err := metaStore.DeleteIntentsByTxn(req.TxnId); err != nil {
				log.Warn().Err(err).Uint64("txn_id", req.TxnId).Msg("Failed to cleanup orphaned intents during commit")
			}
		}
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("transaction %d not found", req.TxnId),
		}, nil
	}

	// Commit the transaction
	err = txnMgr.CommitTransaction(txn)
	if err != nil {
		// Commit failed - clean up intents to avoid blocking future transactions
		metaStore := dbInstance.GetMetaStore()
		if metaStore != nil {
			metaStore.DeleteIntentsByTxn(req.TxnId)
		}
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to commit: %v", err),
		}, nil
	}

	// Remove MutationGuards after successful commit
	if rh.guardRegistry != nil {
		rh.guardRegistry.Remove(req.TxnId)
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
	// Always remove MutationGuards on abort (idempotent)
	if rh.guardRegistry != nil {
		rh.guardRegistry.Remove(req.TxnId)
	}

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
		// Database not found - but if it's the default "marmot" database,
		// we should still try to clean up any orphaned intents that might exist
		// in the system database's meta store
		return &TransactionResponse{Success: true}, nil
	}

	txnMgr := dbInstance.GetTransactionManager()

	// Retrieve transaction from active transactions
	txn := txnMgr.GetTransaction(req.TxnId)
	if txn == nil {
		// Transaction not found in memory - but write intents may still exist in DB!
		// Always clean up intents by txn_id to handle:
		// 1. Race conditions where abort arrives before/after prepare
		// 2. Partial failures where some nodes have intents but txn was removed from memory
		// 3. Client disconnects mid-transaction
		metaStore := dbInstance.GetMetaStore()
		if metaStore != nil {
			if err := metaStore.DeleteIntentsByTxn(req.TxnId); err != nil {
				log.Warn().Err(err).Uint64("txn_id", req.TxnId).Msg("Failed to cleanup orphaned intents during abort")
			}
		}
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

// checkMutationGuardConflicts checks for conflicts using MutationGuard hash list.
// Uses exact hash set intersection - no false positives.
func (rh *ReplicationHandler) checkMutationGuardConflicts(req *TransactionRequest) ConflictResult {
	for table, guard := range req.MutationGuards {
		// Convert hash slice to KeySet for O(1) lookup
		keySet := KeySetFromSlice(guard.KeyHashes)

		// Build active guard for conflict check
		activeGuard := &ActiveGuard{
			TxnID:  req.TxnId,
			Table:  table,
			KeySet: keySet,
			Timestamp: hlc.Timestamp{
				WallTime: req.Timestamp.WallTime,
				Logical:  req.Timestamp.Logical,
				NodeID:   req.Timestamp.NodeId,
			},
			RowCount: guard.ExpectedRowCount,
		}

		// Check for conflicts with existing guards
		result := rh.guardRegistry.CheckConflict(activeGuard)
		if result.HasConflict {
			log.Debug().
				Uint64("txn_id", req.TxnId).
				Uint64("conflicting_txn", result.ConflictingTxn).
				Str("table", table).
				Bool("should_wait", result.ShouldWait).
				Int("key_count", len(keySet)).
				Msg("MutationGuard conflict detected")
			return result
		}
	}

	return ConflictResult{HasConflict: false}
}

// registerMutationGuards registers guards after successful prepare
func (rh *ReplicationHandler) registerMutationGuards(req *TransactionRequest) {
	if rh.guardRegistry == nil {
		return
	}

	for table, guard := range req.MutationGuards {
		// Convert hash slice to KeySet for O(1) lookup
		keySet := KeySetFromSlice(guard.KeyHashes)

		activeGuard := &ActiveGuard{
			TxnID:  req.TxnId,
			Table:  table,
			KeySet: keySet,
			Timestamp: hlc.Timestamp{
				WallTime: req.Timestamp.WallTime,
				Logical:  req.Timestamp.Logical,
				NodeID:   req.Timestamp.NodeId,
			},
			RowCount: guard.ExpectedRowCount,
		}

		if err := rh.guardRegistry.Register(activeGuard); err != nil {
			log.Warn().Err(err).
				Uint64("txn_id", req.TxnId).
				Str("table", table).
				Int("key_count", len(keySet)).
				Msg("Failed to register MutationGuard")
		}
	}
}
