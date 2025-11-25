package db

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// LocalReplicator implements coordinator.Replicator for local application
type LocalReplicator struct {
	nodeID uint64
	dbMgr  DatabaseProvider
	clock  *hlc.Clock
}

// NewLocalReplicator creates a new local replicator
func NewLocalReplicator(nodeID uint64, dbMgr DatabaseProvider, clock *hlc.Clock) *LocalReplicator {
	return &LocalReplicator{
		nodeID: nodeID,
		dbMgr:  dbMgr,
		clock:  clock,
	}
}

// ReplicateTransaction applies transaction locally
func (lr *LocalReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Update clock
	lr.clock.Update(req.StartTS)

	switch req.Phase {
	case coordinator.PhasePrep:
		return lr.handlePrepare(ctx, req)
	case coordinator.PhaseCommit:
		return lr.handleCommit(ctx, req)
	case coordinator.PhaseAbort:
		return lr.handleAbort(ctx, req)
	default:
		return &coordinator.ReplicationResponse{
			Success: false,
			Error:   fmt.Sprintf("unknown phase: %v", req.Phase),
		}, nil
	}
}

func (lr *LocalReplicator) handlePrepare(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Handle CREATE DATABASE and DROP DATABASE using system database for transaction tracking
	// These operations need 2PC but don't have a user database to track the transaction in
	if len(req.Statements) == 1 {
		stmt := req.Statements[0]

		if stmt.Type == protocol.StatementCreateDatabase || stmt.Type == protocol.StatementDropDatabase {
			// Use system database for transaction management
			systemDB, err := lr.dbMgr.GetDatabase("__marmot_system")
			if err != nil {
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("system database not found: %v", err)}, nil
			}

			txnMgr := systemDB.GetTransactionManager()
			txn, err := txnMgr.BeginTransaction(req.NodeID)
			if err != nil {
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to begin transaction: %v", err)}, nil
			}

			// Override transaction ID to match coordinator's
			originalID := txn.ID
			txn.ID = req.TxnID
			txn.StartTS = req.StartTS

			txnMgr.UpdateTransactionID(originalID, req.TxnID)

			// Update transaction record in system database
			_, err = systemDB.GetDB().Exec(`
				UPDATE __marmot__txn_records
				SET txn_id = ?
				WHERE txn_id = ?
			`, req.TxnID, originalID)
			if err != nil {
				txnMgr.AbortTransaction(txn)
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to update txn ID: %v", err)}, nil
			}

			// Add statement to transaction
			if err := txnMgr.AddStatement(txn, stmt); err != nil {
				txnMgr.AbortTransaction(txn)
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to add statement: %v", err)}, nil
			}

			// Create write intent for database operation
			// Use database name as the row key
			rowKey := stmt.Database
			opName := "CREATE_DATABASE"
			if stmt.Type == protocol.StatementDropDatabase {
				opName = "DROP_DATABASE"
			}
			snapshotData := map[string]interface{}{
				"type":          stmt.Type,
				"timestamp":     req.StartTS.WallTime,
				"database_name": stmt.Database,
				"operation":     opName,
			}
			dataSnapshot, _ := SerializeData(snapshotData)

			err = txnMgr.WriteIntent(txn, "__marmot__database_operations", rowKey, stmt, dataSnapshot)
			if err != nil {
				txnMgr.AbortTransaction(txn)
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("write conflict: %v", err)}, nil
			}

			log.Info().
				Str("database", stmt.Database).
				Str("operation", opName).
				Uint64("node_id", lr.nodeID).
				Uint64("txn_id", req.TxnID).
				Msg("Database operation prepared (intent created)")

			return &coordinator.ReplicationResponse{Success: true}, nil
		}
	}

	// Get database
	mvccDB, err := lr.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}, nil
	}

	txnMgr := mvccDB.GetTransactionManager()
	txn, err := txnMgr.BeginTransaction(req.NodeID)
	if err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	// Override ID
	originalID := txn.ID
	txn.ID = req.TxnID
	txn.StartTS = req.StartTS

	txnMgr.UpdateTransactionID(originalID, req.TxnID)

	// Update DB record
	_, err = mvccDB.GetDB().Exec(`
		UPDATE __marmot__txn_records
		SET txn_id = ?
		WHERE txn_id = ?
	`, req.TxnID, originalID)
	if err != nil {
		txnMgr.AbortTransaction(txn)
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	for _, stmt := range req.Statements {
		if err := txnMgr.AddStatement(txn, stmt); err != nil {
			txnMgr.AbortTransaction(txn)
			return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
		}

		// Use pre-extracted row key from statement
		rowKey := stmt.RowKey
		if rowKey == "" {
			// Fallback to hash if no row key was extracted
			sql := stmt.SQL
			if sql == "" {
				sql = "unknown"
			}
			log.Warn().
				Str("sql", sql).
				Msg("LOCAL REPLICATOR: Empty RowKey - using fallback hex hash")
			rowKey = fmt.Sprintf("%x", []byte(sql)[:min(16, len(sql))])
		}

		// Serialize data snapshot (CDC data if available, otherwise SQL)
		snapshotData := map[string]interface{}{
			"type":      stmt.Type,
			"timestamp": req.StartTS.WallTime,
		}
		if len(stmt.NewValues) > 0 || len(stmt.OldValues) > 0 {
			// CDC data available
			snapshotData["cdc_data"] = true
			snapshotData["old_values_count"] = len(stmt.OldValues)
			snapshotData["new_values_count"] = len(stmt.NewValues)
		} else {
			// SQL fallback
			snapshotData["sql"] = stmt.SQL
		}
		dataSnapshot, _ := SerializeData(snapshotData)

		if err := txnMgr.WriteIntent(txn, stmt.TableName, rowKey, stmt, dataSnapshot); err != nil {
			txnMgr.AbortTransaction(txn)
			return &coordinator.ReplicationResponse{
				Success:          false,
				Error:            err.Error(),
				ConflictDetected: true,
				ConflictDetails:  err.Error(),
			}, nil
		}
	}

	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (lr *LocalReplicator) handleCommit(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Check if this is a database operation (CREATE/DROP DATABASE)
	// These are tracked in the system database
	systemDB, err := lr.dbMgr.GetDatabase("__marmot_system")
	if err == nil {
		// Try to find transaction in system database first
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnID)

		if systemTxn != nil && len(systemTxn.Statements) > 0 {
			stmt := systemTxn.Statements[0]

			// Check if it's a database operation
			if stmt.Type == protocol.StatementCreateDatabase || stmt.Type == protocol.StatementDropDatabase {
				// Execute the database operation BEFORE committing the transaction
				dbMgr, ok := lr.dbMgr.(*DatabaseManager)
				if !ok {
					systemTxnMgr.AbortTransaction(systemTxn)
					return &coordinator.ReplicationResponse{Success: false, Error: "database manager does not support database operations"}, nil
				}

				var dbOpErr error
				if stmt.Type == protocol.StatementCreateDatabase {
					log.Info().Str("database", stmt.Database).Uint64("node_id", lr.nodeID).Msg("Executing CREATE DATABASE in commit phase")
					dbOpErr = dbMgr.CreateDatabase(stmt.Database)
				} else {
					log.Info().Str("database", stmt.Database).Uint64("node_id", lr.nodeID).Msg("Executing DROP DATABASE in commit phase")
					dbOpErr = dbMgr.DropDatabase(stmt.Database)
				}

				if dbOpErr != nil {
					opName := "CREATE_DATABASE"
					if stmt.Type == protocol.StatementDropDatabase {
						opName = "DROP_DATABASE"
					}
					log.Error().Err(dbOpErr).Str("database", stmt.Database).Str("operation", opName).Msg("Database operation failed in commit phase")
					systemTxnMgr.AbortTransaction(systemTxn)
					return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("database operation failed: %v", dbOpErr)}, nil
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
					Uint64("node_id", lr.nodeID).
					Msg("Database operation committed successfully")

				return &coordinator.ReplicationResponse{Success: true}, nil
			}
		}
	}

	// Regular operation - get user database
	mvccDB, err := lr.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}, nil
	}

	txnMgr := mvccDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(req.TxnID)
	if txn == nil {
		return &coordinator.ReplicationResponse{Success: false, Error: "transaction not found"}, nil
	}

	if err := txnMgr.CommitTransaction(txn); err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (lr *LocalReplicator) handleAbort(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Check system database first for database operations
	systemDB, err := lr.dbMgr.GetDatabase("__marmot_system")
	if err == nil {
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnID)
		if systemTxn != nil {
			if err := systemTxnMgr.AbortTransaction(systemTxn); err != nil {
				log.Warn().Err(err).Uint64("txn_id", req.TxnID).Msg("Failed to abort system database transaction")
			}
			return &coordinator.ReplicationResponse{Success: true}, nil
		}
	}

	// Try user database
	mvccDB, err := lr.dbMgr.GetDatabase(req.Database)
	if err != nil {
		// If database doesn't exist, consider abort successful
		return &coordinator.ReplicationResponse{Success: true}, nil
	}

	txnMgr := mvccDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(req.TxnID)
	if txn == nil {
		return &coordinator.ReplicationResponse{Success: true}, nil
	}

	if err := txnMgr.AbortTransaction(txn); err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	return &coordinator.ReplicationResponse{Success: true}, nil
}
