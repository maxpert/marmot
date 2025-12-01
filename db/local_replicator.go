package db

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
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
			systemDB, err := lr.dbMgr.GetDatabase(SystemDatabaseName)
			if err != nil {
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("system database not found: %v", err)}, nil
			}

			txnMgr := systemDB.GetTransactionManager()
			// Use coordinator's txn_id directly to avoid ID collision race conditions
			txn, err := txnMgr.BeginTransactionWithID(req.TxnID, req.NodeID, req.StartTS)
			if err != nil {
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to begin transaction: %v", err)}, nil
			}

			// Add statement to transaction
			if err := txnMgr.AddStatement(txn, stmt); err != nil {
				txnMgr.AbortTransaction(txn)
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to add statement: %v", err)}, nil
			}

			// Create write intent for database operation
			// Use database name as the row key
			rowKey := stmt.Database
			opName := OpNameCreateDatabase
			if stmt.Type == protocol.StatementDropDatabase {
				opName = OpNameDropDatabase
			}
			snapshotData := DatabaseOperationSnapshot{
				Type:         int(stmt.Type),
				Timestamp:    req.StartTS.WallTime,
				DatabaseName: stmt.Database,
				Operation:    opName,
			}
			dataSnapshot, err := SerializeData(snapshotData)
			if err != nil {
				return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to serialize data: %v", err)}, nil
			}

			err = txnMgr.WriteIntent(txn, TableDatabaseOperations, rowKey, stmt, dataSnapshot)
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
	metaStore := mvccDB.GetMetaStore()

	// Use coordinator's txn_id directly to avoid ID collision race conditions
	txn, err := txnMgr.BeginTransactionWithID(req.TxnID, req.NodeID, req.StartTS)
	if err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	var stmtSeq uint64 = 0
	for _, stmt := range req.Statements {
		stmtSeq++

		if err := txnMgr.AddStatement(txn, stmt); err != nil {
			txnMgr.AbortTransaction(txn)
			return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
		}

		// Use pre-extracted row key from statement
		rowKey := stmt.RowKey
		if rowKey == "" {
			// For INSERT statements without explicit PK (auto-increment),
			// skip write intent creation - no row-level conflict possible
			if stmt.Type == protocol.StatementInsert {
				log.Trace().
					Str("table", stmt.TableName).
					Msg("LOCAL REPLICATOR: INSERT with auto-increment PK - skipping write intent")
				continue
			}

			// For UPDATE/DELETE without rowKey, skip write intent creation.
			// RowKey MUST come from CDC hooks (preupdate) which extract actual PK values.
			// SQL-derived fallback was catastrophically broken - it made ALL updates
			// on the same table conflict because they share the same SQL prefix.
			// MVCC commit will detect conflicts at row level when applying changes.
			if stmt.Type == protocol.StatementUpdate || stmt.Type == protocol.StatementDelete {
				log.Debug().
					Str("table", stmt.TableName).
					Int("stmt_type", int(stmt.Type)).
					Msg("LOCAL REPLICATOR: Empty RowKey for UPDATE/DELETE - CDC will provide it during commit")
				continue
			}

			// For DDL statements, create write intent using table name as row key
			// This ensures DDL SQL gets stored and executed during commit phase
			if stmt.Type == protocol.StatementDDL {
				ddlRowKey := DDLRowKeyPrefix + stmt.TableName
				snapshotData := map[string]interface{}{
					"type":      stmt.Type,
					"timestamp": req.StartTS.WallTime,
					"sql":       stmt.SQL,
				}
				dataSnapshot, serErr := SerializeData(snapshotData)
				if serErr != nil {
					txnMgr.AbortTransaction(txn)
					return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to serialize DDL data: %v", serErr)}, nil
				}

				if err := txnMgr.WriteIntent(txn, TableDDLOps, ddlRowKey, stmt, dataSnapshot); err != nil {
					txnMgr.AbortTransaction(txn)
					return &coordinator.ReplicationResponse{
						Success:          false,
						Error:            fmt.Sprintf("DDL conflict: %v", err),
						ConflictDetected: true,
						ConflictDetails:  err.Error(),
					}, nil
				}
				log.Debug().
					Str("table", stmt.TableName).
					Str("ddl_row_key", ddlRowKey).
					Msg("LOCAL REPLICATOR: Created write intent for DDL statement")
				continue
			}

			// For other statement types without rowKey, skip write intent
			continue
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
		dataSnapshot, serErr := SerializeData(snapshotData)
		if serErr != nil {
			txnMgr.AbortTransaction(txn)
			return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("failed to serialize data: %v", serErr)}, nil
		}

		if err := txnMgr.WriteIntent(txn, stmt.TableName, rowKey, stmt, dataSnapshot); err != nil {
			txnMgr.AbortTransaction(txn)
			return &coordinator.ReplicationResponse{
				Success:          false,
				Error:            err.Error(),
				ConflictDetected: true,
				ConflictDetails:  err.Error(),
			}, nil
		}

		// CRITICAL: Write CDC intent entry so CommitTransaction can replay it
		// This stores the actual row data (NewValues/OldValues) for later application to SQLite
		if len(stmt.NewValues) > 0 || len(stmt.OldValues) > 0 {
			// Serialize OldValues and NewValues as msgpack
			var oldVals, newVals []byte
			if len(stmt.OldValues) > 0 {
				var err error
				oldVals, err = msgpack.Marshal(stmt.OldValues)
				if err != nil {
					log.Error().Err(err).Str("table", stmt.TableName).Msg("Failed to marshal OldValues")
					txnMgr.AbortTransaction(txn)
					return &coordinator.ReplicationResponse{
						Success: false,
						Error:   fmt.Sprintf("failed to serialize old values: %v", err),
					}, nil
				}
			}
			if len(stmt.NewValues) > 0 {
				var err error
				newVals, err = msgpack.Marshal(stmt.NewValues)
				if err != nil {
					log.Error().Err(err).Str("table", stmt.TableName).Msg("Failed to marshal NewValues")
					txnMgr.AbortTransaction(txn)
					return &coordinator.ReplicationResponse{
						Success: false,
						Error:   fmt.Sprintf("failed to serialize new values: %v", err),
					}, nil
				}
			}

			// Convert statement type to operation code
			op := StatementTypeToOpCode(int(stmt.Type))

			err := metaStore.WriteIntentEntry(req.TxnID, stmtSeq, op, stmt.TableName, rowKey, oldVals, newVals)
			if err != nil {
				log.Error().Err(err).
					Uint64("txn_id", req.TxnID).
					Str("table", stmt.TableName).
					Str("row_key", rowKey).
					Msg("Failed to write CDC intent entry")
				txnMgr.AbortTransaction(txn)
				return &coordinator.ReplicationResponse{
					Success: false,
					Error:   fmt.Sprintf("failed to write CDC entry: %v", err),
				}, nil
			}
		}
	}

	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (lr *LocalReplicator) handleCommit(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Check if this is a database operation (CREATE/DROP DATABASE)
	// These are tracked in the system database
	systemDB, err := lr.dbMgr.GetDatabase(SystemDatabaseName)
	if err == nil {
		// Try to find transaction in system database first
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnID)

		if systemTxn != nil {
			// GetTransaction returns empty Statements, so we check intents instead
			// Intents are persisted during prepare phase and contain operation details
			metaStore := systemDB.GetMetaStore()
			intents, intentErr := metaStore.GetIntentsByTxn(req.TxnID)
			if intentErr == nil && len(intents) > 0 {
				// Check if any intent is for database operations
				for _, intent := range intents {
					if intent.TableName == TableDatabaseOperations {
						// Found a database operation - extract details from DataSnapshot
						var snapshotData DatabaseOperationSnapshot
						if err := DeserializeData(intent.DataSnapshot, &snapshotData); err != nil {
							log.Error().Err(err).Uint64("txn_id", req.TxnID).Msg("Failed to deserialize DB op snapshot")
							continue
						}

						opName := snapshotData.Operation
						if opName == "" {
							log.Error().Uint64("txn_id", req.TxnID).Msg("Missing operation in DB op snapshot")
							continue
						}
						dbName := intent.RowKey // Row key is the database name

						// Execute the database operation BEFORE committing the transaction
						dbMgr, ok := lr.dbMgr.(*DatabaseManager)
						if !ok {
							systemTxnMgr.AbortTransaction(systemTxn)
							return &coordinator.ReplicationResponse{Success: false, Error: "database manager does not support database operations"}, nil
						}

						var dbOpErr error
						if opName == OpNameCreateDatabase {
							log.Info().Str("database", dbName).Uint64("node_id", lr.nodeID).Msg("Executing CREATE DATABASE in commit phase")
							dbOpErr = dbMgr.CreateDatabase(dbName)
						} else if opName == OpNameDropDatabase {
							log.Info().Str("database", dbName).Uint64("node_id", lr.nodeID).Msg("Executing DROP DATABASE in commit phase")
							dbOpErr = dbMgr.DropDatabase(dbName)
						} else {
							log.Error().Str("operation", opName).Uint64("txn_id", req.TxnID).Msg("Unknown database operation")
							continue
						}

						if dbOpErr != nil {
							log.Error().Err(dbOpErr).Str("database", dbName).Str("operation", opName).Msg("Database operation failed in commit phase")
							systemTxnMgr.AbortTransaction(systemTxn)
							return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("database operation failed: %v", dbOpErr)}, nil
						}

						// Now commit the transaction to mark it as completed
						if err := systemTxnMgr.CommitTransaction(systemTxn); err != nil {
							// Database operation succeeded but transaction commit failed
							// This is not ideal but the operation is done
							log.Warn().Err(err).Str("database", dbName).Msg("Database operation succeeded but transaction commit failed")
						}

						log.Info().
							Str("database", dbName).
							Str("operation", opName).
							Uint64("node_id", lr.nodeID).
							Msg("Database operation committed successfully")

						return &coordinator.ReplicationResponse{Success: true}, nil
					}
				}
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
	systemDB, err := lr.dbMgr.GetDatabase(SystemDatabaseName)
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
