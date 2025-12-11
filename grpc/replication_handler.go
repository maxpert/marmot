package grpc

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"
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
	case TransactionPhase_REPLAY:
		return rh.handleReplay(ctx, req)
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
			systemDB, err := rh.dbMgr.GetDatabase(db.SystemDatabaseName)
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
			dbOp := db.DatabaseOpCreate
			if stmtType == protocol.StatementDropDatabase {
				dbOp = db.DatabaseOpDrop
			}
			snapshotData := db.DatabaseOperationSnapshot{
				Type:         int(stmt.Type),
				Timestamp:    req.Timestamp.WallTime,
				DatabaseName: stmt.Database,
				Operation:    dbOp,
			}
			dataSnapshot, err := db.SerializeData(snapshotData)
			if err != nil {
				log.Error().Err(err).Str("database", stmt.Database).Msg("Failed to serialize database operation snapshot")
				txnMgr.AbortTransaction(txn)
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to serialize snapshot: %v", err),
				}, nil
			}

			err = txnMgr.WriteIntent(txn, db.IntentTypeDatabaseOp, "", rowKey, internalStmt, dataSnapshot)
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
				Str("operation", dbOp.String()).
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

	// Get the target database from request - database name is required
	dbName := req.Database
	if dbName == "" {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "database name is required in transaction request",
		}, nil
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

	// Get MetaStore for writing CDC intent entries
	metaStore := dbInstance.GetMetaStore()

	// Process each statement and create write intents
	var stmtSeq uint64 = 0
	for _, stmt := range req.Statements {
		stmtSeq++

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
		// The row key was extracted by the coordinator's CDC hooks during local execution
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

			// For UPDATE/DELETE without rowKey, skip write intent creation.
			// RowKey MUST come from CDC hooks (preupdate) which extract actual PK values.
			// SQL-derived fallback was catastrophically broken - it made ALL updates
			// on the same table conflict because they share the same SQL prefix.
			// MVCC commit will detect conflicts at row level when applying changes.
			if internalStmt.Type == protocol.StatementUpdate || internalStmt.Type == protocol.StatementDelete {
				log.Debug().
					Str("table", stmt.TableName).
					Str("stmt_type", stmt.Type.String()).
					Msg("GRPC: Empty RowKey for UPDATE/DELETE - CDC will provide it during commit")
				continue
			}

			// For DDL statements, create write intent using table name + SQL hash as row key
			// This ensures each DDL SQL gets stored uniquely and executed during commit phase
			// Multiple DDL statements for same table (e.g., CREATE TABLE, CREATE INDEX) need unique keys
			if internalStmt.Type == protocol.StatementDDL {
				// Use table name + hash of SQL to create unique rowKey for each DDL statement
				hash := sha256.Sum256([]byte(stmt.GetSQL()))
				ddlRowKey := stmt.TableName + ":" + hex.EncodeToString(hash[:8])

				snapshotData := db.DDLSnapshot{
					Type:      int(internalStmt.Type),
					Timestamp: req.Timestamp.WallTime,
					SQL:       stmt.GetSQL(),
					TableName: stmt.TableName,
				}
				dataSnapshot, serErr := db.SerializeData(snapshotData)
				if serErr != nil {
					log.Error().Err(serErr).Str("table", stmt.TableName).Msg("Failed to serialize DDL snapshot")
					txnMgr.AbortTransaction(txn)
					return &TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("failed to serialize DDL snapshot: %v", serErr),
					}, nil
				}

				err := txnMgr.WriteIntent(txn, db.IntentTypeDDL, stmt.TableName, ddlRowKey, internalStmt, dataSnapshot)
				if err != nil {
					txnMgr.AbortTransaction(txn)
					return &TransactionResponse{
						Success:          false,
						ErrorMessage:     fmt.Sprintf("DDL conflict: %v", err),
						ConflictDetected: true,
						ConflictDetails:  err.Error(),
					}, nil
				}
				log.Debug().
					Str("table", stmt.TableName).
					Str("ddl_row_key", ddlRowKey).
					Msg("GRPC: Created write intent for DDL statement")
				continue
			}

			// For other statement types without rowKey, skip write intent
			continue
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
		dataSnapshot, serErr := db.SerializeData(snapshotData)
		if serErr != nil {
			log.Error().Err(serErr).Str("table", stmt.TableName).Msg("Failed to serialize snapshot")
			txnMgr.AbortTransaction(txn)
			return &TransactionResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("failed to serialize snapshot: %v", serErr),
			}, nil
		}

		err := txnMgr.WriteIntent(txn, db.IntentTypeDML, stmt.TableName, rowKey, internalStmt, dataSnapshot)
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

		// CRITICAL: Write CDC intent entry so CommitTransaction can replay it
		// This stores the actual row data (NewValues/OldValues) for later application to SQLite
		if stmt.HasCDCData() {
			rowChange := stmt.GetRowChange()

			// Serialize OldValues and NewValues as msgpack using pooled encoder
			var oldVals, newVals []byte
			if len(rowChange.OldValues) > 0 {
				var err error
				oldVals, err = encoding.Marshal(rowChange.OldValues)
				if err != nil {
					log.Error().Err(err).Str("table", stmt.TableName).Msg("Failed to marshal OldValues")
					txnMgr.AbortTransaction(txn)
					return &TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("failed to serialize old values: %v", err),
					}, nil
				}
			}
			if len(rowChange.NewValues) > 0 {
				var err error
				newVals, err = encoding.Marshal(rowChange.NewValues)
				if err != nil {
					log.Error().Err(err).Str("table", stmt.TableName).Msg("Failed to marshal NewValues")
					txnMgr.AbortTransaction(txn)
					return &TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("failed to serialize new values: %v", err),
					}, nil
				}
			}

			// Convert statement type to operation code
			op := uint8(db.StatementTypeToOpType(internalStmt.Type))

			err := metaStore.WriteIntentEntry(req.TxnId, stmtSeq, op, stmt.TableName, rowKey, oldVals, newVals)
			if err != nil {
				log.Error().Err(err).
					Uint64("txn_id", req.TxnId).
					Str("table", stmt.TableName).
					Str("row_key", rowKey).
					Msg("Failed to write CDC intent entry")
				txnMgr.AbortTransaction(txn)
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to write CDC entry: %v", err),
				}, nil
			}
		}
	}

	// Success! Write intents created
	telemetry.ReplicationRequestsTotal.With("prepare", "success").Inc()
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
	log.Debug().
		Uint64("node_id", rh.nodeID).
		Str("database", req.Database).
		Uint64("txn_id", req.TxnId).
		Uint64("source_node", req.SourceNodeId).
		Msg("handleCommit called")

	// Check if this is a database operation (CREATE/DROP DATABASE)
	// These are tracked in the system database
	systemDB, err := rh.dbMgr.GetDatabase(db.SystemDatabaseName)
	if err == nil {
		// Try to find transaction in system database first
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnId)

		if systemTxn != nil {
			// GetTransaction returns empty Statements, so we check intents instead
			// Intents are persisted during prepare phase and contain operation details
			metaStore := systemDB.GetMetaStore()
			intents, intentErr := metaStore.GetIntentsByTxn(req.TxnId)
			if intentErr == nil && len(intents) > 0 {
				// Check if any intent is for database operations
				for _, intent := range intents {
					if intent.IntentType == db.IntentTypeDatabaseOp {
						// Found a database operation - extract details from DataSnapshot
						var snapshotData db.DatabaseOperationSnapshot
						if err := db.DeserializeData(intent.DataSnapshot, &snapshotData); err != nil {
							log.Error().Err(err).Uint64("txn_id", req.TxnId).Msg("Failed to deserialize DB op snapshot")
							continue
						}

						dbOp := snapshotData.Operation
						dbName := intent.RowKey // Row key is the database name

						// Execute the database operation BEFORE committing the transaction
						var dbOpErr error
						switch dbOp {
						case db.DatabaseOpCreate:
							log.Info().Str("database", dbName).Uint64("node_id", rh.nodeID).Msg("Executing CREATE DATABASE in commit phase")
							dbOpErr = rh.dbMgr.CreateDatabase(dbName)
						case db.DatabaseOpDrop:
							log.Info().Str("database", dbName).Uint64("node_id", rh.nodeID).Msg("Executing DROP DATABASE in commit phase")
							dbOpErr = rh.dbMgr.DropDatabase(dbName)
						default:
							log.Error().Str("operation", dbOp.String()).Uint64("txn_id", req.TxnId).Msg("Unknown database operation")
							continue
						}

						if dbOpErr != nil {
							log.Error().Err(dbOpErr).Str("database", dbName).Str("operation", dbOp.String()).Msg("Database operation failed in commit phase")
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
							log.Warn().Err(err).Str("database", dbName).Msg("Database operation succeeded but transaction commit failed")
						}

						log.Info().
							Str("database", dbName).
							Str("operation", dbOp.String()).
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
		}
	}

	// Regular operation - get user database (database name is required)
	dbName := req.Database
	if dbName == "" {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "database name is required in commit request",
		}, nil
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
		log.Error().
			Uint64("node_id", rh.nodeID).
			Uint64("txn_id", req.TxnId).
			Str("database", dbName).
			Msg("REMOTE COMMIT FAILED: transaction not found - possibly GC'd")
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
		telemetry.ReplicationRequestsTotal.With("commit", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to commit: %v", err),
		}, nil
	}

	telemetry.ReplicationRequestsTotal.With("commit", "success").Inc()
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
	systemDB, err := rh.dbMgr.GetDatabase(db.SystemDatabaseName)
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

	// Try user database (database name is required)
	dbName := req.Database
	if dbName == "" {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "database name is required in abort request",
		}, nil
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		// Database not found - transaction may have been for a dropped database
		// Return success since there's nothing to abort
		log.Warn().Str("database", dbName).Uint64("txn_id", req.TxnId).Msg("Abort: database not found, assuming already cleaned up")
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

// handleReplay processes anti-entropy replay: Apply already-committed transactions directly.
// This bypasses 2PC state tracking since these transactions are already committed on the source.
// Used by delta sync to repair divergent nodes without requiring PREPARE phase.
func (rh *ReplicationHandler) handleReplay(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	log.Debug().
		Uint64("node_id", rh.nodeID).
		Str("database", req.Database).
		Uint64("txn_id", req.TxnId).
		Int("num_statements", len(req.Statements)).
		Msg("handleReplay called - applying already-committed transaction")

	// Get the target database from request (database name is required)
	dbName := req.Database
	if dbName == "" {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "database name is required in replay request",
		}, nil
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("database %s not found: %v", dbName, err),
		}, nil
	}

	sqliteDB := dbInstance.GetDB()

	// Execute statements directly in a SQLite transaction
	tx, err := sqliteDB.BeginTx(ctx, nil)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to begin transaction: %v", err),
		}, nil
	}
	defer tx.Rollback()

	for _, stmt := range req.Statements {
		// Check for CDC data (RowChange payload)
		if rowChange := stmt.GetRowChange(); rowChange != nil && (len(rowChange.NewValues) > 0 || len(rowChange.OldValues) > 0) {
			// CDC path: apply row data directly
			if err := rh.applyReplayCDCStatement(tx, stmt); err != nil {
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to apply CDC statement: %v", err),
				}, nil
			}
			continue
		}

		// DDL path: execute SQL directly
		if ddl := stmt.GetDdlChange(); ddl != nil && ddl.Sql != "" {
			if _, err := tx.ExecContext(ctx, ddl.Sql); err != nil {
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to execute DDL: %v", err),
				}, nil
			}
			continue
		}

		// No CDC data and no DDL - this shouldn't happen for replay
		log.Warn().
			Str("table", stmt.TableName).
			Int32("type", int32(stmt.Type)).
			Msg("handleReplay: statement has no CDC data or DDL")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to commit: %v", err),
		}, nil
	}

	// Store TransactionRecord in MetaStore so GetCommittedTxnCount/GetMaxTxnID return correct values.
	// Without this, anti-entropy keeps thinking we're behind because these metrics read from PebbleDB.
	metaStore := dbInstance.GetMetaStore()
	if metaStore != nil {
		commitTS := hlc.Timestamp{
			WallTime: req.Timestamp.WallTime,
			Logical:  req.Timestamp.Logical,
			NodeID:   req.Timestamp.NodeId,
		}

		// Convert gRPC statements to protocol.Statement and serialize for storage.
		// This is CRITICAL: if this node later becomes a delta sync source, it needs
		// to have the statement data to stream to other lagging nodes.
		serializedStatements := rh.serializeReplayStatements(req.Statements, dbName)

		if err := metaStore.StoreReplayedTransaction(req.TxnId, req.Timestamp.NodeId, commitTS, serializedStatements, dbName); err != nil {
			// Log but don't fail - data is already in SQLite
			log.Warn().
				Err(err).
				Uint64("txn_id", req.TxnId).
				Msg("handleReplay: failed to store transaction record in MetaStore")
		}
	}

	log.Debug().
		Uint64("txn_id", req.TxnId).
		Str("database", dbName).
		Int("statements", len(req.Statements)).
		Msg("handleReplay: transaction applied successfully")

	telemetry.ReplicationRequestsTotal.With("replay", "success").Inc()
	return &TransactionResponse{
		Success: true,
		AppliedAt: &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

// applyReplayCDCStatement applies a CDC statement during replay
func (rh *ReplicationHandler) applyReplayCDCStatement(tx *sql.Tx, stmt *Statement) error {
	rowChange := stmt.GetRowChange()
	if rowChange == nil {
		return fmt.Errorf("no row change data")
	}

	switch stmt.Type {
	case StatementType_INSERT, StatementType_REPLACE:
		return rh.applyReplayCDCInsert(tx, stmt.TableName, rowChange.NewValues)
	case StatementType_UPDATE:
		return rh.applyReplayCDCUpdate(tx, stmt.TableName, rowChange.NewValues)
	case StatementType_DELETE:
		return rh.applyReplayCDCDelete(tx, stmt.TableName, rowChange.RowKey, rowChange.OldValues)
	default:
		return fmt.Errorf("unsupported statement type for CDC replay: %v", stmt.Type)
	}
}

// applyReplayCDCInsert performs INSERT OR REPLACE using CDC row data
func (rh *ReplicationHandler) applyReplayCDCInsert(tx *sql.Tx, tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to insert")
	}

	columns := make([]string, 0, len(newValues))
	placeholders := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues))

	for col := range newValues {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")

		var value interface{}
		if err := encoding.Unmarshal(newValues[col], &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(sqlStmt, values...)
	return err
}

// applyReplayCDCUpdate performs UPDATE using CDC row data (upsert semantics)
func (rh *ReplicationHandler) applyReplayCDCUpdate(tx *sql.Tx, tableName string, newValues map[string][]byte) error {
	// Convert UPDATE to INSERT OR REPLACE (upsert semantics)
	return rh.applyReplayCDCInsert(tx, tableName, newValues)
}

// applyReplayCDCDelete performs DELETE using CDC row data
func (rh *ReplicationHandler) applyReplayCDCDelete(tx *sql.Tx, tableName string, rowKey string, oldValues map[string][]byte) error {
	if rowKey == "" && len(oldValues) == 0 {
		return fmt.Errorf("no row key or old values for delete")
	}

	// If we have old values, use them to build WHERE clause
	if len(oldValues) > 0 {
		whereClauses := make([]string, 0, len(oldValues))
		values := make([]interface{}, 0, len(oldValues))

		for col, valBytes := range oldValues {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
			var value interface{}
			if err := encoding.Unmarshal(valBytes, &value); err != nil {
				return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
			}
			values = append(values, value)
		}

		sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
			tableName,
			strings.Join(whereClauses, " AND "))

		_, err := tx.Exec(sqlStmt, values...)
		return err
	}

	// Fallback: use row key (assumes single PK column named 'id')
	_, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName), rowKey)
	return err
}

// serializeReplayStatements converts gRPC Statement slice to protocol.Statement slice
// and serializes them with msgpack for storage in TransactionRecord.SerializedStatements.
// This ensures replayed transactions can be streamed to other lagging nodes.
func (rh *ReplicationHandler) serializeReplayStatements(stmts []*Statement, dbName string) []byte {
	if len(stmts) == 0 {
		return nil
	}

	protoStmts := make([]protocol.Statement, 0, len(stmts))
	for _, stmt := range stmts {
		protoStmt := protocol.Statement{
			Type:      protocol.StatementType(stmt.Type),
			TableName: stmt.TableName,
			Database:  dbName,
		}

		// Extract CDC data from RowChange payload
		if rowChange := stmt.GetRowChange(); rowChange != nil {
			protoStmt.RowKey = rowChange.RowKey
			protoStmt.OldValues = rowChange.OldValues
			protoStmt.NewValues = rowChange.NewValues
		}

		// Extract SQL from DDL payload
		if ddl := stmt.GetDdlChange(); ddl != nil {
			protoStmt.SQL = ddl.Sql
		}

		protoStmts = append(protoStmts, protoStmt)
	}

	data, err := encoding.Marshal(protoStmts)
	if err != nil {
		log.Error().Err(err).Int("statement_count", len(protoStmts)).Msg("serializeReplayStatements: failed to marshal - anti-entropy may not work for this transaction")
		return nil
	}

	return data
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

	// Get the target database from request (database name is required)
	dbName := req.Database
	if dbName == "" {
		return &ReadResponse{
			Timestamp: &HLC{
				WallTime: rh.clock.Now().WallTime,
				Logical:  rh.clock.Now().Logical,
				NodeId:   rh.nodeID,
			},
		}, fmt.Errorf("database name is required in read request")
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
