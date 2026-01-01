package db

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/rs/zerolog/log"
)

// ReplicationEngine encapsulates common replication logic for prepare/commit/abort phases.
// Extracted from LocalReplicator and ReplicationHandler to eliminate code duplication.
type ReplicationEngine struct {
	nodeID uint64
	dbMgr  DatabaseProvider
	clock  *hlc.Clock
}

// PrepareRequest contains parameters for the prepare phase
type PrepareRequest struct {
	TxnID      uint64
	NodeID     uint64
	StartTS    hlc.Timestamp
	Database   string
	Statements []protocol.Statement
}

// PrepareResult contains the result of the prepare phase
type PrepareResult struct {
	Success          bool
	Error            string
	ConflictDetected bool
	ConflictDetails  string
}

// CommitRequest contains parameters for the commit phase
type CommitRequest struct {
	TxnID    uint64
	Database string
}

// CommitResult contains the result of the commit phase
type CommitResult struct {
	Success bool
	Error   string
	DDLSQL  string // SQL for DDL statements (if any)
}

// AbortRequest contains parameters for the abort phase
type AbortRequest struct {
	TxnID    uint64
	Database string
}

// AbortResult contains the result of the abort phase
type AbortResult struct {
	Success bool
	Error   string
}

// NewReplicationEngine creates a new replication engine
func NewReplicationEngine(nodeID uint64, dbMgr DatabaseProvider, clock *hlc.Clock) *ReplicationEngine {
	return &ReplicationEngine{
		nodeID: nodeID,
		dbMgr:  dbMgr,
		clock:  clock,
	}
}

// Prepare handles the prepare phase of 2PC replication
func (re *ReplicationEngine) Prepare(ctx context.Context, req *PrepareRequest) *PrepareResult {
	re.clock.Update(req.StartTS)

	if re.isDatabaseOperation(req.Statements) {
		return re.prepareDatabaseOperation(req)
	}

	return re.prepareRegularTransaction(req)
}

// isDatabaseOperation checks if the request contains a CREATE/DROP DATABASE statement
func (re *ReplicationEngine) isDatabaseOperation(statements []protocol.Statement) bool {
	if len(statements) != 1 {
		return false
	}
	stmt := statements[0]
	return stmt.Type == protocol.StatementCreateDatabase || stmt.Type == protocol.StatementDropDatabase
}

// prepareDatabaseOperation handles CREATE/DROP DATABASE operations using system database
func (re *ReplicationEngine) prepareDatabaseOperation(req *PrepareRequest) *PrepareResult {
	stmt := req.Statements[0]

	systemDB, err := re.dbMgr.GetDatabase(SystemDatabaseName)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("system database not found: %v", err)}
	}

	txnMgr := systemDB.GetTransactionManager()
	txn, err := txnMgr.BeginTransactionWithID(req.TxnID, req.NodeID, req.StartTS)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to begin transaction: %v", err)}
	}

	if err := txnMgr.AddStatement(txn, stmt); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to add statement: %v", err)}
	}

	dbIntentKey := filter.EncodeDBOpIntentKey(stmt.Database)
	dbOp := DatabaseOpCreate
	if stmt.Type == protocol.StatementDropDatabase {
		dbOp = DatabaseOpDrop
	}

	snapshotData := DatabaseOperationSnapshot{
		Type:         int(stmt.Type),
		Timestamp:    req.StartTS.WallTime,
		DatabaseName: stmt.Database,
		Operation:    dbOp,
	}
	dataSnapshot, err := SerializeData(snapshotData)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to serialize data: %v", err)}
	}

	err = txnMgr.WriteIntent(txn, IntentTypeDatabaseOp, "", string(dbIntentKey), stmt, dataSnapshot)
	if err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("write conflict: %v", err)}
	}

	log.Info().
		Str("database", stmt.Database).
		Str("operation", dbOp.String()).
		Uint64("node_id", re.nodeID).
		Uint64("txn_id", req.TxnID).
		Msg("Database operation prepared (intent created)")

	return &PrepareResult{Success: true}
}

// prepareRegularTransaction handles regular transaction preparation
func (re *ReplicationEngine) prepareRegularTransaction(req *PrepareRequest) *PrepareResult {
	replicatedDB, err := re.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}
	}

	txnMgr := replicatedDB.GetTransactionManager()
	metaStore := replicatedDB.GetMetaStore()

	txn, err := txnMgr.BeginTransactionWithID(req.TxnID, req.NodeID, req.StartTS)
	if err != nil {
		return &PrepareResult{Success: false, Error: err.Error()}
	}

	var stmtSeq uint64 = 0
	for _, stmt := range req.Statements {
		stmtSeq++

		if result := re.processStatement(txn, txnMgr, metaStore, stmt, req, stmtSeq); result != nil {
			return result
		}
	}

	return &PrepareResult{Success: true}
}

// processStatement processes a single statement within a transaction
func (re *ReplicationEngine) processStatement(txn *Transaction, txnMgr *TransactionManager, metaStore MetaStore, stmt protocol.Statement, req *PrepareRequest, stmtSeq uint64) *PrepareResult {
	if err := txnMgr.AddStatement(txn, stmt); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: err.Error()}
	}

	intentKey := stmt.IntentKey
	if len(intentKey) == 0 {
		if stmt.Type == protocol.StatementInsert {
			log.Trace().
				Str("table", stmt.TableName).
				Msg("INSERT with auto-increment PK - skipping write intent")
			return nil
		}

		if stmt.Type == protocol.StatementUpdate || stmt.Type == protocol.StatementDelete {
			log.Debug().
				Str("table", stmt.TableName).
				Int("stmt_type", int(stmt.Type)).
				Msg("Empty IntentKey for UPDATE/DELETE - CDC will provide it during commit")
			return nil
		}

		if stmt.Type == protocol.StatementDDL {
			return re.createDDLIntent(txnMgr, txn, stmt, req.StartTS)
		}

		return nil
	}

	return re.createDMLIntent(txnMgr, metaStore, txn, stmt, string(intentKey), req, stmtSeq)
}

// createDDLIntent creates a write intent for DDL statements
func (re *ReplicationEngine) createDDLIntent(txnMgr *TransactionManager, txn *Transaction, stmt protocol.Statement, startTS hlc.Timestamp) *PrepareResult {
	ddlIntentKey := filter.EncodeDDLIntentKey(stmt.TableName)

	snapshotData := DDLSnapshot{
		Type:      int(stmt.Type),
		Timestamp: startTS.WallTime,
		SQL:       stmt.SQL,
		TableName: stmt.TableName,
	}
	dataSnapshot, serErr := SerializeData(snapshotData)
	if serErr != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to serialize DDL data: %v", serErr)}
	}

	if err := txnMgr.WriteIntent(txn, IntentTypeDDL, stmt.TableName, string(ddlIntentKey), stmt, dataSnapshot); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{
			Success:          false,
			Error:            fmt.Sprintf("DDL conflict: %v", err),
			ConflictDetected: true,
			ConflictDetails:  err.Error(),
		}
	}

	log.Debug().
		Str("table", stmt.TableName).
		Str("ddl_intent_key", string(ddlIntentKey)).
		Msg("Created write intent for DDL statement")

	return nil
}

// createDMLIntent creates a write intent and CDC entry for DML statements
func (re *ReplicationEngine) createDMLIntent(txnMgr *TransactionManager, metaStore MetaStore, txn *Transaction, stmt protocol.Statement, intentKey string, req *PrepareRequest, stmtSeq uint64) *PrepareResult {
	snapshotData := map[string]interface{}{
		"type":      stmt.Type,
		"timestamp": req.StartTS.WallTime,
	}
	if len(stmt.NewValues) > 0 || len(stmt.OldValues) > 0 {
		snapshotData["cdc_data"] = true
		snapshotData["old_values_count"] = len(stmt.OldValues)
		snapshotData["new_values_count"] = len(stmt.NewValues)
	} else {
		snapshotData["sql"] = stmt.SQL
	}
	dataSnapshot, serErr := SerializeData(snapshotData)
	if serErr != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to serialize data: %v", serErr)}
	}

	if err := txnMgr.WriteIntent(txn, IntentTypeDML, stmt.TableName, intentKey, stmt, dataSnapshot); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{
			Success:          false,
			Error:            err.Error(),
			ConflictDetected: true,
			ConflictDetails:  err.Error(),
		}
	}

	if len(stmt.NewValues) > 0 || len(stmt.OldValues) > 0 {
		var oldVals, newVals map[string][]byte
		if len(stmt.OldValues) > 0 {
			oldVals = stmt.OldValues
		}
		if len(stmt.NewValues) > 0 {
			newVals = stmt.NewValues
		}

		op := uint8(StatementTypeToOpType(stmt.Type))

		err := metaStore.WriteIntentEntry(req.TxnID, stmtSeq, op, stmt.TableName, intentKey, oldVals, newVals)
		if err != nil {
			log.Error().Err(err).
				Uint64("txn_id", req.TxnID).
				Str("table", stmt.TableName).
				Str("intent_key", intentKey).
				Msg("Failed to write CDC intent entry")
			_ = txnMgr.AbortTransaction(txn)
			return &PrepareResult{
				Success: false,
				Error:   fmt.Sprintf("failed to write CDC entry: %v", err),
			}
		}
	}

	return nil
}

// Commit handles the commit phase of 2PC replication
func (re *ReplicationEngine) Commit(ctx context.Context, req *CommitRequest) *CommitResult {
	// Check if this is a database operation (CREATE/DROP DATABASE)
	// These are tracked in the system database
	systemDB, err := re.dbMgr.GetDatabase(SystemDatabaseName)
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
					if intent.IntentType == IntentTypeDatabaseOp {
						// Found a database operation - extract details from DataSnapshot
						var snapshotData DatabaseOperationSnapshot
						if err := DeserializeData(intent.DataSnapshot, &snapshotData); err != nil {
							log.Error().Err(err).Uint64("txn_id", req.TxnID).Msg("Failed to deserialize DB op snapshot")
							continue
						}

						dbOp := snapshotData.Operation
						dbName := snapshotData.DatabaseName

						// Execute the database operation BEFORE committing the transaction
						dbMgr, ok := re.dbMgr.(*DatabaseManager)
						if !ok {
							_ = systemTxnMgr.AbortTransaction(systemTxn)
							return &CommitResult{Success: false, Error: "database manager does not support database operations"}
						}

						var dbOpErr error
						switch dbOp {
						case DatabaseOpCreate:
							log.Info().Str("database", dbName).Uint64("node_id", re.nodeID).Msg("Executing CREATE DATABASE in commit phase")
							dbOpErr = dbMgr.CreateDatabase(dbName)
						case DatabaseOpDrop:
							log.Info().Str("database", dbName).Uint64("node_id", re.nodeID).Msg("Executing DROP DATABASE in commit phase")
							dbOpErr = dbMgr.DropDatabase(dbName)
						default:
							log.Error().Str("operation", dbOp.String()).Uint64("txn_id", req.TxnID).Msg("Unknown database operation")
							continue
						}

						if dbOpErr != nil {
							log.Error().Err(dbOpErr).Str("database", dbName).Str("operation", dbOp.String()).Msg("Database operation failed in commit phase")
							_ = systemTxnMgr.AbortTransaction(systemTxn)
							return &CommitResult{Success: false, Error: fmt.Sprintf("database operation failed: %v", dbOpErr)}
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
							Uint64("node_id", re.nodeID).
							Msg("Database operation committed successfully")

						return &CommitResult{Success: true}
					}
				}
			}
		}
	}

	// Regular operation - get user database
	replicatedDB, err := re.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &CommitResult{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}
	}

	txnMgr := replicatedDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(req.TxnID)
	if txn == nil {
		log.Error().
			Uint64("txn_id", req.TxnID).
			Uint64("node_id", re.nodeID).
			Str("database", req.Database).
			Msg("COMMIT FAILED: Transaction not found - possibly GC'd or never prepared")
		return &CommitResult{Success: false, Error: "transaction not found"}
	}

	if err := txnMgr.CommitTransaction(txn); err != nil {
		return &CommitResult{Success: false, Error: err.Error()}
	}

	// Extract DDL SQL from committed statements (populated during CommitTransaction)
	var ddlSQL string
	for _, stmt := range txn.Statements {
		if stmt.Type == protocol.StatementDDL {
			ddlSQL = stmt.SQL
			break
		}
	}

	return &CommitResult{
		Success: true,
		DDLSQL:  ddlSQL,
	}
}

// Abort handles the abort phase of 2PC replication
func (re *ReplicationEngine) Abort(ctx context.Context, req *AbortRequest) *AbortResult {
	// Check system database first for database operations
	systemDB, err := re.dbMgr.GetDatabase(SystemDatabaseName)
	if err == nil {
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnID)
		if systemTxn != nil {
			if err := systemTxnMgr.AbortTransaction(systemTxn); err != nil {
				log.Warn().Err(err).Uint64("txn_id", req.TxnID).Msg("Failed to abort system database transaction")
			}
			return &AbortResult{Success: true}
		}
	}

	// Try user database
	replicatedDB, err := re.dbMgr.GetDatabase(req.Database)
	if err != nil {
		// If database doesn't exist, consider abort successful
		return &AbortResult{Success: true}
	}

	txnMgr := replicatedDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(req.TxnID)
	if txn == nil {
		return &AbortResult{Success: true}
	}

	if err := txnMgr.AbortTransaction(txn); err != nil {
		return &AbortResult{Success: false, Error: err.Error()}
	}

	return &AbortResult{Success: true}
}

// ToCoordinatorResponse converts PrepareResult to coordinator.ReplicationResponse
func (pr *PrepareResult) ToCoordinatorResponse() *coordinator.ReplicationResponse {
	return &coordinator.ReplicationResponse{
		Success:          pr.Success,
		Error:            pr.Error,
		ConflictDetected: pr.ConflictDetected,
		ConflictDetails:  pr.ConflictDetails,
	}
}

// ToCoordinatorResponse converts CommitResult to coordinator.ReplicationResponse
func (cr *CommitResult) ToCoordinatorResponse() *coordinator.ReplicationResponse {
	return &coordinator.ReplicationResponse{
		Success: cr.Success,
		Error:   cr.Error,
	}
}

// ToCoordinatorResponse converts AbortResult to coordinator.ReplicationResponse
func (ar *AbortResult) ToCoordinatorResponse() *coordinator.ReplicationResponse {
	return &coordinator.ReplicationResponse{
		Success: ar.Success,
		Error:   ar.Error,
	}
}
