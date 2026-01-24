package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

// ReplicationHandler handles transaction replication with MVCC
type ReplicationHandler struct {
	nodeID           uint64
	dbMgr            *db.DatabaseManager
	clock            *hlc.Clock
	schemaVersionMgr *db.SchemaVersionManager
	engine           *db.ReplicationEngine
}

// NewReplicationHandler creates a new replication handler
func NewReplicationHandler(nodeID uint64, dbMgr *db.DatabaseManager, clock *hlc.Clock, schemaVersionMgr *db.SchemaVersionManager) *ReplicationHandler {
	return &ReplicationHandler{
		nodeID:           nodeID,
		dbMgr:            dbMgr,
		clock:            clock,
		schemaVersionMgr: schemaVersionMgr,
		engine:           db.NewReplicationEngine(nodeID, dbMgr, clock),
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
	prepareStart := time.Now()
	defer func() {
		telemetry.ReplicaPrepareSeconds.Observe(time.Since(prepareStart).Seconds())
	}()

	// Schema version validation - MUST happen before engine call
	if rh.schemaVersionMgr != nil && req.RequiredSchemaVersion > 0 {
		dbName := req.Database
		if dbName != "" && dbName != db.SystemDatabaseName {
			localVersion, err := rh.schemaVersionMgr.GetSchemaVersion(dbName)
			if err != nil {
				log.Warn().Err(err).Str("database", dbName).Msg("Failed to get local schema version during prepare")
			} else if localVersion < req.RequiredSchemaVersion {
				log.Error().
					Str("database", dbName).
					Uint64("local_version", localVersion).
					Uint64("required_version", req.RequiredSchemaVersion).
					Uint64("txn_id", req.TxnId).
					Msg("Schema version mismatch: local version is behind required version")
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("schema version mismatch: local version %d < required version %d", localVersion, req.RequiredSchemaVersion),
				}, nil
			}
		}
	}

	// Convert proto statements to internal format
	statements := make([]protocol.Statement, 0, len(req.Statements))
	for _, stmt := range req.Statements {
		internalStmt := protocol.Statement{
			SQL:       stmt.GetSQL(),
			Type:      common.MustFromWireType(stmt.Type),
			TableName: stmt.TableName,
			Database:  stmt.Database,
			IntentKey: stmt.GetIntentKey(),
		}
		if rowChange := stmt.GetRowChange(); rowChange != nil {
			internalStmt.OldValues = rowChange.OldValues
			internalStmt.NewValues = rowChange.NewValues
		}
		statements = append(statements, internalStmt)
	}

	// Build engine request
	startTS := hlc.Timestamp{
		WallTime: req.Timestamp.WallTime,
		Logical:  req.Timestamp.Logical,
		NodeID:   req.Timestamp.NodeId,
	}
	engineReq := &db.PrepareRequest{
		TxnID:      req.TxnId,
		NodeID:     req.SourceNodeId,
		StartTS:    startTS,
		Database:   req.Database,
		Statements: statements,
	}

	// Call engine
	result := rh.engine.Prepare(ctx, engineReq)

	// Convert to gRPC response
	resp := &TransactionResponse{
		Success:          result.Success,
		ErrorMessage:     result.Error,
		ConflictDetected: result.ConflictDetected,
		ConflictDetails:  result.ConflictDetails,
	}
	if result.Success {
		resp.AppliedAt = &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		}
		telemetry.ReplicationRequestsTotal.With("prepare", "success").Inc()
	} else {
		telemetry.ReplicationRequestsTotal.With("prepare", "failed").Inc()
	}
	return resp, nil
}

// handleCommit processes Phase 2 of 2PC: Commit transaction
func (rh *ReplicationHandler) handleCommit(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	commitStart := time.Now()
	defer func() {
		telemetry.ReplicaCommitSeconds.Observe(time.Since(commitStart).Seconds())
	}()

	// Convert proto statements to internal format (CDC data deferred from PREPARE)
	statements := make([]protocol.Statement, 0, len(req.Statements))
	for _, stmt := range req.Statements {
		internalStmt := protocol.Statement{
			SQL:       stmt.GetSQL(),
			Type:      common.MustFromWireType(stmt.Type),
			TableName: stmt.TableName,
			Database:  stmt.Database,
			IntentKey: stmt.GetIntentKey(),
		}
		if rowChange := stmt.GetRowChange(); rowChange != nil {
			internalStmt.OldValues = rowChange.OldValues
			internalStmt.NewValues = rowChange.NewValues
		}
		statements = append(statements, internalStmt)
	}

	engineReq := &db.CommitRequest{
		TxnID:      req.TxnId,
		Database:   req.Database,
		Statements: statements, // CDC data deferred from PREPARE phase
	}

	result := rh.engine.Commit(ctx, engineReq)

	if !result.Success {
		telemetry.ReplicationRequestsTotal.With("commit", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: result.Error,
		}, nil
	}

	// Schema version increment for DDL transactions (MUST happen after successful commit)
	// DDL information is returned by the engine (extracted from committed intents)
	if rh.schemaVersionMgr != nil && result.DDLSQL != "" {
		_, verErr := rh.schemaVersionMgr.IncrementSchemaVersion(req.Database, result.DDLSQL, req.TxnId)
		if verErr != nil {
			log.Error().Err(verErr).Str("database", req.Database).Uint64("txn_id", req.TxnId).Msg("Failed to increment schema version after DDL replication")
		}
	}

	telemetry.ReplicationRequestsTotal.With("commit", "success").Inc()
	return &TransactionResponse{
		Success: true,
		AppliedAt: &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

// handleAbort processes abort: Rollback transaction
func (rh *ReplicationHandler) handleAbort(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	engineReq := &db.AbortRequest{
		TxnID:    req.TxnId,
		Database: req.Database,
	}

	result := rh.engine.Abort(ctx, engineReq)

	return &TransactionResponse{
		Success:      result.Success,
		ErrorMessage: result.Error,
	}, nil
}

// handleReplay processes anti-entropy replay: Apply already-committed transactions directly.
// This bypasses 2PC state tracking since these transactions are already committed on the source.
// Used by delta sync to repair divergent nodes without requiring PREPARE phase.
func (rh *ReplicationHandler) handleReplay(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	replayStart := time.Now()
	defer func() {
		telemetry.ReplicaReplaySeconds.Observe(time.Since(replayStart).Seconds())
	}()

	log.Debug().
		Uint64("node_id", rh.nodeID).
		Str("database", req.Database).
		Uint64("txn_id", req.TxnId).
		Int("num_statements", len(req.Statements)).
		Msg("handleReplay called - applying already-committed transaction")

	// Get the target database from request (database name is required)
	dbName := req.Database
	if dbName == "" {
		telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "database name is required in replay request",
		}, nil
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("database %s not found: %v", dbName, err),
		}, nil
	}

	sqliteDB := dbInstance.GetDB()

	// Execute statements directly in a SQLite transaction
	tx, err := sqliteDB.BeginTx(ctx, nil)
	if err != nil {
		telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to begin transaction: %v", err),
		}, nil
	}
	defer tx.Rollback()

	// Create schema adapter for CDC operations
	schemaAdapter := &replicationSchemaAdapter{dbMgr: rh.dbMgr, dbName: dbName}

	for _, stmt := range req.Statements {
		// Check for CDC data (RowChange payload)
		if rowChange := stmt.GetRowChange(); rowChange != nil && (len(rowChange.NewValues) > 0 || len(rowChange.OldValues) > 0) {
			// CDC path: apply row data directly using unified applier
			var err error
			switch stmt.Type {
			case pb.StatementType_INSERT, pb.StatementType_REPLACE:
				err = db.ApplyCDCInsert(tx, stmt.TableName, rowChange.NewValues)
			case pb.StatementType_UPDATE:
				err = db.ApplyCDCUpdate(tx, schemaAdapter, stmt.TableName, rowChange.OldValues, rowChange.NewValues)
			case pb.StatementType_DELETE:
				err = db.ApplyCDCDelete(tx, schemaAdapter, stmt.TableName, rowChange.OldValues)
			default:
				err = fmt.Errorf("unsupported statement type for CDC replay: %v", stmt.Type)
			}
			if err != nil {
				telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
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
				telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
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
		telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
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

		rowCount := uint32(len(req.Statements))

		if err := metaStore.StoreReplayedTransaction(req.TxnId, req.Timestamp.NodeId, commitTS, dbName, rowCount); err != nil {
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

// replicationSchemaAdapter adapts DatabaseManager schema access to CDCSchemaProvider
type replicationSchemaAdapter struct {
	dbMgr  *db.DatabaseManager
	dbName string
}

func (a *replicationSchemaAdapter) GetPrimaryKeys(tableName string) ([]string, error) {
	dbInstance, err := a.dbMgr.GetDatabase(a.dbName)
	if err != nil {
		return nil, fmt.Errorf("database %s not found: %w", a.dbName, err)
	}
	schema, err := dbInstance.GetCachedTableSchema(tableName)
	if err != nil {
		return nil, fmt.Errorf("schema not found for table %s: %w", tableName, err)
	}
	return schema.PrimaryKeys, nil
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

// GetAllSchemaVersions returns local schema versions for all databases
// Used by promotion checker to verify schema matches cluster before promoting to ALIVE
func (rh *ReplicationHandler) GetAllSchemaVersions() (map[string]uint64, error) {
	if rh.schemaVersionMgr == nil {
		return nil, nil
	}
	return rh.schemaVersionMgr.GetAllSchemaVersions()
}
