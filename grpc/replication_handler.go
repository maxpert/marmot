package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
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
	client           *Client
	registry         *NodeRegistry
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

// SetRegistry wires the NodeRegistry so the handler can check node status.
func (rh *ReplicationHandler) SetRegistry(registry *NodeRegistry) {
	rh.registry = registry
}

// SetClient wires the gRPC client used for pull-based LOAD DATA chunk fetches.
func (rh *ReplicationHandler) SetClient(client *Client) {
	rh.client = client
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

	// Reject new PREPARE requests when this node is LEAVING the cluster.
	// COMMIT and ABORT are still accepted for transactions that were already
	// prepared before we started leaving — those must be honored.
	if rh.registry != nil && rh.registry.IsLeaving(rh.registry.GetLocalNodeID()) {
		telemetry.ReplicationRequestsTotal.With("prepare", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "node is leaving cluster",
		}, nil
	}

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
		internalStmt := protocolStatementFromProto(stmt)
		if loadData := stmt.GetLoadDataChange(); loadData != nil {
			internalStmt.SQL = loadData.Sql
			internalStmt.LoadDataPayload = loadData.Data
			if len(internalStmt.LoadDataPayload) == 0 && loadData.LoadId != "" {
				payload, err := rh.pullLoadDataPayload(ctx, req.SourceNodeId, loadData.LoadId, loadData.DataSize, loadData.ChunkBytes)
				if err != nil {
					return &TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("failed to fetch LOAD DATA payload during prepare: %v", err),
					}, nil
				}
				internalStmt.LoadDataPayload = payload
			}
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

	// Convert proto statements to internal format. DML row images are already
	// durable from PREPARE; COMMIT may carry only DML intent metadata.
	statements := make([]protocol.Statement, 0, len(req.Statements))
	for _, stmt := range req.Statements {
		statements = append(statements, protocolStatementFromProto(stmt))
	}

	engineReq := &db.CommitRequest{
		TxnID:      req.TxnId,
		Database:   req.Database,
		Statements: statements,
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

func (rh *ReplicationHandler) pullLoadDataPayload(ctx context.Context, sourceNodeID uint64, loadID string, expectedSize uint64, chunkBytes uint32) ([]byte, error) {
	if sourceNodeID == 0 {
		return nil, fmt.Errorf("invalid source node id")
	}
	if rh.client == nil {
		return nil, fmt.Errorf("client not configured")
	}
	if chunkBytes == 0 {
		chunkBytes = 256 * 1024
	}

	var out []byte
	offset := uint64(0)
	for {
		resp, err := rh.client.GetLoadDataChunk(ctx, sourceNodeID, &LoadDataChunkRequest{
			RequestingNodeId: rh.nodeID,
			LoadId:           loadID,
			Offset:           offset,
			MaxBytes:         chunkBytes,
		})
		if err != nil {
			return nil, err
		}
		if resp == nil || len(resp.Data) == 0 {
			break
		}
		out = append(out, resp.Data...)
		offset += uint64(len(resp.Data))
		if resp.TotalSize > 0 && offset >= resp.TotalSize {
			break
		}
	}

	if expectedSize > 0 && uint64(len(out)) != expectedSize {
		return nil, fmt.Errorf("payload size mismatch: got %d want %d", len(out), expectedSize)
	}
	return out, nil
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
	if len(req.Statements) == 1 {
		if change := req.Statements[0].GetVectorIndexChange(); change != nil {
			if err := rh.applyVectorIndexChange(ctx, vectorChangeFromProto(change)); err != nil {
				telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to apply vector index control: %v", err),
				}, nil
			}
			if _, err := StoreAppliedChangeEvent(dbInstance.GetMetaStore(), req.TxnId, req.Timestamp, dbName, req.Statements); err != nil {
				telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to store vector index control rows: %v", err),
				}, nil
			}
			now := rh.clock.Now()
			telemetry.ReplicationRequestsTotal.With("replay", "success").Inc()
			return &TransactionResponse{
				Success: true,
				AppliedAt: &HLC{
					WallTime: now.WallTime,
					Logical:  now.Logical,
					NodeId:   rh.nodeID,
				},
			}, nil
		}
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
	hasDDL := false

	for _, stmt := range req.Statements {
		// Check for CDC data (RowChange payload)
		if rowChange := stmt.GetRowChange(); rowChange != nil && (len(rowChange.NewValues) > 0 || len(rowChange.OldValues) > 0) {
			// CDC path: apply row data directly using unified applier
			opType, opErr := wireDMLToOp(stmt.Type)
			if opErr != nil {
				telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: opErr.Error(),
				}, nil
			}
			if err := db.ApplyCDCValues(tx, schemaAdapter, opType, stmt.TableName, rowChange.OldValues, rowChange.NewValues); err != nil {
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
			if err := db.ApplyDDLSQLInTx(ctx, tx, ddl.Sql); err != nil {
				telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to execute DDL: %v", err),
				}, nil
			}
			hasDDL = true
			continue
		}

		// LOAD DATA path: apply via shared bulk-load executor.
		if loadData := stmt.GetLoadDataChange(); loadData != nil {
			if _, err := db.ApplyLoadDataInTx(tx, loadData.Sql, loadData.Data); err != nil {
				telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to apply LOAD DATA: %v", err),
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
	if err := db.MarkSQLiteTxnApplied(tx, req.TxnId, HLCToTimestamp(req.Timestamp)); err != nil {
		telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to mark applied txn: %v", err),
		}, nil
	}
	if err := tx.Commit(); err != nil {
		telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to commit: %v", err),
		}, nil
	}
	if hasDDL {
		if err := dbInstance.ReloadSchema(); err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("handleReplay: failed to reload schema after DDL")
		}
	}

	// Store TransactionRecord in MetaStore so GetCommittedTxnCount/GetMaxTxnID return correct values.
	// Without this, anti-entropy keeps thinking we're behind because these metrics read from PebbleDB.
	seqNum, err := StoreAppliedChangeEvent(dbInstance.GetMetaStore(), req.TxnId, req.Timestamp, dbName, req.Statements)
	if err != nil {
		telemetry.ReplicationRequestsTotal.With("replay", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to store replay captured rows: %v", err),
		}, nil
	}
	if err := rh.applyVectorCDCFromStatements(ctx, dbName, req.TxnId, seqNum, req.Statements); err != nil {
		log.Error().
			Err(err).
			Str("database", dbName).
			Uint64("txn_id", req.TxnId).
			Uint64("seq_num", seqNum).
			Msg("handleReplay: vector CDC failed after row commit; local vector index is dirty")
	}

	log.Debug().
		Uint64("txn_id", req.TxnId).
		Str("database", dbName).
		Int("statements", len(req.Statements)).
		Msg("handleReplay: transaction applied successfully")

	telemetry.ReplicationRequestsTotal.With("replay", "success").Inc()
	now := rh.clock.Now()
	return &TransactionResponse{
		Success: true,
		AppliedAt: &HLC{
			WallTime: now.WallTime,
			Logical:  now.Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

func (rh *ReplicationHandler) applyVectorIndexChange(ctx context.Context, change common.VectorIndexChange) error {
	vecMgr := rh.dbMgr.GetVectorIndexManager()
	if vecMgr == nil {
		return fmt.Errorf("vector index manager not configured")
	}
	applier, ok := vecMgr.(interface {
		ApplyVectorControl(context.Context, common.VectorIndexChange) error
	})
	if !ok {
		return fmt.Errorf("vector index manager cannot apply replicated control metadata")
	}
	return applier.ApplyVectorControl(ctx, change)
}

func (rh *ReplicationHandler) applyVectorCDCFromStatements(ctx context.Context, database string, txnID, seqNum uint64, statements []*Statement) error {
	entries := make([]common.CDCEntry, 0, len(statements))
	for _, stmt := range statements {
		rowChange := stmt.GetRowChange()
		if rowChange == nil || (len(rowChange.NewValues) == 0 && len(rowChange.OldValues) == 0) {
			continue
		}
		entries = append(entries, common.CDCEntry{
			Table:        stmt.TableName,
			IntentKey:    rowChange.IntentKey,
			OldValues:    rowChange.OldValues,
			NewValues:    rowChange.NewValues,
			CommitTxnID:  txnID,
			CommitSeqNum: seqNum,
		})
	}
	if len(entries) == 0 {
		return nil
	}
	vecMgr := rh.dbMgr.GetVectorIndexManager()
	if vecMgr == nil {
		return nil
	}
	applier, ok := vecMgr.(interface {
		ApplyCommittedVectorCDC(context.Context, string, uint64, uint64, []common.CDCEntry) error
	})
	if !ok {
		return nil
	}
	if err := applier.ApplyCommittedVectorCDC(ctx, database, txnID, seqNum, entries); err != nil {
		return fmt.Errorf("handleReplay: apply vector CDC: %w", err)
	}
	return nil
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
