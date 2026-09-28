package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/mysqlcode"
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
		internalStmt, err := protocolStatementFromProto(stmt)
		if err != nil {
			log.Error().Err(err).Uint64("txn_id", req.TxnId).Str("database", req.Database).Msg("Rejecting PREPARE: unrecognized statement on wire")
			return &TransactionResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("unrecognized statement: %v", err),
			}, nil
		}
		// protocolStatementFromProto already copied SQL and the inline payload
		// from this arm; only the out-of-band fetch below is still needed.
		if loadData := stmt.GetLoadDataChange(); loadData != nil {
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

	// Rolling-upgrade gate, participant half: refuse a PREPARE carrying DDL or
	// a CREATE/DROP DATABASE while any member does not serve the commit-log
	// pull protocol yet, so a coordinator running an older release, which has
	// no gate of its own, cannot reach a quorum for DDL either
	// (coordinator.LegacyMembersDDLRefusal).
	if rh.registry != nil && statementsCarryDDLOrDatabaseOp(statements) {
		if legacy := rh.registry.LegacyLogProtocolMembers(); len(legacy) > 0 {
			telemetry.ReplicationRequestsTotal.With("prepare", "failed").Inc()
			return &TransactionResponse{
				Success:      false,
				Rejected:     true,
				ErrorMessage: coordinator.NewLegacyMembersDDLRefusal(legacy).Error(),
				ErrorCode:    uint32(mysqlcode.ErrCodeDeadlock),
			}, nil
		}
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
		Rejected:         result.Rejected,
		AutoIdStoredBase: result.AutoIDStoredBase,
		ErrorCode:        uint32(result.ErrorCode),
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

// statementsCarryDDLOrDatabaseOp reports whether any statement is DDL or a
// CREATE/DROP DATABASE, matching the isDDL classification in
// coordinator.CoordinatorHandler.handleMutation.
func statementsCarryDDLOrDatabaseOp(statements []protocol.Statement) bool {
	for _, stmt := range statements {
		switch stmt.Type {
		case protocol.StatementDDL, protocol.StatementCreateDatabase, protocol.StatementDropDatabase:
			return true
		}
	}
	return false
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
		internalStmt, err := protocolStatementFromProto(stmt)
		if err != nil {
			log.Error().Err(err).Uint64("txn_id", req.TxnId).Str("database", req.Database).Msg("Rejecting COMMIT: unrecognized statement on wire")
			return &TransactionResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("unrecognized statement: %v", err),
			}, nil
		}
		statements = append(statements, internalStmt)
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

	// The schema version bump for a DDL transaction is already durable, atomic
	// with the DDL itself, in the database's own SQLite file: the
	// engine's commit path bumped and cached it as part of committing above.
	// There is nothing left to do here.

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
