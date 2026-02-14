package grpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// DeltaSyncClient handles incremental replication catch-up
// It streams changes from a peer and applies them locally
type DeltaSyncClient struct {
	client           *Client
	dbManager        *db.DatabaseManager
	nodeID           uint64
	clock            *hlc.Clock
	applyTxnsFn      ApplyTransactionFunc
	schemaVersionMgr *db.SchemaVersionManager
}

// ApplyTransactionFunc applies a replicated transaction to the local database
// This is injected to avoid circular dependencies with the replication handler
type ApplyTransactionFunc func(ctx context.Context, txnReq *TransactionRequest) (*TransactionResponse, error)

// DeltaSyncConfig holds configuration for delta sync
type DeltaSyncConfig struct {
	NodeID           uint64
	Client           *Client
	DBManager        *db.DatabaseManager
	Clock            *hlc.Clock
	ApplyTxnsFn      ApplyTransactionFunc
	SchemaVersionMgr *db.SchemaVersionManager
}

// NewDeltaSyncClient creates a new delta sync client
func NewDeltaSyncClient(config DeltaSyncConfig) *DeltaSyncClient {
	return &DeltaSyncClient{
		client:           config.Client,
		dbManager:        config.DBManager,
		nodeID:           config.NodeID,
		clock:            config.Clock,
		applyTxnsFn:      config.ApplyTxnsFn,
		schemaVersionMgr: config.SchemaVersionMgr,
	}
}

// DeltaSyncResult contains the result of a delta sync operation
type DeltaSyncResult struct {
	Database         string
	TxnsApplied      int
	LastAppliedTxnID uint64
	LastAppliedTS    *hlc.Timestamp
	ExpectedNextTxn  uint64 // Track expected next txn_id for gap detection
	Err              error
}

// SyncFromPeer performs delta sync for a specific database from a peer
// It streams changes from fromTxnID onwards and applies them locally
func (ds *DeltaSyncClient) SyncFromPeer(ctx context.Context, peerNodeID uint64, peerAddr string, database string, fromTxnID uint64) (*DeltaSyncResult, error) {
	result := &DeltaSyncResult{
		Database:         database,
		TxnsApplied:      0,
		LastAppliedTxnID: fromTxnID,
	}

	// Get client connection to peer
	client, err := ds.client.GetClientByAddress(peerAddr)
	if err != nil {
		result.Err = fmt.Errorf("failed to connect to peer %d (%s): %w", peerNodeID, peerAddr, err)
		return result, result.Err
	}

	log.Info().
		Uint64("peer_node", peerNodeID).
		Str("database", database).
		Uint64("from_txn_id", fromTxnID).
		Msg("Starting delta sync from peer")

	// Stream changes from peer
	stream, err := client.StreamChanges(ctx, &StreamRequest{
		FromTxnId:        fromTxnID,
		RequestingNodeId: ds.nodeID,
		Database:         database,
	})
	if err != nil {
		result.Err = fmt.Errorf("failed to start change stream: %w", err)
		return result, result.Err
	}

	// Apply changes as they stream in
	// Initialize expected next txn to detect both initial and intermediate gaps
	result.ExpectedNextTxn = fromTxnID + 1

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			// Stream completed successfully
			break
		}
		if err != nil {
			result.Err = fmt.Errorf("stream error: %w", err)
			return result, result.Err
		}

		// Skip if we've already applied this transaction
		if event.TxnId <= fromTxnID {
			log.Debug().
				Uint64("txn_id", event.TxnId).
				Msg("Skipping already-applied transaction")
			continue
		}

		// Gap detection: Check EVERY event for missing transactions
		// If seq_num is available, use it (more reliable than txn_id)
		// Otherwise fall back to txn_id based gap detection
		if event.SeqNum > 0 && result.ExpectedNextTxn > 0 {
			// Use seq_num based gap detection (preferred)
			if event.SeqNum > result.ExpectedNextTxn {
				gap := event.SeqNum - result.ExpectedNextTxn
				if gap > uint64(cfg.Config.Replication.DeltaSyncThresholdTxns) {
					result.Err = fmt.Errorf(
						"gap detected: expected seq_num %d but received %d (gap: %d, threshold: %d). "+
							"Transactions likely GC'd - full snapshot required",
						result.ExpectedNextTxn,
						event.SeqNum,
						gap,
						cfg.Config.Replication.DeltaSyncThresholdTxns,
					)
					log.Warn().
						Uint64("expected_seq", result.ExpectedNextTxn).
						Uint64("received_seq", event.SeqNum).
						Uint64("gap", gap).
						Int("threshold", cfg.Config.Replication.DeltaSyncThresholdTxns).
						Str("database", database).
						Msg("Gap detected in sequence stream - snapshot required")
					return result, result.Err
				}
			}
			// Update expected next seq_num
			result.ExpectedNextTxn = event.SeqNum + 1
		} else {
			// Fallback to txn_id based gap detection
			if event.TxnId > result.ExpectedNextTxn {
				gap := event.TxnId - result.ExpectedNextTxn
				if gap > uint64(cfg.Config.Replication.DeltaSyncThresholdTxns) {
					result.Err = fmt.Errorf(
						"gap detected: expected txn_id %d but received %d (gap: %d txns, threshold: %d). "+
							"Transactions likely GC'd - full snapshot required",
						result.ExpectedNextTxn,
						event.TxnId,
						gap,
						cfg.Config.Replication.DeltaSyncThresholdTxns,
					)
					log.Warn().
						Uint64("expected_txn", result.ExpectedNextTxn).
						Uint64("received_txn", event.TxnId).
						Uint64("gap", gap).
						Int("threshold", cfg.Config.Replication.DeltaSyncThresholdTxns).
						Str("database", database).
						Msg("Gap detected in transaction stream - snapshot required")
					return result, result.Err
				}
			}
			// Update expected next txn_id
			result.ExpectedNextTxn = event.TxnId + 1
		}

		// Apply the transaction locally
		if err := ds.applyChangeEvent(ctx, event); err != nil {
			result.Err = fmt.Errorf("failed to apply txn %d: %w", event.TxnId, err)
			return result, result.Err
		}

		// Update result
		result.TxnsApplied++
		result.LastAppliedTxnID = event.TxnId
		result.LastAppliedTS = &hlc.Timestamp{
			WallTime: event.Timestamp.WallTime,
			Logical:  event.Timestamp.Logical,
			NodeID:   event.Timestamp.NodeId,
		}

		// Log progress every 100 transactions
		if result.TxnsApplied%100 == 0 {
			log.Info().
				Uint64("peer_node", peerNodeID).
				Str("database", database).
				Int("txns_applied", result.TxnsApplied).
				Uint64("last_txn_id", result.LastAppliedTxnID).
				Msg("Delta sync progress")
		}
	}

	// Update replication state in database
	if result.TxnsApplied > 0 {
		repState := &db.ReplicationState{
			PeerNodeID:        peerNodeID,
			DatabaseName:      database,
			LastAppliedTxnID:  result.LastAppliedTxnID,
			LastAppliedTSWall: result.LastAppliedTS.WallTime,
			LastAppliedTSLog:  result.LastAppliedTS.Logical,
			LastSyncTime:      time.Now().UnixNano(),
			SyncStatus:        "SYNCED",
		}

		if err := ds.dbManager.UpdateReplicationState(repState); err != nil {
			log.Warn().Err(err).Msg("Failed to update replication state")
			// Don't fail the whole sync for this
		}
	}

	log.Info().
		Uint64("peer_node", peerNodeID).
		Str("database", database).
		Int("txns_applied", result.TxnsApplied).
		Uint64("final_txn_id", result.LastAppliedTxnID).
		Msg("Delta sync completed")

	return result, nil
}

// SyncAllDatabasesFromPeer performs delta sync for all databases from a peer
// Returns a map of database name to sync result
func (ds *DeltaSyncClient) SyncAllDatabasesFromPeer(ctx context.Context, peerNodeID uint64, peerAddr string) (map[string]*DeltaSyncResult, error) {
	results := make(map[string]*DeltaSyncResult)

	// Get list of all databases
	databases := ds.dbManager.ListDatabases()

	log.Info().
		Uint64("peer_node", peerNodeID).
		Int("database_count", len(databases)).
		Msg("Starting multi-database delta sync")

	for _, dbName := range databases {
		// Get last applied txn_id for this peer/database
		repState, err := ds.dbManager.GetReplicationState(peerNodeID, dbName)
		var fromTxnID uint64
		if err != nil {
			// No state yet, start from beginning
			fromTxnID = 0
		} else {
			fromTxnID = repState.LastAppliedTxnID
		}

		// Sync this database
		result, err := ds.SyncFromPeer(ctx, peerNodeID, peerAddr, dbName, fromTxnID)
		if err != nil {
			log.Warn().
				Err(err).
				Str("database", dbName).
				Msg("Failed to sync database")
			// Continue with other databases
		}
		results[dbName] = result
	}

	return results, nil
}

// applyChangeEvent applies a single change event to the local database
func (ds *DeltaSyncClient) applyChangeEvent(ctx context.Context, event *ChangeEvent) error {
	// Determine target database - database name is required
	database := event.Database
	if database == "" {
		return fmt.Errorf("change event %d missing database name", event.TxnId)
	}

	// Schema version validation: Check if we need to catch up DDL first
	if ds.schemaVersionMgr != nil && event.RequiredSchemaVersion > 0 {
		localVersion, err := ds.schemaVersionMgr.GetSchemaVersion(database)
		if err != nil {
			log.Warn().Err(err).Str("database", database).Msg("Failed to get local schema version")
		} else if localVersion < event.RequiredSchemaVersion {
			// We're behind! This transaction requires a newer schema version
			// This means we missed a DDL statement - we need to catch up
			log.Warn().
				Str("database", database).
				Uint64("local_version", localVersion).
				Uint64("required_version", event.RequiredSchemaVersion).
				Uint64("txn_id", event.TxnId).
				Msg("Schema version gap detected - transaction requires newer schema")

			// For now, we'll try to apply anyway (DDL should be idempotent)
			// In a more sophisticated implementation, we would:
			// 1. Fetch missing DDL transactions from peer
			// 2. Apply them in order
			// 3. Then apply this transaction
			// But since DDL is replicated through the same stream and is idempotent,
			// we should have already received it if we're streaming in order
		}
	}

	// Convert to TransactionRequest using REPLAY phase
	// REPLAY bypasses 2PC state tracking since these transactions are already committed on source
	txnReq := &TransactionRequest{
		TxnId:        event.TxnId,
		SourceNodeId: event.Timestamp.NodeId, // Use original source node ID
		Statements:   event.Statements,
		Timestamp:    event.Timestamp,
		Phase:        TransactionPhase_REPLAY, // Use REPLAY for anti-entropy catch-up
		Consistency:  ConsistencyLevel_CONSISTENCY_ONE,
		Database:     database,
	}

	// Apply using the injected function
	if ds.applyTxnsFn != nil {
		resp, err := ds.applyTxnsFn(ctx, txnReq)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("replication failed: %s", resp.ErrorMessage)
		}
		return nil
	}

	// Fallback: apply directly to database (simplified)
	// In production, this should go through the full replication handler
	mdb, err := ds.dbManager.GetDatabase(database)
	if err != nil {
		return fmt.Errorf("database %s not found: %w", database, err)
	}

	// Execute each statement in a transaction
	tx, err := mdb.GetDB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	hasDDL := false

	for _, stmt := range event.Statements {
		// Check for CDC data (RowChange payload)
		if rowChange := stmt.GetRowChange(); rowChange != nil && (len(rowChange.NewValues) > 0 || len(rowChange.OldValues) > 0) {
			// CDC path: apply row data directly using unified applier
			schemaAdapter := &deltaSyncSchemaAdapter{dbMgr: ds.dbManager, dbName: database}
			var err error
			switch stmt.Type {
			case pb.StatementType_INSERT, pb.StatementType_REPLACE:
				err = db.ApplyCDCInsert(tx, stmt.TableName, rowChange.NewValues)
			case pb.StatementType_UPDATE:
				err = db.ApplyCDCUpdate(tx, schemaAdapter, stmt.TableName, rowChange.OldValues, rowChange.NewValues)
			case pb.StatementType_DELETE:
				err = db.ApplyCDCDelete(tx, schemaAdapter, stmt.TableName, rowChange.OldValues)
			default:
				err = fmt.Errorf("unsupported statement type for CDC: %v", stmt.Type)
			}
			if err != nil {
				return fmt.Errorf("failed to apply CDC statement: %w", err)
			}
			log.Debug().
				Str("table", stmt.TableName).
				Hex("intent_key", rowChange.IntentKey).
				Int("new_values", len(rowChange.NewValues)).
				Int("old_values", len(rowChange.OldValues)).
				Msg("DELTA-SYNC: Applied CDC data")
			continue
		}

		// LOAD DATA path.
		if loadData := stmt.GetLoadDataChange(); loadData != nil {
			if _, err := db.ApplyLoadDataInTx(tx, loadData.Sql, loadData.Data); err != nil {
				return fmt.Errorf("failed to apply LOAD DATA statement: %w", err)
			}
			log.Debug().Msg("DELTA-SYNC: Applied LOAD DATA")
			continue
		}

		// SQL path: execute SQL statement
		sql := stmt.GetSQL()
		if sql == "" {
			// CRITICAL: Don't silently skip - this means CDC data was lost during serialization
			// Fail the sync so anti-entropy knows to try again or use snapshot
			return fmt.Errorf("statement has no SQL and no CDC data (table=%s, type=%d) - CDC data may have been lost during serialization", stmt.TableName, stmt.Type)
		}
		if err := db.ApplyDDLSQLInTx(ctx, tx, sql); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}
		hasDDL = true
		log.Debug().
			Str("sql_prefix", func() string {
				if len(sql) > 50 {
					return sql[:50]
				}
				return sql
			}()).
			Msg("DELTA-SYNC: Executed SQL")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	if hasDDL {
		if err := mdb.ReloadSchema(); err != nil {
			log.Warn().Err(err).Str("database", database).Msg("DELTA-SYNC: failed to reload schema after DDL")
		}
	}

	return nil
}

// deltaSyncSchemaAdapter adapts DatabaseManager schema access to CDCSchemaProvider
type deltaSyncSchemaAdapter struct {
	dbMgr  *db.DatabaseManager
	dbName string
}

func (a *deltaSyncSchemaAdapter) GetPrimaryKeys(tableName string) ([]string, error) {
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

// GetPeerReplicationLag queries a peer to get its current replication state
// Returns the lag in number of transactions for each database
func (ds *DeltaSyncClient) GetPeerReplicationLag(ctx context.Context, peerAddr string, database string) (map[string]int64, error) {
	client, err := ds.client.GetClientByAddress(peerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer: %w", err)
	}

	// Get peer's replication state
	resp, err := client.GetReplicationState(ctx, &ReplicationStateRequest{
		RequestingNodeId: ds.nodeID,
		Database:         database, // Empty = all databases
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get replication state: %w", err)
	}

	// Calculate lag for each database
	lags := make(map[string]int64)
	for _, state := range resp.States {
		// Lag = peer's current max - what we've applied
		lag := int64(state.CurrentMaxTxnId) - int64(state.LastAppliedTxnId)
		lags[state.DatabaseName] = lag
	}

	return lags, nil
}
