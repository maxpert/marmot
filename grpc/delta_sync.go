package grpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
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
	Database           string
	TxnsApplied        int
	LastAppliedTxnID   uint64
	LastAppliedTS      *hlc.Timestamp
	Err                error
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
	firstEventReceived := false
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

		// Gap detection: Check if the first received transaction indicates missing data
		// This happens when GC has deleted transactions between fromTxnID and the first available one
		if !firstEventReceived {
			firstEventReceived = true
			gap := event.TxnId - fromTxnID

			// If gap is larger than delta sync threshold, we have missing transactions
			// This means GC deleted them and we need a snapshot instead
			if gap > uint64(cfg.Config.Replication.DeltaSyncThresholdTxns) {
				result.Err = fmt.Errorf(
					"gap detected: requested from txn_id %d but first available is %d (gap: %d txns, threshold: %d). "+
						"Transactions likely GC'd - full snapshot required",
					fromTxnID,
					event.TxnId,
					gap,
					cfg.Config.Replication.DeltaSyncThresholdTxns,
				)
				log.Warn().
					Uint64("requested_from", fromTxnID).
					Uint64("first_available", event.TxnId).
					Uint64("gap", gap).
					Int("threshold", cfg.Config.Replication.DeltaSyncThresholdTxns).
					Str("database", database).
					Msg("Gap detected in transaction stream - snapshot required")
				return result, result.Err
			}
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
	// Determine target database
	database := event.Database
	if database == "" {
		database = "marmot" // Default database
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

	// Convert to TransactionRequest
	txnReq := &TransactionRequest{
		TxnId:        event.TxnId,
		SourceNodeId: 0, // This is a replicated transaction, not from a specific source
		Statements:   event.Statements,
		Timestamp:    event.Timestamp,
		Phase:        TransactionPhase_COMMIT, // Already committed on source
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

	for _, stmt := range event.Statements {
		if _, err := tx.ExecContext(ctx, stmt.Sql); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
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
