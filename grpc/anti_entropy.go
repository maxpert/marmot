package grpc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

// AntiEntropyService manages background anti-entropy process
// It periodically checks for lagging peers and brings them up to date
type AntiEntropyService struct {
	nodeID           uint64
	registry         *NodeRegistry
	client           *Client
	dbManager        *db.DatabaseManager
	deltaSync        *DeltaSyncClient
	clock            *hlc.Clock
	snapshotFunc     SnapshotTransferFunc
	schemaVersionMgr *db.SchemaVersionManager

	// Configuration
	interval              time.Duration
	deltaThresholdTxns    int
	deltaThresholdSeconds int
	enabled               bool

	// Control
	stopCh  chan struct{}
	running bool
	mu      sync.Mutex
}

// SnapshotTransferFunc initiates a snapshot transfer to a peer
// This is injected to avoid circular dependencies
type SnapshotTransferFunc func(ctx context.Context, peerNodeID uint64, peerAddr string, database string) error

// AntiEntropyConfig holds configuration for anti-entropy service
type AntiEntropyConfig struct {
	NodeID                uint64
	Registry              *NodeRegistry
	Client                *Client
	DBManager             *db.DatabaseManager
	DeltaSync             *DeltaSyncClient
	Clock                 *hlc.Clock
	SnapshotFunc          SnapshotTransferFunc
	SchemaVersionMgr      *db.SchemaVersionManager
	Interval              time.Duration
	DeltaThresholdTxns    int
	DeltaThresholdSeconds int
	Enabled               bool
}

// NewAntiEntropyService creates a new anti-entropy service
func NewAntiEntropyService(config AntiEntropyConfig) *AntiEntropyService {
	return &AntiEntropyService{
		nodeID:                config.NodeID,
		registry:              config.Registry,
		client:                config.Client,
		dbManager:             config.DBManager,
		deltaSync:             config.DeltaSync,
		clock:                 config.Clock,
		snapshotFunc:          config.SnapshotFunc,
		schemaVersionMgr:      config.SchemaVersionMgr,
		interval:              config.Interval,
		deltaThresholdTxns:    config.DeltaThresholdTxns,
		deltaThresholdSeconds: config.DeltaThresholdSeconds,
		enabled:               config.Enabled,
		stopCh:                make(chan struct{}),
	}
}

// NewAntiEntropyServiceFromConfig creates anti-entropy service from global config
func NewAntiEntropyServiceFromConfig(
	nodeID uint64,
	registry *NodeRegistry,
	client *Client,
	dbManager *db.DatabaseManager,
	deltaSync *DeltaSyncClient,
	clock *hlc.Clock,
	snapshotFunc SnapshotTransferFunc,
	schemaVersionMgr *db.SchemaVersionManager,
) *AntiEntropyService {
	config := cfg.Config.Replication

	return NewAntiEntropyService(AntiEntropyConfig{
		NodeID:                nodeID,
		Registry:              registry,
		Client:                client,
		DBManager:             dbManager,
		DeltaSync:             deltaSync,
		Clock:                 clock,
		SnapshotFunc:          snapshotFunc,
		SchemaVersionMgr:      schemaVersionMgr,
		Interval:              time.Duration(config.AntiEntropyIntervalS) * time.Second,
		DeltaThresholdTxns:    config.DeltaSyncThresholdTxns,
		DeltaThresholdSeconds: config.DeltaSyncThresholdSeconds,
		Enabled:               config.EnableAntiEntropy,
	})
}

// Start starts the anti-entropy background process
func (ae *AntiEntropyService) Start() {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if !ae.enabled {
		log.Info().Msg("Anti-entropy disabled in configuration")
		return
	}

	if ae.running {
		log.Warn().Msg("Anti-entropy service already running")
		return
	}

	ae.running = true
	ae.stopCh = make(chan struct{})

	go ae.runLoop()

	log.Info().
		Dur("interval", ae.interval).
		Int("delta_threshold_txns", ae.deltaThresholdTxns).
		Int("delta_threshold_seconds", ae.deltaThresholdSeconds).
		Msg("Anti-entropy service started")
}

// Stop stops the anti-entropy background process
func (ae *AntiEntropyService) Stop() {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if !ae.running {
		return
	}

	close(ae.stopCh)
	ae.running = false

	log.Info().Msg("Anti-entropy service stopped")
}

// runLoop is the main anti-entropy loop
func (ae *AntiEntropyService) runLoop() {
	ticker := time.NewTicker(ae.interval)
	defer ticker.Stop()

	// Wait briefly for gossip to discover cluster members, then run anti-entropy
	// This ensures we have ALIVE peers to sync with after a restart
	ae.runStartupSync()

	for {
		select {
		case <-ticker.C:
			ae.performAntiEntropy()
		case <-ae.stopCh:
			return
		}
	}
}

// runStartupSync performs anti-entropy at startup, waiting for peers if needed
func (ae *AntiEntropyService) runStartupSync() {
	log.Debug().
		Uint64("node_id", ae.nodeID).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("ANTI-ENTROPY: Running startup sync")

	// Try up to 5 times with 2 second delays to find ALIVE peers
	// This gives gossip time to propagate membership after a restart
	maxAttempts := 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		nodes := ae.registry.GetAll()
		aliveCount := 0
		for _, node := range nodes {
			if node.Status == NodeStatus_ALIVE && node.NodeId != ae.nodeID {
				aliveCount++
			}
		}

		if aliveCount > 0 {
			log.Debug().
				Uint64("node_id", ae.nodeID).
				Int("alive_peers", aliveCount).
				Int("attempt", attempt).
				Str("timestamp", time.Now().Format("15:04:05.000")).
				Msg("ANTI-ENTROPY: Found ALIVE peers, starting sync")
			ae.performAntiEntropy()
			return
		}

		log.Debug().
			Uint64("node_id", ae.nodeID).
			Int("attempt", attempt).
			Int("max_attempts", maxAttempts).
			Str("timestamp", time.Now().Format("15:04:05.000")).
			Msg("ANTI-ENTROPY: No ALIVE peers found, waiting for gossip")

		// Wait for gossip to discover peers (unless this is the last attempt)
		if attempt < maxAttempts {
			select {
			case <-time.After(2 * time.Second):
				// Continue to next attempt
			case <-ae.stopCh:
				return
			}
		}
	}

	log.Debug().
		Uint64("node_id", ae.nodeID).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("ANTI-ENTROPY: No ALIVE peers after startup wait, will sync on next interval")
}

// performAntiEntropy performs a single anti-entropy round
func (ae *AntiEntropyService) performAntiEntropy() {
	roundStart := time.Now()
	telemetry.AntiEntropyRoundsTotal.Inc()

	ctx, cancel := context.WithTimeout(context.Background(), ae.interval)
	defer cancel()
	defer func() {
		telemetry.AntiEntropyDurationSeconds.Observe(time.Since(roundStart).Seconds())
	}()

	log.Debug().Msg("Starting anti-entropy round")

	// Get all ALIVE nodes from registry
	nodes := ae.registry.GetAll()
	aliveNodes := make([]*NodeState, 0)
	for _, node := range nodes {
		if node.Status == NodeStatus_ALIVE && node.NodeId != ae.nodeID {
			aliveNodes = append(aliveNodes, node)
		}
	}

	if len(aliveNodes) == 0 {
		log.Debug().Msg("No alive peers to sync with")
		return
	}

	log.Debug().
		Uint64("node_id", ae.nodeID).
		Int("peer_count", len(aliveNodes)).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("ANTI-ENTROPY: Starting round")

	// For each database, find the peer with highest max_txn_id and sync ONLY from that peer
	// This prevents race conditions from concurrent snapshot downloads
	databases := ae.dbManager.ListDatabases()
	log.Debug().
		Uint64("node_id", ae.nodeID).
		Int("database_count", len(databases)).
		Strs("databases", databases).
		Msg("ANTI-ENTROPY: Checking databases for sync")

	for _, dbName := range databases {
		bestPeer := ae.findBestPeerForDatabase(ctx, aliveNodes, dbName)
		if bestPeer != nil {
			log.Debug().
				Uint64("node_id", ae.nodeID).
				Uint64("best_peer", bestPeer.NodeId).
				Str("database", dbName).
				Msg("ANTI-ENTROPY: Found best peer, starting sync")
			if err := ae.syncPeerDatabase(ctx, bestPeer, dbName); err != nil {
				log.Warn().
					Err(err).
					Uint64("peer_node", bestPeer.NodeId).
					Str("database", dbName).
					Msg("Failed to sync from best peer")
			}
		} else {
			log.Debug().
				Uint64("node_id", ae.nodeID).
				Str("database", dbName).
				Msg("ANTI-ENTROPY: No better peer found (we may be up to date)")
		}
	}

	log.Debug().Msg("Anti-entropy round completed")
}

// findBestPeerForDatabase finds the peer with highest max_txn_id or most transactions
// Uses two criteria:
// 1. Higher max_txn_id indicates more recent transactions
// 2. Higher committed_txn_count indicates more data (tiebreaker when max_txn_id is the same)
// CRITICAL: Skips peers with older schema versions to prevent data loss from DDL changes
// Returns nil if we're already up to date with all peers
func (ae *AntiEntropyService) findBestPeerForDatabase(ctx context.Context, aliveNodes []*NodeState, database string) *NodeState {
	localMaxTxnID, err := ae.dbManager.GetMaxTxnID(database)
	if err != nil {
		log.Warn().Err(err).Str("database", database).Msg("Failed to get local max txn_id")
		localMaxTxnID = 0
	}

	localTxnCount, err := ae.dbManager.GetCommittedTxnCount(database)
	if err != nil {
		log.Warn().Err(err).Str("database", database).Msg("Failed to get local txn count")
		localTxnCount = 0
	}

	var localSchemaVersion uint64 = 0
	if ae.schemaVersionMgr != nil {
		localSchemaVersion, err = ae.schemaVersionMgr.GetSchemaVersion(database)
		if err != nil {
			log.Warn().Err(err).Str("database", database).Msg("Failed to get local schema version, treating as 0")
			localSchemaVersion = 0
		}
	}

	log.Debug().
		Uint64("node_id", ae.nodeID).
		Str("database", database).
		Uint64("local_max_txn", localMaxTxnID).
		Int64("local_txn_count", localTxnCount).
		Uint64("local_schema_version", localSchemaVersion).
		Int("peers_to_check", len(aliveNodes)).
		Msg("ANTI-ENTROPY: Comparing with peers")

	var bestPeer *NodeState
	var bestMaxTxnID uint64 = localMaxTxnID
	var bestTxnCount int64 = localTxnCount

	for _, peer := range aliveNodes {
		peerSchemaVersion := uint64(0)
		if peer.DatabaseSchemaVersions != nil {
			if version, exists := peer.DatabaseSchemaVersions[database]; exists {
				peerSchemaVersion = version
			}
		}

		if peerSchemaVersion < localSchemaVersion {
			log.Debug().
				Uint64("node_id", ae.nodeID).
				Uint64("peer_node", peer.NodeId).
				Str("database", database).
				Uint64("local_schema_version", localSchemaVersion).
				Uint64("peer_schema_version", peerSchemaVersion).
				Msg("ANTI-ENTROPY: Skipping peer with older schema version")
			continue
		}

		client, err := ae.client.GetClientByAddress(peer.Address)
		if err != nil {
			log.Debug().
				Err(err).
				Uint64("node_id", ae.nodeID).
				Uint64("peer_node", peer.NodeId).
				Msg("ANTI-ENTROPY: Failed to get client for peer")
			continue
		}

		resp, err := client.GetReplicationState(ctx, &ReplicationStateRequest{
			RequestingNodeId: ae.nodeID,
			Database:         database,
		})
		if err != nil {
			log.Debug().
				Err(err).
				Uint64("node_id", ae.nodeID).
				Uint64("peer_node", peer.NodeId).
				Msg("ANTI-ENTROPY: Failed to get replication state from peer")
			continue
		}

		for _, state := range resp.States {
			if state.DatabaseName != database {
				continue
			}

			peerMaxTxn := state.CurrentMaxTxnId
			peerTxnCount := state.CommittedTxnCount

			log.Debug().
				Uint64("node_id", ae.nodeID).
				Uint64("peer_node", peer.NodeId).
				Str("state_db", state.DatabaseName).
				Uint64("peer_max_txn", peerMaxTxn).
				Int64("peer_txn_count", peerTxnCount).
				Uint64("peer_schema_version", peerSchemaVersion).
				Uint64("best_max_txn", bestMaxTxnID).
				Int64("best_txn_count", bestTxnCount).
				Msg("ANTI-ENTROPY: Checking peer state")

			// Select peer if:
			// 1. Peer has higher max_txn_id, OR
			// 2. Peer has same max_txn_id but more committed transactions (more data)
			shouldSelect := peerMaxTxn > bestMaxTxnID ||
				(peerMaxTxn == bestMaxTxnID && peerTxnCount > bestTxnCount)

			if shouldSelect {
				bestMaxTxnID = peerMaxTxn
				bestTxnCount = peerTxnCount
				bestPeer = peer
				log.Debug().
					Uint64("node_id", ae.nodeID).
					Uint64("new_best_peer", peer.NodeId).
					Uint64("new_best_txn", bestMaxTxnID).
					Int64("new_best_count", bestTxnCount).
					Msg("ANTI-ENTROPY: Found better peer")
			}
		}
	}

	if bestPeer != nil {
		log.Debug().
			Uint64("node_id", ae.nodeID).
			Uint64("best_peer", bestPeer.NodeId).
			Str("database", database).
			Uint64("local_max_txn", localMaxTxnID).
			Int64("local_txn_count", localTxnCount).
			Uint64("peer_max_txn", bestMaxTxnID).
			Int64("peer_txn_count", bestTxnCount).
			Msg("ANTI-ENTROPY: Found best peer for sync")
	} else {
		log.Debug().
			Uint64("node_id", ae.nodeID).
			Str("database", database).
			Uint64("local_max_txn", localMaxTxnID).
			Int64("local_txn_count", localTxnCount).
			Msg("ANTI-ENTROPY: We have the most data, no sync needed")
	}

	return bestPeer
}

// syncPeer checks and syncs a single peer across all databases
func (ae *AntiEntropyService) syncPeer(ctx context.Context, peer *NodeState) {
	log.Debug().
		Uint64("peer_node", peer.NodeId).
		Str("peer_addr", peer.Address).
		Msg("Checking peer replication state")

	// Get list of all databases
	databases := ae.dbManager.ListDatabases()

	for _, dbName := range databases {
		if err := ae.syncPeerDatabase(ctx, peer, dbName); err != nil {
			log.Warn().
				Err(err).
				Uint64("peer_node", peer.NodeId).
				Str("database", dbName).
				Msg("Failed to sync database with peer")
			// Continue with other databases
		}
	}
}

// syncPeerDatabase syncs a single database with a peer
// In a leaderless system, we need to compare our local max txn ID with the peer's
// If peer has more transactions, we pull from them
func (ae *AntiEntropyService) syncPeerDatabase(ctx context.Context, peer *NodeState, database string) error {
	// Get peer's replication state (peer returns their max txn ID)
	client, err := ae.client.GetClientByAddress(peer.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	resp, err := client.GetReplicationState(ctx, &ReplicationStateRequest{
		RequestingNodeId: ae.nodeID,
		Database:         database,
	})
	if err != nil {
		return fmt.Errorf("failed to get replication state: %w", err)
	}

	// Find the state for this database
	var dbState *DatabaseReplicationState
	for _, state := range resp.States {
		if state.DatabaseName == database {
			dbState = state
			break
		}
	}

	if dbState == nil {
		// Peer doesn't have this database yet
		log.Debug().
			Uint64("peer_node", peer.NodeId).
			Str("database", database).
			Msg("Peer doesn't have replication state for database")
		return nil
	}

	// Get our local max txn ID for comparison
	localMaxTxnID, err := ae.dbManager.GetMaxTxnID(database)
	if err != nil {
		log.Warn().Err(err).Str("database", database).Msg("Failed to get local max txn_id")
		localMaxTxnID = 0
	}

	peerMaxTxnID := dbState.CurrentMaxTxnId
	peerTxnCount := dbState.CommittedTxnCount

	// Get local committed txn count
	localTxnCount, err := ae.dbManager.GetCommittedTxnCount(database)
	if err != nil {
		log.Warn().Err(err).Str("database", database).Msg("Failed to get local txn count")
		localTxnCount = 0
	}

	log.Debug().
		Uint64("node_id", ae.nodeID).
		Uint64("peer_node", peer.NodeId).
		Str("database", database).
		Uint64("local_max_txn", localMaxTxnID).
		Uint64("peer_max_txn", peerMaxTxnID).
		Int64("local_txn_count", localTxnCount).
		Int64("peer_txn_count", peerTxnCount).
		Msg("ANTI-ENTROPY: Comparing with peer")

	// Determine if we need to sync from peer
	// Primary metric: transaction COUNT (number of committed transactions)
	// Secondary metric: max txn ID (HLC timestamp) as tiebreaker when counts are equal
	// This avoids spurious syncs due to HLC drift between nodes
	needsSync := peerTxnCount > localTxnCount ||
		(peerTxnCount == localTxnCount && peerMaxTxnID > localMaxTxnID)

	if needsSync {
		// Use transaction count difference for lag calculation, NOT HLC difference
		// HLC differences can be millions even for near-simultaneous transactions
		txnCountDiff := peerTxnCount - localTxnCount
		var lag uint64
		if txnCountDiff > 0 {
			lag = uint64(txnCountDiff)
		}

		log.Debug().
			Uint64("node_id", ae.nodeID).
			Uint64("peer_node", peer.NodeId).
			Str("database", database).
			Uint64("lag_txns", lag).
			Int64("txn_count_diff", txnCountDiff).
			Msg("ANTI-ENTROPY: We are behind peer, pulling transactions")

		// If txn_count is the same but max_txn_id differs, we have divergent HLC timestamps
		// This is normal in a leaderless system - different nodes may commit at slightly different times
		// Delta sync should work fine since both nodes have the same number of transactions
		// Only use snapshot if we can't determine which transactions we're missing
		if lag == 0 && peerMaxTxnID > localMaxTxnID {
			log.Debug().
				Uint64("node_id", ae.nodeID).
				Uint64("peer_node", peer.NodeId).
				Str("database", database).
				Int64("local_txn_count", localTxnCount).
				Int64("peer_txn_count", peerTxnCount).
				Uint64("local_max_txn", localMaxTxnID).
				Uint64("peer_max_txn", peerMaxTxnID).
				Msg("ANTI-ENTROPY: Same txn count but different max HLC - using delta sync to verify")

			// Try delta sync - it will pull any transactions we're missing
			// If there are no missing transactions, it will be a no-op
		}

		// Calculate time lag based on last sync time
		// If LastSyncTime is 0 (first sync), use 0 duration to avoid ~54 year lag
		var timeLag time.Duration
		if dbState.LastSyncTime > 0 {
			timeLag = time.Since(time.Unix(0, dbState.LastSyncTime))
		}

		// Decide: delta sync or snapshot?
		if ae.shouldUseDeltaSync(lag, timeLag) {
			// Use delta sync - pull transactions from peer starting after our local max
			log.Debug().
				Uint64("peer_node", peer.NodeId).
				Str("database", database).
				Uint64("from_txn_id", localMaxTxnID).
				Msg("ANTI-ENTROPY: Using delta sync for catch-up")

			result, err := ae.deltaSync.SyncFromPeer(
				ctx,
				peer.NodeId,
				peer.Address,
				database,
				localMaxTxnID, // Start from our local max, pull newer transactions
			)
			if err != nil {
				// Check if error is due to gap detection (missing transactions)
				// If so, fall back to snapshot instead of failing
				errMsg := err.Error()
				if strings.Contains(errMsg, "gap detected") {
					log.Warn().
						Err(err).
						Uint64("peer_node", peer.NodeId).
						Str("database", database).
						Msg("Gap detected in delta sync, falling back to snapshot")

					// Try snapshot instead
					if ae.snapshotFunc != nil {
						if snapErr := ae.snapshotFunc(ctx, peer.NodeId, peer.Address, database); snapErr != nil {
							telemetry.AntiEntropySyncsTotal.With("snapshot", "failed").Inc()
							return fmt.Errorf("delta sync failed with gap (transactions GC'd) and snapshot fallback failed: %w", snapErr)
						}
						telemetry.AntiEntropySyncsTotal.With("snapshot", "success").Inc()
						return nil
					}
					telemetry.AntiEntropySyncsTotal.With("delta", "failed").Inc()
					return fmt.Errorf("delta sync failed with gap (transactions GC'd) but snapshot function not configured: %w", err)
				}
				// Other errors - fail normally
				telemetry.AntiEntropySyncsTotal.With("delta", "failed").Inc()
				return fmt.Errorf("delta sync failed: %w", err)
			}

			telemetry.AntiEntropySyncsTotal.With("delta", "success").Inc()
			telemetry.DeltaSyncTxnsTotal.Add(float64(result.TxnsApplied))
			log.Debug().
				Uint64("peer_node", peer.NodeId).
				Str("database", database).
				Int("txns_applied", result.TxnsApplied).
				Msg("ANTI-ENTROPY: Delta sync completed")
		} else {
			// Use snapshot (full database transfer)
			log.Debug().
				Uint64("peer_node", peer.NodeId).
				Str("database", database).
				Uint64("lag_txns", lag).
				Msg("ANTI-ENTROPY: Lag too large, using snapshot")

			if ae.snapshotFunc != nil {
				if err := ae.snapshotFunc(ctx, peer.NodeId, peer.Address, database); err != nil {
					telemetry.AntiEntropySyncsTotal.With("snapshot", "failed").Inc()
					return fmt.Errorf("snapshot transfer failed: %w", err)
				}
				telemetry.AntiEntropySyncsTotal.With("snapshot", "success").Inc()
			} else {
				log.Warn().Msg("Snapshot function not configured, skipping snapshot transfer")
			}
		}
	}

	return nil
}

// shouldUseDeltaSync determines whether to use delta sync vs snapshot
// Returns true if lag is small enough for delta sync
func (ae *AntiEntropyService) shouldUseDeltaSync(lagTxns uint64, lagTime time.Duration) bool {
	// Use delta sync if BOTH conditions are met:
	// 1. Transaction lag is below threshold
	// 2. Time lag is below threshold

	txnThreshold := uint64(ae.deltaThresholdTxns)
	timeThreshold := time.Duration(ae.deltaThresholdSeconds) * time.Second

	return lagTxns < txnThreshold && lagTime < timeThreshold
}

// ForceSync forces an immediate anti-entropy round
// Useful for testing or manual catch-up
func (ae *AntiEntropyService) ForceSync() {
	log.Info().Msg("Forcing anti-entropy sync")
	ae.performAntiEntropy()
}

// GetStats returns current anti-entropy statistics
func (ae *AntiEntropyService) GetStats() map[string]interface{} {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	return map[string]interface{}{
		"enabled":                 ae.enabled,
		"running":                 ae.running,
		"interval_seconds":        ae.interval.Seconds(),
		"delta_threshold_txns":    ae.deltaThresholdTxns,
		"delta_threshold_seconds": ae.deltaThresholdSeconds,
	}
}
