package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// AntiEntropyService manages background anti-entropy process
// It periodically checks for lagging peers and brings them up to date
type AntiEntropyService struct {
	nodeID       uint64
	registry     *NodeRegistry
	client       *Client
	dbManager    *db.DatabaseManager
	deltaSync    *DeltaSyncClient
	clock        *hlc.Clock
	snapshotFunc SnapshotTransferFunc

	// Configuration
	interval                    time.Duration
	deltaThresholdTxns          int
	deltaThresholdSeconds       int
	enabled                     bool

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
	NodeID                  uint64
	Registry                *NodeRegistry
	Client                  *Client
	DBManager               *db.DatabaseManager
	DeltaSync               *DeltaSyncClient
	Clock                   *hlc.Clock
	SnapshotFunc            SnapshotTransferFunc
	Interval                time.Duration
	DeltaThresholdTxns      int
	DeltaThresholdSeconds   int
	Enabled                 bool
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

	// Run once immediately on startup
	ae.performAntiEntropy()

	for {
		select {
		case <-ticker.C:
			ae.performAntiEntropy()
		case <-ae.stopCh:
			return
		}
	}
}

// performAntiEntropy performs a single anti-entropy round
func (ae *AntiEntropyService) performAntiEntropy() {
	ctx, cancel := context.WithTimeout(context.Background(), ae.interval)
	defer cancel()

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

	log.Info().
		Int("peer_count", len(aliveNodes)).
		Msg("Checking replication lag across peers")

	// Check and sync each peer
	var wg sync.WaitGroup
	for _, node := range aliveNodes {
		wg.Add(1)
		go func(peerNode *NodeState) {
			defer wg.Done()
			ae.syncPeer(ctx, peerNode)
		}(node)
	}

	wg.Wait()
	log.Debug().Msg("Anti-entropy round completed")
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
func (ae *AntiEntropyService) syncPeerDatabase(ctx context.Context, peer *NodeState, database string) error {
	// Get peer's replication state
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
		// Peer doesn't have this database yet, might need to sync
		log.Debug().
			Uint64("peer_node", peer.NodeId).
			Str("database", database).
			Msg("Peer doesn't have replication state for database")
		return nil
	}

	// Calculate lag
	lag := dbState.CurrentMaxTxnId - dbState.LastAppliedTxnId
	if lag == 0 {
		log.Debug().
			Uint64("peer_node", peer.NodeId).
			Str("database", database).
			Msg("Peer is up to date")
		return nil
	}

	// Calculate time lag
	timeLag := time.Since(time.Unix(0, dbState.LastSyncTime))

	log.Info().
		Uint64("peer_node", peer.NodeId).
		Str("database", database).
		Uint64("lag_txns", lag).
		Dur("lag_time", timeLag).
		Str("sync_status", dbState.SyncStatus).
		Msg("Peer is lagging, initiating catch-up")

	// Decide: delta sync or snapshot?
	if ae.shouldUseDeltaSync(lag, timeLag) {
		// Use delta sync (incremental)
		log.Info().
			Uint64("peer_node", peer.NodeId).
			Str("database", database).
			Uint64("from_txn_id", dbState.LastAppliedTxnId).
			Msg("Using delta sync for catch-up")

		result, err := ae.deltaSync.SyncFromPeer(
			ctx,
			peer.NodeId,
			peer.Address,
			database,
			dbState.LastAppliedTxnId,
		)
		if err != nil {
			return fmt.Errorf("delta sync failed: %w", err)
		}

		log.Info().
			Uint64("peer_node", peer.NodeId).
			Str("database", database).
			Int("txns_applied", result.TxnsApplied).
			Msg("Delta sync completed")
	} else {
		// Use snapshot (full database transfer)
		log.Info().
			Uint64("peer_node", peer.NodeId).
			Str("database", database).
			Uint64("lag_txns", lag).
			Msg("Lag too large, using snapshot transfer")

		if ae.snapshotFunc != nil {
			if err := ae.snapshotFunc(ctx, peer.NodeId, peer.Address, database); err != nil {
				return fmt.Errorf("snapshot transfer failed: %w", err)
			}

			log.Info().
				Uint64("peer_node", peer.NodeId).
				Str("database", database).
				Msg("Snapshot transfer completed")
		} else {
			log.Warn().Msg("Snapshot function not configured, skipping snapshot transfer")
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
		"enabled":                   ae.enabled,
		"running":                   ae.running,
		"interval_seconds":          ae.interval.Seconds(),
		"delta_threshold_txns":      ae.deltaThresholdTxns,
		"delta_threshold_seconds":   ae.deltaThresholdSeconds,
	}
}
