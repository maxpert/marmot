package replica

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/db/snapshot"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// StreamClient manages the change stream from the master node
type StreamClient struct {
	nodeID    uint64
	dbManager *db.DatabaseManager
	clock     *hlc.Clock
	replica   *Replica

	conn   *grpc.ClientConn
	client marmotgrpc.MarmotServiceClient

	lastTxnID     map[string]uint64        // per-database last applied txn_id
	txnWaitQueues map[string]*TxnWaitQueue // per-database wait queues
	mu            sync.RWMutex

	// snapshotInProgress prevents concurrent snapshot apply and change events
	snapshotMu sync.RWMutex

	// Cluster awareness
	clusterMu     sync.RWMutex
	clusterNodes  map[uint64]*marmotgrpc.NodeState // All known nodes
	currentNodeID uint64                           // Node we're streaming from
	currentAddr   string                           // Address of current node

	// Discovery
	discoveryCtx    context.Context
	discoveryCancel context.CancelFunc

	reconnectInterval time.Duration
	maxBackoff        time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Background retry for failed snapshots
	retryWg sync.WaitGroup
}

// NewStreamClient creates a new stream client
func NewStreamClient(followAddrs []string, nodeID uint64, dbManager *db.DatabaseManager, clock *hlc.Clock, replica *Replica) *StreamClient {
	ctx, cancel := context.WithCancel(context.Background())
	discoveryCtx, discoveryCancel := context.WithCancel(context.Background())

	// Initialize cluster nodes from follow addresses
	clusterNodes := make(map[uint64]*marmotgrpc.NodeState)
	for _, addr := range followAddrs {
		// Use address hash as temporary ID until we discover real node IDs
		tempID := hashAddress(addr)
		clusterNodes[tempID] = &marmotgrpc.NodeState{
			NodeId:  tempID,
			Address: addr,
			Status:  marmotgrpc.NodeStatus_ALIVE,
		}
	}

	var currentAddr string
	if len(followAddrs) > 0 {
		currentAddr = followAddrs[0]
	}

	return &StreamClient{
		nodeID:            nodeID,
		dbManager:         dbManager,
		clock:             clock,
		replica:           replica,
		lastTxnID:         make(map[string]uint64),
		txnWaitQueues:     make(map[string]*TxnWaitQueue),
		clusterNodes:      clusterNodes,
		currentAddr:       currentAddr,
		discoveryCtx:      discoveryCtx,
		discoveryCancel:   discoveryCancel,
		reconnectInterval: time.Duration(cfg.Config.Replica.ReconnectIntervalSec) * time.Second,
		maxBackoff:        time.Duration(cfg.Config.Replica.ReconnectMaxBackoffSec) * time.Second,
		ctx:               ctx,
		cancel:            cancel,
	}
}

// hashAddress creates a temporary node ID from an address
func hashAddress(addr string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(addr))
	return h.Sum64()
}

// Bootstrap performs initial sync from master with partial failure handling
func (s *StreamClient) Bootstrap(ctx context.Context) error {
	log.Info().Str("master", s.currentAddr).Msg("Connecting to master for bootstrap")

	// Connect to master
	if err := s.connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer s.disconnect()

	// Check if we have local data
	localTxnIDs, err := s.getLocalMaxTxnIDs()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get local txn IDs, assuming empty")
	}

	// Get master's max txn IDs
	masterTxnIDs, err := s.getMasterMaxTxnIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get master state: %w", err)
	}

	log.Info().
		Interface("local_txn_ids", localTxnIDs).
		Interface("master_txn_ids", masterTxnIDs).
		Msg("Comparing local and master state")

	// Apply database filter
	replicateDatabases := cfg.Config.Replica.ReplicateDatabases
	filteredMasterTxnIDs := make(map[string]uint64)
	for dbName, txnID := range masterTxnIDs {
		if dbName == "__marmot_system" {
			continue
		}
		if s.shouldReplicateDatabase(dbName, replicateDatabases) {
			filteredMasterTxnIDs[dbName] = txnID
		}
	}

	// If no databases to replicate, return success
	if len(filteredMasterTxnIDs) == 0 {
		s.mu.Lock()
		s.lastTxnID = localTxnIDs
		s.mu.Unlock()
		log.Info().Msg("No databases to replicate, bootstrap completed")
		return nil
	}

	// Determine bootstrap strategy per database
	var snapshotDatabases []string
	var deltaDatabases []string

	if len(localTxnIDs) == 0 {
		// No local data - need full snapshot
		for dbName := range filteredMasterTxnIDs {
			snapshotDatabases = append(snapshotDatabases, dbName)
		}
		log.Info().Msg("No local data - full snapshot required")
	} else {
		// Check each database
		for dbName, masterTxnID := range filteredMasterTxnIDs {
			localTxnID := localTxnIDs[dbName]
			gap := masterTxnID - localTxnID

			if gap == 0 {
				log.Info().Str("database", dbName).Msg("Database is up to date")
				// Set lastTxnID for up-to-date databases
				s.mu.Lock()
				s.lastTxnID[dbName] = masterTxnID
				s.mu.Unlock()
				continue
			}

			if gap > uint64(cfg.Config.Replication.DeltaSyncThresholdTxns) {
				// Large gap - need snapshot for this database
				snapshotDatabases = append(snapshotDatabases, dbName)
				log.Info().
					Str("database", dbName).
					Uint64("gap", gap).
					Msg("Large gap detected - snapshot required")
			} else {
				deltaDatabases = append(deltaDatabases, dbName)
			}
		}
	}

	// Perform delta sync for databases with small gaps
	for _, dbName := range deltaDatabases {
		if err := s.deltaSync(ctx, dbName, localTxnIDs[dbName]); err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Delta sync failed, will retry during streaming")
		} else {
			s.mu.Lock()
			txnID, _ := s.getLocalMaxTxnIDs()
			if id, ok := txnID[dbName]; ok {
				s.lastTxnID[dbName] = id
			}
			s.mu.Unlock()
		}
	}

	// Download snapshots for databases that need it
	if len(snapshotDatabases) > 0 {
		successCount, failedDatabases := s.downloadSnapshotsWithPartialFailure(ctx, snapshotDatabases, filteredMasterTxnIDs)

		// If ALL databases failed, return error
		if successCount == 0 && len(failedDatabases) == len(snapshotDatabases) {
			return fmt.Errorf("all databases failed to snapshot: %d failures", len(failedDatabases))
		}

		// Start background retry for failed databases
		if len(failedDatabases) > 0 {
			for _, dbName := range failedDatabases {
				txnID := filteredMasterTxnIDs[dbName]
				s.startBackgroundRetry(dbName, txnID)
			}
		}
	}

	s.mu.RLock()
	log.Info().Interface("last_txn_ids", s.lastTxnID).Msg("Bootstrap completed")
	s.mu.RUnlock()
	return nil
}

// downloadSnapshotsWithPartialFailure downloads snapshots for multiple databases in parallel,
// continuing even if some fail. Returns success count and list of failed databases.
// Uses worker pool pattern with configurable concurrency limit.
func (s *StreamClient) downloadSnapshotsWithPartialFailure(ctx context.Context, databases []string, masterTxnIDs map[string]uint64) (int, []string) {
	// Get concurrency limit from config (default: 3)
	concurrency := cfg.Config.Replica.SnapshotConcurrency
	if concurrency <= 0 {
		concurrency = 3
	}

	// If only 1 database, no need for parallel execution
	if len(databases) == 1 {
		if err := s.downloadSnapshotForDatabaseSingle(ctx, databases[0]); err != nil {
			log.Warn().Err(err).Str("database", databases[0]).Msg("Snapshot download failed, will retry in background")
			return 0, databases
		}
		s.mu.Lock()
		s.lastTxnID[databases[0]] = masterTxnIDs[databases[0]]
		s.mu.Unlock()
		log.Info().Str("database", databases[0]).Uint64("txn_id", masterTxnIDs[databases[0]]).Msg("Database snapshot completed successfully")
		return 1, nil
	}

	log.Info().
		Int("databases", len(databases)).
		Int("concurrency", concurrency).
		Msg("Starting parallel snapshot downloads")

	// Worker pool: semaphore limits concurrent downloads
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrency)

	// Thread-safe result collection
	var resultMu sync.Mutex
	successCount := 0
	var failedDatabases []string

	for _, dbName := range databases {
		wg.Add(1)

		go func(db string) {
			defer wg.Done()

			// Acquire semaphore (blocks if concurrency limit reached)
			semaphore <- struct{}{}
			defer func() { <-semaphore }() // Release semaphore

			// Download snapshot for this database
			log.Debug().Str("database", db).Msg("Starting snapshot download")
			err := s.downloadSnapshotForDatabaseSingle(ctx, db)

			// Collect results (thread-safe)
			resultMu.Lock()
			if err != nil {
				log.Warn().Err(err).Str("database", db).Msg("Snapshot download failed, will retry in background")
				failedDatabases = append(failedDatabases, db)
			} else {
				// Success - update lastTxnID
				s.mu.Lock()
				s.lastTxnID[db] = masterTxnIDs[db]
				s.mu.Unlock()
				successCount++
				log.Info().Str("database", db).Uint64("txn_id", masterTxnIDs[db]).Msg("Database snapshot completed successfully")
			}
			resultMu.Unlock()
		}(dbName)
	}

	// Wait for all downloads to complete
	wg.Wait()

	log.Info().
		Int("total", len(databases)).
		Int("success", successCount).
		Int("failed", len(failedDatabases)).
		Msg("Parallel snapshot downloads completed")

	return successCount, failedDatabases
}

// downloadSnapshotForDatabaseSingle downloads snapshot for a specific database (single database only)
func (s *StreamClient) downloadSnapshotForDatabaseSingle(ctx context.Context, dbName string) error {
	log.Info().Str("database", dbName).Msg("Downloading snapshot for database")

	// For now, we download the full snapshot (could be optimized to download single database)
	// This is a simplified implementation - in production, you'd want per-database snapshot support
	return s.downloadSnapshot(ctx)
}

// startBackgroundRetry starts a background goroutine to retry snapshot download for a failed database
func (s *StreamClient) startBackgroundRetry(dbName string, txnID uint64) {
	s.retryWg.Add(1)
	go s.retrySnapshotDownload(dbName, txnID)
}

// retrySnapshotDownload retries snapshot download with exponential backoff
func (s *StreamClient) retrySnapshotDownload(dbName string, txnID uint64) {
	defer s.retryWg.Done()

	const (
		initialBackoff = 5 * time.Second
		maxBackoff     = 60 * time.Second
		multiplier     = 2.0
		maxAttempts    = 10
		jitterPercent  = 0.1
	)

	backoff := initialBackoff

	log.Info().
		Str("database", dbName).
		Dur("initial_backoff", initialBackoff).
		Int("max_attempts", maxAttempts).
		Msg("Starting background retry for failed database")

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Add jitter: ±10%
		jitter := time.Duration(float64(backoff) * (rand.Float64()*2*jitterPercent - jitterPercent))
		actualBackoff := backoff + jitter

		// Wait with context cancellation support
		select {
		case <-time.After(actualBackoff):
			// Continue with retry
		case <-s.ctx.Done():
			log.Debug().Str("database", dbName).Msg("Retry cancelled due to shutdown")
			return
		}

		log.Debug().
			Str("database", dbName).
			Int("attempt", attempt).
			Int("max_attempts", maxAttempts).
			Msg("Retrying snapshot download")

		// Attempt snapshot download
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
		err := s.downloadSnapshotForDatabaseSingle(ctx, dbName)
		cancel()

		if err == nil {
			// Success - update lastTxnID
			s.mu.Lock()
			s.lastTxnID[dbName] = txnID
			s.mu.Unlock()

			log.Info().
				Str("database", dbName).
				Int("attempt", attempt).
				Uint64("txn_id", txnID).
				Msg("Background retry succeeded, database is now synced")
			return
		}

		// Check if context was cancelled
		if s.ctx.Err() != nil {
			log.Debug().Str("database", dbName).Msg("Retry cancelled due to shutdown")
			return
		}

		log.Debug().
			Err(err).
			Str("database", dbName).
			Int("attempt", attempt).
			Msg("Retry attempt failed")

		// Double backoff, cap at max
		backoff = time.Duration(float64(backoff) * multiplier)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	log.Error().
		Str("database", dbName).
		Int("max_attempts", maxAttempts).
		Msg("Max retry attempts reached, giving up on database snapshot")
}

// Start begins the streaming loop (runs until Stop is called)
func (s *StreamClient) Start(ctx context.Context) {
	s.wg.Add(1)
	defer s.wg.Done()
	defer s.discoveryCancel()

	// Start discovery loops in background
	go s.startDiscoveryLoop()
	go s.startDatabaseDiscoveryLoop(ctx)

	backoff := s.reconnectInterval

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Stream client context cancelled, stopping")
			return
		case <-s.ctx.Done():
			log.Info().Msg("Stream client stopped")
			return
		default:
		}

		// Connect to master
		if err := s.connect(ctx); err != nil {
			log.Warn().Err(err).Dur("retry_in", backoff).Msg("Failed to connect to master")
			s.replica.SetState(StateReconnecting)
			s.replica.SetConnected(false)

			// Try failover instead of just backing off
			if err := s.failover(); err != nil {
				log.Warn().Err(err).Msg("Failover failed, backing off")
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				case <-s.ctx.Done():
					return
				}

				// Exponential backoff
				backoff = min(backoff*2, s.maxBackoff)
				continue
			}
		}

		// Reset backoff on successful connection
		backoff = s.reconnectInterval
		s.replica.SetConnected(true)
		s.replica.SetState(StateStreaming)

		log.Info().Str("master", s.currentAddr).Msg("Connected to master, starting change stream")

		// Stream changes
		if err := s.streamChanges(ctx); err != nil {
			if ctx.Err() != nil || s.ctx.Err() != nil {
				return // Shutdown requested
			}
			log.Warn().Err(err).Msg("Stream disconnected, will reconnect")
			s.replica.SetConnected(false)
			s.disconnect()
		}
	}
}

// Stop gracefully stops the stream client
func (s *StreamClient) Stop() {
	s.cancel()
	s.wg.Wait()
	s.retryWg.Wait()
	s.disconnect()
}

// connect establishes connection to master with PSK authentication
func (s *StreamClient) connect(ctx context.Context) error {
	if s.conn != nil {
		return nil // Already connected
	}

	// Get replica secret for PSK authentication
	replicaSecret := cfg.GetReplicaSecret()
	if replicaSecret == "" {
		return fmt.Errorf("replica.secret is required for connecting to master")
	}

	// Get keepalive settings from config
	keepaliveTime := time.Duration(cfg.Config.GRPCClient.KeepaliveTimeSeconds) * time.Second
	keepaliveTimeout := time.Duration(cfg.Config.GRPCClient.KeepaliveTimeoutSeconds) * time.Second

	conn, err := grpc.NewClient(s.currentAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
		grpc.WithChainUnaryInterceptor(marmotgrpc.UnaryClientInterceptorWithSecret(replicaSecret)),
		grpc.WithChainStreamInterceptor(marmotgrpc.StreamClientInterceptorWithSecret(replicaSecret)),
	)
	if err != nil {
		return fmt.Errorf("failed to create client for master: %w", err)
	}

	s.conn = conn
	s.client = marmotgrpc.NewMarmotServiceClient(conn)

	log.Info().Str("master", s.currentAddr).Msg("Connected to master with PSK authentication")
	return nil
}

// disconnect closes connection to master
func (s *StreamClient) disconnect() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
		s.client = nil
	}
}

// streamChanges streams and applies changes from master
func (s *StreamClient) streamChanges(ctx context.Context) error {
	// Get list of databases to stream
	databases := s.dbManager.ListDatabases()
	if len(databases) == 0 {
		databases = []string{"marmot"} // Default database
	}

	// Stream changes for each database
	// For simplicity, we stream all databases sequentially
	// In production, you might want parallel streams
	for _, dbName := range databases {
		s.mu.RLock()
		fromTxnID := s.lastTxnID[dbName]
		s.mu.RUnlock()

		log.Info().
			Str("database", dbName).
			Uint64("from_txn_id", fromTxnID).
			Msg("Starting change stream for database")

		stream, err := s.client.StreamChanges(ctx, &marmotgrpc.StreamRequest{
			FromTxnId:        fromTxnID,
			RequestingNodeId: s.nodeID,
			Database:         dbName,
		})
		if err != nil {
			return fmt.Errorf("failed to start stream for %s: %w", dbName, err)
		}

		// Process stream
		firstEvent := true
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				// Stream ended, move to next database
				break
			}
			if err != nil {
				return fmt.Errorf("stream error: %w", err)
			}

			// Skip already applied transactions
			if event.TxnId <= fromTxnID {
				continue
			}

			// Gap detection on first event
			if firstEvent {
				firstEvent = false
				gap := event.TxnId - fromTxnID
				if gap > uint64(cfg.Config.Replication.DeltaSyncThresholdTxns) {
					log.Warn().
						Str("database", dbName).
						Uint64("gap", gap).
						Msg("Gap detected - triggering snapshot")
					// Gap too large - need snapshot
					if err := s.downloadSnapshotForDatabase(ctx, dbName); err != nil {
						return fmt.Errorf("snapshot download failed for %s: %w", dbName, err)
					}
					// Restart stream after snapshot
					break
				}
			}

			// Apply change
			if err := s.applyChangeEvent(ctx, event); err != nil {
				log.Error().Err(err).
					Uint64("txn_id", event.TxnId).
					Str("database", dbName).
					Msg("Failed to apply change event")
				// Continue anyway - anti-entropy will fix it
				continue
			}

			// Update last txn ID
			s.mu.Lock()
			s.lastTxnID[dbName] = event.TxnId
			s.mu.Unlock()

			// Notify waiters
			if q, ok := s.txnWaitQueues[dbName]; ok {
				q.NotifyUpTo(event.TxnId)
			}

			log.Debug().
				Uint64("txn_id", event.TxnId).
				Str("database", dbName).
				Int("statements", len(event.Statements)).
				Msg("Applied change event")
		}
	}

	return nil
}

// deltaSync performs delta sync for a single database
func (s *StreamClient) deltaSync(ctx context.Context, dbName string, fromTxnID uint64) error {
	log.Info().
		Str("database", dbName).
		Uint64("from_txn_id", fromTxnID).
		Msg("Starting delta sync")

	stream, err := s.client.StreamChanges(ctx, &marmotgrpc.StreamRequest{
		FromTxnId:        fromTxnID,
		RequestingNodeId: s.nodeID,
		Database:         dbName,
	})
	if err != nil {
		return fmt.Errorf("failed to start delta stream: %w", err)
	}

	count := 0
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("delta stream error: %w", err)
		}

		if event.TxnId <= fromTxnID {
			continue
		}

		if err := s.applyChangeEvent(ctx, event); err != nil {
			return fmt.Errorf("failed to apply delta event %d: %w", event.TxnId, err)
		}

		count++
	}

	log.Info().
		Str("database", dbName).
		Int("events_applied", count).
		Msg("Delta sync completed")

	return nil
}

// downloadSnapshot downloads full snapshot from master
func (s *StreamClient) downloadSnapshot(ctx context.Context) error {
	log.Info().Msg("Downloading full snapshot from master")

	// Get snapshot info
	snapshotInfo, err := s.client.GetSnapshotInfo(ctx, &marmotgrpc.SnapshotInfoRequest{
		RequestingNodeId: s.nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to get snapshot info: %w", err)
	}

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Int64("size_bytes", snapshotInfo.SnapshotSizeBytes).
		Int32("total_chunks", snapshotInfo.TotalChunks).
		Int("databases", len(snapshotInfo.Databases)).
		Msg("Received snapshot info")

	// Stream and apply snapshot
	if err := s.applySnapshot(ctx, snapshotInfo); err != nil {
		return err
	}

	// Update lastTxnID for all databases in snapshot to prevent re-downloading
	s.mu.Lock()
	for _, dbInfo := range snapshotInfo.Databases {
		s.lastTxnID[dbInfo.Name] = snapshotInfo.SnapshotTxnId
		log.Debug().
			Str("database", dbInfo.Name).
			Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
			Msg("Updated lastTxnID after snapshot")
	}
	s.mu.Unlock()

	return nil
}

// downloadSnapshotForDatabase downloads snapshot for a specific database
func (s *StreamClient) downloadSnapshotForDatabase(ctx context.Context, dbName string) error {
	log.Info().Str("database", dbName).Msg("Downloading snapshot for database")

	// For now, download full snapshot
	// In production, could optimize to download single database
	return s.downloadSnapshot(ctx)
}

// applySnapshot applies a snapshot from the master.
// Uses the unified snapshot.Restorer for atomic download and apply.
// The Restorer handles:
// 1. Download to temp directory
// 2. Verify integrity (SHA256)
// 3. Acquire locks and swap files
// 4. Reopen connections
func (s *StreamClient) applySnapshot(ctx context.Context, snapshotInfo *marmotgrpc.SnapshotInfoResponse) error {
	log.Info().Msg("Applying snapshot using unified restorer")

	// Stream snapshot from master
	stream, err := s.client.StreamSnapshot(ctx, &marmotgrpc.SnapshotRequest{
		RequestingNodeId: s.nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to start snapshot stream: %w", err)
	}

	// Convert gRPC DatabaseFileInfo to snapshot.DatabaseFileInfo
	// Filter out system database - replicas maintain their own independent system DB
	files := make([]snapshot.DatabaseFileInfo, 0, len(snapshotInfo.Databases))
	for _, dbInfo := range snapshotInfo.Databases {
		if dbInfo.Name == db.SystemDatabaseName {
			continue
		}
		files = append(files, snapshot.DatabaseFileInfo{
			Name:           dbInfo.Name,
			Filename:       dbInfo.Filename,
			SizeBytes:      dbInfo.SizeBytes,
			SHA256Checksum: dbInfo.Sha256Checksum,
		})
	}

	// Create adapter for gRPC stream
	adapter := &replicaSnapshotStreamAdapter{stream: stream}

	// Wrap dbManager as ConnectionManager for the Restorer
	connMgr := &dbManagerConnectionAdapter{dbManager: s.dbManager}

	// Acquire snapshot lock before restore (to block change event processing)
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	// Use snapshot.Restorer for atomic download and apply
	restorer := snapshot.NewRestorer(cfg.Config.DataDir, connMgr)

	if err := restorer.RestoreFromStream(adapter, files); err != nil {
		return fmt.Errorf("snapshot restore failed: %w", err)
	}

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Msg("Snapshot applied successfully via unified restorer")

	return nil
}

// replicaSnapshotStreamAdapter adapts MarmotService_StreamSnapshotClient to snapshot.ChunkReceiver
type replicaSnapshotStreamAdapter struct {
	stream marmotgrpc.MarmotService_StreamSnapshotClient
}

func (a *replicaSnapshotStreamAdapter) Recv() (*snapshot.Chunk, error) {
	chunk, err := a.stream.Recv()
	if err != nil {
		return nil, err // Passes through io.EOF
	}

	return &snapshot.Chunk{
		Filename:      chunk.GetFilename(),
		ChunkIndex:    chunk.GetChunkIndex(),
		TotalChunks:   chunk.GetTotalChunks(),
		Data:          chunk.GetData(),
		MD5Checksum:   chunk.GetChecksum(),
		IsLastForFile: chunk.GetIsLastForFile(),
	}, nil
}

// dbManagerConnectionAdapter adapts db.DatabaseManager to snapshot.ConnectionManager
type dbManagerConnectionAdapter struct {
	dbManager *db.DatabaseManager
}

func (a *dbManagerConnectionAdapter) CloseDatabaseConnections(name string) error {
	return a.dbManager.CloseDatabaseConnections(name)
}

func (a *dbManagerConnectionAdapter) OpenDatabaseConnections(name string) error {
	return a.dbManager.OpenDatabaseConnections(name)
}

// applyChangeEvent applies a single change event
// Acquires read lock on snapshotMu to prevent concurrent execution with snapshot apply
func (s *StreamClient) applyChangeEvent(ctx context.Context, event *marmotgrpc.ChangeEvent) error {
	// Block if snapshot is in progress
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()

	database := event.Database
	if database == "" {
		return fmt.Errorf("change event %d missing database name", event.TxnId)
	}

	// Check first statement for database-level operations (these are always single-statement events)
	if len(event.Statements) > 0 {
		stmt := event.Statements[0]
		switch stmt.Type {
		case pb.StatementType_CREATE_DATABASE:
			return s.applyCreateDatabase(stmt, database)
		case pb.StatementType_DROP_DATABASE:
			return s.applyDropDatabase(stmt, database)
		}
	}

	mdb, err := s.dbManager.GetDatabase(database)
	if err != nil {
		return fmt.Errorf("database %s not found: %w", database, err)
	}

	// Check if connections are available (they may be closed during snapshot)
	sqlDB := mdb.GetDB()
	if sqlDB == nil {
		return fmt.Errorf("database %s connections are closed", database)
	}

	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, stmt := range event.Statements {
		if err := s.applyStatement(ctx, tx, mdb, stmt); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// applyCreateDatabase handles CREATE DATABASE replication
func (s *StreamClient) applyCreateDatabase(stmt *marmotgrpc.Statement, fallbackDB string) error {
	dbName := stmt.Database
	if dbName == "" {
		dbName = fallbackDB
	}
	err := s.dbManager.CreateDatabase(dbName)
	if err == nil {
		log.Info().Str("database", dbName).Msg("Created database via replication")
		return nil
	}
	// Idempotent: database may already exist from previous replay
	if strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return fmt.Errorf("failed to create database %s: %w", dbName, err)
}

// applyDropDatabase handles DROP DATABASE replication
func (s *StreamClient) applyDropDatabase(stmt *marmotgrpc.Statement, fallbackDB string) error {
	dbName := stmt.Database
	if dbName == "" {
		dbName = fallbackDB
	}
	err := s.dbManager.DropDatabase(dbName)
	if err == nil {
		log.Info().Str("database", dbName).Msg("Dropped database via replication")
		return nil
	}
	// Idempotent: database may not exist from previous replay
	if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") {
		return nil
	}
	return fmt.Errorf("failed to drop database %s: %w", dbName, err)
}

// applyStatement applies a single statement within a transaction
func (s *StreamClient) applyStatement(ctx context.Context, tx *sql.Tx, mdb *db.ReplicatedDatabase, stmt *marmotgrpc.Statement) error {
	// CDC path: row-level changes
	if rowChange := stmt.GetRowChange(); rowChange != nil && (len(rowChange.NewValues) > 0 || len(rowChange.OldValues) > 0) {
		return s.applyCDCStatement(tx, mdb, stmt)
	}

	// DDL path: schema changes with idempotency rewriting
	if ddlChange := stmt.GetDdlChange(); ddlChange != nil && ddlChange.Sql != "" {
		idempotentSQL := protocol.RewriteDDLForIdempotency(ddlChange.Sql)
		if _, err := tx.ExecContext(ctx, idempotentSQL); err != nil {
			return fmt.Errorf("DDL failed: %w", err)
		}
		return nil
	}

	// Fallback: raw SQL (legacy path, shouldn't reach here for proper CDC)
	if sqlStr := stmt.GetSQL(); sqlStr != "" {
		if _, err := tx.ExecContext(ctx, sqlStr); err != nil {
			return fmt.Errorf("SQL failed: %w", err)
		}
		return nil
	}

	log.Warn().Str("table", stmt.TableName).Int32("type", int32(stmt.Type)).Msg("Empty statement, skipping")
	return nil
}

// applyCDCStatement applies a CDC statement using unified CDC applier.
// Uses cached schema from mdb - does NOT query SQLite PRAGMA.
func (s *StreamClient) applyCDCStatement(tx *sql.Tx, mdb *db.ReplicatedDatabase, stmt *marmotgrpc.Statement) error {
	rowChange := stmt.GetRowChange()
	if rowChange == nil {
		return fmt.Errorf("no row change data")
	}

	// Create schema adapter for PK lookups using cached schema
	schemaAdapter := &streamClientSchemaAdapter{mdb: mdb}

	switch stmt.Type {
	case pb.StatementType_INSERT, pb.StatementType_REPLACE:
		return db.ApplyCDCInsert(tx, stmt.TableName, rowChange.NewValues)
	case pb.StatementType_UPDATE:
		return db.ApplyCDCUpdate(tx, schemaAdapter, stmt.TableName, rowChange.OldValues, rowChange.NewValues)
	case pb.StatementType_DELETE:
		return db.ApplyCDCDelete(tx, schemaAdapter, stmt.TableName, rowChange.OldValues)
	default:
		return fmt.Errorf("unsupported statement type: %v", stmt.Type)
	}
}

// streamClientSchemaAdapter adapts ReplicatedDatabase schema access to db.CDCSchemaProvider
type streamClientSchemaAdapter struct {
	mdb *db.ReplicatedDatabase
}

func (a *streamClientSchemaAdapter) GetPrimaryKeys(tableName string) ([]string, error) {
	schema, err := a.mdb.GetCachedTableSchema(tableName)
	if err != nil {
		return nil, fmt.Errorf("schema not cached for table %s: %w", tableName, err)
	}
	return schema.PrimaryKeys, nil
}

// getLocalMaxTxnIDs returns the max committed txn_id for each database
func (s *StreamClient) getLocalMaxTxnIDs() (map[string]uint64, error) {
	result := make(map[string]uint64)

	databases := s.dbManager.ListDatabases()
	for _, dbName := range databases {
		mdb, err := s.dbManager.GetDatabase(dbName)
		if err != nil {
			continue
		}

		// Use MetaStore to get max committed txn ID
		maxTxnID, err := mdb.GetMetaStore().GetMaxCommittedTxnID()
		if err == nil {
			result[dbName] = maxTxnID
		}
	}

	return result, nil
}

// getMasterMaxTxnIDs gets the max txn_id for each database from master
func (s *StreamClient) getMasterMaxTxnIDs(ctx context.Context) (map[string]uint64, error) {
	resp, err := s.client.GetReplicationState(ctx, &marmotgrpc.ReplicationStateRequest{
		RequestingNodeId: s.nodeID,
	})
	if err != nil {
		return nil, err
	}

	result := make(map[string]uint64)
	for _, state := range resp.States {
		result[state.DatabaseName] = state.CurrentMaxTxnId
	}

	return result, nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// startDiscoveryLoop periodically queries cluster nodes for membership updates
func (s *StreamClient) startDiscoveryLoop() {
	interval := time.Duration(cfg.Config.Replica.DiscoveryIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.discoveryCtx.Done():
			return
		case <-ticker.C:
			s.discoverClusterNodes()
		}
	}
}

// discoverClusterNodes queries a subset of nodes for cluster membership
func (s *StreamClient) discoverClusterNodes() {
	s.clusterMu.RLock()
	addrs := make([]string, 0, len(s.clusterNodes))
	for _, node := range s.clusterNodes {
		if node.Status != marmotgrpc.NodeStatus_DEAD &&
			node.Status != marmotgrpc.NodeStatus_REMOVED {
			addrs = append(addrs, node.Address)
		}
	}
	s.clusterMu.RUnlock()

	// Query subset of nodes (max 3) to avoid flooding
	if len(addrs) > 3 {
		rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })
		addrs = addrs[:3]
	}

	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(s.discoveryCtx, 5*time.Second)
		nodes, err := s.queryClusterNodes(ctx, addr)
		cancel()

		if err != nil {
			log.Debug().Err(err).Str("addr", addr).Msg("Discovery query failed")
			continue
		}

		s.mergeClusterView(nodes)
	}
}

// queryClusterNodes queries a single node for cluster membership
func (s *StreamClient) queryClusterNodes(ctx context.Context, addr string) ([]*marmotgrpc.NodeState, error) {
	// Create temporary connection for discovery
	conn, err := s.dialWithPSK(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := marmotgrpc.NewMarmotServiceClient(conn)
	resp, err := client.GetClusterNodes(ctx, &marmotgrpc.GetClusterNodesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

// dialWithPSK creates a gRPC connection with PSK authentication
func (s *StreamClient) dialWithPSK(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	replicaSecret := cfg.GetReplicaSecret()
	if replicaSecret == "" {
		return nil, fmt.Errorf("replica.secret is required for connecting to cluster")
	}

	keepaliveTime := time.Duration(cfg.Config.GRPCClient.KeepaliveTimeSeconds) * time.Second
	keepaliveTimeout := time.Duration(cfg.Config.GRPCClient.KeepaliveTimeoutSeconds) * time.Second

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
		grpc.WithChainUnaryInterceptor(marmotgrpc.UnaryClientInterceptorWithSecret(replicaSecret)),
		grpc.WithChainStreamInterceptor(marmotgrpc.StreamClientInterceptorWithSecret(replicaSecret)),
	)
	return conn, err
}

// mergeClusterView merges discovered nodes into local cluster view
func (s *StreamClient) mergeClusterView(nodes []*marmotgrpc.NodeState) {
	s.clusterMu.Lock()
	defer s.clusterMu.Unlock()

	for _, node := range nodes {
		existing, ok := s.clusterNodes[node.NodeId]
		if !ok || node.Incarnation > existing.Incarnation {
			s.clusterNodes[node.NodeId] = node

			// Remove temp entry if we now have real node ID
			for id, n := range s.clusterNodes {
				if n.Address == node.Address && id != node.NodeId {
					delete(s.clusterNodes, id)
					break
				}
			}
		}
	}
}

// selectAliveNode selects a random alive node (excluding current node)
func (s *StreamClient) selectAliveNode() (string, uint64, error) {
	s.clusterMu.RLock()
	defer s.clusterMu.RUnlock()

	var candidates []*marmotgrpc.NodeState
	for _, node := range s.clusterNodes {
		if node.Status == marmotgrpc.NodeStatus_ALIVE && node.Address != s.currentAddr {
			candidates = append(candidates, node)
		}
	}

	if len(candidates) == 0 {
		return "", 0, fmt.Errorf("no alive nodes available")
	}

	// Random selection for load distribution
	selected := candidates[rand.Intn(len(candidates))]
	return selected.Address, selected.NodeId, nil
}

// failover attempts to connect to a different alive node
func (s *StreamClient) failover() error {
	deadline := time.Now().Add(time.Duration(cfg.Config.Replica.FailoverTimeoutSec) * time.Second)

	for time.Now().Before(deadline) {
		// Refresh cluster view
		s.discoverClusterNodes()

		addr, nodeID, err := s.selectAliveNode()
		if err != nil {
			log.Debug().Err(err).Msg("No alive nodes, retrying...")
			time.Sleep(time.Second)
			continue
		}

		log.Info().
			Str("from", s.currentAddr).
			Str("to", addr).
			Uint64("node_id", nodeID).
			Msg("Attempting failover")

		s.currentAddr = addr
		if err := s.connect(s.ctx); err != nil {
			// Mark as potentially unhealthy
			s.clusterMu.Lock()
			if node, ok := s.clusterNodes[nodeID]; ok {
				node.Status = marmotgrpc.NodeStatus_SUSPECT
			}
			s.clusterMu.Unlock()
			continue
		}

		s.currentNodeID = nodeID
		return nil
	}

	return fmt.Errorf("failover timeout after %ds", cfg.Config.Replica.FailoverTimeoutSec)
}

// startDatabaseDiscoveryLoop periodically polls for new databases
func (s *StreamClient) startDatabaseDiscoveryLoop(ctx context.Context) {
	interval := time.Duration(cfg.Config.Replica.DatabaseDiscoveryIntervalSec) * time.Second
	if interval == 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info().Dur("interval", interval).Msg("Starting database discovery loop")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Database discovery loop stopped")
			return
		case <-ticker.C:
			s.discoverNewDatabases(ctx)
		}
	}
}

// discoverNewDatabases polls GetLatestTxnIDs and requests snapshots for new databases
func (s *StreamClient) discoverNewDatabases(ctx context.Context) {
	if s.client == nil {
		return
	}

	// Get latest database info from primary
	resp, err := s.client.GetLatestTxnIDs(ctx, &marmotgrpc.LatestTxnIDsRequest{
		RequestingNodeId: s.nodeID,
	})
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get latest txn IDs during database discovery")
		return
	}

	// Get configured database filter
	replicateDatabases := cfg.Config.Replica.ReplicateDatabases

	for _, dbInfo := range resp.DatabaseInfo {
		dbName := dbInfo.Name

		// Always skip system database
		if dbName == "__marmot_system" {
			continue
		}

		// Apply database filter if configured
		if !s.shouldReplicateDatabase(dbName, replicateDatabases) {
			continue
		}

		// Check if database is new (not yet synced)
		s.mu.RLock()
		_, exists := s.lastTxnID[dbName]
		s.mu.RUnlock()

		if !exists {
			// New database detected - mark it for tracking
			// The actual snapshot download will happen during normal streaming
			log.Info().
				Str("database", dbName).
				Uint64("max_txn_id", dbInfo.MaxTxnId).
				Msg("Discovered new database, will sync via streaming")

			s.mu.Lock()
			s.lastTxnID[dbName] = 0 // Start from beginning
			s.mu.Unlock()
		}
	}
}

// shouldReplicateDatabase checks if a database should be replicated based on filter
func (s *StreamClient) shouldReplicateDatabase(dbName string, filter []string) bool {
	// Empty filter means replicate all databases
	if len(filter) == 0 {
		return true
	}

	// Check for exact match or glob pattern
	for _, pattern := range filter {
		// Exact match
		if pattern == dbName {
			return true
		}

		// Glob pattern match
		matched, err := filepath.Match(pattern, dbName)
		if err != nil {
			log.Warn().
				Err(err).
				Str("pattern", pattern).
				Str("database", dbName).
				Msg("Invalid glob pattern in replicate_databases")
			continue
		}
		if matched {
			return true
		}
	}

	return false
}

// GetClient returns the gRPC client for forwarding queries.
// Returns nil if not connected.
func (s *StreamClient) GetClient() marmotgrpc.MarmotServiceClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.client
}

// GetNodeID returns the local node ID
func (s *StreamClient) GetNodeID() uint64 {
	return s.nodeID
}

// GetLastTxnID returns the last applied transaction ID for a database.
// Used for read-your-writes consistency checking.
func (s *StreamClient) GetLastTxnID(database string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastTxnID[database]
}

// getOrCreateWaitQueue returns the wait queue for a database, creating if needed.
func (s *StreamClient) getOrCreateWaitQueue(database string) *TxnWaitQueue {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.txnWaitQueues[database]; ok {
		return q
	}
	q := NewTxnWaitQueue()
	s.txnWaitQueues[database] = q
	return q
}

// WaitForTxn blocks until the given txnID is replicated or context is cancelled.
// Returns nil if txnID is already replicated or successfully waited for.
func (s *StreamClient) WaitForTxn(ctx context.Context, database string, txnID uint64) error {
	// Fast path: already replicated
	s.mu.RLock()
	if s.lastTxnID[database] >= txnID {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	q := s.getOrCreateWaitQueue(database)
	return q.Wait(ctx, txnID)
}
