package replica

import (
	"context"
	"database/sql"
	"fmt"
	"io"
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
	masterAddr string
	nodeID     uint64
	dbManager  *db.DatabaseManager
	clock      *hlc.Clock
	replica    *Replica

	conn   *grpc.ClientConn
	client marmotgrpc.MarmotServiceClient

	lastTxnID map[string]uint64 // per-database last applied txn_id
	mu        sync.RWMutex

	// snapshotInProgress prevents concurrent snapshot apply and change events
	snapshotMu sync.RWMutex

	reconnectInterval time.Duration
	maxBackoff        time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewStreamClient creates a new stream client
func NewStreamClient(masterAddr string, nodeID uint64, dbManager *db.DatabaseManager, clock *hlc.Clock, replica *Replica) *StreamClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &StreamClient{
		masterAddr:        masterAddr,
		nodeID:            nodeID,
		dbManager:         dbManager,
		clock:             clock,
		replica:           replica,
		lastTxnID:         make(map[string]uint64),
		reconnectInterval: time.Duration(cfg.Config.Replica.ReconnectIntervalSec) * time.Second,
		maxBackoff:        time.Duration(cfg.Config.Replica.ReconnectMaxBackoffSec) * time.Second,
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Bootstrap performs initial sync from master
func (s *StreamClient) Bootstrap(ctx context.Context) error {
	log.Info().Str("master", s.masterAddr).Msg("Connecting to master for bootstrap")

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

	// Determine bootstrap strategy
	needsSnapshot := false
	var deltaDatabases []string

	if len(localTxnIDs) == 0 {
		// No local data - need full snapshot
		needsSnapshot = true
		log.Info().Msg("No local data - full snapshot required")
	} else {
		// Check each database
		for dbName, masterTxnID := range masterTxnIDs {
			localTxnID := localTxnIDs[dbName]
			gap := masterTxnID - localTxnID

			if gap == 0 {
				log.Info().Str("database", dbName).Msg("Database is up to date")
				continue
			}

			if gap > uint64(cfg.Config.Replication.DeltaSyncThresholdTxns) {
				// Large gap - need snapshot for this database
				needsSnapshot = true
				log.Info().
					Str("database", dbName).
					Uint64("gap", gap).
					Msg("Large gap detected - snapshot required")
				break
			}

			deltaDatabases = append(deltaDatabases, dbName)
		}
	}

	if needsSnapshot {
		if err := s.downloadSnapshot(ctx); err != nil {
			return fmt.Errorf("snapshot download failed: %w", err)
		}
		// downloadSnapshot already sets s.lastTxnID with snapshot txn IDs
	} else if len(deltaDatabases) > 0 {
		for _, dbName := range deltaDatabases {
			if err := s.deltaSync(ctx, dbName, localTxnIDs[dbName]); err != nil {
				log.Warn().Err(err).Str("database", dbName).Msg("Delta sync failed, will retry during streaming")
			}
		}
		// Update lastTxnID from MetaStore after delta sync
		s.mu.Lock()
		s.lastTxnID, _ = s.getLocalMaxTxnIDs()
		s.mu.Unlock()
	} else {
		// No sync needed, just set lastTxnID from current state
		s.mu.Lock()
		s.lastTxnID = localTxnIDs
		s.mu.Unlock()
	}

	s.mu.RLock()
	log.Info().Interface("last_txn_ids", s.lastTxnID).Msg("Bootstrap completed")
	s.mu.RUnlock()
	return nil
}

// Start begins the streaming loop (runs until Stop is called)
func (s *StreamClient) Start(ctx context.Context) {
	s.wg.Add(1)
	defer s.wg.Done()

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

		// Reset backoff on successful connection
		backoff = s.reconnectInterval
		s.replica.SetConnected(true)
		s.replica.SetState(StateStreaming)

		log.Info().Str("master", s.masterAddr).Msg("Connected to master, starting change stream")

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

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, s.masterAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
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
		return fmt.Errorf("failed to dial master: %w", err)
	}

	s.conn = conn
	s.client = marmotgrpc.NewMarmotServiceClient(conn)

	log.Info().Str("master", s.masterAddr).Msg("Connected to master with PSK authentication")
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
	files := make([]snapshot.DatabaseFileInfo, 0, len(snapshotInfo.Databases))
	for _, dbInfo := range snapshotInfo.Databases {
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
