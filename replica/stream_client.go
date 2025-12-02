package replica

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/maxpert/marmot/hlc"
	"github.com/vmihailenco/msgpack/v5"

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

// sanitizeSnapshotFilename validates and sanitizes a filename from snapshot stream.
// Returns error if filename contains path traversal or absolute paths.
// This prevents malicious masters from writing files outside the data directory.
func sanitizeSnapshotFilename(filename string) (string, error) {
	if filename == "" {
		return "", fmt.Errorf("empty filename")
	}

	// Reject absolute paths
	if filepath.IsAbs(filename) {
		return "", fmt.Errorf("absolute path not allowed: %s", filename)
	}

	// Clean the path and check for traversal
	cleaned := filepath.Clean(filename)

	// Reject if cleaned path starts with ..
	if strings.HasPrefix(cleaned, "..") {
		return "", fmt.Errorf("path traversal not allowed: %s", filename)
	}

	// Reject if path contains .. anywhere after cleaning
	for _, part := range strings.Split(cleaned, string(filepath.Separator)) {
		if part == ".." {
			return "", fmt.Errorf("path traversal not allowed: %s", filename)
		}
	}

	// Only allow specific patterns for snapshot files
	if !isValidSnapshotPath(cleaned) {
		return "", fmt.Errorf("invalid snapshot filename pattern: %s", filename)
	}

	return cleaned, nil
}

// isValidSnapshotPath checks if the path matches expected snapshot file patterns
func isValidSnapshotPath(path string) bool {
	// System database at root
	if path == "__marmot_system.db" {
		return true
	}
	// User databases in databases/ subdirectory
	if strings.HasPrefix(path, "databases"+string(filepath.Separator)) && strings.HasSuffix(path, ".db") {
		// Ensure no additional directory traversal within databases/
		relPath := strings.TrimPrefix(path, "databases"+string(filepath.Separator))
		if !strings.Contains(relPath, string(filepath.Separator)) {
			return true
		}
	}
	return false
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
		// Download full snapshot
		if err := s.downloadSnapshot(ctx); err != nil {
			return fmt.Errorf("snapshot download failed: %w", err)
		}

		// Get updated local txn IDs after snapshot
		localTxnIDs, _ = s.getLocalMaxTxnIDs()
	} else if len(deltaDatabases) > 0 {
		// Perform delta sync for databases that are slightly behind
		for _, dbName := range deltaDatabases {
			fromTxnID := localTxnIDs[dbName]
			if err := s.deltaSync(ctx, dbName, fromTxnID); err != nil {
				log.Warn().Err(err).Str("database", dbName).Msg("Delta sync failed, will try again during streaming")
			}
		}

		// Update local txn IDs
		localTxnIDs, _ = s.getLocalMaxTxnIDs()
	}

	// Store last txn IDs
	s.mu.Lock()
	s.lastTxnID = localTxnIDs
	s.mu.Unlock()

	log.Info().Interface("last_txn_ids", localTxnIDs).Msg("Bootstrap completed")
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
	return s.applySnapshot(ctx, snapshotInfo)
}

// downloadSnapshotForDatabase downloads snapshot for a specific database
func (s *StreamClient) downloadSnapshotForDatabase(ctx context.Context, dbName string) error {
	log.Info().Str("database", dbName).Msg("Downloading snapshot for database")

	// For now, download full snapshot
	// In production, could optimize to download single database
	return s.downloadSnapshot(ctx)
}

// applySnapshot applies a snapshot from the master
func (s *StreamClient) applySnapshot(ctx context.Context, snapshotInfo *marmotgrpc.SnapshotInfoResponse) error {
	// Ensure databases directory exists
	dbsDir := filepath.Join(cfg.Config.DataDir, "databases")
	if err := os.MkdirAll(dbsDir, 0755); err != nil {
		return fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Stream snapshot chunks
	stream, err := s.client.StreamSnapshot(ctx, &marmotgrpc.SnapshotRequest{
		RequestingNodeId: s.nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to start snapshot stream: %w", err)
	}

	// Track open files
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	totalChunks := int32(0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("snapshot stream error: %w", err)
		}

		// Verify checksum
		actualChecksum := fmt.Sprintf("%x", md5.Sum(chunk.Data))
		if actualChecksum != chunk.Checksum {
			return fmt.Errorf("checksum mismatch for chunk %d of %s", chunk.ChunkIndex, chunk.Filename)
		}

		// Sanitize filename to prevent path traversal attacks
		sanitizedFilename, err := sanitizeSnapshotFilename(chunk.Filename)
		if err != nil {
			return fmt.Errorf("invalid snapshot filename: %w", err)
		}

		// Get or create file
		file, exists := openFiles[sanitizedFilename]
		if !exists {
			// Determine target path
			targetPath := filepath.Join(dbsDir, sanitizedFilename)

			// Backup existing file if any
			if _, err := os.Stat(targetPath); err == nil {
				backupPath := targetPath + ".backup"
				os.Rename(targetPath, backupPath)
			}

			// Create new file
			file, err = os.Create(targetPath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", targetPath, err)
			}
			openFiles[sanitizedFilename] = file
		}

		// Write chunk
		if _, err := file.Write(chunk.Data); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}

		totalChunks++

		// Close file if last chunk for this file
		if chunk.IsLastForFile {
			file.Close()
			delete(openFiles, sanitizedFilename)
			log.Info().Str("file", sanitizedFilename).Msg("Completed receiving file")
		}
	}

	log.Info().
		Int32("total_chunks", totalChunks).
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Msg("Snapshot applied successfully")

	return nil
}

// applyChangeEvent applies a single change event
func (s *StreamClient) applyChangeEvent(ctx context.Context, event *marmotgrpc.ChangeEvent) error {
	database := event.Database
	if database == "" {
		database = "marmot"
	}

	mdb, err := s.dbManager.GetDatabase(database)
	if err != nil {
		return fmt.Errorf("database %s not found: %w", database, err)
	}

	// Execute in transaction
	tx, err := mdb.GetDB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, stmt := range event.Statements {
		// Check for CDC data
		if rowChange := stmt.GetRowChange(); rowChange != nil && (len(rowChange.NewValues) > 0 || len(rowChange.OldValues) > 0) {
			if err := s.applyCDCStatement(tx, stmt); err != nil {
				return fmt.Errorf("failed to apply CDC statement: %w", err)
			}
			continue
		}

		// Check for DDL
		if ddlChange := stmt.GetDdlChange(); ddlChange != nil && ddlChange.Sql != "" {
			if _, err := tx.ExecContext(ctx, ddlChange.Sql); err != nil {
				return fmt.Errorf("failed to execute DDL: %w", err)
			}
			continue
		}

		// Fallback to SQL (shouldn't reach here for proper CDC)
		sqlStr := stmt.GetSQL()
		if sqlStr == "" {
			log.Warn().
				Str("table", stmt.TableName).
				Int32("type", int32(stmt.Type)).
				Msg("Statement has no SQL and no CDC data, skipping")
			continue
		}

		if _, err := tx.ExecContext(ctx, sqlStr); err != nil {
			return fmt.Errorf("failed to execute SQL: %w", err)
		}
	}

	return tx.Commit()
}

// applyCDCStatement applies a CDC statement
func (s *StreamClient) applyCDCStatement(tx *sql.Tx, stmt *marmotgrpc.Statement) error {
	rowChange := stmt.GetRowChange()
	if rowChange == nil {
		return fmt.Errorf("no row change data")
	}

	switch stmt.Type {
	case marmotgrpc.StatementType_INSERT, marmotgrpc.StatementType_REPLACE:
		return s.applyCDCInsert(tx, stmt.TableName, rowChange.NewValues)
	case marmotgrpc.StatementType_UPDATE:
		return s.applyCDCInsert(tx, stmt.TableName, rowChange.NewValues) // Use INSERT OR REPLACE
	case marmotgrpc.StatementType_DELETE:
		return s.applyCDCDelete(tx, stmt.TableName, rowChange.RowKey, rowChange.OldValues)
	default:
		return fmt.Errorf("unsupported statement type: %v", stmt.Type)
	}
}

// applyCDCInsert performs INSERT OR REPLACE
func (s *StreamClient) applyCDCInsert(tx *sql.Tx, tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to insert")
	}

	columns := make([]string, 0, len(newValues))
	placeholders := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues))

	for col := range newValues {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")

		var value interface{}
		if err := msgpack.Unmarshal(newValues[col], &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(sqlStmt, values...)
	return err
}

// applyCDCDelete performs DELETE
func (s *StreamClient) applyCDCDelete(tx *sql.Tx, tableName string, rowKey string, oldValues map[string][]byte) error {
	if len(oldValues) == 0 && rowKey == "" {
		return fmt.Errorf("no row key or old values for delete")
	}

	if len(oldValues) > 0 {
		// Use all old values as WHERE clause
		whereClauses := make([]string, 0, len(oldValues))
		values := make([]interface{}, 0, len(oldValues))

		for col, valBytes := range oldValues {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
			var value interface{}
			if err := msgpack.Unmarshal(valBytes, &value); err != nil {
				return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
			}
			values = append(values, value)
		}

		sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
			tableName,
			strings.Join(whereClauses, " AND "))

		_, err := tx.Exec(sqlStmt, values...)
		return err
	}

	// Fallback: use row key
	_, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName), rowKey)
	return err
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
