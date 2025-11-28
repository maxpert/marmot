package grpc

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CatchUpStrategy determines how a node should catch up with the cluster
type CatchUpStrategy int

const (
	// NO_CATCHUP - Node is up to date, no catch-up needed
	NO_CATCHUP CatchUpStrategy = iota
	// DELTA_SYNC - Node is slightly behind, use transaction log replay
	DELTA_SYNC
	// FULL_SNAPSHOT - Node is far behind or has no data, need full snapshot
	FULL_SNAPSHOT
)

// DeltaSyncThreshold - If behind by more than this many transactions, use snapshot
const DeltaSyncThreshold = 1000

// sanitizeSnapshotFilename validates and sanitizes a filename from snapshot stream.
// Returns error if filename contains path traversal or absolute paths.
// This prevents malicious servers from writing files outside the data directory.
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

// calculateFileSHA256 computes SHA256 checksum of a file
func calculateFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// verifySnapshotIntegrity verifies downloaded snapshot files against manifest
func verifySnapshotIntegrity(dataDir string, manifest []*DatabaseFileInfo) error {
	for _, expected := range manifest {
		// Skip verification if no checksum provided (backwards compatibility)
		if expected.Sha256Checksum == "" {
			continue
		}

		filePath := filepath.Join(dataDir, expected.Filename)

		// Verify file exists
		info, err := os.Stat(filePath)
		if err != nil {
			return fmt.Errorf("missing file %s: %w", expected.Filename, err)
		}

		// Verify size
		if info.Size() != expected.SizeBytes {
			return fmt.Errorf("size mismatch for %s: expected %d, got %d",
				expected.Filename, expected.SizeBytes, info.Size())
		}

		// Verify SHA256
		actualHash, err := calculateFileSHA256(filePath)
		if err != nil {
			return fmt.Errorf("failed to hash %s: %w", expected.Filename, err)
		}
		if actualHash != expected.Sha256Checksum {
			return fmt.Errorf("SHA256 mismatch for %s: expected %s, got %s",
				expected.Filename, expected.Sha256Checksum, actualHash)
		}

		log.Debug().
			Str("file", expected.Filename).
			Str("sha256", actualHash[:16]+"...").
			Msg("Verified snapshot file integrity")
	}
	return nil
}

// CatchUpClient handles the client-side of node catch-up
type CatchUpClient struct {
	nodeID    uint64
	dataDir   string
	registry  *NodeRegistry
	seedAddrs []string
}

// NewCatchUpClient creates a new catch-up client
func NewCatchUpClient(nodeID uint64, dataDir string, registry *NodeRegistry, seedAddrs []string) *CatchUpClient {
	return &CatchUpClient{
		nodeID:    nodeID,
		dataDir:   dataDir,
		registry:  registry,
		seedAddrs: seedAddrs,
	}
}

// CatchUpFromPeer downloads a snapshot of a specific database from a peer
// Used by anti-entropy to trigger snapshots for lagging databases
func (c *CatchUpClient) CatchUpFromPeer(ctx context.Context, peerNodeID uint64, peerAddr string, database string) error {
	log.Info().
		Uint64("peer_node", peerNodeID).
		Str("peer_addr", peerAddr).
		Str("database", database).
		Msg("Starting snapshot download for database from peer")

	// Connect to peer
	conn, err := grpc.DialContext(ctx, peerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)

	// Get snapshot info (will include all databases, we filter later)
	snapshotInfo, err := client.GetSnapshotInfo(ctx, &SnapshotInfoRequest{
		RequestingNodeId: c.nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to get snapshot info: %w", err)
	}

	log.Info().
		Str("database", database).
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Int64("size_bytes", snapshotInfo.SnapshotSizeBytes).
		Msg("Received snapshot info for database")

	// Apply snapshot for this database
	if err := c.applySnapshot(ctx, client, snapshotInfo); err != nil {
		return fmt.Errorf("failed to apply snapshot: %w", err)
	}

	log.Info().
		Str("database", database).
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Msg("Snapshot download completed for database")

	return nil
}

// CatchUp performs the full catch-up process
// Returns the snapshot txn_id after which normal replication can begin
func (c *CatchUpClient) CatchUp(ctx context.Context) (uint64, error) {
	// Mark ourselves as JOINING
	c.registry.MarkJoining(c.nodeID)

	log.Info().
		Uint64("node_id", c.nodeID).
		Msg("Starting catch-up process")

	// Find a seed node to catch up from
	seedAddr, err := c.findAvailableSeed(ctx)
	if err != nil {
		return 0, fmt.Errorf("no available seed node: %w", err)
	}

	log.Info().Str("seed", seedAddr).Msg("Catching up from seed node")

	// Connect to seed
	conn, err := grpc.DialContext(ctx, seedAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to seed: %w", err)
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)

	// Step 1: Get snapshot info
	snapshotInfo, err := client.GetSnapshotInfo(ctx, &SnapshotInfoRequest{
		RequestingNodeId: c.nodeID,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get snapshot info: %w", err)
	}

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Int64("size_bytes", snapshotInfo.SnapshotSizeBytes).
		Int32("total_chunks", snapshotInfo.TotalChunks).
		Int("databases", len(snapshotInfo.Databases)).
		Msg("Received snapshot info")

	// Step 2: Stream and apply snapshot
	if err := c.applySnapshot(ctx, client, snapshotInfo); err != nil {
		return 0, fmt.Errorf("failed to apply snapshot: %w", err)
	}

	// Step 3: Apply delta changes (transactions after snapshot)
	// Note: For full snapshot-based sync, this may not be needed immediately
	// The snapshot itself should be consistent

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Msg("Catch-up completed successfully - node stays JOINING until fully initialized")

	return snapshotInfo.SnapshotTxnId, nil
}

// findAvailableSeed finds an available seed node to catch up from
func (c *CatchUpClient) findAvailableSeed(ctx context.Context) (string, error) {
	// Try configured seed addresses first
	for _, addr := range c.seedAddrs {
		if c.checkNodeAvailable(ctx, addr) {
			return addr, nil
		}
	}

	// Try nodes from registry
	for _, node := range c.registry.GetAlive() {
		if node.NodeId == c.nodeID {
			continue // Skip self
		}
		if c.checkNodeAvailable(ctx, node.Address) {
			return node.Address, nil
		}
	}

	return "", fmt.Errorf("no available seed nodes")
}

// checkNodeAvailable checks if a node is available for catch-up
func (c *CatchUpClient) checkNodeAvailable(ctx context.Context, addr string) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	_, err = client.Ping(ctx, &PingRequest{SourceNodeId: c.nodeID})
	return err == nil
}

// applySnapshot downloads and applies a snapshot from the seed node
func (c *CatchUpClient) applySnapshot(ctx context.Context, client MarmotServiceClient, info *SnapshotInfoResponse) error {
	log.Info().Msg("Downloading snapshot")

	// Create data directory structure
	if err := os.MkdirAll(c.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(c.dataDir, "databases"), 0755); err != nil {
		return fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Stream snapshot
	stream, err := client.StreamSnapshot(ctx, &SnapshotRequest{
		RequestingNodeId: c.nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to start snapshot stream: %w", err)
	}

	// Track open files for each database
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	chunksReceived := 0
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk: %w", err)
		}

		// Verify checksum
		actualChecksum := fmt.Sprintf("%x", md5.Sum(chunk.Data))
		if actualChecksum != chunk.Checksum {
			return fmt.Errorf("checksum mismatch for %s chunk %d", chunk.Filename, chunk.ChunkIndex)
		}

		// Sanitize filename to prevent path traversal attacks
		sanitizedFilename, err := sanitizeSnapshotFilename(chunk.Filename)
		if err != nil {
			return fmt.Errorf("invalid snapshot filename: %w", err)
		}

		// Get or create file
		file, exists := openFiles[sanitizedFilename]
		if !exists {
			filePath := filepath.Join(c.dataDir, sanitizedFilename)

			// Remove existing file if present
			os.Remove(filePath)
			os.Remove(filePath + "-wal")
			os.Remove(filePath + "-shm")

			file, err = os.Create(filePath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", filePath, err)
			}
			openFiles[sanitizedFilename] = file
		}

		// Write chunk
		if _, err := file.Write(chunk.Data); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}

		chunksReceived++

		// Close file if this is the last chunk
		if chunk.IsLastForFile {
			file.Close()
			delete(openFiles, sanitizedFilename)
			log.Debug().Str("file", sanitizedFilename).Msg("Finished receiving file")
		}

		// Progress logging
		if chunksReceived%100 == 0 {
			log.Info().
				Int("chunks_received", chunksReceived).
				Int32("total_chunks", chunk.TotalChunks).
				Msg("Snapshot download progress")
		}
	}

	log.Info().
		Int("chunks_received", chunksReceived).
		Msg("Snapshot download completed")

	// Verify snapshot integrity using SHA256 checksums from manifest
	if err := verifySnapshotIntegrity(c.dataDir, info.Databases); err != nil {
		return fmt.Errorf("snapshot integrity verification failed: %w", err)
	}

	log.Info().Msg("Snapshot integrity verified")
	return nil
}

// NeedsCatchUp checks if this node needs to catch up (e.g., empty data directory)
// DEPRECATED: Use DetermineCatchUpStrategy instead
func (c *CatchUpClient) NeedsCatchUp() bool {
	// Check if system database exists
	systemDBPath := filepath.Join(c.dataDir, "__marmot_system.db")
	if _, err := os.Stat(systemDBPath); os.IsNotExist(err) {
		return true
	}

	// Check if default database exists
	defaultDBPath := filepath.Join(c.dataDir, "databases", "marmot.db")
	if _, err := os.Stat(defaultDBPath); os.IsNotExist(err) {
		return true
	}

	return false
}

// DatabaseTxnInfo holds transaction state for a database
type DatabaseTxnInfo struct {
	DatabaseName string
	MaxTxnID     uint64
}

// GetLocalMaxTxnID queries the local database files to get the max transaction ID per database
// This allows us to compare our state with peers to determine if we're behind
func (c *CatchUpClient) GetLocalMaxTxnID(ctx context.Context) (map[string]uint64, error) {
	result := make(map[string]uint64)

	// Check if system database exists
	systemDBPath := filepath.Join(c.dataDir, "__marmot_system.db")
	if _, err := os.Stat(systemDBPath); os.IsNotExist(err) {
		// No system database - we have no data
		return result, nil
	}

	// Open system database with WAL mode and busy timeout to avoid conflicts
	// Use config timeout (in seconds) converted to milliseconds
	busyTimeoutMS := cfg.Config.MVCC.LockWaitTimeoutSeconds * 1000
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&mode=ro", systemDBPath, busyTimeoutMS)
	systemDB, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open system database: %w", err)
	}
	defer systemDB.Close()

	// Query database registry
	rows, err := systemDB.Query("SELECT name, path FROM __marmot_databases")
	if err != nil {
		// Table doesn't exist yet - empty database
		return result, nil
	}
	defer rows.Close()

	databases := make(map[string]string)
	for rows.Next() {
		var name, path string
		if err := rows.Scan(&name, &path); err != nil {
			log.Warn().Err(err).Msg("Failed to scan database metadata")
			continue
		}
		databases[name] = path
	}

	// Query max txn_id from each database
	for dbName, dbPath := range databases {
		fullPath := filepath.Join(c.dataDir, dbPath)
		maxTxnID, err := c.getMaxTxnIDFromDB(fullPath)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to get max txn_id")
			continue
		}
		result[dbName] = maxTxnID
	}

	return result, nil
}

// getMaxTxnIDFromDB queries the maximum transaction ID from a database file
func (c *CatchUpClient) getMaxTxnIDFromDB(dbPath string) (uint64, error) {
	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return 0, nil
	}

	// Open database with WAL mode and busy timeout to avoid conflicts
	// Use config timeout (in seconds) converted to milliseconds
	busyTimeoutMS := cfg.Config.MVCC.LockWaitTimeoutSeconds * 1000
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&mode=ro", dbPath, busyTimeoutMS)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return 0, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	var maxTxnID uint64
	err = db.QueryRow(`
		SELECT COALESCE(MAX(txn_id), 0)
		FROM __marmot__txn_records
		WHERE status = 'COMMITTED'
	`).Scan(&maxTxnID)
	if err != nil {
		// Table might not exist yet
		return 0, nil
	}

	return maxTxnID, nil
}

// GetPeerMaxTxnIDs queries a peer node for its latest transaction IDs per database
func (c *CatchUpClient) GetPeerMaxTxnIDs(ctx context.Context, peerAddr string) (map[string]uint64, error) {
	// Connect to peer
	conn, err := grpc.DialContext(ctx, peerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %w", peerAddr, err)
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)

	// Query peer for latest txn IDs
	resp, err := client.GetLatestTxnIDs(ctx, &LatestTxnIDsRequest{
		RequestingNodeId: c.nodeID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get latest txn IDs from peer: %w", err)
	}

	return resp.DatabaseTxnIds, nil
}

// CatchUpDecision contains the strategy and sync information
type CatchUpDecision struct {
	Strategy       CatchUpStrategy
	PeerAddr       string
	DatabaseDeltas map[string]DeltaInfo // Per-database sync info
}

// DeltaInfo contains delta sync information for a database
type DeltaInfo struct {
	DatabaseName string
	LocalTxnID   uint64
	PeerTxnID    uint64
	TxnsBehind   uint64
}

// DetermineCatchUpStrategy determines the best catch-up strategy by comparing local vs cluster state
// This is the main entry point for catch-up detection
func (c *CatchUpClient) DetermineCatchUpStrategy(ctx context.Context) (*CatchUpDecision, error) {
	decision := &CatchUpDecision{
		Strategy:       NO_CATCHUP,
		DatabaseDeltas: make(map[string]DeltaInfo),
	}

	// Step 1: Get local transaction IDs
	localTxnIDs, err := c.GetLocalMaxTxnID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get local txn IDs: %w", err)
	}

	// Step 2: Find an available seed node
	seedAddr, err := c.findAvailableSeed(ctx)
	if err != nil {
		return nil, fmt.Errorf("no available seed node: %w", err)
	}
	decision.PeerAddr = seedAddr

	// Step 3: Get peer transaction IDs
	peerTxnIDs, err := c.GetPeerMaxTxnIDs(ctx, seedAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get peer txn IDs: %w", err)
	}

	// Step 4: Compare local vs peer state
	if len(localTxnIDs) == 0 && len(peerTxnIDs) > 0 {
		// We have no data, peer has data - need full snapshot
		decision.Strategy = FULL_SNAPSHOT
		log.Info().
			Str("peer", seedAddr).
			Msg("No local data found - full snapshot required")
		return decision, nil
	}

	if len(peerTxnIDs) == 0 {
		// Peer has no data - we're up to date (or we're the first node)
		decision.Strategy = NO_CATCHUP
		log.Info().Msg("Peer has no data - no catch-up needed")
		return decision, nil
	}

	// Step 5: Calculate deltas per database
	var totalTxnsBehind uint64
	var maxDeltaForAnyDB uint64

	// Check all databases that exist on peer
	for dbName, peerTxnID := range peerTxnIDs {
		localTxnID := localTxnIDs[dbName] // 0 if database doesn't exist locally

		if peerTxnID > localTxnID {
			delta := peerTxnID - localTxnID
			totalTxnsBehind += delta

			if delta > maxDeltaForAnyDB {
				maxDeltaForAnyDB = delta
			}

			decision.DatabaseDeltas[dbName] = DeltaInfo{
				DatabaseName: dbName,
				LocalTxnID:   localTxnID,
				PeerTxnID:    peerTxnID,
				TxnsBehind:   delta,
			}

			log.Debug().
				Str("database", dbName).
				Uint64("local_txn_id", localTxnID).
				Uint64("peer_txn_id", peerTxnID).
				Uint64("behind_by", delta).
				Msg("Database delta calculated")
		}
	}

	// Step 6: Determine strategy based on delta size
	if totalTxnsBehind == 0 {
		decision.Strategy = NO_CATCHUP
		log.Info().Msg("Node is up to date - no catch-up needed")
	} else if maxDeltaForAnyDB > DeltaSyncThreshold {
		// Any database is too far behind - use snapshot
		decision.Strategy = FULL_SNAPSHOT
		log.Info().
			Uint64("max_delta", maxDeltaForAnyDB).
			Uint64("threshold", DeltaSyncThreshold).
			Msg("Delta too large for any database - full snapshot required")
	} else {
		// Small delta - use incremental sync
		decision.Strategy = DELTA_SYNC
		log.Info().
			Uint64("total_txns_behind", totalTxnsBehind).
			Uint64("max_delta", maxDeltaForAnyDB).
			Int("databases_to_sync", len(decision.DatabaseDeltas)).
			Msg("Delta sync strategy selected")
	}

	return decision, nil
}

// PerformDeltaSync performs incremental catch-up using transaction logs
// This is called when the node is only slightly behind and can catch up via delta sync
func (c *CatchUpClient) PerformDeltaSync(ctx context.Context, decision *CatchUpDecision, deltaSyncClient *DeltaSyncClient) error {
	if deltaSyncClient == nil {
		return fmt.Errorf("delta sync client not provided")
	}

	log.Info().
		Int("databases_to_sync", len(decision.DatabaseDeltas)).
		Str("peer", decision.PeerAddr).
		Msg("Starting delta sync")

	// Mark ourselves as JOINING during sync
	c.registry.MarkJoining(c.nodeID)

	// Sync each database that's behind
	for dbName, deltaInfo := range decision.DatabaseDeltas {
		log.Info().
			Str("database", dbName).
			Uint64("from_txn_id", deltaInfo.LocalTxnID).
			Uint64("to_txn_id", deltaInfo.PeerTxnID).
			Uint64("txns_to_apply", deltaInfo.TxnsBehind).
			Msg("Starting database delta sync")

		// Use existing DeltaSyncClient to sync from peer
		result, err := deltaSyncClient.SyncFromPeer(
			ctx,
			0, // peerNodeID will be determined by address
			decision.PeerAddr,
			dbName,
			deltaInfo.LocalTxnID,
		)

		if err != nil {
			return fmt.Errorf("failed to sync database %s: %w", dbName, err)
		}

		log.Info().
			Str("database", dbName).
			Int("txns_applied", result.TxnsApplied).
			Uint64("final_txn_id", result.LastAppliedTxnID).
			Msg("Database delta sync completed")
	}

	log.Info().
		Int("databases_synced", len(decision.DatabaseDeltas)).
		Msg("Delta sync completed successfully")

	return nil
}
