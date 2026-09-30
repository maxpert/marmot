package grpc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/db/snapshot"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CatchUpStrategy determines how a node should catch up with the cluster
type CatchUpStrategy int

const (
	// NO_CATCHUP - Node is up to date, no catch-up needed
	NO_CATCHUP CatchUpStrategy = iota
	// DELTA_SYNC - Node has data: pull the seed's log for every database
	DELTA_SYNC
	// FULL_SNAPSHOT - Node has no data, need full snapshot
	FULL_SNAPSHOT
)

// CatchUpClient handles the client-side of node catch-up
type CatchUpClient struct {
	nodeID    uint64
	dataDir   string
	registry  *NodeRegistry
	seedAddrs []string

	// dbManager is set once the DatabaseManager exists (after the startup join
	// path completes). Anti-entropy snapshot repairs run at runtime, long after
	// NewDatabaseManager has opened the system MetaStore on this same dataDir -
	// so restoring schema versions must write through that live store instead
	// of opening a second handle on the same Pebble directory, which deadlocks
	// against Pebble's per-process exclusive lock. nil during the startup join
	// path, where no DatabaseManager exists yet and the MetaStore is opened
	// directly by path.
	dbManager atomic.Pointer[db.DatabaseManager]
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

// SetDatabaseManager wires the live DatabaseManager once it exists, so runtime
// snapshot restores (anti-entropy) write schema versions through the
// already-open system MetaStore rather than opening a second handle on the
// same Pebble directory. Must be called before anti-entropy starts invoking
// CatchUpFromPeer.
func (c *CatchUpClient) SetDatabaseManager(dbMgr *db.DatabaseManager) {
	c.dbManager.Store(dbMgr)
}

// CatchUpFromPeer downloads a snapshot of a specific database from a peer
// Used by anti-entropy to trigger snapshots for lagging databases
func (c *CatchUpClient) CatchUpFromPeer(ctx context.Context, peerNodeID uint64, peerAddr string, database string) error {
	log.Info().
		Uint64("peer_node", peerNodeID).
		Str("peer_addr", peerAddr).
		Str("database", database).
		Msg("Starting snapshot download for database from peer")

	// Connect to peer with common dial options (includes compression)
	conn, err := grpc.NewClient(peerAddr, createDialOptions()...)
	if err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)

	// Take the database out of service before the peer describes and takes
	// its snapshot and keep it out until its file is replaced, so no write is
	// ACKed into a file this restore then discards; the reattach opens
	// whatever file is in place, the peer's or, if the restore failed first,
	// the old one. A detach whose drain did not finish leaves the file in
	// place: a write that passed the gate first may still commit into it.
	dbMgr := c.dbManager.Load()
	if dbMgr == nil {
		return fmt.Errorf("catch-up of %s needs the running node's DatabaseManager", database)
	}
	var snapshotInfo *SnapshotInfoResponse
	var applyErr error
	switch detachErr := dbMgr.DetachDatabase(ctx, database); {
	case errors.Is(detachErr, db.ErrDrainIncomplete):
		applyErr = fmt.Errorf("restore of %s aborted before replacing its file: %w", database, detachErr)
	case detachErr != nil:
		return fmt.Errorf("failed to detach %s for restore: %w", database, detachErr)
	default:
		snapshotInfo, applyErr = c.downloadDatabaseSnapshot(ctx, client, database)
	}
	if err := dbMgr.AttachDatabase(database); err != nil {
		return errors.Join(applyErr, fmt.Errorf("failed to reattach %s after restore: %w", database, err))
	}
	if applyErr != nil {
		return fmt.Errorf("failed to apply snapshot: %w", applyErr)
	}

	log.Info().
		Str("database", database).
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Msg("Snapshot download completed for database")

	return nil
}

// downloadDatabaseSnapshot gets the peer's snapshot info for database alone
// and streams and installs that database's file. Both requests name the
// database, so the peer refuses them only while that database is out of
// service there, not while another of its databases awaits a restore.
func (c *CatchUpClient) downloadDatabaseSnapshot(ctx context.Context, client MarmotServiceClient, database string) (*SnapshotInfoResponse, error) {
	snapshotInfo, err := client.GetSnapshotInfo(ctx, &SnapshotInfoRequest{
		RequestingNodeId: c.nodeID,
		Database:         database,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot info: %w", err)
	}

	log.Info().
		Str("database", database).
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Int64("size_bytes", snapshotInfo.SnapshotSizeBytes).
		Msg("Received snapshot info for database")

	if err := c.applySnapshot(ctx, client, snapshotInfo, database); err != nil {
		return nil, err
	}
	return snapshotInfo, nil
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
	_, seedAddr, err := c.findAvailableSeed(ctx)
	if err != nil {
		return 0, fmt.Errorf("no available seed node: %w", err)
	}

	log.Info().Str("seed", seedAddr).Msg("Catching up from seed node")

	// Connect to seed with common dial options (includes compression)
	conn, err := grpc.NewClient(seedAddr, createDialOptions()...)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to seed: %w", err)
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)

	// Steps 1 and 2, retried while the seed's snapshot is unavailable (a
	// database of its own is out of service for a restore) until ctx ends.
	var snapshotInfo *SnapshotInfoResponse
	for {
		snapshotInfo, err = c.downloadFullSnapshot(ctx, client)
		if status.Code(err) != codes.Unavailable {
			break
		}
		log.Warn().Err(err).Str("seed", seedAddr).Msg("Seed snapshot unavailable, retrying")
		select {
		case <-ctx.Done():
			return 0, errors.Join(err, ctx.Err())
		case <-time.After(snapshotRetryInterval):
		}
	}
	if err != nil {
		return 0, err
	}

	// Step 3: Apply delta changes (transactions after snapshot)
	// Note: For full snapshot-based sync, this may not be needed immediately
	// The snapshot itself should be consistent

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Msg("Catch-up completed successfully - node stays JOINING until fully initialized")

	return snapshotInfo.SnapshotTxnId, nil
}

// snapshotRetryInterval is how long CatchUp waits before asking a seed again
// whose snapshot is unavailable.
const snapshotRetryInterval = time.Second

// downloadFullSnapshot gets the seed's snapshot info (step 1) and streams and
// applies every database in it (step 2).
func (c *CatchUpClient) downloadFullSnapshot(ctx context.Context, client MarmotServiceClient) (*SnapshotInfoResponse, error) {
	snapshotInfo, err := client.GetSnapshotInfo(ctx, &SnapshotInfoRequest{
		RequestingNodeId: c.nodeID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot info: %w", err)
	}

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Int64("size_bytes", snapshotInfo.SnapshotSizeBytes).
		Int32("total_chunks", snapshotInfo.TotalChunks).
		Int("databases", len(snapshotInfo.Databases)).
		Msg("Received snapshot info")

	if err := c.applySnapshot(ctx, client, snapshotInfo, ""); err != nil {
		return nil, fmt.Errorf("failed to apply snapshot: %w", err)
	}
	return snapshotInfo, nil
}

// findAvailableSeed finds an available seed node to catch up from
// Returns (nodeID, address, error). For seed addresses not in registry, nodeID will be 0.
func (c *CatchUpClient) findAvailableSeed(ctx context.Context) (uint64, string, error) {
	// Try configured seed addresses first
	// Note: Seed addresses may not be in registry yet, so node ID may be unknown (0)
	for _, addr := range c.seedAddrs {
		if c.checkNodeAvailable(ctx, addr) {
			// Try to find node ID from registry
			for _, node := range c.registry.GetAlive() {
				if node.Address == addr {
					return node.NodeId, addr, nil
				}
			}
			// Seed not in registry yet - return 0 for node ID
			return 0, addr, nil
		}
	}

	// Try nodes from registry (these always have node IDs)
	for _, node := range c.registry.GetAlive() {
		if node.NodeId == c.nodeID {
			continue // Skip self
		}
		if c.checkNodeAvailable(ctx, node.Address) {
			return node.NodeId, node.Address, nil
		}
	}

	return 0, "", fmt.Errorf("no available seed nodes")
}

// checkNodeAvailable checks if a node is available for catch-up
func (c *CatchUpClient) checkNodeAvailable(ctx context.Context, addr string) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, createDialOptions()...)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	_, err = client.Ping(ctx, &PingRequest{SourceNodeId: c.nodeID})
	return err == nil
}

// applySnapshot downloads and applies a snapshot from the seed node.
// Uses the unified snapshot.Restorer for atomic download and apply.
//
// database names the one database to install, or is empty to install every
// file the snapshot carries (startup catch-up, before any database is open).
// A running node must name its database: every other file, the system
// database included, is open and in use, and swapping it would leave its
// connections writing to an unlinked file while the next restart loads the
// peer's copy. A system database that is installed has this node's
// AUTO_INCREMENT claim bases merged into it first, so none is ever lowered.
func (c *CatchUpClient) applySnapshot(ctx context.Context, client MarmotServiceClient, info *SnapshotInfoResponse, database string) error {
	log.Info().Msg("Downloading snapshot using unified restorer")

	// Create data directory structure
	if err := os.MkdirAll(c.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(c.dataDir, "databases"), 0755); err != nil {
		return fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Stream the snapshot: every database, or only database when named.
	stream, err := client.StreamSnapshot(ctx, &SnapshotRequest{
		RequestingNodeId: c.nodeID,
		Database:         database,
	})
	if err != nil {
		return fmt.Errorf("failed to start snapshot stream: %w", err)
	}

	files, err := snapshotFilesToRestore(info.Databases, database)
	if err != nil {
		return err
	}

	// Create adapter for gRPC stream
	adapter := &grpcSnapshotStreamAdapter{stream: stream}

	// Use snapshot.Restorer for atomic download and apply. It is given no
	// ConnectionManager: during startup catch-up nothing is open yet, and on a
	// running node CatchUpFromPeer has detached the database for the whole
	// restore. Closing the pools here instead would nil them under callers
	// that already hold the database.
	restorer := snapshot.NewRestorer(c.dataDir, nil)
	restorer.SetSystemDBMerge(db.RaiseAutoIncBasesFrom)

	if err := restorer.RestoreFromStream(adapter, files); err != nil {
		return fmt.Errorf("snapshot restore failed: %w", err)
	}

	// From this point on the snapshot's files are already swapped onto disk.
	// Each installed SQLite file carries its own __marmot_schema_version
	// table, so nothing needs to be restored out of band here any more.

	log.Info().
		Uint64("snapshot_txn_id", info.SnapshotTxnId).
		Msg("Snapshot applied successfully via unified restorer")

	return nil
}

// snapshotFilesToRestore converts a snapshot's file list into the restorer's
// form, keeping only database when it is non-empty.
func snapshotFilesToRestore(databases []*DatabaseFileInfo, database string) ([]snapshot.DatabaseFileInfo, error) {
	files := make([]snapshot.DatabaseFileInfo, 0, len(databases))
	for _, dbInfo := range databases {
		if database != "" && dbInfo.Name != database {
			continue
		}
		files = append(files, snapshot.DatabaseFileInfo{
			Name:           dbInfo.Name,
			Filename:       dbInfo.Filename,
			SizeBytes:      dbInfo.SizeBytes,
			SHA256Checksum: dbInfo.Sha256Checksum,
		})
	}
	if database != "" && len(files) == 0 {
		return nil, fmt.Errorf("peer snapshot carries no database %q", database)
	}
	return files, nil
}

// grpcSnapshotStreamAdapter adapts MarmotService_StreamSnapshotClient to snapshot.ChunkReceiver
type grpcSnapshotStreamAdapter struct {
	stream MarmotService_StreamSnapshotClient
}

func (a *grpcSnapshotStreamAdapter) Recv() (*snapshot.Chunk, error) {
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
		FileSHA256:    chunk.GetFileSha256(),
		FileSizeBytes: chunk.GetFileSizeBytes(),
	}, nil
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
	busyTimeoutMS := cfg.Config.Transaction.LockWaitTimeoutSeconds * 1000
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&mode=ro", systemDBPath, busyTimeoutMS)
	systemDB, err := sql.Open(db.SQLiteDriverName, dsn)
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

// getMaxTxnIDFromDB queries the maximum transaction ID from a database's MetaStore
func (c *CatchUpClient) getMaxTxnIDFromDB(dbPath string) (uint64, error) {
	// MetaStore path is {dbPath without .db}_meta.pebble
	metaPath := strings.TrimSuffix(dbPath, ".db") + "_meta.pebble"

	// Check if MetaStore directory exists
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return 0, nil
	}

	// Open a temporary PebbleMetaStore (read-only, minimal memory)
	metaStore, err := db.NewPebbleMetaStore(metaPath, db.PebbleMetaStoreOptions{
		CacheSizeMB:           16, // Minimal for read-only lookup
		MemTableSizeMB:        16,
		MemTableCount:         1,
		L0CompactionThreshold: 4,
		L0StopWrites:          12,
	})
	if err != nil {
		return 0, nil // MetaStore might not be initialized yet
	}
	defer metaStore.Close()

	maxTxnID, err := metaStore.GetMaxCommittedTxnID()
	if err != nil {
		return 0, nil // No committed transactions yet
	}

	return maxTxnID, nil
}

// GetPeerMaxTxnIDs queries a peer node for its latest transaction IDs per database
func (c *CatchUpClient) GetPeerMaxTxnIDs(ctx context.Context, peerAddr string) (map[string]uint64, error) {
	// Connect to peer with common dial options (includes compression)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(peerAddr, createDialOptions()...)
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

// CatchUpDecision is the chosen strategy and the seed it runs against.
type CatchUpDecision struct {
	Strategy   CatchUpStrategy
	PeerNodeID uint64
	PeerAddr   string
}

// catchUpStrategyFor decides how a starting node catches up with a seed from
// the max txn ids each side reports per database:
//   - a seed with no data: nothing to catch up;
//   - a node with no data of its own: a full snapshot;
//   - otherwise a log pull of every database the seed has. Whether a node is
//     behind cannot be read from txn ids (they are not a position in any
//     one node's log), and a
//     pull that finds nothing new costs one listing; PerformLogPull restores
//     a database from a snapshot only when the seed's log no longer covers
//     this node's cursor into it.
func catchUpStrategyFor(localTxnIDs, peerTxnIDs map[string]uint64) CatchUpStrategy {
	peerHasData := false
	for _, txnID := range peerTxnIDs {
		if txnID > 0 {
			peerHasData = true
			break
		}
	}
	if !peerHasData {
		return NO_CATCHUP
	}
	if len(localTxnIDs) == 0 {
		return FULL_SNAPSHOT
	}
	return DELTA_SYNC
}

// DetermineCatchUpStrategy determines the best catch-up strategy by comparing local vs cluster state
// This is the main entry point for catch-up detection
func (c *CatchUpClient) DetermineCatchUpStrategy(ctx context.Context) (*CatchUpDecision, error) {
	decision := &CatchUpDecision{Strategy: NO_CATCHUP}

	// Step 1: Get local transaction IDs
	localTxnIDs, err := c.GetLocalMaxTxnID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get local txn IDs: %w", err)
	}

	// Step 2: Find an available seed node
	seedNodeID, seedAddr, err := c.findAvailableSeed(ctx)
	if err != nil {
		return nil, fmt.Errorf("no available seed node: %w", err)
	}
	decision.PeerNodeID = seedNodeID
	decision.PeerAddr = seedAddr

	// Step 3: Get peer transaction IDs
	peerTxnIDs, err := c.GetPeerMaxTxnIDs(ctx, seedAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get peer txn IDs: %w", err)
	}

	decision.Strategy = catchUpStrategyFor(localTxnIDs, peerTxnIDs)
	log.Info().
		Str("peer", seedAddr).
		Int("strategy", int(decision.Strategy)).
		Msg("Catch-up strategy selected")
	return decision, nil
}

// PerformLogPull catches a starting node up with its seed: it merges the seed's database registry, so a database created
// while this node was down exists before its log is pulled, then runs one
// bounded LogPuller.PullPair pass over the seed for every database, restoring
// one from the seed's snapshot when the seed's log no longer covers this
// node's cursor into it. Every step terminates: ListCommittedLog is a bounded
// RPC and FetchTransactions ends with EOF.
//
// A database that fails is logged and left to anti-entropy, which pulls every
// member's log every round; startup is not failed for it. Only a missing
// puller or database manager is an error.
func (c *CatchUpClient) PerformLogPull(ctx context.Context, decision *CatchUpDecision, lp *LogPuller, client *Client) error {
	if lp == nil {
		return fmt.Errorf("log puller not provided")
	}
	dbMgr := c.dbManager.Load()
	if dbMgr == nil {
		return fmt.Errorf("startup log pull needs the running node's DatabaseManager")
	}

	log.Info().Str("peer", decision.PeerAddr).Msg("Starting startup log pull")

	// A node the pull finds behind is marked JOINING, so it stays out of
	// quorums until the promotion checker admits it. A node the pull finds
	// current keeps its status: every startup of a node with data runs this
	// pull, and a current node is exactly what a restart of an idle cluster
	// brings back.
	behind := false
	defer func() {
		if behind {
			c.registry.MarkJoining(c.nodeID)
		}
	}()

	if err := reconcileRegistryWithPeer(ctx, client, dbMgr, c.nodeID, decision.PeerAddr); err != nil {
		log.Warn().Err(err).Str("peer", decision.PeerAddr).
			Msg("Startup log pull: database registry reconciliation failed; anti-entropy will retry it")
	}

	peer := PeerRef{NodeID: decision.PeerNodeID, Address: decision.PeerAddr}
	for _, dbName := range dbMgr.ListDatabases() {
		result, err := lp.PullPair(ctx, peer, dbName)
		if err != nil || result.NeedsSnapshot || result.Applied > 0 || !result.CaughtUp {
			behind = true
		}
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).
				Msg("Startup log pull failed; anti-entropy will retry it")
			continue
		}
		if result.NeedsSnapshot {
			log.Info().Str("database", dbName).Uint64("peer", decision.PeerNodeID).
				Msg("Startup log pull: the seed's log no longer covers this node's cursor, restoring from its snapshot")
			if err := c.restoreFromSeed(ctx, dbName, peer, lp); err != nil {
				log.Warn().Err(err).Str("database", dbName).
					Msg("Startup restore failed; anti-entropy will retry it")
			}
			continue
		}
		log.Info().Str("database", dbName).Int("txns_applied", result.Applied).Bool("caught_up", result.CaughtUp).
			Msg("Startup log pull: database pass completed")
	}
	return nil
}

// restoreFromSeed restores dbName from peer's snapshot through the same
// restore sequence anti-entropy runs (restoreDatabase).
func (c *CatchUpClient) restoreFromSeed(ctx context.Context, dbName string, peer PeerRef, lp *LogPuller) error {
	dbMgr := c.dbManager.Load()
	if dbMgr == nil {
		return fmt.Errorf("startup log pull of %s needs the running node's DatabaseManager", dbName)
	}
	return restoreDatabase(ctx, dbMgr, lp, dbName, func(ctx context.Context) error {
		if err := c.CatchUpFromPeer(ctx, peer.NodeID, peer.Address, dbName); err != nil {
			return fmt.Errorf("failed to restore database %s from peer: %w", dbName, err)
		}
		return nil
	})
}
