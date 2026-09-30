package grpc

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AntiEntropyService periodically reconciles this node's databases and
// their local commit logs with every alive cluster member: it merges each
// peer's database
// registry, pulls each database's log through a LogPuller with a
// per-pair deadline, and falls back to a full snapshot plus local-log
// re-apply when a peer's log has been truncated past this node's cursor.
type AntiEntropyService struct {
	nodeID       uint64
	registry     *NodeRegistry
	client       *Client
	dbManager    *db.DatabaseManager
	logPuller    *LogPuller
	snapshotFunc SnapshotTransferFunc

	// Configuration
	interval time.Duration
	enabled  bool

	// snapshotRestoreTimeout bounds the whole restore sequence
	// (restoreContext), independent of interval: a restore
	// transfers a database's full current size and can run far longer than
	// one anti-entropy round.
	snapshotRestoreTimeout time.Duration

	// Control
	stopCh  chan struct{}
	running bool
	mu      sync.Mutex

	// Per-database status as of the last completed round,
	// read by admin and tests.
	statusMu       sync.RWMutex
	caughtUp       map[string]bool
	promotionReady map[string]bool

	// lagGauge is telemetry.ReplicationLagTxns; lagPeers holds the peer
	// label values it currently exports, so a peer that leaves membership
	// has its series deleted.
	lagGauge telemetry.GaugeVec
	lagMu    sync.Mutex
	lagPeers map[uint64]struct{}
}

// SnapshotTransferFunc initiates a snapshot transfer to a peer
// This is injected to avoid circular dependencies
type SnapshotTransferFunc func(ctx context.Context, peerNodeID uint64, peerAddr string, database string) error

// AntiEntropyConfig holds configuration for anti-entropy service
type AntiEntropyConfig struct {
	NodeID       uint64
	Registry     *NodeRegistry
	Client       *Client
	DBManager    *db.DatabaseManager
	LogPuller    *LogPuller
	SnapshotFunc SnapshotTransferFunc
	Interval     time.Duration
	Enabled      bool
	// SnapshotRestoreTimeout bounds a snapshot restore's own deadline,
	// independent of Interval. Zero or negative falls back to
	// defaultSnapshotRestoreTimeout.
	SnapshotRestoreTimeout time.Duration
}

// defaultSnapshotRestoreTimeout is used when AntiEntropyConfig does not name
// one - direct construction (tests, embedded use) rather than
// NewAntiEntropyServiceFromConfig, which always sets it from
// cfg.Config.Replication.SnapshotRestoreTimeoutS.
const defaultSnapshotRestoreTimeout = 30 * time.Minute

// NewAntiEntropyService creates a new anti-entropy service
func NewAntiEntropyService(config AntiEntropyConfig) *AntiEntropyService {
	restoreTimeout := config.SnapshotRestoreTimeout
	if restoreTimeout <= 0 {
		restoreTimeout = defaultSnapshotRestoreTimeout
	}
	return &AntiEntropyService{
		nodeID:                 config.NodeID,
		registry:               config.Registry,
		client:                 config.Client,
		dbManager:              config.DBManager,
		logPuller:              config.LogPuller,
		snapshotFunc:           config.SnapshotFunc,
		interval:               config.Interval,
		enabled:                config.Enabled,
		snapshotRestoreTimeout: restoreTimeout,
		stopCh:                 make(chan struct{}),
		caughtUp:               make(map[string]bool),
		promotionReady:         make(map[string]bool),
		lagGauge:               telemetry.ReplicationLagTxns,
		lagPeers:               make(map[uint64]struct{}),
	}
}

// NewAntiEntropyServiceFromConfig creates anti-entropy service from global config
func NewAntiEntropyServiceFromConfig(
	nodeID uint64,
	registry *NodeRegistry,
	client *Client,
	dbManager *db.DatabaseManager,
	logPuller *LogPuller,
	snapshotFunc SnapshotTransferFunc,
) *AntiEntropyService {
	config := cfg.Config.Replication

	return NewAntiEntropyService(AntiEntropyConfig{
		NodeID:                 nodeID,
		Registry:               registry,
		Client:                 client,
		DBManager:              dbManager,
		LogPuller:              logPuller,
		SnapshotFunc:           snapshotFunc,
		Interval:               time.Duration(config.AntiEntropyIntervalS) * time.Second,
		Enabled:                config.EnableAntiEntropy,
		SnapshotRestoreTimeout: time.Duration(config.SnapshotRestoreTimeoutS) * time.Second,
	})
}

// restoreContext returns a context bounded by snapshotRestoreTimeout, used by
// every call site that runs a snapshot restore (syncDatabase's fallback and
// restoreAwaitingDatabases): deliberately not ae.interval, which bounds one
// anti-entropy round, not a one-shot transfer of a database's full current
// size.
func (ae *AntiEntropyService) restoreContext() (context.Context, context.CancelFunc) {
	timeout := ae.snapshotRestoreTimeout
	if timeout <= 0 {
		// A zero value here means an AntiEntropyService built as a struct
		// literal rather than through NewAntiEntropyService (some tests do
		// this) - fall back rather than expire the context immediately.
		timeout = defaultSnapshotRestoreTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
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

// runStartupSync performs anti-entropy at startup, waiting briefly for peers
// if needed, and always runs one round itself before returning: a JOINING
// node's first round must not wait a full ae.interval for the ticker
// (the promotion gate - checkPromotionCriteria needs a completed
// round to see PromotionReady at all).
func (ae *AntiEntropyService) runStartupSync() {
	// Try up to 5 times with 2 second delays to find ALIVE peers
	// This gives gossip time to propagate membership after a restart
	maxAttempts := 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, alive := ae.currentMembers()

		if len(alive) > 0 {
			log.Debug().
				Uint64("node_id", ae.nodeID).
				Int("alive_peers", len(alive)).
				Int("attempt", attempt).
				Msg("ANTI-ENTROPY: Found ALIVE peers, starting sync")
			ae.performAntiEntropy()
			return
		}

		log.Debug().
			Uint64("node_id", ae.nodeID).
			Int("attempt", attempt).
			Int("max_attempts", maxAttempts).
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
		Msg("ANTI-ENTROPY: No ALIVE peers found within the startup wait, running the first round anyway")
	// performAntiEntropy is a cheap no-op with no alive peers; running it now
	// still means the first round runs at Start(), not after waiting a full
	// ae.interval on the ticker.
	ae.performAntiEntropy()
}

// currentMembers returns two views of this node's cluster membership,
// self excluded: members is every registry
// node whose status is not REMOVED (LEAVING, DEAD and SUSPECT are members;
// only ALIVE ones are contacted), and alive is the subset of those that are
// ALIVE.
func (ae *AntiEntropyService) currentMembers() (members, alive []*NodeState) {
	for _, node := range ae.registry.GetAll() {
		if node.NodeId == ae.nodeID || node.Status == NodeStatus_REMOVED {
			continue
		}
		members = append(members, node)
		if node.Status == NodeStatus_ALIVE {
			alive = append(alive, node)
		}
	}
	return members, alive
}

// performAntiEntropy performs a single anti-entropy round. Every network
// step gets its own deadline: no pair, and no peer's registry listing,
// shares one with another, so one slow peer cannot starve the rest.
func (ae *AntiEntropyService) performAntiEntropy() {
	roundStart := time.Now()
	telemetry.AntiEntropyRoundsTotal.Inc()
	defer func() {
		telemetry.AntiEntropyDurationSeconds.Observe(time.Since(roundStart).Seconds())
	}()

	members, alive := ae.currentMembers()
	ae.deleteDepartedPeerLag(members)
	if len(alive) == 0 {
		log.Debug().Msg("Anti-entropy: no alive peers to sync with")
		return
	}

	log.Debug().
		Uint64("node_id", ae.nodeID).
		Int("member_count", len(members)).
		Int("alive_count", len(alive)).
		Msg("ANTI-ENTROPY: Starting round")

	// Reconcile the database set with every alive peer before pulling any
	// log, so a database this node missed CREATE for exists before its log
	// is ever pulled.
	ae.reconcileDatabaseRegistry(alive)

	lag := newReplicationLag()
	for _, dbName := range ae.dbManager.ListDatabases() {
		ae.syncDatabase(dbName, members, alive, lag)
	}
	ae.publishLag(lag)

	ae.restoreAwaitingDatabases(alive)

	log.Debug().Msg("Anti-entropy round completed")
}

// reconcileDatabaseRegistry merges every alive peer's database registry
// into this node's own (reconcileRegistryWithPeer). A peer that answers
// Unimplemented (rolling upgrade) has its registry skipped; every other
// per-peer error is logged and never aborts the round.
func (ae *AntiEntropyService) reconcileDatabaseRegistry(alive []*NodeState) {
	for _, peer := range alive {
		ctx, cancel := context.WithTimeout(context.Background(), ae.interval)
		err := reconcileRegistryWithPeer(ctx, ae.client, ae.dbManager, ae.nodeID, peer.Address)
		cancel()
		if err == nil {
			continue
		}
		if status.Code(err) == codes.Unimplemented {
			log.Warn().Uint64("peer_node", peer.NodeId).
				Msg("anti-entropy: peer does not support database registry listing yet, skipping (rolling upgrade)")
			continue
		}
		log.Debug().Err(err).Uint64("peer_node", peer.NodeId).
			Msg("anti-entropy: database registry reconciliation failed")
	}
}

// reconcileRegistryWithPeer merges the database registry of the peer at
// address into dbMgr's own: a database this node
// missed CREATE for is created, and one this node still has that the peer
// already tombstoned is dropped - never the reverse, by DatabaseRegistryKey's
// total order (db.DatabaseManager.ApplyDatabaseOp). A failure to apply one
// entry is logged and does not stop the others; the returned error is the
// listing's own.
//
// A legacy live entry (see db.DatabaseManager.ApplyDatabaseOp's doc
// comment) is never applied when this node has no row at all for the name:
// that is the one case where applying it would CREATE the database, spreading
// a pre-upgrade divergence this node never had a chance to see for itself.
// Every other entry, legacy or not, is merged as usual.
func reconcileRegistryWithPeer(ctx context.Context, client *Client, dbMgr *db.DatabaseManager, nodeID uint64, address string) error {
	conn, err := client.GetClientByAddress(address)
	if err != nil {
		return err
	}
	resp, err := conn.ListDatabaseRegistry(ctx, &DatabaseRegistryRequest{RequestingNodeId: nodeID})
	if err != nil {
		return err
	}
	for _, entry := range resp.Entries {
		key := db.DatabaseRegistryKey{Generation: entry.Generation, Dropped: entry.Dropped}
		if entry.Legacy && !entry.Dropped {
			local, err := dbMgr.RegistryKey(entry.Name)
			if err != nil {
				log.Warn().Err(err).Str("peer", address).Str("database", entry.Name).
					Msg("failed to read local database registry key")
				continue
			}
			if local.Generation == 0 {
				log.Debug().Str("peer", address).Str("database", entry.Name).
					Msg("anti-entropy: skipping peer's legacy live registry row for a database this node never had (pre-upgrade divergence, not repaired)")
				continue
			}
		}
		if _, err := dbMgr.ApplyDatabaseOp(entry.Name, key); err != nil {
			log.Warn().Err(err).Str("peer", address).Str("database", entry.Name).
				Msg("failed to apply peer database registry entry")
		}
	}
	return nil
}

// snapshotSource is the alive member selected as a database's restore
// source: the one with the highest schema version among the members that
// answered this round, ties broken by the lowest node id.
type snapshotSource struct {
	peer          *NodeState
	schemaVersion uint64
}

// considerSnapshotSource updates best with candidate if candidate outranks
// it (higher schema version, or the same version and a lower node id).
func considerSnapshotSource(best *snapshotSource, candidate snapshotSource) *snapshotSource {
	if best == nil {
		return &candidate
	}
	if candidate.schemaVersion > best.schemaVersion {
		return &candidate
	}
	if candidate.schemaVersion == best.schemaVersion && candidate.peer.NodeId < best.peer.NodeId {
		return &candidate
	}
	return best
}

// syncDatabase runs one database's pull round against every alive peer,
// each pair under its own deadline, falls back
// to a snapshot restore when any member's log no longer covers this node's
// cursor into it, and records the resulting caught-up status and each
// pair's unapplied count in lag.
func (ae *AntiEntropyService) syncDatabase(dbName string, members, alive []*NodeState, lag *replicationLag) {
	// Finish a pending re-apply of this node's own log, left by a
	// restore that crashed or failed before completing it, before pulling
	// anything more for dbName.
	ae.reapplyIfPending(dbName)

	dbCaughtUp := len(alive) == len(members)
	dbPromotionReady, needsSnapshot, source := ae.pullEveryAlivePeer(dbName, alive, &dbCaughtUp, lag)

	if needsSnapshot {
		dbCaughtUp = false
		ctx, cancel := ae.restoreContext()
		err := ae.restoreFromPeer(ctx, dbName, source.peer)
		cancel()
		if err != nil {
			telemetry.AntiEntropySyncsTotal.With("snapshot", "failed").Inc()
			log.Warn().Err(err).Str("database", dbName).Uint64("source_peer", source.peer.NodeId).
				Msg("anti-entropy: snapshot fallback failed")
		} else {
			telemetry.AntiEntropySyncsTotal.With("snapshot", "success").Inc()
		}
	}

	ae.setCaughtUp(dbName, dbCaughtUp)
	ae.setPromotionReady(dbName, dbPromotionReady)
}

// pullEveryAlivePeer runs dbName's PullPair against every alive peer, each
// under its own deadline, and folds the results into three outputs:
// dbCaughtUp (an in-out pointer, cleared by any pair that did not fully
// succeed - it starts set from len(alive)==len(members)), promotionReady
// (see below) and the best snapshot source seen, for the
// needsSnapshot fallback. Each pair's outcome is also recorded in lag.
//
// promotionReady is deliberately over alive peers only, never all members
// (checkPromotionCriteria): requiring CaughtUp over every member, DEAD ones
// included, would keep a restarted node JOINING forever while any member
// stays down. It starts true and is cleared by any alive peer's pair that
// did not fully succeed; it also requires at least one alive peer was
// actually pulled, so a node with no alive peers yet is never
// promotion-ready.
func (ae *AntiEntropyService) pullEveryAlivePeer(dbName string, alive []*NodeState, dbCaughtUp *bool, lag *replicationLag) (promotionReady, needsSnapshot bool, source *snapshotSource) {
	promotionReady = len(alive) > 0

	for _, peer := range alive {
		pairCtx, cancel := context.WithTimeout(context.Background(), ae.interval)
		result, err := ae.logPuller.PullPair(pairCtx, PeerRef{NodeID: peer.NodeId, Address: peer.Address}, dbName)
		cancel()
		lag.record(peer.NodeId, result, err)

		if err != nil {
			log.Warn().Err(err).Uint64("peer_node", peer.NodeId).Str("database", dbName).
				Msg("anti-entropy: pull failed")
			*dbCaughtUp = false
			promotionReady = false
			continue
		}
		if result.Unimplemented || result.DatabaseAbsent {
			// A rolling-upgrade peer, or a peer whose registry has not caught
			// up yet: its log cannot count toward caught-up this round.
			*dbCaughtUp = false
			promotionReady = false
			continue
		}
		source = considerSnapshotSource(source, snapshotSource{peer: peer, schemaVersion: result.PeerSchemaVersion})
		if result.NeedsSnapshot {
			needsSnapshot = true
			promotionReady = false
		}
		if !result.CaughtUp {
			*dbCaughtUp = false
			promotionReady = false
		}
	}
	return promotionReady, needsSnapshot, source
}

// replicationLag sums one round's PairResult.Unapplied per peer over the
// databases pulled. A peer with any pair of unknown count (an error, a
// snapshot fallback, an older peer that cannot report the rest of its log)
// is marked unknown and not published that round: a partial sum would
// understate its lag.
type replicationLag struct {
	unapplied map[uint64]uint64
	unknown   map[uint64]bool
}

func newReplicationLag() *replicationLag {
	return &replicationLag{unapplied: make(map[uint64]uint64), unknown: make(map[uint64]bool)}
}

// record adds one pair's outcome for peer.
func (l *replicationLag) record(peer uint64, result PairResult, err error) {
	if err != nil || !result.UnappliedKnown {
		l.unknown[peer] = true
		return
	}
	l.unapplied[peer] += result.Unapplied
}

// publishLag sets ReplicationLagTxns for every peer lag counted in full,
// leaving every other peer's series as it was.
func (ae *AntiEntropyService) publishLag(lag *replicationLag) {
	ae.lagMu.Lock()
	defer ae.lagMu.Unlock()
	for peer, n := range lag.unapplied {
		if lag.unknown[peer] {
			continue
		}
		ae.lagGauge.With(strconv.FormatUint(peer, 10)).Set(float64(n))
		ae.lagPeers[peer] = struct{}{}
	}
}

// deleteDepartedPeerLag deletes the ReplicationLagTxns series of every peer
// no longer among members.
func (ae *AntiEntropyService) deleteDepartedPeerLag(members []*NodeState) {
	current := make(map[uint64]struct{}, len(members))
	for _, m := range members {
		current[m.NodeId] = struct{}{}
	}
	ae.lagMu.Lock()
	defer ae.lagMu.Unlock()
	for peer := range ae.lagPeers {
		if _, ok := current[peer]; !ok {
			ae.lagGauge.Delete(strconv.FormatUint(peer, 10))
			delete(ae.lagPeers, peer)
		}
	}
}

// restoreFromPeer restores dbName from source's snapshot and then runs the
// rest of the restore sequence (restoreDatabase).
func (ae *AntiEntropyService) restoreFromPeer(ctx context.Context, dbName string, source *NodeState) error {
	if ae.snapshotFunc == nil {
		return fmt.Errorf("snapshot transfer not configured")
	}
	return restoreDatabase(ctx, ae.dbManager, ae.logPuller, dbName, func(ctx context.Context) error {
		return ae.snapshotFunc(ctx, source.NodeId, source.Address, dbName)
	})
}

// restoreDatabase is the one restore sequence, used by anti-entropy's snapshot fallback, its retry of databases awaiting
// a restore, and startup catch-up:
//  1. durably mark the re-apply of this node's own log pending, before the
//     restore replaces the file, so a crash anywhere after it still re-applies;
//  2. restore;
//  3. re-apply every entry of this node's own log the restored file lacks,
//     clearing the mark only on success - a failure leaves it for
//     the next round (reapplyIfPending);
//  4. let every member's next pull move its cursor up to that member's
//     truncation point (LogPuller.MarkRestored).
func restoreDatabase(ctx context.Context, dbMgr *db.DatabaseManager, lp *LogPuller, dbName string, restore func(context.Context) error) error {
	if mdb, err := dbMgr.GetDatabase(dbName); err == nil {
		if err := mdb.GetMetaStore().SetReapplyPending(true); err != nil {
			return fmt.Errorf("mark local-log re-apply pending for %s: %w", dbName, err)
		}
	}
	if err := restore(ctx); err != nil {
		return err
	}
	mdb, err := dbMgr.GetDatabase(dbName)
	if err != nil {
		return fmt.Errorf("database %s not available after restore: %w", dbName, err)
	}
	if err := mdb.GetMetaStore().SetReapplyPending(true); err != nil {
		return fmt.Errorf("mark local-log re-apply pending for %s: %w", dbName, err)
	}
	if applied, err := mdb.ReapplyLocalLogIfPending(ctx); err != nil {
		log.Warn().Err(err).Str("database", dbName).Int("applied", applied).
			Msg("local-log re-apply after restore failed; retried every anti-entropy round")
	}
	lp.MarkRestored(dbName)
	return nil
}

// reapplyIfPending runs dbName's pending local-log re-apply, if its durable
// mark is set.
func (ae *AntiEntropyService) reapplyIfPending(dbName string) {
	mdb, err := ae.dbManager.GetDatabase(dbName)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), ae.interval)
	defer cancel()
	applied, err := mdb.ReapplyLocalLogIfPending(ctx)
	if err != nil {
		log.Warn().Err(err).Str("database", dbName).Int("applied", applied).
			Msg("anti-entropy: local-log re-apply failed, will retry next round")
		return
	}
	if applied > 0 {
		log.Info().Str("database", dbName).Int("applied", applied).
			Msg("anti-entropy: local-log re-apply completed after restore")
	}
}

// restoreAwaitingDatabases retries the snapshot restore of every database
// whose reattach after an earlier restore failed (DatabasesAwaitingRestore).
// Such a database is out of service and absent from ListDatabases, so the
// loop in performAntiEntropy never reaches it; each alive peer is tried in
// turn until one restore succeeds.
func (ae *AntiEntropyService) restoreAwaitingDatabases(alive []*NodeState) {
	if ae.snapshotFunc == nil {
		return
	}
	for _, dbName := range ae.dbManager.DatabasesAwaitingRestore() {
		for _, peer := range alive {
			ctx, cancel := ae.restoreContext()
			err := ae.restoreFromPeer(ctx, dbName, peer)
			cancel()
			if err == nil {
				telemetry.AntiEntropySyncsTotal.With("snapshot", "success").Inc()
				log.Info().Uint64("peer_node", peer.NodeId).Str("database", dbName).
					Msg("Restored a database whose earlier reattach failed")
				break
			}
			telemetry.AntiEntropySyncsTotal.With("snapshot", "failed").Inc()
			log.Warn().Err(err).Uint64("peer_node", peer.NodeId).Str("database", dbName).
				Msg("Restore of a database awaiting one failed")
		}
	}
}

// setCaughtUp records dbName's caught-up status for the round just
// completed.
func (ae *AntiEntropyService) setCaughtUp(dbName string, caughtUp bool) {
	ae.statusMu.Lock()
	defer ae.statusMu.Unlock()
	ae.caughtUp[dbName] = caughtUp
}

// CaughtUp reports whether, in anti-entropy's last completed round for
// database, every current member's log was reachable and this node's
// cursor into it had reached that member's stable point. False before
// anti-entropy has run a round
// for database.
func (ae *AntiEntropyService) CaughtUp(database string) bool {
	ae.statusMu.RLock()
	defer ae.statusMu.RUnlock()
	return ae.caughtUp[database]
}

// setPromotionReady records dbName's promotion-readiness for the round just
// completed (see syncDatabase's dbPromotionReady comment).
func (ae *AntiEntropyService) setPromotionReady(dbName string, ready bool) {
	ae.statusMu.Lock()
	defer ae.statusMu.Unlock()
	ae.promotionReady[dbName] = ready
}

// PromotionReady reports whether database is ready for this node to be
// promoted from JOINING to ALIVE: true iff, in anti-entropy's
// last completed round, at least one alive peer was pulled and every alive
// peer's pair returned no error, was not Unimplemented or DatabaseAbsent,
// reported CaughtUp and did not need a snapshot. Unlike CaughtUp, this is
// deliberately evaluated over alive peers only, never every member: waiting
// for a DEAD member's log too would keep a restarted node JOINING forever
// while that member stays down. False before anti-entropy has run a round
// for database.
func (ae *AntiEntropyService) PromotionReady(database string) bool {
	ae.statusMu.RLock()
	defer ae.statusMu.RUnlock()
	return ae.promotionReady[database]
}

// StuckTxnCount returns how many of database's transactions LogPuller
// currently reports STUCK on a deterministic replay failure.
func (ae *AntiEntropyService) StuckTxnCount(database string) int {
	count := 0
	for _, t := range ae.logPuller.StuckTxns() {
		if t.Database == database {
			count++
		}
	}
	return count
}

// ForceSync forces an immediate anti-entropy round
// Useful for testing or manual catch-up
func (ae *AntiEntropyService) ForceSync() {
	log.Info().Msg("Forcing anti-entropy sync")
	ae.performAntiEntropy()
}

// GetStats returns current anti-entropy statistics: overall service state
// plus, per database, whether it is caught up and how many of its
// transactions are stuck.
func (ae *AntiEntropyService) GetStats() map[string]interface{} {
	ae.mu.Lock()
	enabled := ae.enabled
	running := ae.running
	ae.mu.Unlock()

	databases := ae.dbManager.ListDatabases()
	dbStats := make(map[string]interface{}, len(databases))
	for _, dbName := range databases {
		dbStats[dbName] = map[string]interface{}{
			"caught_up":       ae.CaughtUp(dbName),
			"promotion_ready": ae.PromotionReady(dbName),
			"stuck_txns":      ae.StuckTxnCount(dbName),
		}
	}

	return map[string]interface{}{
		"enabled":          enabled,
		"running":          running,
		"interval_seconds": ae.interval.Seconds(),
		"databases":        dbStats,
	}
}
