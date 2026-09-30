package grpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// aeRegNode is one in-process node for AntiEntropyService regression tests:
// a real db.DatabaseManager, a real CatchUpClient (for the snapshot
// fallback), and, once served, a real gRPC MarmotServiceServer - so a round
// exercises the exact wire path (ListCommittedLog, FetchTransactions,
// ListDatabaseRegistry, snapshot transfer) a production round uses.
//
// This duplicates a little of log_pull_harness_test.go's pullTestNode by
// design: that harness does not expose its DatabaseManager's data
// directory, which the snapshot fallback needs for CatchUpClient.
type aeRegNode struct {
	id       uint64
	dir      string
	dm       *db.DatabaseManager
	registry *NodeRegistry
	client   *Client
	catchUp  *CatchUpClient
	addr     string
}

func newAERegNode(t *testing.T, id uint64, databases ...string) *aeRegNode {
	t.Helper()
	dir := t.TempDir()
	dm, err := db.NewDatabaseManager(dir, id, hlc.NewClock(id))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dm.Close() })

	for _, name := range databases {
		require.NoError(t, dm.CreateDatabase(name))
		mdb, err := dm.GetDatabase(name)
		require.NoError(t, err)
		_, err = mdb.GetWriteDB().Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
		require.NoError(t, err)
		require.NoError(t, mdb.ReloadSchema())
	}

	registry := NewNodeRegistry(id, "127.0.0.1:0")
	catchUp := NewCatchUpClient(id, dir, registry, nil)
	catchUp.SetDatabaseManager(dm)

	return &aeRegNode{id: id, dir: dir, dm: dm, registry: registry, client: NewClient(id), catchUp: catchUp}
}

// serve starts n over real gRPC and records its listen address.
func (n *aeRegNode) serve(t *testing.T) {
	t.Helper()
	n.serveWrapped(t, nil)
}

// serveWrapped is serve through wrap, when non-nil (a peer that behaves
// differently on one RPC, say).
func (n *aeRegNode) serveWrapped(t *testing.T, wrap func(*Server) MarmotServiceServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &Server{nodeID: n.id, registry: NewNodeRegistry(n.id, listener.Addr().String())}
	server.SetDatabaseManager(n.dm)
	var service MarmotServiceServer = server
	if wrap != nil {
		service = wrap(server)
	}
	gs := grpc.NewServer()
	RegisterMarmotServiceServer(gs, service)
	go func() { _ = gs.Serve(listener) }()
	t.Cleanup(gs.Stop)
	n.addr = listener.Addr().String()
}

// seed commits txnID (coordinated by origin, committed at wall) into
// database d through ReplicatedDatabase.ApplyReplayedTxn - the same path a
// real replay uses - inserting one row (rowID, v) into table t.
func (n *aeRegNode) seed(t *testing.T, d string, txnID, origin uint64, wall int64, rowID int64, v string) {
	t.Helper()
	mdb, err := n.dm.GetDatabase(d)
	require.NoError(t, err)

	row := &db.EncodedCapturedRow{
		Table:     "t",
		Op:        uint8(db.OpTypeInsert),
		IntentKey: []byte(fmt.Sprintf("t:%d", rowID)),
		NewValues: encodeSeedValues(t, map[string]interface{}{"id": rowID, "v": v}),
	}
	applied, err := mdb.ApplyReplayedTxn(context.Background(), &db.ReplayTxn{
		TxnID:        txnID,
		OriginNodeID: origin,
		CommitTS:     hlc.Timestamp{WallTime: wall, NodeID: origin},
		Rows:         []*db.EncodedCapturedRow{row},
	}, true)
	require.NoError(t, err)
	require.True(t, applied)
}

// truncateThrough forces d's GC to delete every currently committed log
// entry (every position safe, no minimum retention), so ListCommittedLog reports a
// non-zero TruncatedThrough to any puller whose cursor is still behind it.
// Used to force a PullPair NeedsSnapshot outcome deterministically, without
// waiting out a real retention window.
func (n *aeRegNode) truncateThrough(t *testing.T, d string) {
	t.Helper()
	mdb, err := n.dm.GetDatabase(d)
	require.NoError(t, err)
	deleted, err := mdb.GetMetaStore().CleanupOldTransactionRecords(0, 0, db.LogPosition{Seq: ^uint64(0), TxnID: ^uint64(0)})
	require.NoError(t, err)
	require.NotZero(t, deleted, "nothing was truncated")
}

// rows reads table t of database d.
func (n *aeRegNode) rows(t *testing.T, d string) map[int64]string {
	t.Helper()
	mdb, err := n.dm.GetDatabase(d)
	require.NoError(t, err)
	rs, err := mdb.GetReadDB().Query("SELECT id, v FROM t")
	require.NoError(t, err)
	defer rs.Close()

	got := map[int64]string{}
	for rs.Next() {
		var id int64
		var v string
		require.NoError(t, rs.Scan(&id, &v))
		got[id] = v
	}
	require.NoError(t, rs.Err())
	return got
}

// antiEntropyFor builds a real AntiEntropyService for n, wired against n's
// own registry, client, database manager and CatchUpFromPeer.
func (n *aeRegNode) antiEntropyFor() *AntiEntropyService {
	lp := NewLogPuller(LogPullerConfig{NodeID: n.id, Client: n.client, DBManager: n.dm})
	return NewAntiEntropyService(AntiEntropyConfig{
		NodeID:       n.id,
		Registry:     n.registry,
		Client:       n.client,
		DBManager:    n.dm,
		LogPuller:    lp,
		SnapshotFunc: n.catchUp.CatchUpFromPeer,
		Interval:     30 * time.Second,
		Enabled:      true,
	})
}

func addPeer(registry *NodeRegistry, id uint64, addr string, status NodeStatus) {
	registry.Add(&NodeState{NodeId: id, Address: addr, Status: status, Incarnation: 1})
}

// TestAntiEntropySyncsTwoLaggingDatabasesInOneRound: every lagging database
// is pulled in the same round. An anti-entropy loop that picks one best peer
// per database and only pulls that one leaves a second lagging database
// unsynced until a later round.
func TestAntiEntropySyncsTwoLaggingDatabasesInOneRound(t *testing.T) {
	peer := newAERegNode(t, 2, "db1", "db2")
	peer.serve(t)
	peer.seed(t, "db1", 101, 2, 1000, 1, "p1")
	peer.seed(t, "db2", 201, 2, 1000, 1, "q1")

	local := newAERegNode(t, 1, "db1", "db2")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)

	local.antiEntropyFor().performAntiEntropy()

	require.Equal(t, map[int64]string{1: "p1"}, local.rows(t, "db1"), "db1 was not synced in a single anti-entropy round")
	require.Equal(t, map[int64]string{1: "q1"}, local.rows(t, "db2"),
		"db2 was not synced in the same round as db1 (only-the-first-database-synced defect)")
}

// TestAntiEntropyPullsEveryPeersLog: every peer's log is pulled. Syncing
// each database only from the peer with the highest max txn id never fetches
// a txn only another peer holds - one committed by a quorum that excluded the
// chosen peer.
func TestAntiEntropyPullsEveryPeersLog(t *testing.T) {
	p2 := newAERegNode(t, 2, "app")
	p2.serve(t)
	p2.seed(t, "app", 1000, 2, 1000, 1, "a")
	p2.seed(t, "app", 3000, 2, 3000, 3, "c")
	p3 := newAERegNode(t, 3, "app")
	p3.serve(t)
	p3.seed(t, "app", 2000, 4, 2000, 2, "b")

	local := newAERegNode(t, 1, "app")
	addPeer(local.registry, 2, p2.addr, NodeStatus_ALIVE)
	addPeer(local.registry, 3, p3.addr, NodeStatus_ALIVE)

	ae := local.antiEntropyFor()
	ae.performAntiEntropy()

	require.Equal(t, map[int64]string{1: "a", 2: "b", 3: "c"}, local.rows(t, "app"),
		"a txn held only by the peer with the lower max txn id was not fetched")
	require.True(t, ae.CaughtUp("app"))
}

// TestAntiEntropyReconcilesMissedCreateAndDrop fails when anti-entropy never
// reconciles the database set: a database
// created on a peer while this node was gone is never created locally, and
// one dropped on a peer while this node was gone is never dropped locally.
func TestAntiEntropyReconcilesMissedCreateAndDrop(t *testing.T) {
	peer := newAERegNode(t, 2, "shared")
	peer.serve(t)
	require.NoError(t, peer.dm.CreateDatabase("newdb"))
	require.NoError(t, peer.dm.DropDatabase("shared"))

	local := newAERegNode(t, 1, "shared")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)
	require.NotContains(t, local.dm.ListDatabases(), "newdb")

	local.antiEntropyFor().performAntiEntropy()

	require.Contains(t, local.dm.ListDatabases(), "newdb",
		"a database created on a peer was never picked up by registry reconciliation")
	require.NotContains(t, local.dm.ListDatabases(), "shared",
		"a database dropped on a peer was never dropped locally by registry reconciliation")
}

// TestAntiEntropyRegistryReconciliationNeverResurrectsALocalDrop exercises
// the same reconciliation path against a peer whose registry is older than
// this node's own DROP: DatabaseRegistryKey's total order
// (db.DatabaseManager.ApplyDatabaseOp) must refuse the peer's stale, still-
// live key.
func TestAntiEntropyRegistryReconciliationNeverResurrectsALocalDrop(t *testing.T) {
	peer := newAERegNode(t, 2, "shared") // peer never drops "shared" - a stale registry.
	peer.serve(t)

	local := newAERegNode(t, 1, "shared")
	require.NoError(t, local.dm.DropDatabase("shared"))
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)

	local.antiEntropyFor().performAntiEntropy()

	require.NotContains(t, local.dm.ListDatabases(), "shared",
		"a peer with an older (still-live) registry resurrected a local drop")
}

// TestAntiEntropyCaughtUpReflectsUnreachablePeers fails if CaughtUp does not
// account for every current member's log being reachable: it must be false while any member cannot be pulled from, and
// true once every current member has answered and been fully drained.
func TestAntiEntropyCaughtUpReflectsUnreachablePeers(t *testing.T) {
	peer := newAERegNode(t, 2, "db")
	peer.serve(t)
	peer.seed(t, "db", 301, 2, 1000, 1, "a1")

	local := newAERegNode(t, 1, "db")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)

	// Node 3 is ALIVE in the registry but its address refuses connections.
	closedListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachableAddr := closedListener.Addr().String()
	require.NoError(t, closedListener.Close())
	addPeer(local.registry, 3, unreachableAddr, NodeStatus_ALIVE)

	ae := local.antiEntropyFor()
	ae.performAntiEntropy()
	require.False(t, ae.CaughtUp("db"), "expected not caught up while a current member is unreachable")
	require.Equal(t, map[int64]string{1: "a1"}, local.rows(t, "db"), "the reachable peer's row was not pulled")

	// Node 3 leaves membership entirely; every remaining member now answers
	// and is fully drained.
	require.NoError(t, local.registry.MarkRemoved(3))
	ae.performAntiEntropy()
	require.True(t, ae.CaughtUp("db"), "expected caught up once every current member answered and was drained")
}

// TestAntiEntropySnapshotFallbackReappliesLocalOnlyTxn exercises the
// snapshot fallback end to end: the peer's log has been truncated past
// this node's cursor, so anti-entropy restores from it, then re-applies a
// local-only transaction the source's snapshot lacks, from this node's own
// durable local log, and resets its pull cursor.
func TestAntiEntropySnapshotFallbackReappliesLocalOnlyTxn(t *testing.T) {
	source := newAERegNode(t, 2, "app")
	// Seed and immediately truncate a throwaway entry so the peer's
	// TruncatedThrough moves past position zero - runs before the row we
	// actually want the restore to carry, so only the throwaway is
	// eligible for this unconditional (maxRetention=0) cleanup.
	source.seed(t, "app", 1, 2, 1000, 99, "throwaway")
	source.truncateThrough(t, "app")
	source.seed(t, "app", 101, 2, 2000, 1, "fromsource")
	source.serve(t)

	local := newAERegNode(t, 1, "app")
	// A transaction local holds in its own durable log that the source
	// never had (e.g. local replayed it from a third peer that has since
	// been GC'd elsewhere) - restoring from source's snapshot alone would
	// silently lose it.
	local.seed(t, "app", 501, 1, 3000, 2, "localonly")
	addPeer(local.registry, 2, source.addr, NodeStatus_ALIVE)

	ae := local.antiEntropyFor()
	ae.performAntiEntropy()

	// The throwaway row's SQLite data was already durably committed on
	// source before its log entry was truncated - GC only removes log
	// metadata, never table data - so it legitimately survives in source's
	// snapshot alongside "fromsource".
	require.Equal(t, map[int64]string{1: "fromsource", 2: "localonly", 99: "throwaway"}, local.rows(t, "app"),
		"the snapshot fallback did not carry both the source's rows and the re-applied local-only row")

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	pending, err := mdb.GetMetaStore().ReapplyPending()
	require.NoError(t, err)
	require.False(t, pending, "the re-apply mark was not cleared after a successful re-apply")

	// The next round resumes the source's log from its truncation point
	// instead of restoring again, and drains it.
	ae.performAntiEntropy()
	cursor, err := mdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	require.NotEqual(t, db.LogPosition{}, cursor, "the pull cursor for the restore's source was not moved past its truncation point")
	require.True(t, ae.CaughtUp("app"), "the round after the restore did not drain the source's log")
}

// TestPerformLogPullCatchesUpAStartingNodeAndReturns: startup catch-up of a
// node that has data merges the seed's registry (a database created while
// the node was down, with its DDL and rows in the seed's log), pulls every
// database's log, and returns. On 7a66596 the startup delta sync streamed
// StreamChanges, which never ends, so it could only return at its 5-minute
// deadline and then log.Fatal.
func TestPerformLogPullCatchesUpAStartingNodeAndReturns(t *testing.T) {
	seed := newAERegNode(t, 2, "app")
	seed.seed(t, "app", 1000, 2, 1000, 1, "missed-while-down")
	require.NoError(t, seed.dm.CreateDatabase("newdb"))
	newdb, err := seed.dm.GetDatabase("newdb")
	require.NoError(t, err)
	applied, err := newdb.ApplyReplayedTxn(context.Background(), &db.ReplayTxn{
		TxnID: 1100, OriginNodeID: 2, CommitTS: hlc.Timestamp{WallTime: 1100, NodeID: 2},
		Rows: []*db.EncodedCapturedRow{{Table: "t", Op: uint8(db.OpTypeDDL), DDLSQL: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}},
	}, true)
	require.NoError(t, err)
	require.True(t, applied)
	require.NoError(t, newdb.ReloadSchema())
	seed.seed(t, "newdb", 1200, 2, 1200, 7, "created-while-down")
	seed.serve(t)

	local := newAERegNode(t, 1, "app")
	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: local.client, DBManager: local.dm})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	start := time.Now()
	require.NoError(t, local.catchUp.PerformLogPull(ctx, &CatchUpDecision{Strategy: DELTA_SYNC, PeerNodeID: 2, PeerAddr: seed.addr}, lp, local.client))
	require.Less(t, time.Since(start), 30*time.Second, "startup catch-up ran into its deadline instead of returning")

	require.Equal(t, map[int64]string{1: "missed-while-down"}, local.rows(t, "app"))
	_, err = local.dm.GetDatabase("newdb")
	require.NoError(t, err, "a database created while the node was down was not created at startup")
	require.Equal(t, map[int64]string{7: "created-while-down"}, local.rows(t, "newdb"),
		"a database created while the node was down was not created and pulled at startup")
	self, ok := local.registry.Get(1)
	require.True(t, ok)
	require.Equal(t, NodeStatus_JOINING, self.Status, "a node the startup pull found behind was not marked JOINING")
}

// markLegacy flips name's registry row to legacy=1 directly in n's system
// database, simulating a row that survived an upgrade from a release
// before generation-stamped registry rows
// (migrateDatabaseRegistrySchema) rather than one this test drove through
// db.DatabaseManager.CreateDatabase/DropDatabase - which always stamp
// legacy=0, since only a real migration ever produces a legacy row.
func (n *aeRegNode) markLegacy(t *testing.T, name string) {
	t.Helper()
	_, err := n.dm.GetSystemDatabase().GetDB().Exec(
		"UPDATE __marmot_databases SET legacy = 1 WHERE name = ?", name)
	require.NoError(t, err)
}

// TestAntiEntropyLegacyLiveRowNeverCreatesOnAPeerThatLacksTheName: a
// registry row migrated from an older release's registry (legacy=1, live)
// must never CREATE the database on a peer that has no row for the name at
// all - reconciliation does not spread a pre-upgrade divergence (a node that missed a DROP DATABASE before the upgrade) cluster
// wide. An ordinary, non-legacy live row still reconciles normally.
func TestAntiEntropyLegacyLiveRowNeverCreatesOnAPeerThatLacksTheName(t *testing.T) {
	peer := newAERegNode(t, 2)
	require.NoError(t, peer.dm.CreateDatabase("legacyx"))
	peer.markLegacy(t, "legacyx")
	require.NoError(t, peer.dm.CreateDatabase("stampedx")) // ordinary, non-legacy live row
	peer.serve(t)

	local := newAERegNode(t, 1)
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)
	require.False(t, local.dm.DatabaseExists("legacyx"))

	local.antiEntropyFor().performAntiEntropy()

	require.False(t, local.dm.DatabaseExists("legacyx"),
		"a peer's legacy live registry row created the database on a peer that never had the name")
	require.True(t, local.dm.DatabaseExists("stampedx"),
		"an ordinary (non-legacy) live registry row was not reconciled")
}

// TestAntiEntropyLegacyRowStopsBeingLegacyAfterAStampedCreate: once a real, post-upgrade DROP followed by a real
// CREATE re-stamps a legacy row's name with a fresh generation, it is an
// ordinary (non-legacy) live registry key from then on, and reconciles
// normally even on a peer that never had the name at all - the one case
// reconcileRegistryWithPeer otherwise refuses for a legacy live row.
func TestAntiEntropyLegacyRowStopsBeingLegacyAfterAStampedCreate(t *testing.T) {
	peer := newAERegNode(t, 2)
	require.NoError(t, peer.dm.CreateDatabase("wasLegacy"))
	peer.markLegacy(t, "wasLegacy")
	require.NoError(t, peer.dm.DropDatabase("wasLegacy"))
	require.NoError(t, peer.dm.CreateDatabase("wasLegacy")) // real, post-upgrade stamped re-create: clears legacy
	peer.serve(t)

	local := newAERegNode(t, 1) // never had "wasLegacy" at all
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)

	local.antiEntropyFor().performAntiEntropy()

	require.True(t, local.dm.DatabaseExists("wasLegacy"),
		"a stamped re-create that followed a legacy row was still treated as legacy and skipped")
}

// TestCheckPromotionCriteriaRequiresPromotionReadyForEveryDatabase: matching
// schema versions is not enough to promote a JOINING node - a database can
// be current on schema while still missing rows anti-entropy has not pulled
// yet. checkPromotionCriteria must also require AntiEntropyService.
// PromotionReady for every local database.
func TestCheckPromotionCriteriaRequiresPromotionReadyForEveryDatabase(t *testing.T) {
	peer := newAERegNode(t, 2, "pdb")
	peer.seed(t, "pdb", 101, 2, 1000, 1, "unpulled") // no DDL: schema versions already match
	peer.serve(t)

	local := newAERegNode(t, 1, "pdb")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)

	clock := hlc.NewClock(1)
	schemaVersionMgr := db.NewSchemaVersionManager(local.dm)
	replicationHandler := NewReplicationHandler(1, local.dm, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           local.registry,
		dbManager:          local.dm,
		replicationHandler: replicationHandler,
	}
	ae := local.antiEntropyFor()
	server.SetAntiEntropy(ae)

	require.False(t, server.checkPromotionCriteria(),
		"expected promotion refused while a database is not PromotionReady, even with matching schema versions")

	ae.performAntiEntropy() // pulls the row; pdb becomes PromotionReady

	require.True(t, server.checkPromotionCriteria(),
		"expected promotion once every local database is PromotionReady")
}

// TestAntiEntropyStartRunsFirstRoundWithoutWaitingForATick: a JOINING
// node's first anti-entropy round
// runs promptly at Start(), not after a full ae.interval on the ticker: a
// long interval must not delay checkPromotionCriteria's first look at
// PromotionReady.
func TestAntiEntropyStartRunsFirstRoundWithoutWaitingForATick(t *testing.T) {
	peer := newAERegNode(t, 2, "app")
	peer.seed(t, "app", 1, 2, 1000, 1, "a")
	peer.serve(t)

	local := newAERegNode(t, 1, "app")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)

	ae := NewAntiEntropyService(AntiEntropyConfig{
		NodeID:    1,
		Registry:  local.registry,
		Client:    local.client,
		DBManager: local.dm,
		LogPuller: NewLogPuller(LogPullerConfig{NodeID: 1, Client: local.client, DBManager: local.dm}),
		Interval:  time.Hour, // long enough that the ticker must never fire during this test
		Enabled:   true,
	})
	ae.Start()
	t.Cleanup(ae.Stop)

	require.Eventually(t, func() bool { return ae.CaughtUp("app") }, 5*time.Second, 20*time.Millisecond,
		"the first anti-entropy round did not run promptly at Start(); it appears to be waiting for the interval's first tick")
}

// TestPerformLogPullLeavesACurrentNodeOutOfJoining: every startup of a node
// with data runs the log pull, so a node that is already current - the whole
// cluster restarted idle - must not be demoted to JOINING by it; it would
// then refuse to count toward quorums until promoted, and the first write
// after the restart would fail with "prepare quorum not achieved".
//
// Mutation: mark JOINING before the pull unconditionally. "a current node
// was marked JOINING" fires.
func TestPerformLogPullLeavesACurrentNodeOutOfJoining(t *testing.T) {
	seed := newAERegNode(t, 2, "app")
	seed.seed(t, "app", 1000, 2, 1000, 1, "both")
	seed.serve(t)

	local := newAERegNode(t, 1, "app")
	local.seed(t, "app", 1000, 2, 1000, 1, "both")
	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: local.client, DBManager: local.dm})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, local.catchUp.PerformLogPull(ctx, &CatchUpDecision{Strategy: DELTA_SYNC, PeerNodeID: 2, PeerAddr: seed.addr}, lp, local.client))

	self, ok := local.registry.Get(1)
	require.True(t, ok)
	require.NotEqual(t, NodeStatus_JOINING, self.Status, "a current node was marked JOINING")
}
