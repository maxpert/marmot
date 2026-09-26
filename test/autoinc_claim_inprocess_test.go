//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/db/snapshot"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// This file drives the AUTO_INCREMENT range-claim protocol
// (coordinator.WriteCoordinator.ClaimRange, coordinator/autoinc_claim.go;
// db.ReplicationEngine.prepareAutoIncClaim, db/replication_engine.go:310)
// through the REAL coordinator and REAL participant engines, replacing only
// the gRPC transport. test/autoinc_claim_cluster_test.go correctly found
// that the multi-process harness (test/crash_recovery_test.go) cannot drive
// these five tests: nothing reachable over the MySQL wire calls ClaimRange
// yet (the range allocator is not wired). This fixture is the honest substitute:
// three real db.DatabaseManager + db.LocalReplicator + coordinator.WriteCoordinator
// triples, wired to each other by an in-process fanout instead of gRPC.

// prepareRecord is one fanout-observed PREPARE round-trip for a node.
type prepareRecord struct {
	req  *coordinator.ReplicationRequest
	resp *coordinator.ReplicationResponse
	err  error
}

// fanoutReplicator implements coordinator.Replicator (coordinator/write_coordinator.go:35)
// by routing a ReplicateTransaction call to the target node's own
// *db.LocalReplicator (db/local_replicator.go:20). It is used as every
// WriteCoordinator's remote `replicator`; each coordinator's own
// `localReplicator` is its own node's LocalReplicator directly, matching
// how WriteCoordinator.executePreparePhase dispatches to self
// (coordinator/write_coordinator.go:779-781) versus otherNodes
// (coordinator/write_coordinator.go:774-776) - so a coordinator's own
// PREPARE/COMMIT of itself never passes through this fanout, only the calls
// it makes to its peers do.
//
// It also records every PREPARE round per node, mutex-guarded because
// ClaimRange fans PREPARE out to all nodes concurrently
// (coordinator/write_coordinator.go:774-781), so tests can assert a stale
// proposal was actually REJECTED by a specific participant rather than
// merely superseded.
type fanoutReplicator struct {
	mu           sync.Mutex
	nodes        map[uint64]*db.LocalReplicator
	prepareCalls map[uint64][]prepareRecord
	unreachable  map[uint64]bool
}

func newFanoutReplicator() *fanoutReplicator {
	return &fanoutReplicator{
		nodes:        make(map[uint64]*db.LocalReplicator),
		prepareCalls: make(map[uint64][]prepareRecord),
		unreachable:  make(map[uint64]bool),
	}
}

// setUnreachable makes every call to nodeID fail as a transport error, so the
// node misses whatever the cluster decides meanwhile.
func (f *fanoutReplicator) setUnreachable(nodeID uint64, unreachable bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreachable[nodeID] = unreachable
}

func (f *fanoutReplicator) register(nodeID uint64, lr *db.LocalReplicator) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[nodeID] = lr
}

func (f *fanoutReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	f.mu.Lock()
	lr := f.nodes[nodeID]
	unreachable := f.unreachable[nodeID]
	f.mu.Unlock()
	if unreachable {
		return nil, fmt.Errorf("node %d unreachable", nodeID)
	}

	resp, err := lr.ReplicateTransaction(ctx, nodeID, req)

	if req.Phase == coordinator.PhasePrep {
		f.mu.Lock()
		f.prepareCalls[nodeID] = append(f.prepareCalls[nodeID], prepareRecord{req: req, resp: resp, err: err})
		f.mu.Unlock()
	}

	return resp, err
}

// prepareCallCount is a checkpoint marker: tests snapshot this before an
// operation and inspect only the records after it via prepareCallsSince.
func (f *fanoutReplicator) prepareCallCount(nodeID uint64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.prepareCalls[nodeID])
}

func (f *fanoutReplicator) prepareCallsSince(nodeID uint64, since int) []prepareRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	all := f.prepareCalls[nodeID]
	if since >= len(all) {
		return nil
	}
	out := make([]prepareRecord, len(all)-since)
	copy(out, all[since:])
	return out
}

// fixedNodeProvider implements coordinator.NodeProvider (coordinator/node_provider.go:6)
// with a node set that changes only when a test grows the cluster
// (setNodes); the interface's four methods are all that is needed. HasSeedNodes is false: this is a
// legitimate fixed-size deployment, not a node still learning its peers
// (coordinator/cluster.go:90's quorum-of-one guard), matching
// coordinator's own mockNodeProvider zero value
// (coordinator/full_replication_test.go:15-18).
type fixedNodeProvider struct {
	mu    sync.Mutex
	nodes []uint64
}

func (p *fixedNodeProvider) GetAliveNodes() ([]uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint64(nil), p.nodes...), nil
}

func (p *fixedNodeProvider) GetClusterSize() int { return p.GetTotalMembershipSize() }

func (p *fixedNodeProvider) GetTotalMembershipSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.nodes)
}

func (p *fixedNodeProvider) HasSeedNodes() bool { return false }

// setNodes replaces the membership every node sees.
func (p *fixedNodeProvider) setNodes(nodes []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodes = append([]uint64(nil), nodes...)
}

// inprocNode is one participant: its own data directory, clock, DatabaseManager,
// LocalReplicator (db/local_replicator.go:20, which owns the real
// *db.ReplicationEngine) and WriteCoordinator (coordinator/write_coordinator.go:117).
type inprocNode struct {
	id      uint64
	dataDir string
	clock   *hlc.Clock
	dm      *db.DatabaseManager
	lr      *db.LocalReplicator
	wc      *coordinator.WriteCoordinator
}

// claimStore wraps this node's own system database as the AUTO_INCREMENT
// claim store (db/autoinc_claim.go:108, db/database_manager.go:504).
func (n *inprocNode) claimStore() *db.AutoIncClaimStore {
	return db.NewAutoIncClaimStore(n.dm.GetSystemDatabase())
}

// inprocCluster is three inprocNodes sharing one fanoutReplicator and one
// fixedNodeProvider.
type inprocCluster struct {
	t        *testing.T
	provider *fixedNodeProvider
	fanout   *fanoutReplicator
	nodes    map[uint64]*inprocNode
}

const inprocCoordinatorTimeout = 30 * time.Second

// newInprocCluster builds nodeIDs participants, each with its own t.TempDir()
// data directory (db/replication_engine_test.go:64-78's own-tempdir
// pattern). It registers a cleanup that closes whatever DatabaseManager is
// current for each node at test end, so a test that reopens a node (restart,
// meta wipe, snapshot restore) is still cleaned up correctly.
func newInprocCluster(t *testing.T, nodeIDs []uint64) *inprocCluster {
	t.Helper()
	c := &inprocCluster{
		t:        t,
		provider: &fixedNodeProvider{nodes: append([]uint64(nil), nodeIDs...)},
		fanout:   newFanoutReplicator(),
		nodes:    make(map[uint64]*inprocNode),
	}
	for _, id := range nodeIDs {
		c.nodes[id] = c.buildNode(id, t.TempDir())
	}
	c.releaseNewCluster()
	t.Cleanup(func() {
		for _, n := range c.nodes {
			_ = n.dm.Close()
		}
	})
	return c
}

func (c *inprocCluster) buildNode(id uint64, dataDir string) *inprocNode {
	c.t.Helper()
	clock := hlc.NewClock(id)
	dm, err := db.NewDatabaseManager(dataDir, id, clock)
	require.NoError(c.t, err)
	// Every node counts the cluster exactly as every claimant does.
	dm.SetClusterMembership(c.provider.GetTotalMembershipSize)
	lr := db.NewLocalReplicator(id, dm, clock)
	wc := coordinator.NewWriteCoordinator(id, c.provider, c.fanout, lr, inprocCoordinatorTimeout, clock)
	c.fanout.register(id, lr)
	return &inprocNode{id: id, dataDir: dataDir, clock: clock, dm: dm, lr: lr, wc: wc}
}

// releaseNewCluster releases the votes of a cluster whose every node is held
// because it initialised its system database: once every member answers
// held, each releases with no bases (grpc.autoIncMergeSafe, all-held case),
// which is what the merge loop does in a node process. A cluster reopened
// over existing claim history has no held node and is left alone.
func (c *inprocCluster) releaseNewCluster() {
	c.t.Helper()
	for _, n := range c.nodes {
		held, err := n.dm.AutoIncVotesHeld()
		require.NoError(c.t, err)
		if !held {
			return
		}
	}
	for _, n := range c.nodes {
		require.NoError(c.t, n.dm.MergeAutoIncBasesAndReleaseVotes(nil))
	}
}

// closeNode stops a node's DatabaseManager without reopening it.
func (c *inprocCluster) closeNode(id uint64) {
	c.t.Helper()
	require.NoError(c.t, c.nodes[id].dm.Close())
}

// reopenNodeFresh rebuilds a node's DatabaseManager, LocalReplicator and
// WriteCoordinator from its (possibly just-mutated) data directory and
// rewires the shared fanout, WITHOUT closing anything first - the caller
// must already have stopped and prepared the data directory. It reuses the
// node's own hlc.Clock: only storage identity is being reopened.
func (c *inprocCluster) reopenNodeFresh(id uint64) {
	c.t.Helper()
	old := c.nodes[id]
	fresh := c.buildNode(id, old.dataDir)
	fresh.clock = old.clock
	c.nodes[id] = fresh
}

// reopenNode closes then reopens a node from the same data directory -
// db.NewDatabaseManager(dataDir, ...) (db/database_manager.go:54) against a
// directory a prior DatabaseManager already populated, exactly like a
// process restart's storage-level effect. It does not model kill -9: a
// crash skips Close's graceful shutdown, which this cannot reach from
// inside one process. See TestClaimRange_RestartRecoversCommittedBase's doc
// comment.
func (c *inprocCluster) reopenNode(id uint64) {
	c.closeNode(id)
	c.reopenNodeFresh(id)
}

// newInprocClusterFromDirs rebuilds an inprocCluster from data directories a
// prior process already populated - one per node ID - instead of creating
// fresh t.TempDir() storage the way newInprocCluster does. This is what a
// node process reopening its own on-disk state, potentially from a separate
// os.Process that no longer exists, looks like from the test's side; it is
// how TestClaimRange_KillNineRecoversCommittedBase inspects a cluster after
// killing the child process that built it.
func newInprocClusterFromDirs(t *testing.T, dataDirs map[uint64]string) *inprocCluster {
	t.Helper()
	nodeIDs := make([]uint64, 0, len(dataDirs))
	for id := range dataDirs {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	c := &inprocCluster{
		t:        t,
		provider: &fixedNodeProvider{nodes: nodeIDs},
		fanout:   newFanoutReplicator(),
		nodes:    make(map[uint64]*inprocNode),
	}
	for _, id := range nodeIDs {
		c.nodes[id] = c.buildNode(id, dataDirs[id])
	}
	c.releaseNewCluster()
	t.Cleanup(func() {
		for _, n := range c.nodes {
			_ = n.dm.Close()
		}
	})
	return c
}

// driveDDL replicates DDL through one coordinator's WriteTransaction
// (coordinator/write_coordinator.go:139), the real 2PC path that also seeds
// __marmot__autoinc via db.TransactionManager.applyNonDMLIntents ->
// seedAutoIncBasesForDDL (db/autoinc_seed.go:97, wired by
// DatabaseManager.wireGCCoordination at db/database_manager.go:199).
func driveDDL(t *testing.T, node *inprocNode, database, table, ddl string) {
	t.Helper()
	startTS := node.clock.Now()
	txn := &coordinator.Transaction{
		ID:               startTS.ToTxnID(),
		NodeID:           node.id,
		StartTS:          startTS,
		Database:         database,
		WriteConsistency: protocol.ConsistencyQuorum,
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       ddl,
			TableName: table,
			Database:  database,
		}},
	}
	require.NoError(t, node.wc.WriteTransaction(context.Background(), txn))
}

// waitForBase polls a node's own claim store until it reads `want` for
// (database, table) or a 5s deadline passes. Polling replaces a sleep: the
// commit phase only waits for (quorum-1) remote ACKs before returning
// (coordinator/write_coordinator.go:623,636-639,663-668), so a coordinator's
// WriteTransaction / ClaimRange can return success before every node -
// specifically the one node whose ACK it did not wait for - has actually
// applied the commit.
func waitForBase(t *testing.T, node *inprocNode, database, table string, want uint64) {
	t.Helper()
	store := node.claimStore()
	deadline := time.Now().Add(5 * time.Second)
	var last uint64
	var lastErr error
	for {
		last, lastErr = store.ReadBase(database, table)
		if lastErr == nil && last == want {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, lastErr, "node %d: final ReadBase(%s.%s)", node.id, database, table)
	require.Equal(t, want, last, "node %d: base for %s.%s did not converge in time", node.id, database, table)
}

// waitForQuorumBase polls until at least minNodes of nodes read `want` for
// (database, table), or a 5s deadline passes.
//
// This is intentionally weaker than waitForBase's "every node" check, and
// only TestClaimRange_ConcurrentBoundary uses it. When two claimants
// genuinely race - PREPARE rounds literally overlapping, not one committing
// before the other starts - it is possible, and was observed directly in
// this fixture, for the LOSING side of one round's write-write conflict
// (db/meta_store_pebble.go:1109-1124, the ordinary MVCC lock on the shared
// intent key, protocol/autoinc_claim.go:53's AutoIncClaimKey) to be a
// participant that PREPARED and locked an EARLIER, different attempt but
// was not part of THAT attempt's winning quorum - so it never receives a
// COMMIT (it wasn't in prepResponses for the round it rejected) or an ABORT
// for the round it silently lost (WriteTransaction only calls
// abortTransaction when PREPARE fails outright, coordinator/write_coordinator.go:178-181,
// not when quorum succeeds without unanimity). That participant is now a
// genuine straggler on this key: coordinator/write_coordinator.go's own
// top-of-file design notes name "Straggler catch-up: Dead nodes sync via
// snapshots + delta logs" as a SEPARATE mechanism from the write path
// itself, and this minimal in-process fixture wires no gossip or
// anti-entropy to perform it. QUORUM (majority) is what
// protocol.ConsistencyQuorum actually promises
// (coordinator/quorum.go, coordinator/cluster.go:100); asserting literally
// every node converges after true concurrent contention asserts a stronger
// guarantee than the system gives, so this test checks the quorum the
// coordinator itself required (RequiredQuorum for 3 nodes = 2, coordinator/cluster.go:100).
func waitForQuorumBase(t *testing.T, nodes []*inprocNode, database, table string, want uint64, minNodes int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastCount int
	for {
		lastCount = 0
		for _, n := range nodes {
			if b, err := n.claimStore().ReadBase(database, table); err == nil && b == want {
				lastCount++
			}
		}
		if lastCount >= minNodes {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.GreaterOrEqual(t, lastCount, minNodes,
		"only %d/%d nodes converged to %d for %s.%s within the deadline", lastCount, len(nodes), want, database, table)
}

// rangesDisjoint reports whether two claimed ranges (newBase, newBase+size]
// overlap. A claim's owned ids are newBase+1 .. newBase+size
// (db/autoinc_claim.go:229's doc: "The claimant owns newBase+1 .. newBase+size").
func rangesDisjoint(newBase1, size1, newBase2, size2 uint64) bool {
	lo1, hi1 := newBase1+1, newBase1+size1
	lo2, hi2 := newBase2+1, newBase2+size2
	return hi1 < lo2 || hi2 < lo1
}

const markedTableDDL = `CREATE TABLE %s (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)`

// setupClaimCluster builds a 3-node in-process cluster, creates `database` on
// every node directly - CreateDatabase is not itself replicated by DDL, so
// each node's own DatabaseManager.CreateDatabase must be called explicitly -
// then drives the marked-table DDL through node
// 1's coordinator so all three nodes apply it and each seeds its own claim
// row through the real DDL-time seeding path. It verifies the seed landed on
// all three nodes before returning, which is also the fixture's proof that
// driving DDL through WriteTransaction DOES seed correctly - no fallback to
// direct Seed() calls was needed.
func setupClaimCluster(t *testing.T, database, table string) *inprocCluster {
	t.Helper()
	c := newInprocCluster(t, []uint64{1, 2, 3})
	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, c.nodes[id].dm.CreateDatabase(database))
	}
	assertAbsentClaimRowBackfilled(t, c, database, table+"_absent")
	assertWidthCeilingRejected(t, c, database, table+"_narrow")
	driveDDL(t, c.nodes[1], database, table, sprintfDDL(table))
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], database, table, 0)
	}
	return c
}

// narrowMarkedTableDDL declares an 8-bit signed AUTO_INCREMENT column, whose
// ceiling (intmarker.Attributes.WidthMax, protocol/query/transform/intmarker/intmarker.go:57-65)
// is 2^7-1 = 127.
const narrowMarkedTableDDL = `CREATE TABLE %s (id INTEGER /*M:8a*/ PRIMARY KEY, v TEXT)`

// assertWidthCeilingRejected creates a table with an 8-bit marked column
// (ceiling 127) through the normal replicated-DDL-plus-seeding path, then
// asserts a claim whose range exceeds that ceiling is rejected.
//
// This is run ahead of every one of the five tests for the same reason
// assertAbsentClaimRowBackfilled is: every one of prepareAutoIncClaim's
// rejection clauses needs at least one assertion that isolates it, or a
// mutation to that specific clause can slip through five tests whose own
// claim sizes never approach the (very large, 32-bit) ceiling their main
// table uses.
//
// Mutation: a regression that drops prepareAutoIncClaim's
// `newBase+size <= widthMax` clause (db/replication_engine.go:366-372) -
// nothing else in the condition depends on the column's width - would
// wrongly accept a range of 200 against a 127-ceiling column, where this
// assertion expects rejection.
func assertWidthCeilingRejected(t *testing.T, c *inprocCluster, database, table string) {
	t.Helper()
	driveDDL(t, c.nodes[1], database, table, strings.Replace(narrowMarkedTableDDL, "%s", table, 1))
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], database, table, 0)
	}

	mark2 := c.fanout.prepareCallCount(2)
	_, _, err := c.nodes[1].wc.ClaimRange(context.Background(), database, table, 0, fixedClaimSize(200))
	require.Error(t, err, "a claim whose range exceeds the column's width ceiling must be rejected")

	round1Node2 := c.fanout.prepareCallsSince(2, mark2)
	require.NotEmpty(t, round1Node2)
	require.False(t, round1Node2[0].resp.Success)
	require.True(t, round1Node2[0].resp.Rejected)
	require.Contains(t, round1Node2[0].resp.Error, "exhausts the column",
		"rejection must name the width-ceiling cause, not some other failure")
}

func sprintfDDL(table string) string {
	return strings.Replace(markedTableDDL, "%s", table, 1)
}

// assertAbsentClaimRowBackfilled creates `table` directly on every node's
// own SQLite connection - bypassing the coordinator's replicated DDL and its
// automatic claim-row seeding (db/autoinc_seed.go's seedAutoIncBasesForDDL)
// - with ids 1..5 already in it, so the table has a valid, width-resolvable
// schema on every node but NO claim row anywhere. A claim proposing base 0
// must then be rejected with the base each participant backfills from its
// own table (5), and the retry above it granted.
//
// It is run ahead of every one of the five tests (setupClaimCluster is their
// shared entry point) because "the claim row does not exist yet" is a state
// a table created before claim rows existed passes through.
//
// Mutation: a regression that treats an absent claim row as base 0 instead
// of backfilling it would accept the round-1 proposal at base 0, over ids
// 1..5, where this assertion expects a rejection carrying base 5.
func assertAbsentClaimRowBackfilled(t *testing.T, c *inprocCluster, database, table string) {
	t.Helper()
	for _, id := range []uint64{1, 2, 3} {
		mdb, err := c.nodes[id].dm.GetDatabase(database)
		require.NoError(t, err)
		_, err = mdb.GetWriteDB().Exec(sprintfDDL(table))
		require.NoError(t, err)
		for row := 1; row <= 5; row++ {
			_, err = mdb.GetWriteDB().Exec(fmt.Sprintf("INSERT INTO %s (id, v) VALUES (?, 'x')", table), row)
			require.NoError(t, err)
		}
		require.NoError(t, mdb.ReloadSchema())
	}

	mark2 := c.fanout.prepareCallCount(2)
	newBase, _, err := c.nodes[1].wc.ClaimRange(context.Background(), database, table, 0, fixedClaimSize(1))

	// The vote is checked before the claim's outcome: a participant that read
	// the absent row as 0 would accept here, and only a later COMMIT would
	// then fail, which must not be what reports it.
	round1Node2 := c.fanout.prepareCallsSince(2, mark2)
	require.NotEmpty(t, round1Node2)
	require.False(t, round1Node2[0].resp.Success,
		"a claim against a table with a real schema but no claim row must never be accepted as base 0")
	require.True(t, round1Node2[0].resp.Rejected)
	require.Equal(t, uint64(5), round1Node2[0].resp.AutoIDStoredBase,
		"the rejection must carry the base backfilled from the table's own ids")

	require.NoError(t, err, "a claim against a table with no claim row must backfill and be granted above the table's ids")
	require.Equal(t, uint64(5), newBase, "the range must start above the ids the table already holds")
}

// ---------------------------------------------------------------------
// Test 1: Sequential claim safety (the primary acceptance test).
// ---------------------------------------------------------------------

// TestClaimRange_SequentialSafety is THE primary acceptance test. Node 1
// claims [1,100]. Node 2 then proposes the same starting base from a stale
// view (prevBase=0, which was already true before node 1's claim landed).
// Node 2's coordinator PREPAREs against all three participants, including
// its own node - every participant's prepareAutoIncClaim
// (db/replication_engine.go:310) evaluates storedBase(=100) <= prevBase(=0),
// which is false, so every participant, including node 2 itself, rejects.
// Node 2's own rejection makes this a LocalPrepareError
// (coordinator/errors.go:34), which coordinator/write_coordinator.go:869-875
// treats as deterministic and returns immediately without needing quorum to
// fail first. ClaimRange (coordinator/autoinc_claim.go:62) then retries
// above the reported base (100) and succeeds, landing [101,150].
//
// Mutation: a broken storedBase<=prevBase staleness check in
// prepareAutoIncClaim (db/replication_engine.go) would let node 2's stale
// round-1 proposal be accepted instead of rejected. The assertion this test
// pins that such a regression would fail is: node 2's round-1 PREPARE
// recorded on node 1 must be an explicit Rejected=true response carrying
// AutoIDStoredBase=100, and node 1's own range [1,100] must still read back
// as owned after node 2 commits.
func TestClaimRange_SequentialSafety(t *testing.T) {
	c := setupClaimCluster(t, "testdb", "seqsafety")

	// Node 1 claims [1,100].
	newBase1, granted1, err := c.nodes[1].wc.ClaimRange(context.Background(), "testdb", "seqsafety", 0, fixedClaimSize(100))
	require.NoError(t, err)
	require.Equal(t, uint64(0), newBase1)
	require.Equal(t, uint64(100), granted1)
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "seqsafety", 100)
	}

	// Node 2 proposes the SAME stale prevBase=0, unaware of node 1's claim.
	mark1 := c.fanout.prepareCallCount(1)
	mark3 := c.fanout.prepareCallCount(3)
	newBase2, granted2, err := c.nodes[2].wc.ClaimRange(context.Background(), "testdb", "seqsafety", 0, fixedClaimSize(50))
	require.NoError(t, err, "node 2's retry must still succeed after the stale round is rejected")
	require.Equal(t, uint64(100), newBase2, "node 2 must retry starting at node 1's committed base, not spin on 0")
	require.Equal(t, uint64(50), granted2)

	// Assert the FIRST round was actually rejected by the other participants,
	// each carrying the real stored base (100), not merely superseded by a
	// later successful round.
	round1Node1 := c.fanout.prepareCallsSince(1, mark1)
	require.NotEmpty(t, round1Node1)
	require.False(t, round1Node1[0].resp.Success, "node 1 must have rejected node 2's stale round-1 proposal")
	require.True(t, round1Node1[0].resp.Rejected)
	require.Equal(t, uint64(100), round1Node1[0].resp.AutoIDStoredBase)

	round1Node3 := c.fanout.prepareCallsSince(3, mark3)
	require.NotEmpty(t, round1Node3)
	require.False(t, round1Node3[0].resp.Success, "node 3 must have rejected node 2's stale round-1 proposal")
	require.True(t, round1Node3[0].resp.Rejected)
	require.Equal(t, uint64(100), round1Node3[0].resp.AutoIDStoredBase)

	// Node 2's retry must be a SECOND round against these same participants.
	require.Len(t, c.fanout.prepareCallsSince(1, mark1), 2, "node 2's claim must retry exactly once against node 1")
	require.Len(t, c.fanout.prepareCallsSince(3, mark3), 2, "node 2's claim must retry exactly once against node 3")

	// The two granted ranges are disjoint - node 1's [1,100] was not silently
	// overwritten by node 2's claim.
	require.True(t, rangesDisjoint(newBase1, granted1, newBase2, granted2),
		"node 1's range [%d,%d] and node 2's range [%d,%d] must be disjoint",
		newBase1+1, newBase1+granted1, newBase2+1, newBase2+granted2)

	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "seqsafety", 150)
	}
}

// ---------------------------------------------------------------------
// Test 2: Concurrent claims at a range boundary.
// ---------------------------------------------------------------------

type claimOutcome struct {
	claimant uint64
	newBase  uint64
	granted  uint64
	err      error
}

// maxLockWaitClientRetries bounds claimRangeRetryingOnLockWait's own retry
// loop, exactly as maxClaimAttempts bounds ClaimRange's internal one
// (coordinator/autoinc_claim.go:16) - a caller always gets a definitive
// answer instead of spinning against sustained contention.
const maxLockWaitClientRetries = 10

// claimRangeRetryingOnLockWait calls ClaimRange and, on the specific
// "lock wait timeout" signal (MySQL 1205, protocol.ErrCodeLockTimeout),
// retries the WHOLE call rather than treating it as final.
//
// This is necessary, and is not a workaround for a bug: when two
// claimants' PREPARE rounds genuinely overlap in time on the same node
// (true concurrency, not one committing before the other starts), the
// SECOND one to arrive does not see prepareAutoIncClaim's own
// storedBase-vs-prevBase staleness check (db/replication_engine.go:352-355) -
// storedBase is still whatever it was before either claim, since neither has
// committed yet. It instead hits the ordinary MVCC write-write-conflict
// check on the shared intent key (db/transaction.go's WriteIntent, the same
// mechanism that serialises two concurrent DML statements on one row),
// which carries no AutoIDStoredBase. ClaimRange's own retry loop
// (coordinator/autoinc_claim.go:102-105) only retries a failure that
// carries a usable base; a bare write-write conflict does not, so
// ClaimRange correctly ends its own attempt loop immediately and returns
// 1205 - "the standard signal this codebase already uses for retry me"
// (coordinator/autoinc_claim.go:53-54). Retrying the call is exactly what a
// real client (or the range allocator that will wrap ClaimRange) does on
// that signal; this helper stands in for that caller.
func claimRangeRetryingOnLockWait(ctx context.Context, node *inprocNode, database, table string, prevBase, size uint64) (newBase, granted uint64, err error) {
	for attempt := 0; attempt < maxLockWaitClientRetries; attempt++ {
		newBase, granted, err = node.wc.ClaimRange(ctx, database, table, prevBase, fixedClaimSize(size))
		if err == nil {
			return newBase, granted, nil
		}
		var mysqlErr *protocol.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Code != protocol.ErrCodeLockTimeout {
			return 0, 0, err
		}
	}
	return 0, 0, err
}

// TestClaimRange_ConcurrentBoundary runs node 1's and node 2's ClaimRange
// concurrently from the same stale prevBase=0 view, in two goroutines.
// Neither goroutine calls t.Fatal/require directly - both send their
// outcome over a channel, and every assertion runs back on the test
// goroutine.
//
// Each goroutine calls ClaimRange through claimRangeRetryingOnLockWait, not
// ClaimRange directly: when both PREPARE rounds genuinely overlap, the loser
// hits the ordinary MVCC write-write conflict on the shared intent key
// rather than prepareAutoIncClaim's own staleness check (neither side has
// committed yet, so there is no base to be stale against), and that failure
// carries no retry base for ClaimRange's own internal loop to use - it
// returns the retryable MySQL 1205 (lock wait timeout) instead. Both calls
// succeeding here is therefore not "neither call ever errors": one or both
// sides legitimately receive 1205 first and claimRangeRetryingOnLockWait
// retries the whole call past it (up to maxLockWaitClientRetries times). See
// the helper's doc for the full mechanism. The convergence check below also
// only requires a QUORUM of nodes (2 of 3), not all three: the participant
// that lost the lock race on an earlier attempt is left out of that
// attempt's COMMIT and can end up a genuine straggler on this key - see
// waitForQuorumBase's doc.
//
// Mutation: a broken storedBase<=prevBase staleness check in
// prepareAutoIncClaim (db/replication_engine.go) would let a claimant retry
// from an already-superseded prevBase=0 and succeed instead of being
// rejected - the rangesDisjoint assertion below is what catches the
// resulting overlap.
func TestClaimRange_ConcurrentBoundary(t *testing.T) {
	c := setupClaimCluster(t, "testdb", "concurrent")

	const size1, size2 = 30, 20
	ch := make(chan claimOutcome, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		nb, g, err := claimRangeRetryingOnLockWait(context.Background(), c.nodes[1], "testdb", "concurrent", 0, size1)
		ch <- claimOutcome{claimant: 1, newBase: nb, granted: g, err: err}
	}()
	go func() {
		defer wg.Done()
		nb, g, err := claimRangeRetryingOnLockWait(context.Background(), c.nodes[2], "testdb", "concurrent", 0, size2)
		ch <- claimOutcome{claimant: 2, newBase: nb, granted: g, err: err}
	}()
	wg.Wait()
	close(ch)

	results := make(map[uint64]claimOutcome, 2)
	for r := range ch {
		results[r.claimant] = r
	}
	require.Len(t, results, 2)

	r1, r2 := results[1], results[2]
	require.NoError(t, r1.err, "node 1's concurrent claim must succeed")
	require.NoError(t, r2.err, "node 2's concurrent claim must succeed")
	require.Equal(t, uint64(size1), r1.granted)
	require.Equal(t, uint64(size2), r2.granted)

	require.True(t, rangesDisjoint(r1.newBase, r1.granted, r2.newBase, r2.granted),
		"concurrent claims must land on disjoint ranges: node1=[%d,%d] node2=[%d,%d]",
		r1.newBase+1, r1.newBase+r1.granted, r2.newBase+1, r2.newBase+r2.granted)

	// The final stored base must equal the starting base (0) plus BOTH sizes -
	// one range stacked above the other, never one silently dropped - on at
	// least a QUORUM of nodes (2 of 3). See waitForQuorumBase's doc for why
	// this test checks quorum rather than every node: true concurrent
	// contention can legitimately leave the loser of one round's write-write
	// conflict as an uncaught-up straggler on this key, which is a property
	// of the write-coordinator's quorum-commit design (no synchronous
	// straggler catch-up), not a claim-protocol defect.
	allNodes := []*inprocNode{c.nodes[1], c.nodes[2], c.nodes[3]}
	waitForQuorumBase(t, allNodes, "testdb", "concurrent", size1+size2, 2)
}

// ---------------------------------------------------------------------
// Test 3: Restart recovers the committed base.
// ---------------------------------------------------------------------

// TestClaimRange_RestartRecoversCommittedBase claims a range, then closes
// node 3's DatabaseManager and reopens it from the SAME data directory
// (db.NewDatabaseManager, db/database_manager.go:54), rebuilding its
// LocalReplicator and rewiring the fanout. It asserts the reopened node's
// stored base is the committed one, and that a subsequent claim (issued by
// node 1, so it must go through node 3 as a remote participant, proving node
// 3 itself - not some cached view - is voting correctly) starts strictly
// ABOVE it rather than reissuing the committed range.
//
// This covers only the GRACEFUL half of "stop then restart": dm.Close() runs
// SQLite's and Pebble's normal shutdown path, which a real process kill
// skips entirely. TestClaimRange_KillNineRecoversCommittedBase (below)
// covers that other half with a real SIGKILL, sent to a re-exec'd child
// process - there is no way to signal only part of one Go process.
//
// Mutation: the same broken storedBase<=prevBase staleness check that
// TestClaimRange_SequentialSafety pins fires here too, but the interesting
// failure mode this test adds is a regression that read node 3's base from
// something other than its own freshly reopened SQLite file (e.g. a cached
// in-memory value surviving the swap) - the subsequent claim's round-1
// rejection carrying the wrong base would expose that.
func TestClaimRange_RestartRecoversCommittedBase(t *testing.T) {
	c := setupClaimCluster(t, "testdb", "restart")

	newBase, granted, err := c.nodes[1].wc.ClaimRange(context.Background(), "testdb", "restart", 0, fixedClaimSize(40))
	require.NoError(t, err)
	require.Equal(t, uint64(0), newBase)
	require.Equal(t, uint64(40), granted)
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "restart", 40)
	}

	c.reopenNode(3)

	base, err := c.nodes[3].claimStore().ReadBase("testdb", "restart")
	require.NoError(t, err)
	require.Equal(t, uint64(40), base, "reopened node 3 must read back the committed base from its own SQLite file")

	// A subsequent claim from node 1, issued with a stale prevBase=0, must be
	// rejected (including by node 3, over the rewired fanout) and retry above
	// 40 - never reissue [1,40].
	mark3 := c.fanout.prepareCallCount(3)
	newBase2, granted2, err := c.nodes[1].wc.ClaimRange(context.Background(), "testdb", "restart", 0, fixedClaimSize(25))
	require.NoError(t, err)
	require.Equal(t, uint64(40), newBase2, "subsequent claim must start above the base node 3 recovered on reopen")
	require.Equal(t, uint64(25), granted2)

	round1Node3 := c.fanout.prepareCallsSince(3, mark3)
	require.NotEmpty(t, round1Node3)
	require.False(t, round1Node3[0].resp.Success)
	require.True(t, round1Node3[0].resp.Rejected)
	require.Equal(t, uint64(40), round1Node3[0].resp.AutoIDStoredBase, "reopened node 3 must vote with its real recovered base")

	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "restart", 65)
	}
}

// ---------------------------------------------------------------------
// Test 3b: a real kill -9, via a re-exec'd child process.
// ---------------------------------------------------------------------

const (
	// killNineDataRootEnv, when set in the environment, marks the current
	// process as the child TestClaimRange_KillNineRecoversCommittedBase
	// re-execs, and names the directory its three node data directories
	// live under.
	killNineDataRootEnv = "MARMOT_KILLNINE_DATA_ROOT"
	killNineDatabase    = "testdb"
	killNineTable       = "killnine"
	killNineClaimSize   = 20
	// killNineChildTimeout bounds every wait on the child process, so a
	// child that never reports a claim - or never dies after SIGKILL -
	// cannot hang the suite.
	killNineChildTimeout = 45 * time.Second
)

// killNineNodeDir is the data directory a node with the given ID uses under
// root, shared by the parent (which creates it) and the child (which opens
// it) so both agree on the layout without passing more than one env var.
func killNineNodeDir(root string, id uint64) string {
	return filepath.Join(root, fmt.Sprintf("node%d", id))
}

// TestClaimRange_KillNineRecoversCommittedBase covers the half
// TestClaimRange_RestartRecoversCommittedBase cannot: recovery after a real
// SIGKILL, not a graceful dm.Close(). There is no way to signal only part of
// one Go process, so this re-execs the current test binary as a child
// (TestClaimRangeKillNineChild), which builds its own 3-node in-process
// cluster over data directories this parent creates, commits one claim,
// reports it on stdout, then blocks forever. The parent waits for that
// report, sends SIGKILL, confirms the child died by signal, then reopens all
// three nodes from the same directories and asserts the committed base and
// the claim protocol's staleness rejection both survived the kill.
func TestClaimRange_KillNineRecoversCommittedBase(t *testing.T) {
	root := t.TempDir()
	dataDirs := make(map[uint64]string, 3)
	for _, id := range []uint64{1, 2, 3} {
		dir := killNineNodeDir(root, id)
		require.NoError(t, os.MkdirAll(dir, 0755))
		dataDirs[id] = dir
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestClaimRangeKillNineChild$", "-test.v")
	cmd.Env = append(os.Environ(), killNineDataRootEnv+"="+root)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start(), "start child process")

	claimLine := make(chan string, 1)
	scanDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "CLAIMED ") {
				claimLine <- line
				return
			}
		}
		scanDone <- scanner.Err()
	}()

	var line string
	select {
	case line = <-claimLine:
	case scanErr := <-scanDone:
		_ = cmd.Process.Kill()
		t.Fatalf("child exited before reporting a claim: %v\nstderr:\n%s", scanErr, stderr.String())
	case <-time.After(killNineChildTimeout):
		_ = cmd.Process.Kill()
		t.Fatalf("timed out waiting for child to report a claim\nstderr:\n%s", stderr.String())
	}

	var newBase, granted uint64
	if _, err := fmt.Sscanf(line, "CLAIMED %d %d", &newBase, &granted); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("unparseable child output line %q: %v", line, err)
	}
	require.Equal(t, uint64(0), newBase)
	require.Equal(t, uint64(killNineClaimSize), granted)

	require.NoError(t, cmd.Process.Kill(), "SIGKILL child")

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case waitErr := <-waitDone:
		var exitErr *exec.ExitError
		require.ErrorAs(t, waitErr, &exitErr, "child must exit with a non-nil error after SIGKILL")
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		require.True(t, ok, "unexpected process exit status type %T", exitErr.Sys())
		require.True(t, status.Signaled(), "child must have died by signal, exit status: %v", status)
		require.Equal(t, syscall.SIGKILL, status.Signal(), "child must have died specifically from SIGKILL")
	case <-time.After(killNineChildTimeout):
		t.Fatalf("timed out waiting for killed child to exit\nstderr:\n%s", stderr.String())
	}

	c := newInprocClusterFromDirs(t, dataDirs)
	for _, id := range []uint64{1, 2, 3} {
		base, err := c.nodes[id].claimStore().ReadBase(killNineDatabase, killNineTable)
		require.NoError(t, err)
		require.Equal(t, newBase+granted, base, "node %d must recover the committed base after a real SIGKILL", id)
	}

	// A stale claim (prevBase=0) must be rejected and retried above the
	// recovered base, not reissue [1,20].
	newBase2, granted2, err := c.nodes[1].wc.ClaimRange(context.Background(), killNineDatabase, killNineTable, 0, fixedClaimSize(15))
	require.NoError(t, err)
	require.Equal(t, newBase+granted, newBase2, "post-kill claim must start above the recovered base, not reissue the same range")
	require.Equal(t, uint64(15), granted2)
}

// TestClaimRangeKillNineChild is the child TestClaimRange_KillNineRecoversCommittedBase
// re-execs to receive a real SIGKILL. It never participates in a normal test
// run: it skips unless killNineDataRootEnv is set, which only that parent
// test sets, on the child it starts.
func TestClaimRangeKillNineChild(t *testing.T) {
	root := os.Getenv(killNineDataRootEnv)
	if root == "" {
		t.Skip("only runs as the re-exec'd child of TestClaimRange_KillNineRecoversCommittedBase")
	}

	dataDirs := make(map[uint64]string, 3)
	for _, id := range []uint64{1, 2, 3} {
		dataDirs[id] = killNineNodeDir(root, id)
	}
	c := newInprocClusterFromDirs(t, dataDirs)
	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, c.nodes[id].dm.CreateDatabase(killNineDatabase))
	}
	driveDDL(t, c.nodes[1], killNineDatabase, killNineTable, sprintfDDL(killNineTable))
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], killNineDatabase, killNineTable, 0)
	}

	newBase, granted, err := c.nodes[1].wc.ClaimRange(context.Background(), killNineDatabase, killNineTable, 0, fixedClaimSize(killNineClaimSize))
	require.NoError(t, err)
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], killNineDatabase, killNineTable, newBase+granted)
	}

	// The parent watches stdout for exactly this line, then sends SIGKILL -
	// so nothing after it may run any cleanup a graceful shutdown would.
	fmt.Printf("CLAIMED %d %d\n", newBase, granted)
	select {}
}

// ---------------------------------------------------------------------
// Test 4: Meta-store wipe.
// ---------------------------------------------------------------------

// metaPebbleDirFor returns the Pebble meta directory path for a database
// file, using the SAME derivation db.DatabaseManager itself uses:
// strings.TrimSuffix(<db file path>, ".db") + "_meta.pebble"
// (db/database_manager.go:431, and identically at :851 for cleanupMetaStoreFiles).
func metaPebbleDirFor(dbFilePath string) string {
	return strings.TrimSuffix(dbFilePath, ".db") + "_meta.pebble"
}

// TestClaimRange_SurvivesMetaStoreWipe claims a range, then deletes ONLY
// node 3's Pebble meta directories - both the system database's
// (dataDir/__marmot_system_meta.pebble, derived from the system db path
// db/database_manager.go:90) and the user database's (derived from
// dm.GetDatabasePath, db/database_manager.go:649, before closing) - while
// leaving every .db file untouched, then reopens that node. Because the
// committed AUTO_INCREMENT base lives in a SQLite table in the system
// database (db/autoinc_claim.go:19-34's doc: "deliberately does NOT live in
// Pebble"), wiping Pebble state must not make node 3 forget it: it must
// still vote with its REAL base, rejecting a stale proposal.
//
// Mutation: a broken storedBase<=prevBase staleness check in
// prepareAutoIncClaim (db/replication_engine.go) would let node 2's stale
// round-1 proposal be accepted by node 3 instead of rejected - the
// assertion that node 3's round-1 PREPARE response is Rejected=true with
// AutoIDStoredBase=10 is what catches that, proving node 3 votes with its
// real base recovered from SQLite, not from the wiped Pebble state.
func TestClaimRange_SurvivesMetaStoreWipe(t *testing.T) {
	c := setupClaimCluster(t, "testdb", "metawipe")

	newBase, granted, err := c.nodes[1].wc.ClaimRange(context.Background(), "testdb", "metawipe", 0, fixedClaimSize(10))
	require.NoError(t, err)
	require.Equal(t, uint64(0), newBase)
	require.Equal(t, uint64(10), granted)
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "metawipe", 10)
	}

	node3 := c.nodes[3]
	systemDBPath := filepath.Join(node3.dataDir, db.SystemDatabaseName+".db")
	userDBPath, err := node3.dm.GetDatabasePath("testdb")
	require.NoError(t, err)
	systemMetaDir := metaPebbleDirFor(systemDBPath)
	userMetaDir := metaPebbleDirFor(userDBPath)

	c.closeNode(3)
	require.NoError(t, os.RemoveAll(systemMetaDir))
	require.NoError(t, os.RemoveAll(userMetaDir))
	// The .db files themselves must survive untouched.
	require.FileExists(t, systemDBPath)
	require.FileExists(t, userDBPath)
	c.reopenNodeFresh(3)
	// node3 was replaced in the cluster map by reopenNodeFresh; re-fetch it.
	node3 = c.nodes[3]

	base, err := node3.claimStore().ReadBase("testdb", "metawipe")
	require.NoError(t, err)
	require.Equal(t, uint64(10), base, "node 3 must still read its real committed base after its Pebble meta store was wiped")

	// A stale proposal from node 2 must be rejected BY NODE 3 specifically,
	// carrying its real base.
	mark3 := c.fanout.prepareCallCount(3)
	newBase2, granted2, err := c.nodes[2].wc.ClaimRange(context.Background(), "testdb", "metawipe", 0, fixedClaimSize(5))
	require.NoError(t, err)
	require.Equal(t, uint64(10), newBase2)
	require.Equal(t, uint64(5), granted2)

	round1Node3 := c.fanout.prepareCallsSince(3, mark3)
	require.NotEmpty(t, round1Node3)
	require.False(t, round1Node3[0].resp.Success, "node 3 must reject the stale round-1 proposal after its meta store was wiped")
	require.True(t, round1Node3[0].resp.Rejected)
	require.Equal(t, uint64(10), round1Node3[0].resp.AutoIDStoredBase)

	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "metawipe", 15)
	}
}

// ---------------------------------------------------------------------
// Test 5: Full-cluster snapshot restore.
// ---------------------------------------------------------------------

// TestClaimRange_SurvivesFullClusterSnapshotRestore snapshots all three
// nodes (db.DatabaseManager.TakeSnapshotToDir, db/database_manager.go:989),
// stops all three, wipes their data directories entirely (including Pebble
// meta state), restores each node's files from its own snapshot, and
// restarts all three. It asserts the committed base survives on every node
// and the next claim starts above it - proving the base's deliberate
// placement in SQLite rather than Pebble (db/autoinc_claim.go:33's doc: "a
// restored node has no Pebble state, so a full-cluster restore would make
// every voter a yes-man at once") actually holds when every node is restored
// at once, not just one.
//
// Mutation: a broken storedBase<=prevBase staleness check in
// prepareAutoIncClaim (db/replication_engine.go) would let the post-restore
// stale claim (prevBase=0) reissue [1,20] instead of being rejected and
// retried above the restored base - the assertion that the retried claim
// starts at 20, not 0, is what catches that, on every node at once since a
// full-cluster restore leaves no node with pre-restore state to fall back
// on.
func TestClaimRange_SurvivesFullClusterSnapshotRestore(t *testing.T) {
	c := setupClaimCluster(t, "testdb", "snaprestore")

	newBase, granted, err := c.nodes[1].wc.ClaimRange(context.Background(), "testdb", "snaprestore", 0, fixedClaimSize(20))
	require.NoError(t, err)
	require.Equal(t, uint64(0), newBase)
	require.Equal(t, uint64(20), granted)
	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "snaprestore", 20)
	}

	snapDirByNode := make(map[uint64]string, 3)
	snapshotsByNode := make(map[uint64][]db.SnapshotInfo, 3)

	// Snapshot every node BEFORE any of them stop.
	for _, id := range []uint64{1, 2, 3} {
		snapDir := t.TempDir()
		snapshots, _, _, err := c.nodes[id].dm.TakeSnapshotToDir(snapDir)
		require.NoError(t, err)
		require.NotEmpty(t, snapshots)
		snapDirByNode[id] = snapDir
		snapshotsByNode[id] = snapshots
	}

	// Stop all three, then wipe each node's data directory entirely.
	for _, id := range []uint64{1, 2, 3} {
		c.closeNode(id)
	}
	for _, id := range []uint64{1, 2, 3} {
		dataDir := c.nodes[id].dataDir
		require.NoError(t, os.RemoveAll(dataDir))
		require.NoError(t, os.MkdirAll(dataDir, 0755))
	}

	// Restore each node's files into its fresh, empty directory through the
	// SAME production restorer a real node uses to apply a snapshot
	// (db/snapshot/restorer.go) - not a fixture-only file copy. Each node gets
	// back its OWN snapshot, its own claim history, so the restorer runs
	// without the system-database merge hook: that hook belongs to catching
	// up from a PEER (grpc/catch_up.go), and there it holds a node with no
	// local system database until it merges bases from a majority
	// (db.AutoIncHoldTable) - which in a cluster where every node was rebuilt
	// that way nobody could ever supply. The manager is fully closed at this
	// point, not live, exactly like a node coming up against a freshly
	// provisioned data directory, so no ConnectionManager is needed.
	for _, id := range []uint64{1, 2, 3} {
		dataDir := c.nodes[id].dataDir
		files := make([]snapshot.DatabaseFileInfo, 0, len(snapshotsByNode[id]))
		for _, info := range snapshotsByNode[id] {
			files = append(files, snapshot.DatabaseFileInfo{
				Name:           info.Name,
				Filename:       info.Filename,
				SizeBytes:      info.Size,
				SHA256Checksum: info.SHA256,
			})
		}
		r := snapshot.NewRestorer(dataDir, nil)
		require.NoError(t, r.RestoreFiles(snapDirByNode[id], files))
	}

	// Restart all three.
	for _, id := range []uint64{1, 2, 3} {
		c.reopenNodeFresh(id)
	}

	for _, id := range []uint64{1, 2, 3} {
		base, err := c.nodes[id].claimStore().ReadBase("testdb", "snaprestore")
		require.NoError(t, err)
		require.Equal(t, uint64(20), base, "node %d must recover its committed base from the restored snapshot", id)
	}

	// A stale claim, from node 1, must retry above the restored base rather
	// than reissue [1,20].
	newBase2, granted2, err := c.nodes[1].wc.ClaimRange(context.Background(), "testdb", "snaprestore", 0, fixedClaimSize(15))
	require.NoError(t, err)
	require.Equal(t, uint64(20), newBase2, "post-restore claim must start above the restored base, not reissue [1,20]")
	require.Equal(t, uint64(15), granted2)

	for _, id := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[id], "testdb", "snaprestore", 35)
	}
}

// fixedClaimSize is a RangeSizer asking for the same size above any base:
// these tests pin the claim protocol, not the allocator's sizing policy.
func fixedClaimSize(size uint64) id.RangeSizer {
	return func(uint64) (uint64, error) { return size, nil }
}
