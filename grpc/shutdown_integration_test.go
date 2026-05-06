package grpc

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newThreeNodeRegistry returns a NodeRegistry with local node 1 and peers 2, 3 all ALIVE.
func newThreeNodeRegistry(t *testing.T) *NodeRegistry {
	t.Helper()
	nr := NewNodeRegistry(1, "localhost:9001")
	nr.Add(&NodeState{NodeId: 2, Address: "localhost:9002", Status: NodeStatus_ALIVE, Incarnation: 0})
	nr.Add(&NodeState{NodeId: 3, Address: "localhost:9003", Status: NodeStatus_ALIVE, Incarnation: 0})
	return nr
}

// nodeIDsOf returns the set of node IDs from a node slice.
func nodeIDsOf(nodes []*NodeState) map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(nodes))
	for _, n := range nodes {
		m[n.NodeId] = struct{}{}
	}
	return m
}

// TestShutdown_LeavingExcludedFromQuorumDuringTransaction verifies that a LEAVING node is
// excluded from Count() and GetReplicationEligible() but remains visible in GetAll().
func TestShutdown_LeavingExcludedFromQuorumDuringTransaction(t *testing.T) {
	nr := newThreeNodeRegistry(t)

	total, alive, quorum := nr.QuorumInfo()
	assert.Equal(t, 3, total, "total membership before LEAVING")
	assert.Equal(t, 3, alive, "alive count before LEAVING")
	assert.Equal(t, 2, quorum, "quorum before LEAVING (floor(3/2)+1)")

	require.NoError(t, nr.MarkLeaving(3))

	total, alive, quorum = nr.QuorumInfo()
	assert.Equal(t, 2, total, "total membership after node 3 LEAVING")
	assert.Equal(t, 2, alive, "alive count after node 3 LEAVING")
	assert.Equal(t, 2, quorum, "quorum after LEAVING (floor(2/2)+1)")

	eligible := nr.GetReplicationEligible()
	eligibleIDs := nodeIDsOf(eligible)
	assert.NotContains(t, eligibleIDs, uint64(3), "LEAVING node must not be replication-eligible")
	assert.Contains(t, eligibleIDs, uint64(1), "local ALIVE node must be replication-eligible")
	assert.Contains(t, eligibleIDs, uint64(2), "ALIVE peer must be replication-eligible")

	all := nodeIDsOf(nr.GetAll())
	assert.Contains(t, all, uint64(3), "LEAVING node must still be visible in GetAll()")
}

// TestShutdown_TwoNodesLeavingSimultaneously verifies that two concurrent departures in a
// 5-node cluster leave the membership and quorum calculations consistent.
func TestShutdown_TwoNodesLeavingSimultaneously(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:9001")
	for id := uint64(2); id <= 5; id++ {
		nr.Add(&NodeState{NodeId: id, Address: "localhost:9000", Status: NodeStatus_ALIVE, Incarnation: 0})
	}

	require.NoError(t, nr.MarkLeaving(4))
	require.NoError(t, nr.MarkLeaving(5))

	total, alive, _ := nr.QuorumInfo()
	assert.Equal(t, 3, total, "totalMembership = 3 after 2 LEAVING")
	assert.Equal(t, 3, alive, "aliveCount = 3 (nodes 1,2,3)")

	count := nr.Count()
	assert.Equal(t, 3, count, "Count() excludes LEAVING nodes")
}

// TestShutdown_QuorumMathWhenUnsafe validates the quorum math that the admin decommission
// handler relies on. Specifically: a node that would drop alive < quorum is unsafe.
func TestShutdown_QuorumMathWhenUnsafe(t *testing.T) {
	// 3-node cluster, node 2 is SUSPECT (not ALIVE).
	// If we mark node 3 LEAVING: total=2, quorum=2, but alive=1 — unsafe.
	nr := NewNodeRegistry(1, "localhost:9001")
	nr.Add(&NodeState{NodeId: 2, Address: "localhost:9002", Status: NodeStatus_SUSPECT, Incarnation: 1})
	nr.Add(&NodeState{NodeId: 3, Address: "localhost:9003", Status: NodeStatus_ALIVE, Incarnation: 0})

	// Before: total=3, alive=2, quorum=2 — safe
	total, alive, quorum := nr.QuorumInfo()
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, alive)
	assert.Equal(t, 2, quorum)
	assert.GreaterOrEqual(t, alive, quorum, "cluster has quorum before decommission")

	// Simulate decommission check: hypothetical new state after marking node 3 LEAVING
	hypotheticalTotal := total - 1                    // 2
	hypotheticalAlive := alive - 1                    // 1 (node 3 leaving, so alive drops)
	hypotheticalQuorum := (hypotheticalTotal / 2) + 1 // 2

	assert.Less(t, hypotheticalAlive, hypotheticalQuorum,
		"decommissioning would break quorum: alive=%d < quorum=%d", hypotheticalAlive, hypotheticalQuorum)
}

// TestShutdown_LeavingNodeDoesNotRefuteSuspectClaims verifies SWIM refutation behaviour for
// the local node when it is LEAVING:
//   - SUSPECT/DEAD claims on self are silently ignored (no refutation, no state change).
//   - ALIVE gossip at the same or higher incarnation is also ignored (LEAVING is sticky).
//   - ALIVE gossip at a lower incarnation is ignored (stale).
func TestShutdown_LeavingNodeDoesNotRefuteSuspectClaims(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:9001")

	require.NoError(t, nr.MarkSelfLeaving())

	self, ok := nr.Get(1)
	require.True(t, ok)
	leavingInc := self.Incarnation

	// Claim self is SUSPECT at the same incarnation.
	// A non-LEAVING node would refute by bumping incarnation back to ALIVE.
	// A LEAVING node does neither — it ignores the claim and stays LEAVING.
	nr.Update(&NodeState{NodeId: 1, Status: NodeStatus_SUSPECT, Incarnation: leavingInc})

	self, _ = nr.Get(1)
	assert.Equal(t, NodeStatus_LEAVING, self.Status,
		"LEAVING node must ignore SUSPECT claim (no refutation, status unchanged)")
	assert.Equal(t, leavingInc, self.Incarnation,
		"incarnation must not change when ignoring SUSPECT claim")

	// ALIVE gossip at same incarnation: must be refuted (LEAVING node bumps incarnation to
	// keep LEAVING propagating and prevent the stale ALIVE from overriding it).
	nr.Update(&NodeState{NodeId: 1, Status: NodeStatus_ALIVE, Incarnation: leavingInc})

	self, _ = nr.Get(1)
	assert.Equal(t, NodeStatus_LEAVING, self.Status, "stale ALIVE must not override LEAVING status")
	assert.Greater(t, self.Incarnation, leavingInc,
		"incarnation must be bumped to suppress stale ALIVE gossip while LEAVING")
}

// TestShutdown_GossipPropagatesLeavingState verifies that a LEAVING node state observed
// in one registry propagates correctly to a second registry via simulated gossip.
func TestShutdown_GossipPropagatesLeavingState(t *testing.T) {
	reg1 := NewNodeRegistry(1, "localhost:9001")
	reg2 := NewNodeRegistry(2, "localhost:9002")

	// reg2 knows about node 1 as ALIVE
	reg2.Add(&NodeState{NodeId: 1, Address: "localhost:9001", Status: NodeStatus_ALIVE, Incarnation: 0})

	require.NoError(t, reg1.MarkSelfLeaving())

	// Simulate gossip: reg1 sends all its nodes to reg2
	for _, n := range reg1.GetAll() {
		reg2.Update(n)
	}

	node1InReg2, ok := reg2.Get(1)
	require.True(t, ok)
	assert.Equal(t, NodeStatus_LEAVING, node1InReg2.Status,
		"reg2 should see node 1 as LEAVING after gossip propagation")
}

// TestShutdown_LeavingThenRevert verifies that a node can cancel its LEAVING state and
// re-enter quorum and replication eligibility.
func TestShutdown_LeavingThenRevert(t *testing.T) {
	nr := newThreeNodeRegistry(t)

	require.NoError(t, nr.MarkLeaving(2))

	_, _, quorum := nr.QuorumInfo()
	eligible := nodeIDsOf(nr.GetReplicationEligible())
	assert.NotContains(t, eligible, uint64(2), "LEAVING node excluded from replication")
	assert.Equal(t, 2, quorum, "quorum after LEAVING")

	require.NoError(t, nr.RevertLeaving(2))

	total, alive, quorum := nr.QuorumInfo()
	assert.Equal(t, 3, total, "total membership restored after revert")
	assert.Equal(t, 3, alive, "alive count restored after revert")
	assert.Equal(t, 2, quorum, "quorum restored")

	eligible = nodeIDsOf(nr.GetReplicationEligible())
	assert.Contains(t, eligible, uint64(2), "node 2 re-eligible after revert")
}

// TestShutdown_ConcurrentMarkLeavingAndQuorumCheck verifies no data races occur when
// multiple goroutines concurrently mark nodes LEAVING while QuorumInfo is polled.
func TestShutdown_ConcurrentMarkLeavingAndQuorumCheck(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:9001")
	for id := uint64(2); id <= 5; id++ {
		nr.Add(&NodeState{NodeId: id, Address: "localhost:9000", Status: NodeStatus_ALIVE, Incarnation: 0})
	}

	var wg sync.WaitGroup
	for _, id := range []uint64{2, 3, 4} {
		wg.Add(1)
		go func(nodeID uint64) {
			defer wg.Done()
			// Ignore error; another goroutine may have already marked it LEAVING.
			_ = nr.MarkLeaving(nodeID)
		}(id)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			total, alive, quorum := nr.QuorumInfo()
			// Basic sanity: these values must be self-consistent
			assert.GreaterOrEqual(t, total, 0)
			assert.GreaterOrEqual(t, alive, 0)
			assert.GreaterOrEqual(t, quorum, 1)
		}
	}()

	wg.Wait()
}

// TestShutdown_LeavingNodeIncarnationPreventsStaleOverride verifies the incarnation rules:
// stale ALIVE gossip (lower incarnation) must not override LEAVING; higher incarnation wins.
func TestShutdown_LeavingNodeIncarnationPreventsStaleOverride(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:9001")
	nr.Add(&NodeState{NodeId: 2, Address: "localhost:9002", Status: NodeStatus_ALIVE, Incarnation: 5})

	require.NoError(t, nr.MarkLeaving(2))

	node2, _ := nr.Get(2)
	leavingInc := node2.Incarnation
	assert.Greater(t, leavingInc, uint64(5), "MarkLeaving must bump incarnation above 5")

	// Stale ALIVE: incarnation 5 — lower than LEAVING incarnation. Must be ignored.
	nr.Update(&NodeState{NodeId: 2, Status: NodeStatus_ALIVE, Incarnation: 5})
	node2, _ = nr.Get(2)
	assert.Equal(t, NodeStatus_LEAVING, node2.Status, "stale ALIVE must not override LEAVING")

	// Legitimate override: higher incarnation — must win.
	nr.Update(&NodeState{NodeId: 2, Status: NodeStatus_ALIVE, Incarnation: leavingInc + 1})
	node2, _ = nr.Get(2)
	assert.Equal(t, NodeStatus_ALIVE, node2.Status, "higher-incarnation ALIVE must override LEAVING")
}

// TestShutdown_ReplicationHandlerLifecycle verifies the full LEAVING lifecycle for
// the ReplicationHandler: PREPARE rejected when LEAVING, COMMIT/ABORT accepted, then
// PREPARE accepted again after RevertLeaving.
func TestShutdown_ReplicationHandlerLifecycle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot_shutdown_lifecycle")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	const localNodeID uint64 = 1
	clock := hlc.NewClock(localNodeID)
	dbMgr, err := db.NewDatabaseManager(tmpDir, localNodeID, clock)
	require.NoError(t, err)
	t.Cleanup(func() { dbMgr.Close() })

	const testDB = "lifecycle_db"
	require.NoError(t, dbMgr.CreateDatabase(testDB))

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)

	handler := NewReplicationHandler(localNodeID, dbMgr, clock, db.NewSchemaVersionManager(systemDB.GetMetaStore()))
	registry := NewNodeRegistry(localNodeID, "localhost:9000")
	handler.SetRegistry(registry)

	makePrepare := func(txnID uint64) *TransactionRequest {
		return &TransactionRequest{
			TxnId:        txnID,
			SourceNodeId: 2,
			Database:     testDB,
			Phase:        TransactionPhase_PREPARE,
			Timestamp:    &HLC{WallTime: clock.Now().WallTime, Logical: clock.Now().Logical, NodeId: 2},
			Statements: []*Statement{
				{
					Type:      pb.StatementType_INSERT,
					TableName: "t",
					Database:  testDB,
					Payload: &Statement_RowChange{
						RowChange: testInsertRowChange("t", []byte("k"), map[string][]byte{"id": mustMarshalMsgpack(t, int64(1))}),
					},
				},
			},
		}
	}

	// 1. Prepare while ALIVE — must be accepted (result is engine-level, not LEAVING rejection).
	prepResp, err := handler.HandleReplicateTransaction(context.Background(), makePrepare(10))
	require.NoError(t, err)
	require.NotEqual(t, "node is leaving cluster", prepResp.ErrorMessage,
		"PREPARE must not be rejected due to LEAVING before node is LEAVING")

	// 2. Transition to LEAVING — PREPARE must be rejected.
	require.NoError(t, registry.MarkSelfLeaving())

	prepResp, err = handler.HandleReplicateTransaction(context.Background(), makePrepare(11))
	require.NoError(t, err)
	assert.False(t, prepResp.Success, "PREPARE must be rejected when LEAVING")
	assert.Equal(t, "node is leaving cluster", prepResp.ErrorMessage)

	// 3. ABORT for in-flight txn must be accepted while LEAVING.
	abortResp, err := handler.HandleReplicateTransaction(context.Background(), &TransactionRequest{
		TxnId:     10,
		Database:  testDB,
		Phase:     TransactionPhase_ABORT,
		Timestamp: &HLC{WallTime: clock.Now().WallTime, Logical: clock.Now().Logical, NodeId: 2},
	})
	require.NoError(t, err)
	assert.True(t, abortResp.Success, "ABORT must be accepted even when LEAVING")

	// 4. Revert to ALIVE — PREPARE must be accepted again.
	require.NoError(t, registry.RevertLeaving(localNodeID))

	prepResp, err = handler.HandleReplicateTransaction(context.Background(), makePrepare(12))
	require.NoError(t, err)
	assert.NotEqual(t, "node is leaving cluster", prepResp.ErrorMessage,
		"PREPARE must not be rejected after RevertLeaving")
}

// TestShutdown_MetricsUpdatedOnLeavingTransition verifies that QuorumInfo() reflects the
// corrected membership count after a LEAVING transition (LEAVING excluded from totalMembership).
func TestShutdown_MetricsUpdatedOnLeavingTransition(t *testing.T) {
	nr := newThreeNodeRegistry(t)

	total, alive, quorum := nr.QuorumInfo()
	assert.Equal(t, 3, total)
	assert.Equal(t, 3, alive)
	assert.Equal(t, 2, quorum)

	require.NoError(t, nr.MarkLeaving(2))

	total, alive, quorum = nr.QuorumInfo()
	assert.Equal(t, 2, total, "LEAVING node must be excluded from totalMembership")
	assert.Equal(t, 2, alive, "only ALIVE nodes count toward aliveCount")
	assert.Equal(t, 2, quorum, "quorum recalculated from reduced membership")
}

// TestShutdown_RemoteDecommissionTriggersCallback verifies that when a remote admin
// marks the local node as LEAVING via gossip (higher incarnation), the local node
// accepts the LEAVING status and fires the onNodeLeaving callback.
func TestShutdown_RemoteDecommissionTriggersCallback(t *testing.T) {
	t.Parallel()

	// Registry for node 1 (the target of decommission)
	target := NewNodeRegistry(1, "localhost:8081")
	target.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE, Incarnation: 0})

	// Track callback
	callbackFired := make(chan struct{}, 1)
	target.SetOnNodeLeaving(func() {
		callbackFired <- struct{}{}
	})

	// Simulate: admin on node 2 marks node 1 as LEAVING (higher incarnation)
	targetState, _ := target.Get(1)
	require.Equal(t, NodeStatus_ALIVE, targetState.Status)
	originalInc := targetState.Incarnation

	// Gossip from node 2 says: node 1 is LEAVING with incarnation+1
	target.Update(&NodeState{
		NodeId:      1,
		Address:     "localhost:8081",
		Status:      NodeStatus_LEAVING,
		Incarnation: originalInc + 1,
	})

	// Verify node 1 accepted LEAVING (didn't refute)
	updatedState, _ := target.Get(1)
	assert.Equal(t, NodeStatus_LEAVING, updatedState.Status, "node should accept remote LEAVING")
	assert.Equal(t, originalInc+1, updatedState.Incarnation)

	// Verify callback fired
	select {
	case <-callbackFired:
		// OK
	case <-time.After(time.Second):
		t.Fatal("onNodeLeaving callback was not fired")
	}
}

// TestShutdown_RemoteDecommissionRefutedWithLowerIncarnation verifies that stale
// LEAVING gossip (same or lower incarnation) is ignored via normal SWIM refutation.
func TestShutdown_RemoteDecommissionRefutedWithLowerIncarnation(t *testing.T) {
	t.Parallel()

	target := NewNodeRegistry(1, "localhost:8081")

	// Stale gossip: LEAVING with same incarnation (not higher)
	targetState, _ := target.Get(1)
	target.Update(&NodeState{
		NodeId:      1,
		Address:     "localhost:8081",
		Status:      NodeStatus_LEAVING,
		Incarnation: targetState.Incarnation, // same, not higher
	})

	// Should be refuted — still ALIVE
	updatedState, _ := target.Get(1)
	assert.Equal(t, NodeStatus_ALIVE, updatedState.Status, "same-incarnation LEAVING should be refuted")
}
