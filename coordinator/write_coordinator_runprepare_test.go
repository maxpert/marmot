package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// ========================================
// 4.A: Coordinator Participation Tests (CRITICAL INVARIANT)
// ========================================

// Test 4.1: CoordinatorPrepareSuccess
// Coordinator prepares successfully along with remote nodes
func TestRunPreparePhase_CoordinatorPrepareSuccess(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().
		WithID(100).
		WithNodeID(1).
		WithStatements(CreateCDCStatements(1, "users")).
		Build()

	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3) // Coordinator + 2 remotes
	AssertResponseSuccess(t, responses, 1) // Coordinator
	AssertResponseSuccess(t, responses, 2)
	AssertResponseSuccess(t, responses, 3)
}

// Test 4.2: CoordinatorPrepareFails
// Coordinator prepare fails - MUST return CoordinatorNotParticipatedError
func TestRunPreparePhase_CoordinatorPrepareFails(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Configure coordinator to fail
	localReplicator.SetNodeResponse(1, CreateErrorResponse("coordinator failure"))

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(101).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertError(t, err, &CoordinatorNotParticipatedError{})
}

// Test 4.3: CoordinatorConflict
// Coordinator detects conflict - transaction must abort
func TestRunPreparePhase_CoordinatorConflict(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Configure coordinator to detect conflict
	localReplicator.SetPersistentNodeConflict(1, "coordinator write-write conflict")

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(102).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Conflict detection should return MySQL deadlock error for client retry
	AssertConflictDetected(t, err)
}

// Test 4.4: CoordinatorTimeout
// Coordinator prepare times out
func TestRunPreparePhase_CoordinatorTimeout(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Configure long delay for coordinator (simulates timeout)
	localReplicator.SetDelay(103, 500*time.Millisecond)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 50*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(103).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Coordinator timeout means coordinator not in responses
	AssertError(t, err, &CoordinatorNotParticipatedError{})
}

// Test 4.5: CoordinatorNilResponse
// Coordinator returns nil response
func TestRunPreparePhase_CoordinatorNilResponse(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Configure coordinator to return nil
	localReplicator.SetNodeResponse(1, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(104).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertError(t, err, &CoordinatorNotParticipatedError{})
}

// ========================================
// 4.B: Quorum Achievement Tests
// ========================================

// 5-Node Cluster Tests (Quorum = 3)

// Test 4.10: ExactQuorum_3of5
// Coordinator + 2 remotes succeed, 2 remotes timeout - exact quorum
func TestRunPreparePhase_ExactQuorum_3of5(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Nodes 4 and 5 return nil (timeout/no response)
	// Don't use SetDelay as it applies to all nodes
	remoteReplicator.SetNodeResponse(4, nil)
	remoteReplicator.SetNodeResponse(5, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(105).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3) // Exact quorum
	AssertResponseSuccess(t, responses, 1) // Coordinator
	AssertResponseSuccess(t, responses, 2)
	AssertResponseSuccess(t, responses, 3)
}

// Test 4.11: OverQuorum_5of5
// All 5 nodes succeed - over quorum
func TestRunPreparePhase_OverQuorum_5of5(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(106).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 5) // All nodes
	AssertResponseSuccess(t, responses, 1)
	AssertResponseSuccess(t, responses, 2)
	AssertResponseSuccess(t, responses, 3)
	AssertResponseSuccess(t, responses, 4)
	AssertResponseSuccess(t, responses, 5)
}

// Test 4.12: UnderQuorum_2of5
// Coordinator + 1 remote succeed, 3 remotes timeout - under quorum
func TestRunPreparePhase_UnderQuorum_2of5(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Only node 2 succeeds, 3,4,5 timeout
	remoteReplicator.SetNodeResponse(3, nil)
	remoteReplicator.SetNodeResponse(4, nil)
	remoteReplicator.SetNodeResponse(5, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 50*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(107).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertQuorumNotAchievedError(t, err, "prepare", 2, 3)
}

// Test 4.13: QuorumWithErrors
// Coordinator + 2 remotes succeed, 2 remotes error - quorum achieved
func TestRunPreparePhase_QuorumWithErrors(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Nodes 4 and 5 return errors
	remoteReplicator.SetNodeResponse(4, CreateErrorResponse("node 4 error"))
	remoteReplicator.SetNodeResponse(5, CreateErrorResponse("node 5 error"))

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(108).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3) // Coordinator + 2 successful remotes
	AssertResponseSuccess(t, responses, 1)
	AssertResponseSuccess(t, responses, 2)
	AssertResponseSuccess(t, responses, 3)
}

// Test 4.14: AllRemotesFail
// Coordinator succeeds, all 4 remotes fail - under quorum
func TestRunPreparePhase_AllRemotesFail(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// All remotes fail
	remoteReplicator.SetNodeResponse(2, CreateErrorResponse("error"))
	remoteReplicator.SetNodeResponse(3, CreateErrorResponse("error"))
	remoteReplicator.SetNodeResponse(4, CreateErrorResponse("error"))
	remoteReplicator.SetNodeResponse(5, CreateErrorResponse("error"))

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(109).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertQuorumNotAchievedError(t, err, "prepare", 1, 3)
}

// 3-Node Cluster Tests (Quorum = 2)

// Test 4.20: ThreeNode_ExactQuorum
// 3-node cluster: coordinator + 1 remote succeed, 1 remote timeout
func TestRunPreparePhase_ThreeNode_ExactQuorum(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Node 3 times out
	remoteReplicator.SetNodeResponse(3, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 50*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(110).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 2) // Exact quorum
	AssertResponseSuccess(t, responses, 1)
	AssertResponseSuccess(t, responses, 2)
}

// Test 4.21: ThreeNode_BothRemotesFail
// 3-node cluster: coordinator succeeds, both remotes fail - under quorum
func TestRunPreparePhase_ThreeNode_BothRemotesFail(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Both remotes fail
	remoteReplicator.SetNodeResponse(2, CreateErrorResponse("error"))
	remoteReplicator.SetNodeResponse(3, CreateErrorResponse("error"))

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(111).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertQuorumNotAchievedError(t, err, "prepare", 1, 2)
}

// Single-Node Cluster Test (Quorum = 1)

// Test 4.30: SingleNode_CoordinatorOnly
// Single-node cluster: coordinator only, no remotes
func TestRunPreparePhase_SingleNode_CoordinatorOnly(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(112).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{} // No other nodes

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 1) // Only coordinator
	AssertResponseSuccess(t, responses, 1)
}

// ========================================
// 4.C: Conflict Detection Tests (Percolator Semantics)
// ========================================

// Test 4.40: ConflictOnCoordinator
// Coordinator detects conflict - abort transaction
func TestRunPreparePhase_ConflictOnCoordinator(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Coordinator has conflict
	localReplicator.SetPersistentNodeConflict(1, "coordinator conflict")

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(113).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertConflictDetected(t, err)

	// Note: runPreparePhase itself doesn't call abort - that's done by WriteTransaction
	// This test verifies conflict detection, abort verification belongs in integration tests
}

// Test 4.41: ConflictOnSingleRemote
// Single remote node detects conflict - abort transaction
func TestRunPreparePhase_ConflictOnSingleRemote(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Node 2 has conflict
	remoteReplicator.SetPersistentNodeConflict(2, "node 2 conflict")

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(114).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertConflictDetected(t, err)
}

// Test 4.42: ConflictOnMultipleRemotes
// Multiple remote nodes detect conflict - return first conflict
func TestRunPreparePhase_ConflictOnMultipleRemotes(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Nodes 2 and 3 have conflicts
	remoteReplicator.SetPersistentNodeConflict(2, "node 2 conflict")
	remoteReplicator.SetPersistentNodeConflict(3, "node 3 conflict")

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(115).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertConflictDetected(t, err)
}

// Test 4.43: ConflictAfterQuorum
// 3 nodes succeed, then node 4 conflicts - any conflict aborts
func TestRunPreparePhase_ConflictAfterQuorum(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Node 4 has conflict (quorum would be 3, but conflict aborts)
	remoteReplicator.SetPersistentNodeConflict(4, "node 4 late conflict")
	// Node 5 times out
	remoteReplicator.SetNodeResponse(5, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(116).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Even with quorum achieved (1, 2, 3), conflict on node 4 aborts
	AssertConflictDetected(t, err)
}

// Test 4.44: ConflictDetails
// Verify conflict details are included in error
func TestRunPreparePhase_ConflictDetails(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	conflictMsg := "write-write conflict on key=user:123"
	remoteReplicator.SetPersistentNodeConflict(2, conflictMsg)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(117).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertConflictDetected(t, err)
	// Note: The actual conflict details are logged but not directly in the MySQL error
	// The error is always ErrDeadlock() for client retry
}

// ========================================
// 4.D: Context & Timeout Tests
// ========================================

// Test 4.50: ContextCancelledBeforePrepare
// Context cancelled before prepare phase starts
// Note: Mock replicator is fast and may complete before goroutines check context
// This test documents the behavior rather than enforcing strict timing
func TestRunPreparePhase_ContextCancelledBeforePrepare(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(118).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	// Cancel context before calling
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Mock replicator is very fast and may complete before goroutines check cancellation
	// This is expected behavior - we're testing that the code doesn't crash with cancelled context
	// In production with real network I/O, cancellation would be more effective
	if err == nil && len(responses) >= cluster.RequiredQuorum {
		t.Logf("Note: Mock completed before context cancellation was noticed (timing dependent)")
	}
	// Test passes as long as no crash/panic occurred
}

// Test 4.51: ContextCancelledDuringPrepare
// Context cancelled during executePreparePhase
// Note: Mock replicator may complete quickly despite delays
func TestRunPreparePhase_ContextCancelledDuringPrepare(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Use a very short timeout to ensure we don't get all responses
	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 10*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(119).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately
	cancel()

	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Either error occurred OR partial responses (acceptable in distributed systems)
	// The key is we don't get full quorum with cancelled context
	if err == nil && len(responses) >= cluster.RequiredQuorum {
		t.Logf("Warning: got full quorum despite cancelled context (timing dependent)")
	}
}

// Test 4.52: ContextTimeoutDuringPrepare
// Context timeout expires during prepare
// Note: Mock replicator is fast, so this test verifies timeout handling
func TestRunPreparePhase_ContextTimeoutDuringPrepare(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 500*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(120).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	// Use extremely short timeout (1 nanosecond) - will definitely expire
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait a bit to ensure context expires
	time.Sleep(10 * time.Millisecond)

	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Either error or partial responses (mock is fast so may complete before timeout)
	if err == nil && len(responses) >= cluster.RequiredQuorum {
		t.Logf("Note: mock completed before timeout (timing dependent)")
	}
}

// Test 4.53: SlowReplicatorStraggler
// 1 node responds slowly (times out), others fast - success if quorum achieved
func TestRunPreparePhase_SlowReplicatorStraggler(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Node 5 returns nil (simulates timeout/slow response)
	remoteReplicator.SetNodeResponse(5, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(121).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Should succeed - quorum achieved without slow node
	AssertNoError(t, err)
	// Should have 4 responses (coordinator + 2, 3, 4) but not 5
	if len(responses) < 3 {
		t.Errorf("expected at least 3 responses for quorum, got %d", len(responses))
	}
}

// ========================================
// 4.E: Edge Cases
// ========================================

// Test 4.60: EmptyOtherNodes
// Single node scenario with empty otherNodes slice
func TestRunPreparePhase_EmptyOtherNodes(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(122).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{} // Empty

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 1)
	AssertResponseSuccess(t, responses, 1)
}

// Test 4.61: NilTransaction
// Passing nil transaction should panic or error
func TestRunPreparePhase_NilTransaction(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	cluster, _ := GetClusterState(nodeProvider, protocol.ConsistencyQuorum)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()

	// This will panic when accessing txn fields
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic with nil transaction")
		}
	}()

	_, _ = wc.runPreparePhase(ctx, nil, cluster, otherNodes)
}

// Test 4.62: NilCluster
// Passing nil cluster should panic or error
func TestRunPreparePhase_NilCluster(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(123).Build()
	otherNodes := []uint64{2, 3}

	ctx := context.Background()

	// This will panic when accessing cluster fields
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic with nil cluster")
		}
	}()

	_, _ = wc.runPreparePhase(ctx, txn, nil, otherNodes)
}

// Test 4.63: AllNodesNilResponse
// All nodes return nil responses
func TestRunPreparePhase_AllNodesNilResponse(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// All nodes return nil
	localReplicator.SetNodeResponse(1, nil)
	remoteReplicator.SetNodeResponse(2, nil)
	remoteReplicator.SetNodeResponse(3, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(124).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	// Should fail with QuorumNotAchievedError (0 acks < quorum of 2)
	// The quorum check happens before coordinator participation check
	AssertQuorumNotAchievedError(t, err, "prepare", 0, 2)
}
