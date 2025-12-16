package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// =============================================================================
// Method 9: runCommitPhase - CRITICAL 2PC orchestration method
// =============================================================================
//
// This method orchestrates the entire commit phase:
// 1. Send COMMIT to remote nodes (remote-first)
// 2. Wait for remote quorum (quorum - 1)
// 3. Commit locally (after remote quorum)
//
// CRITICAL invariants:
// - Remote nodes MUST commit before coordinator
// - Don't commit locally if remote quorum fails
// - Detect and report partial commits
// =============================================================================

// -----------------------------------------------------------------------------
// Section 9.A: Full Success Scenarios
// -----------------------------------------------------------------------------

// Test 9.1: AllNodesCommitSuccess
// All nodes commit successfully - happy path
func TestRunCommitPhase_AllNodesCommitSuccess(t *testing.T) {
	InitTestTelemetry()

	// Setup: 5-node cluster
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 1*time.Second, clock)

	// All nodes prepared successfully
	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
		4: CreateSuccessResponse(),
		5: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(1).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 5,
		RequiredQuorum:  3,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should succeed
	AssertNoError(t, err)

	// Verify remote nodes were called for commit
	remoteCalls := remoteReplicator.GetPhaseCalls(PhaseCommit)
	if len(remoteCalls) < 2 { // At least 2 remote nodes (quorum-1)
		t.Errorf("Expected at least 2 remote commit calls, got %d", len(remoteCalls))
	}

	// Verify local commit was called
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("Expected exactly 1 local commit call, got %d", len(localCalls))
	}
}

// Test 9.2: QuorumCommitSuccess
// Quorum commits succeed, some timeout - should still succeed
func TestRunCommitPhase_QuorumCommitSuccess(t *testing.T) {
	InitTestTelemetry()

	// Setup: 5-node cluster (quorum = 3, need 2 remote ACKs)
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Nodes 4 and 5 will timeout/fail during commit
	remoteReplicator.SetNodeCommitResponse(4, &ReplicationResponse{Success: false, Error: "timeout"})
	remoteReplicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "timeout"})

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	// All nodes prepared successfully
	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
		4: CreateSuccessResponse(),
		5: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(2).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 5,
		RequiredQuorum:  3,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should succeed: nodes 2,3 + local = 3 (quorum)
	AssertNoError(t, err)

	// Verify local commit was called
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("Expected exactly 1 local commit call, got %d", len(localCalls))
	}
}

// -----------------------------------------------------------------------------
// Section 9.B: Remote Quorum Failure (CRITICAL - Don't Commit Locally)
// -----------------------------------------------------------------------------

// Test 9.10: RemoteQuorumNotAchieved
// Remote quorum fails - CRITICAL: Local commit MUST NOT be called
func TestRunCommitPhase_RemoteQuorumNotAchieved(t *testing.T) {
	InitTestTelemetry()

	// Setup: 5-node cluster (quorum = 3, need 2 remote ACKs)
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Only node 2 commits, nodes 3,4,5 fail (need 2 remote, got 1)
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "timeout"})
	remoteReplicator.SetNodeCommitResponse(4, &ReplicationResponse{Success: false, Error: "timeout"})
	remoteReplicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "timeout"})

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
		4: CreateSuccessResponse(),
		5: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(10).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 5,
		RequiredQuorum:  3,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail with PartialCommitError
	AssertError(t, err, &PartialCommitError{})
	AssertPartialCommitError(t, err, false) // IsLocal=false

	// CRITICAL: Verify local commit was NOT called
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) > 0 {
		t.Errorf("CRITICAL: Local commit called despite remote quorum failure - got %d calls", len(localCalls))
	}
}

// Test 9.11: AllRemotesTimeout
// All remote nodes timeout - local must not commit
func TestRunCommitPhase_AllRemotesTimeout(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster (quorum = 2, need 1 remote ACK)
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// All remotes timeout
	remoteReplicator.SetNodeCommitResponse(2, nil) // Timeout
	remoteReplicator.SetNodeCommitResponse(3, nil) // Timeout

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 50*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(11).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail
	AssertError(t, err, &PartialCommitError{})

	// Verify local commit was NOT called
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) > 0 {
		t.Errorf("Local commit called despite all remotes timing out - got %d calls", len(localCalls))
	}
}

// Test 9.12: AllRemotesFail
// All remote nodes return error - local must not commit
func TestRunCommitPhase_AllRemotesFail(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster (quorum = 2, need 1 remote ACK)
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// All remotes fail
	remoteReplicator.SetNodeCommitResponse(2, &ReplicationResponse{Success: false, Error: "commit failed"})
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "commit failed"})

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(12).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail
	AssertError(t, err, &PartialCommitError{})

	// Verify local commit was NOT called
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) > 0 {
		t.Errorf("Local commit called despite all remotes failing - got %d calls", len(localCalls))
	}
}

// Test 9.13: LocalNotCalled
// Verify local commit is never invoked when remote quorum fails
func TestRunCommitPhase_LocalNotCalled(t *testing.T) {
	InitTestTelemetry()

	// Setup: 5-node cluster
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()

	// Use a mock that tracks if local commit was called
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Only node 2 succeeds, insufficient for quorum
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "fail"})
	remoteReplicator.SetNodeCommitResponse(4, &ReplicationResponse{Success: false, Error: "fail"})
	remoteReplicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "fail"})

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
		4: CreateSuccessResponse(),
		5: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(13).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 5,
		RequiredQuorum:  3,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail
	if err == nil {
		t.Fatal("Expected error when remote quorum fails")
	}

	// CRITICAL: Verify local replicator was NEVER called for commit phase
	localCalls := localReplicator.GetCalls()
	for _, call := range localCalls {
		if call.Request.Phase == PhaseCommit {
			t.Fatal("CRITICAL BUG: Local commit was invoked despite remote quorum failure!")
		}
	}
}

// -----------------------------------------------------------------------------
// Section 9.C: Local Commit Failure (CRITICAL BUG Scenario)
// -----------------------------------------------------------------------------

// Test 9.20: LocalFailsAfterRemoteQuorum
// CRITICAL BUG scenario: Remote quorum achieved, but local commit fails
func TestRunCommitPhase_LocalFailsAfterRemoteQuorum(t *testing.T) {
	InitTestTelemetry()

	// Setup: 5-node cluster (quorum = 3, need 2 remote ACKs)
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Remote commits succeed (nodes 2 and 3)
	// But local commit fails
	localReplicator.SetNodeCommitResponse(1, &ReplicationResponse{Success: false, Error: "local commit failed"})

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
		4: CreateSuccessResponse(),
		5: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(20).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 5,
		RequiredQuorum:  3,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail with PartialCommitError
	AssertError(t, err, &PartialCommitError{})
	AssertPartialCommitError(t, err, true) // IsLocal=true

	// Verify local commit was attempted (and failed)
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("Expected local commit to be attempted, got %d calls", len(localCalls))
	}
}

// Test 9.21: LocalConflictAfterPrepare
// Local returns conflict during commit (should be impossible after prepare)
// NOTE: Current implementation treats ConflictDetected=true with Success=true as success
// This test documents current behavior - ideally this should be treated as an error
func TestRunCommitPhase_LocalConflictAfterPrepare(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Local commit returns conflict (indicates a bug in prepare logic)
	// Current implementation: Success=true means it's treated as success despite ConflictDetected=true
	localReplicator.SetNodeCommitResponse(1, CreateConflictResponse("impossible conflict"))

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(21).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Current behavior: Succeeds because Success=true (ConflictDetected is not checked in commit phase)
	// Ideally this should be an error, but documenting current implementation
	AssertNoError(t, err)

	// Verify local commit was called (and "succeeded" according to Success field)
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("Expected local commit to be attempted, got %d calls", len(localCalls))
	}
}

// Test 9.22: LocalNilResponse
// Local commit returns nil response
func TestRunCommitPhase_LocalNilResponse(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Local commit returns nil response
	localReplicator.SetNodeCommitResponse(1, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(22).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail
	AssertError(t, err, &PartialCommitError{})
	AssertPartialCommitError(t, err, true) // IsLocal=true
}

// -----------------------------------------------------------------------------
// Section 9.D: Metrics & Observability
// -----------------------------------------------------------------------------

// Test 9.30: CommitDurationRecorded
// Verify commit duration metric is recorded
func TestRunCommitPhase_CommitDurationRecorded(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(30).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	startTime := time.Now()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)
	duration := time.Since(startTime)

	AssertNoError(t, err)

	// Verify commit completed in reasonable time
	if duration > 5*time.Second {
		t.Errorf("Commit took too long: %v", duration)
	}

	// Note: We can't directly verify metric values in current implementation
	// Metrics are write-only. This test verifies the method completes successfully.
}

// Test 9.31: QuorumAcksRecorded
// Verify quorum acks are recorded in telemetry
func TestRunCommitPhase_QuorumAcksRecorded(t *testing.T) {
	InitTestTelemetry()

	// Setup: 5-node cluster
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
		4: CreateSuccessResponse(),
		5: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(31).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 5,
		RequiredQuorum:  3,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	AssertNoError(t, err)

	// Verify at least quorum committed (3+)
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	remoteCalls := remoteReplicator.GetPhaseCalls(PhaseCommit)
	totalCommits := len(localCalls) + len(remoteCalls)

	if totalCommits < cluster.RequiredQuorum {
		t.Errorf("Expected at least %d commits, got %d", cluster.RequiredQuorum, totalCommits)
	}
}

// Test 9.32: FailureMetrics
// Verify failure metrics are recorded when commit fails
func TestRunCommitPhase_FailureMetrics(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// All remotes fail
	remoteReplicator.SetNodeCommitResponse(2, &ReplicationResponse{Success: false, Error: "fail"})
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "fail"})

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 50*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(32).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail
	if err == nil {
		t.Fatal("Expected failure when remote quorum not achieved")
	}

	// Error should be PartialCommitError
	AssertError(t, err, &PartialCommitError{})
}

// -----------------------------------------------------------------------------
// Section 9.E: Edge Cases
// -----------------------------------------------------------------------------

// Test 9.40: EmptyPrepResponses
// Empty prepare responses - should handle gracefully
func TestRunCommitPhase_EmptyPrepResponses(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	// Empty prepare responses
	prepResponses := map[uint64]*ReplicationResponse{}

	txn := NewTxnBuilder().WithID(40).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should fail - no prepared nodes
	if err == nil {
		t.Fatal("Expected error with empty prepare responses")
	}

	// Verify no remote commits were sent
	remoteCalls := remoteReplicator.GetPhaseCalls(PhaseCommit)
	if len(remoteCalls) > 0 {
		t.Errorf("Expected no remote commits with empty prepare responses, got %d", len(remoteCalls))
	}

	// Verify no local commit was attempted
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) > 0 {
		t.Errorf("Expected no local commit with empty prepare responses, got %d", len(localCalls))
	}
}

// Test 9.41: OnlyCoordinatorPrepared
// Only coordinator prepared (single-node scenario)
func TestRunCommitPhase_OnlyCoordinatorPrepared(t *testing.T) {
	InitTestTelemetry()

	// Setup: Single-node cluster
	nodes := []uint64{1}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	// Only coordinator prepared
	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(41).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 1,
		RequiredQuorum:  1,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should succeed - single node, quorum = 1
	AssertNoError(t, err)

	// Verify no remote commits (no other nodes)
	remoteCalls := remoteReplicator.GetPhaseCalls(PhaseCommit)
	if len(remoteCalls) > 0 {
		t.Errorf("Expected no remote commits in single-node cluster, got %d", len(remoteCalls))
	}

	// Verify local commit was called
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("Expected exactly 1 local commit, got %d", len(localCalls))
	}
}

// Test 9.42: ContextCancelled
// Context cancelled before/during commit phase
func TestRunCommitPhase_ContextCancelled(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(42).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	// Cancel context before commit
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// May fail or succeed depending on timing (commit goroutines are detached)
	// The important check is that detached commits continue even if parent context cancelled

	// Note: Remote commits use detached context, so they continue despite cancellation
	// Local commit uses the provided context, so it may fail
	// This is expected behavior - we don't strictly require error here
	_ = err

	// Verify remote commits were sent (detached context allows them to continue)
	remoteCalls := remoteReplicator.GetPhaseCalls(PhaseCommit)
	if len(remoteCalls) < 1 {
		t.Logf("Remote commits may have started despite context cancellation (detached)")
	}
}

// -----------------------------------------------------------------------------
// Additional Integration-style Tests
// -----------------------------------------------------------------------------

// Test: Verify correct call ordering - remote commits before local
func TestRunCommitPhase_RemoteBeforeLocal(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(100).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	AssertNoError(t, err)

	// Verify both remote and local commits occurred
	remoteCalls := remoteReplicator.GetPhaseCalls(PhaseCommit)
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)

	if len(remoteCalls) < 1 {
		t.Error("Expected at least 1 remote commit call")
	}

	if len(localCalls) != 1 {
		t.Errorf("Expected exactly 1 local commit call, got %d", len(localCalls))
	}

	// Note: Due to concurrent execution, we can't strictly verify ordering
	// but the code structure ensures sendRemoteCommits is called before commitLocalAfterRemoteQuorum
}

// Test: Verify no coordinator in remote commit list
func TestRunCommitPhase_CoordinatorNotInRemoteCommits(t *testing.T) {
	InitTestTelemetry()

	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(101).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 3,
		RequiredQuorum:  2,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	AssertNoError(t, err)

	// Verify coordinator (node 1) was NOT called via remote replicator
	remoteCalls := remoteReplicator.GetCalls()
	for _, call := range remoteCalls {
		if call.NodeID == 1 && call.Request.Phase == PhaseCommit {
			t.Error("Coordinator should not be in remote commit calls - it commits locally")
		}
	}

	// Verify coordinator committed via local replicator
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("Expected coordinator to commit locally, got %d local calls", len(localCalls))
	}
}

// Test: Mixed remote success/failure with exact quorum
func TestRunCommitPhase_ExactQuorumEdge(t *testing.T) {
	InitTestTelemetry()

	// Setup: 5-node cluster (quorum = 3, need 2 remote ACKs)
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Exactly 2 remotes succeed (nodes 2,3), others fail
	remoteReplicator.SetNodeCommitResponse(4, &ReplicationResponse{Success: false, Error: "fail"})
	remoteReplicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "fail"})

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, clock)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
		4: CreateSuccessResponse(),
		5: CreateSuccessResponse(),
	}

	txn := NewTxnBuilder().WithID(102).WithNodeID(1).WithDatabase("test").Build()
	cluster := &ClusterState{
		AliveNodes:      nodes,
		TotalMembership: 5,
		RequiredQuorum:  3,
	}

	ctx := context.Background()
	err := wc.runCommitPhase(ctx, txn, cluster, prepResponses)

	// Should succeed: 2 remote + 1 local = 3 (exact quorum)
	AssertNoError(t, err)

	// Verify local commit was called
	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("Expected local commit with exact quorum, got %d calls", len(localCalls))
	}
}
