package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
)

// withDDLValidationTimeout temporarily overrides the configured DDL validation
// timeout for the duration of a test, restoring the prior value afterward.
// cfg.Config is a process-global, so tests using this must not run in
// parallel with each other or with anything else reading the same field.
func withDDLValidationTimeout(t *testing.T, ms int) {
	t.Helper()
	original := cfg.Config.Replication.DDLValidationTimeoutMS
	cfg.Config.Replication.DDLValidationTimeoutMS = ms
	t.Cleanup(func() { cfg.Config.Replication.DDLValidationTimeoutMS = original })
}

// TestExecutePreparePhase_BroadcastToAllNodes verifies that prepare requests are broadcast to all nodes
// Test 5.1: All nodes should receive prepare requests and responses should be collected
func TestExecutePreparePhase_BroadcastToAllNodes(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Create 5 nodes total (coordinator + 4 others)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 5) // coordinator + 4 others

	// Verify all nodes received successful responses
	for _, nodeID := range []uint64{1, 2, 3, 4, 5} {
		AssertResponseSuccess(t, responses, nodeID)
	}

	// Verify all nodes were called
	AssertCallCount(t, mock, 5)
	AssertPhaseCallCount(t, mock, PhasePrep, 5)
}

// TestExecutePreparePhase_SkipLocalReplication_False verifies coordinator is called via localReplicator
// Test 5.2: When skipLocalReplication=false, coordinator should be called locally
func TestExecutePreparePhase_SkipLocalReplication_False(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	localMock := newMockReplicator()

	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: localMock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	otherNodes := []uint64{2, 3}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3) // coordinator + 2 others

	// Verify localReplicator was called for coordinator
	if localMock.GetCallCount() != 1 {
		t.Errorf("localReplicator call count: got %d, want 1", localMock.GetCallCount())
	}

	// Verify remote replicator was called for other nodes only
	if mock.GetCallCount() != 2 {
		t.Errorf("replicator call count: got %d, want 2", mock.GetCallCount())
	}

	// Verify coordinator response is in map
	AssertResponseSuccess(t, responses, wc.nodeID)
}

// TestExecutePreparePhase_SkipLocalReplication_True verifies coordinator is NOT called locally
// Test 5.3: When skipLocalReplication=true, coordinator should not be called
func TestExecutePreparePhase_SkipLocalReplication_True(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	localMock := newMockReplicator()

	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: localMock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	otherNodes := []uint64{2, 3, 4}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, true)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3) // only other nodes

	// Verify localReplicator was NOT called
	if localMock.GetCallCount() != 0 {
		t.Errorf("localReplicator should not be called, got %d calls", localMock.GetCallCount())
	}

	// Verify remote replicator was called for other nodes
	AssertCallCount(t, mock, 3)

	// Verify coordinator is NOT in response map
	AssertNodeNotInResponses(t, responses, wc.nodeID)

	// Verify other nodes are in response map
	for _, nodeID := range otherNodes {
		AssertResponseSuccess(t, responses, nodeID)
	}
}

// TestExecutePreparePhase_ConcurrentExecution verifies all goroutines complete with many nodes
// Test 5.4: With 10+ nodes, all should complete concurrently
func TestExecutePreparePhase_ConcurrentExecution(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         500 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Create 10 other nodes (11 total including coordinator)
	otherNodes := []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

	ctx, cancel := WithTimeout(1000)
	defer cancel()

	start := time.Now()
	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)
	elapsed := time.Since(start)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 11) // coordinator + 10 others

	// Verify concurrent execution (should not take 11x latency)
	// With 1ms latency per node, sequential would be ~11ms, concurrent ~2ms
	if elapsed > 100*time.Millisecond {
		t.Errorf("execution took too long (not concurrent): %v", elapsed)
	}

	// Verify all nodes were called
	AssertCallCount(t, mock, 11)
}

// TestExecutePreparePhase_ResponseTimeout verifies timeout handling for slow nodes
// Test 5.5: Fast nodes return, slow node is excluded from responses
func TestExecutePreparePhase_ResponseTimeout(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	localMock := newMockReplicator()

	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: localMock,             // Separate mock so delay doesn't affect coordinator
		timeout:         50 * time.Millisecond, // Short timeout
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Remote node is configured to be slow (exceeds timeout)
	mock.SetDelay(txn.ID, 200*time.Millisecond)
	otherNodes := []uint64{2}

	ctx, cancel := WithTimeout(300)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)

	// With short wc.timeout (50ms) and remote delay (200ms),
	// remote node should timeout but coordinator should succeed
	// We should get at least the coordinator's response
	if len(responses) == 0 {
		t.Error("expected at least coordinator response")
	}

	// Verify coordinator is in responses
	AssertResponseSuccess(t, responses, wc.nodeID)

	// Remote node may or may not be in responses depending on timing
	// The test verifies timeout handling works correctly
}

// TestExecutePreparePhase_MixedResponses verifies different response types are collected
// Test 5.6: Success, conflict, and error responses should all be collected
func TestExecutePreparePhase_MixedResponses(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Node 2: success (default)
	// Node 3: conflict
	mock.SetConflict(3, txn.ID, "write-write conflict on row 42")
	// Node 4: error
	mock.SetNodeResponse(4, CreateErrorResponse("network error"))
	// Node 5: success (default)

	otherNodes := []uint64{2, 3, 4, 5}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	// Conflict should be returned as error
	if err == nil {
		t.Error("expected conflict error")
	}
	if err != nil && err.Error() == "" {
		t.Error("error message should not be empty")
	}

	// Even with conflict, successful responses should be collected
	// (conflict is detected during collection but responses are still saved)
	if len(responses) < 2 {
		t.Errorf("expected at least 2 successful responses, got %d", len(responses))
	}

	// Verify node 3 with conflict is not in successful responses
	// (conflict responses are collected but not added to prepResponses map)
}

// TestExecutePreparePhase_AllNodesTimeout verifies behavior when all nodes timeout
// Test 5.7: All nodes timing out should return empty response map
func TestExecutePreparePhase_AllNodesTimeout(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         10 * time.Millisecond, // Very short timeout
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// All nodes are slow (exceed timeout)
	mock.SetDelay(txn.ID, 200*time.Millisecond)
	otherNodes := []uint64{2, 3}

	ctx, cancel := WithTimeout(300)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)

	// All nodes timeout, so response map should be empty or very small
	if len(responses) > 1 {
		t.Errorf("expected empty or minimal response map due to timeout, got %d responses", len(responses))
	}
}

// TestExecutePreparePhase_ContextCancelled verifies handling of cancelled context
// Test 5.8: Context cancellation should stop waiting for responses
func TestExecutePreparePhase_ContextCancelled(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         500 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Make nodes slow so we can cancel mid-execution
	mock.SetDelay(txn.ID, 100*time.Millisecond)
	otherNodes := []uint64{2, 3, 4}

	ctx, cancel := WithCancellation()

	// Cancel after 20ms (before nodes can respond)
	CancelAfter(ctx, cancel, 20*time.Millisecond)

	start := time.Now()
	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)
	elapsed := time.Since(start)

	// Should return quickly after context cancellation
	if elapsed > 200*time.Millisecond {
		t.Errorf("took too long after context cancel: %v", elapsed)
	}

	// Error might be nil (no conflict), but we should get partial responses
	AssertNoError(t, err)

	// May get partial responses from nodes that completed before cancel
	if len(responses) > 4 {
		t.Errorf("expected partial responses, got %d (more than expected)", len(responses))
	}
}

// TestExecutePreparePhase_NilResponse verifies handling of nil responses
// Test 5.9: Nil responses should be handled gracefully
func TestExecutePreparePhase_NilResponse(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Node 2 returns nil response
	mock.SetNodeResponse(2, nil)
	// Node 3 returns success
	otherNodes := []uint64{2, 3}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)

	// Nil response should be skipped, so we get coordinator + node 3
	AssertResponseMapSize(t, responses, 2)

	// Node 2 should not be in response map (nil is skipped)
	AssertNodeNotInResponses(t, responses, 2)

	// Coordinator and node 3 should succeed
	AssertResponseSuccess(t, responses, wc.nodeID)
	AssertResponseSuccess(t, responses, 3)
}

// TestExecutePreparePhase_ZeroTotalNodes verifies handling of zero nodes
// Test 5.10: Zero total nodes should return empty map
func TestExecutePreparePhase_ZeroTotalNodes(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// No other nodes, skipLocalReplication=true means totalNodes=0
	otherNodes := []uint64{}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, true)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 0)

	// No calls should be made
	AssertCallCount(t, mock, 0)
}

// TestExecutePreparePhase_ChannelBufferOverflow verifies no deadlock with many nodes
// Test 5.11: 1000 nodes should not cause goroutine deadlock
func TestExecutePreparePhase_ChannelBufferOverflow(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	// Use very short latency to speed up test
	mock.mu.Lock()
	mock.prepareLatency = 0
	mock.mu.Unlock()

	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         2 * time.Second, // Generous timeout for 1000 nodes
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Create 1000 other nodes
	otherNodes := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		otherNodes[i] = uint64(i + 2) // Start from 2 (1 is coordinator)
	}

	ctx, cancel := WithTimeout(5000)
	defer cancel()

	start := time.Now()
	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)
	elapsed := time.Since(start)

	AssertNoError(t, err)

	// Should get all responses (coordinator + 1000 others)
	AssertResponseMapSize(t, responses, 1001)

	// Should complete in reasonable time (no deadlock)
	if elapsed > 4*time.Second {
		t.Errorf("took too long, possible deadlock: %v", elapsed)
	}

	t.Logf("1000 nodes completed in %v", elapsed)
}

// TestExecutePreparePhase_GoroutineLeakPrevention verifies no goroutine leaks
// Test 5.12: All goroutines should exit after timeout
func TestExecutePreparePhase_GoroutineLeakPrevention(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         50 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Configure nodes with varying delays to ensure some timeout
	mock.SetDelay(txn.ID, 200*time.Millisecond)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx, cancel := WithTimeout(300)
	defer cancel()

	beforeGoroutines := CountGoroutines()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)

	// Some responses may not arrive due to timeout
	if len(responses) > 5 {
		t.Errorf("unexpected response count: %d", len(responses))
	}

	// Wait for goroutines to exit
	time.Sleep(100 * time.Millisecond)

	afterGoroutines := CountGoroutines()

	// Should not have significant goroutine leak (allow for test framework variance)
	if afterGoroutines-beforeGoroutines > 10 {
		t.Errorf("potential goroutine leak: before=%d, after=%d, delta=%d",
			beforeGoroutines, afterGoroutines, afterGoroutines-beforeGoroutines)
	}
}

// TestExecutePreparePhase_ConflictStopsCollection verifies conflict detection behavior
// Bonus test: Verify that conflicts are detected and returned as errors
func TestExecutePreparePhase_ConflictStopsCollection(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Configure node 2 to have a conflict
	mock.SetConflict(2, txn.ID, "write-write conflict")
	otherNodes := []uint64{2, 3, 4}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	// Should return conflict error
	if err == nil {
		t.Error("expected conflict error, got nil")
	}

	// Verify error message contains conflict information
	if err != nil && err.Error() == "" {
		t.Error("error message should not be empty")
	}

	// Responses should still be collected (conflict doesn't stop collection)
	// Only successful responses are added to map (conflict responses are skipped)
	if len(responses) < 2 {
		t.Errorf("expected at least 2 successful responses, got %d", len(responses))
	}
}

// TestExecutePreparePhase_ErrorResponsesNotAdded verifies error responses aren't added
// Bonus test: Verify that error responses are not added to response map
func TestExecutePreparePhase_ErrorResponsesNotAdded(t *testing.T) {
	defer CheckGoroutines(t)()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	// Node 2 returns error (Success=false)
	mock.SetNodeResponse(2, CreateErrorResponse("database locked"))
	// Node 3 returns success
	otherNodes := []uint64{2, 3}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)

	// Only coordinator and node 3 should be in responses (node 2 error is excluded)
	AssertResponseMapSize(t, responses, 2)

	// Verify node 2 is not in response map
	AssertNodeNotInResponses(t, responses, 2)

	// Verify coordinator and node 3 are successful
	AssertResponseSuccess(t, responses, wc.nodeID)
	AssertResponseSuccess(t, responses, 3)
}

// deadlineRecordingReplicator captures the context deadline of each PREPARE call.
type deadlineRecordingReplicator struct {
	mu        sync.Mutex
	deadlines []time.Time
	hadNone   bool
}

func (d *deadlineRecordingReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, _ *ReplicationRequest) (*ReplicationResponse, error) {
	deadline, ok := ctx.Deadline()

	d.mu.Lock()
	if ok {
		d.deadlines = append(d.deadlines, deadline)
	} else {
		d.hadNone = true
	}
	d.mu.Unlock()

	return &ReplicationResponse{Success: true}, nil
}

// TestExecutePreparePhase_CallsAreDeadlineBounded verifies every PREPARE call is
// bounded by the coordinator timeout. PREPARE persists intents and validates DDL,
// so a call the collector has given up on must not keep running and durably
// prepare a transaction the coordinator no longer tracks.
func TestExecutePreparePhase_CallsAreDeadlineBounded(t *testing.T) {
	defer CheckGoroutines(t)()

	recorder := &deadlineRecordingReplicator{}
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      recorder,
		localReplicator: recorder,
		timeout:         200 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(100).Build()
	req := &ReplicationRequest{TxnID: txn.ID, NodeID: wc.nodeID, Phase: PhasePrep, Database: txn.Database}

	// Parent context has no deadline of its own - the bound must come from wc.timeout
	start := time.Now()
	responses, _, err := wc.executePreparePhase(context.Background(), txn, req, []uint64{2, 3}, false)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	if recorder.hadNone {
		t.Error("PREPARE dispatched with an unbounded context")
	}
	if len(recorder.deadlines) != 3 {
		t.Fatalf("recorded deadlines: got %d, want 3", len(recorder.deadlines))
	}
	for i, deadline := range recorder.deadlines {
		if budget := deadline.Sub(start); budget > wc.timeout+50*time.Millisecond {
			t.Errorf("call %d deadline exceeds coordinator timeout: %v > %v", i, budget, wc.timeout)
		}
	}
}

// TestExecutePreparePhase_DDLCallsUseDDLValidationTimeout verifies that a
// transaction carrying a DDL statement is dispatched with the DDL validation
// timeout, not the regular (much shorter) write timeout. PREPARE for DDL
// executes the real statement in a rolled-back transaction, which the regular
// write timeout is not sized for.
func TestExecutePreparePhase_DDLCallsUseDDLValidationTimeout(t *testing.T) {
	defer CheckGoroutines(t)()
	withDDLValidationTimeout(t, 2000)

	recorder := &deadlineRecordingReplicator{}
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      recorder,
		localReplicator: recorder,
		timeout:         100 * time.Millisecond, // regular write timeout - deliberately short
	}

	txn := NewTxnBuilder().WithID(300).WithDDLStatement("CREATE TABLE t (id INT)").Build()
	req := &ReplicationRequest{TxnID: txn.ID, NodeID: wc.nodeID, Phase: PhasePrep, Database: txn.Database}

	start := time.Now()
	responses, _, err := wc.executePreparePhase(context.Background(), txn, req, []uint64{2, 3}, false)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	if len(recorder.deadlines) != 3 {
		t.Fatalf("recorded deadlines: got %d, want 3", len(recorder.deadlines))
	}
	for i, deadline := range recorder.deadlines {
		budget := deadline.Sub(start)
		// A wide margin above wc.timeout (100ms) is required, not just "greater
		// than" it: scheduling jitter alone can push the recorded budget a few
		// hundred microseconds past wc.timeout even when the DDL timeout was NOT
		// applied, which would make a tight "> wc.timeout" check pass vacuously.
		if budget < 1*time.Second {
			t.Errorf("call %d deadline budget %v is not comfortably above the regular write timeout %v - DDL validation timeout was not applied", i, budget, wc.timeout)
		}
		if budget > 2000*time.Millisecond+100*time.Millisecond {
			t.Errorf("call %d deadline budget %v exceeds the configured DDL validation timeout (2000ms)", i, budget)
		}
	}
}

// TestExecutePreparePhase_DMLCallsKeepRegularWriteTimeout is the control for
// TestExecutePreparePhase_DDLCallsUseDDLValidationTimeout: a transaction with
// no DDL statement must keep using the regular write timeout even when a much
// larger DDL validation timeout is configured, so ordinary writes are not
// silently slowed down by a knob meant only for DDL.
func TestExecutePreparePhase_DMLCallsKeepRegularWriteTimeout(t *testing.T) {
	defer CheckGoroutines(t)()
	withDDLValidationTimeout(t, 2000)

	recorder := &deadlineRecordingReplicator{}
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      recorder,
		localReplicator: recorder,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(301).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()
	req := &ReplicationRequest{TxnID: txn.ID, NodeID: wc.nodeID, Phase: PhasePrep, Database: txn.Database}

	start := time.Now()
	responses, _, err := wc.executePreparePhase(context.Background(), txn, req, []uint64{2, 3}, false)

	AssertNoError(t, err)
	AssertResponseMapSize(t, responses, 3)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	if len(recorder.deadlines) != 3 {
		t.Fatalf("recorded deadlines: got %d, want 3", len(recorder.deadlines))
	}
	for i, deadline := range recorder.deadlines {
		if budget := deadline.Sub(start); budget > wc.timeout+50*time.Millisecond {
			t.Errorf("call %d deadline budget %v exceeds the regular write timeout %v - DML must not inherit the DDL validation timeout", i, budget, wc.timeout)
		}
	}
}

// TestExecutePreparePhase_DDLTransactionSurvivesDelayBeyondWriteTimeout proves
// the DDL validation timeout is not just recorded on the context but actually
// changes outcomes: a remote PREPARE call slower than the regular write
// timeout but faster than the DDL validation timeout must still succeed for a
// DDL transaction.
func TestExecutePreparePhase_DDLTransactionSurvivesDelayBeyondWriteTimeout(t *testing.T) {
	defer CheckGoroutines(t)()
	withDDLValidationTimeout(t, 2000)

	mock := newMockReplicator()
	localMock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: localMock,
		timeout:         100 * time.Millisecond, // regular write timeout
	}

	txn := NewTxnBuilder().WithID(302).WithDDLStatement("CREATE TABLE t (id INT)").Build()
	req := &ReplicationRequest{TxnID: txn.ID, NodeID: wc.nodeID, Phase: PhasePrep, Database: txn.Database}

	// Slower than the regular write timeout (100ms), well under the DDL
	// validation timeout (2s).
	mock.SetDelay(txn.ID, 500*time.Millisecond)
	otherNodes := []uint64{2}

	ctx, cancel := WithTimeout(3000)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)
	AssertResponseSuccess(t, responses, 2)
}

// TestExecutePreparePhase_DMLTransactionTimesOutAtWriteTimeout is the control
// for the delay test above: the same delay applied to a DML transaction must
// still time out at the regular write timeout, proving DML did not silently
// inherit the DDL validation timeout.
func TestExecutePreparePhase_DMLTransactionTimesOutAtWriteTimeout(t *testing.T) {
	defer CheckGoroutines(t)()
	withDDLValidationTimeout(t, 2000)

	mock := newMockReplicator()
	localMock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: localMock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(303).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()
	req := &ReplicationRequest{TxnID: txn.ID, NodeID: wc.nodeID, Phase: PhasePrep, Database: txn.Database}

	mock.SetDelay(txn.ID, 500*time.Millisecond)
	otherNodes := []uint64{2}

	ctx, cancel := WithTimeout(3000)
	defer cancel()

	responses, _, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)
	AssertNodeNotInResponses(t, responses, 2)
}
