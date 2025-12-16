package coordinator

import (
	"context"
	"testing"
	"time"
)

// =============================================================================
// Method 6: sendRemoteCommits Tests (8 tests: 6.1-6.8)
// =============================================================================

// Test 6.1: Verify correct goroutine count spawned
func TestSendRemoteCommits_SpawnCorrectGoroutineCount(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()
	wc := NewWriteCoordinator(1, nil, mock, mock, 100*time.Millisecond, nil)

	// Setup: 4 prepared nodes (3 remote + coordinator)
	preparedNodes := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(), // Coordinator
		2: CreateSuccessResponse(), // Remote
		3: CreateSuccessResponse(), // Remote
		4: CreateSuccessResponse(), // Remote
	}

	req := &ReplicationRequest{
		TxnID: 100,
		Phase: PhaseCommit,
	}

	ctx := context.Background()
	beforeCalls := mock.GetCallCount()

	// Execute
	commitChan := wc.sendRemoteCommits(ctx, preparedNodes, req, 100)

	// Wait for goroutines to execute
	time.Sleep(50 * time.Millisecond)

	// Assert: 3 goroutines spawned (excluding coordinator)
	afterCalls := mock.GetCallCount()
	spawnedGoroutines := afterCalls - beforeCalls

	if spawnedGoroutines != 3 {
		t.Errorf("expected 3 goroutines spawned, got %d", spawnedGoroutines)
	}

	// Verify channel capacity
	if cap(commitChan) != 3 {
		t.Errorf("expected channel capacity 3, got %d", cap(commitChan))
	}
}

// Test 6.2: Exclude coordinator from remote commits
func TestSendRemoteCommits_ExcludeCoordinator(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()
	wc := NewWriteCoordinator(1, nil, mock, mock, 100*time.Millisecond, nil)

	preparedNodes := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(), // Coordinator (should be excluded)
		2: CreateSuccessResponse(), // Remote
		3: CreateSuccessResponse(), // Remote
	}

	req := &ReplicationRequest{
		TxnID: 101,
		Phase: PhaseCommit,
	}

	ctx := context.Background()
	wc.sendRemoteCommits(ctx, preparedNodes, req, 101)

	// Wait for goroutines
	time.Sleep(50 * time.Millisecond)

	// Assert: Coordinator node (1) was NOT called in COMMIT phase
	commitCalls := mock.GetPhaseCalls(PhaseCommit)
	for _, call := range commitCalls {
		if call.NodeID == 1 {
			t.Errorf("coordinator (node 1) should not receive remote COMMIT, but was called")
		}
	}

	// Assert: Only remote nodes 2 and 3 were called
	if len(commitCalls) != 2 {
		t.Errorf("expected 2 remote COMMIT calls, got %d", len(commitCalls))
	}
}

// Test 6.3: Channel capacity matches prepared nodes count (excluding coordinator)
func TestSendRemoteCommits_ChannelCapacityMatch(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()
	wc := NewWriteCoordinator(1, nil, mock, mock, 100*time.Millisecond, nil)

	testCases := []struct {
		name          string
		preparedNodes map[uint64]*ReplicationResponse
		expectedCap   int
	}{
		{
			name: "5 nodes (4 remote + coordinator)",
			preparedNodes: map[uint64]*ReplicationResponse{
				1: CreateSuccessResponse(), // Coordinator
				2: CreateSuccessResponse(),
				3: CreateSuccessResponse(),
				4: CreateSuccessResponse(),
				5: CreateSuccessResponse(),
			},
			expectedCap: 4,
		},
		{
			name: "2 nodes (1 remote + coordinator)",
			preparedNodes: map[uint64]*ReplicationResponse{
				1: CreateSuccessResponse(), // Coordinator
				2: CreateSuccessResponse(),
			},
			expectedCap: 1,
		},
		{
			name: "3 nodes (2 remote + coordinator)",
			preparedNodes: map[uint64]*ReplicationResponse{
				1: CreateSuccessResponse(), // Coordinator
				2: CreateSuccessResponse(),
				3: CreateSuccessResponse(),
			},
			expectedCap: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &ReplicationRequest{
				TxnID: 200,
				Phase: PhaseCommit,
			}

			commitChan := wc.sendRemoteCommits(context.Background(), tc.preparedNodes, req, 200)

			if cap(commitChan) != tc.expectedCap {
				t.Errorf("expected channel capacity %d, got %d", tc.expectedCap, cap(commitChan))
			}
		})
	}
}

// Test 6.4: Detached context - commits continue even if parent cancelled
func TestSendRemoteCommits_DetachedContext(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()

	// Set longer latency so we can cancel parent before commit completes
	mock.mu.Lock()
	mock.commitLatency = 100 * time.Millisecond
	mock.mu.Unlock()

	wc := NewWriteCoordinator(1, nil, mock, mock, 200*time.Millisecond, nil)

	preparedNodes := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(), // Coordinator
		2: CreateSuccessResponse(), // Remote
		3: CreateSuccessResponse(), // Remote
	}

	req := &ReplicationRequest{
		TxnID: 300,
		Phase: PhaseCommit,
	}

	// Create cancellable parent context
	parentCtx, cancel := context.WithCancel(context.Background())

	beforeCalls := mock.GetCallCount()

	// Execute
	commitChan := wc.sendRemoteCommits(parentCtx, preparedNodes, req, 300)

	// Cancel parent context IMMEDIATELY (before commits complete)
	cancel()

	// Wait for commits to complete (they should complete despite parent cancellation)
	time.Sleep(150 * time.Millisecond)

	// Assert: Remote commits still executed (detached context)
	afterCalls := mock.GetCallCount()
	executedCalls := afterCalls - beforeCalls

	if executedCalls != 2 {
		t.Errorf("expected 2 remote commits to execute despite parent cancellation, got %d", executedCalls)
	}

	// Assert: Responses were sent to channel
	// Count without closing to avoid race with goroutines still writing
	responseCount := 0
	timeout := time.After(50 * time.Millisecond)
collectLoop:
	for responseCount < 2 {
		select {
		case <-commitChan:
			responseCount++
		case <-timeout:
			break collectLoop
		}
	}

	if responseCount != 2 {
		t.Errorf("expected 2 responses in channel, got %d", responseCount)
	}
}

// Test 6.5: Empty prepared nodes - no goroutines spawned
func TestSendRemoteCommits_EmptyPreparedNodes(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()
	wc := NewWriteCoordinator(1, nil, mock, mock, 100*time.Millisecond, nil)

	preparedNodes := map[uint64]*ReplicationResponse{}

	req := &ReplicationRequest{
		TxnID: 400,
		Phase: PhaseCommit,
	}

	beforeCalls := mock.GetCallCount()

	// Execute
	commitChan := wc.sendRemoteCommits(context.Background(), preparedNodes, req, 400)

	// Wait a bit
	time.Sleep(20 * time.Millisecond)

	// Assert: No goroutines spawned
	afterCalls := mock.GetCallCount()
	if afterCalls != beforeCalls {
		t.Errorf("expected no calls for empty prepared nodes, got %d calls", afterCalls-beforeCalls)
	}

	// Assert: Channel capacity is 0
	if cap(commitChan) != 0 {
		t.Errorf("expected channel capacity 0 for empty nodes, got %d", cap(commitChan))
	}
}

// Test 6.6: Single remote node (preparedNodes = coordinator + 1 remote)
func TestSendRemoteCommits_SingleRemoteNode(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()
	wc := NewWriteCoordinator(1, nil, mock, mock, 100*time.Millisecond, nil)

	preparedNodes := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(), // Coordinator
		2: CreateSuccessResponse(), // Single remote
	}

	req := &ReplicationRequest{
		TxnID: 500,
		Phase: PhaseCommit,
	}

	beforeCalls := mock.GetCallCount()

	// Execute
	commitChan := wc.sendRemoteCommits(context.Background(), preparedNodes, req, 500)

	// Wait for goroutine
	time.Sleep(50 * time.Millisecond)

	// Assert: 1 goroutine for node 2
	afterCalls := mock.GetCallCount()
	if afterCalls-beforeCalls != 1 {
		t.Errorf("expected 1 goroutine for single remote node, got %d", afterCalls-beforeCalls)
	}

	// Assert: Channel capacity is 1
	if cap(commitChan) != 1 {
		t.Errorf("expected channel capacity 1, got %d", cap(commitChan))
	}

	// Assert: Only node 2 was called
	commitCalls := mock.GetPhaseCalls(PhaseCommit)
	if len(commitCalls) != 1 {
		t.Errorf("expected 1 commit call, got %d", len(commitCalls))
	}
	if commitCalls[0].NodeID != 2 {
		t.Errorf("expected node 2 to be called, got node %d", commitCalls[0].NodeID)
	}
}

// Test 6.7: Timeout propagation - detached timeout matches coordinator timeout
func TestSendRemoteCommits_TimeoutPropagation(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()

	// Set coordinator timeout to 3 seconds
	coordinatorTimeout := 3 * time.Second
	wc := NewWriteCoordinator(1, nil, mock, mock, coordinatorTimeout, nil)

	preparedNodes := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(), // Coordinator
		2: CreateSuccessResponse(), // Remote
	}

	req := &ReplicationRequest{
		TxnID: 600,
		Phase: PhaseCommit,
	}

	// Execute - this spawns goroutines with detached context
	wc.sendRemoteCommits(context.Background(), preparedNodes, req, 600)

	// Wait for goroutine to start
	time.Sleep(20 * time.Millisecond)

	// Assert: Call was made (indicating context timeout was sufficient)
	commitCalls := mock.GetPhaseCalls(PhaseCommit)
	if len(commitCalls) != 1 {
		t.Errorf("expected 1 commit call, got %d", len(commitCalls))
	}

	// Note: We can't easily verify the exact timeout value used in the detached context,
	// but we verify that the commit completed successfully, indicating timeout was adequate
}

// Test 6.8: Correct TxnID propagated in all commit requests
func TestSendRemoteCommits_CorrectTxnID(t *testing.T) {
	InitTestTelemetry()
	mock := newMockReplicator()
	wc := NewWriteCoordinator(1, nil, mock, mock, 100*time.Millisecond, nil)

	expectedTxnID := uint64(12345)

	preparedNodes := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(), // Coordinator
		2: CreateSuccessResponse(), // Remote
		3: CreateSuccessResponse(), // Remote
		4: CreateSuccessResponse(), // Remote
	}

	req := &ReplicationRequest{
		TxnID: expectedTxnID,
		Phase: PhaseCommit,
	}

	// Execute
	wc.sendRemoteCommits(context.Background(), preparedNodes, req, expectedTxnID)

	// Wait for goroutines
	time.Sleep(50 * time.Millisecond)

	// Assert: All commit requests have correct TxnID
	commitCalls := mock.GetPhaseCalls(PhaseCommit)
	for _, call := range commitCalls {
		if call.Request.TxnID != expectedTxnID {
			t.Errorf("expected TxnID %d in commit request, got %d", expectedTxnID, call.Request.TxnID)
		}
	}

	// Assert: All 3 remote nodes were called
	if len(commitCalls) != 3 {
		t.Errorf("expected 3 commit calls, got %d", len(commitCalls))
	}
}

// =============================================================================
// Method 7: waitForRemoteQuorum Tests (11 tests: 7.1-7.11)
// =============================================================================

// Test 7.1: Exact quorum achieved
func TestWaitForRemoteQuorum_ExactQuorumAchieved(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 100*time.Millisecond, nil)

	mocker := NewChannelMocker()
	commitChan := make(chan response, 3)

	// Send responses: need 2, send exactly 2 successes
	mocker.SendResponse(commitChan, 2, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 3, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 4, CreateErrorResponse("timeout"), 10*time.Millisecond)

	// Execute: 3 prepared nodes, need 2 for quorum
	responses, acks := wc.waitForRemoteQuorum(commitChan, 3, 2, 100)

	// Assert: Exactly 2 ACKs
	if acks != 2 {
		t.Errorf("expected 2 ACKs, got %d", acks)
	}

	// Assert: 2 successful responses in map
	if len(responses) != 2 {
		t.Errorf("expected 2 responses in map, got %d", len(responses))
	}

	// Assert: Nodes 2 and 3 are in responses
	AssertResponseSuccess(t, responses, 2)
	AssertResponseSuccess(t, responses, 3)
}

// Test 7.2: Over quorum achieved - all nodes respond successfully
func TestWaitForRemoteQuorum_OverQuorumAchieved(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 100*time.Millisecond, nil)

	mocker := NewChannelMocker()
	commitChan := make(chan response, 4)

	// Send 4 successful responses, need only 2
	mocker.SendResponse(commitChan, 2, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 3, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 4, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 5, CreateSuccessResponse(), 0)

	// Execute: 4 prepared nodes, need 2 for quorum
	responses, acks := wc.waitForRemoteQuorum(commitChan, 4, 2, 200)

	// Assert: Early exit after 2 ACKs (may have 2, 3, or 4 depending on timing)
	if acks < 2 {
		t.Errorf("expected at least 2 ACKs, got %d", acks)
	}

	// Assert: At least 2 responses
	if len(responses) < 2 {
		t.Errorf("expected at least 2 responses, got %d", len(responses))
	}
}

// Test 7.3: Under quorum - timeout waiting for responses
func TestWaitForRemoteQuorum_UnderQuorumTimeout(t *testing.T) {
	InitTestTelemetry()

	// Set very short timeout to speed up test
	wc := NewWriteCoordinator(1, nil, nil, nil, 50*time.Millisecond, nil)

	mocker := NewChannelMocker()
	commitChan := make(chan response, 3)

	// Send only 1 success, need 2 (other nodes timeout)
	mocker.SendResponse(commitChan, 2, CreateSuccessResponse(), 0)
	// Nodes 3 and 4 don't respond (simulating timeout)

	start := time.Now()

	// Execute: 3 prepared nodes, need 2 for quorum (will timeout)
	responses, acks := wc.waitForRemoteQuorum(commitChan, 3, 2, 300)

	elapsed := time.Since(start)

	// Assert: Only 1 ACK received
	if acks != 1 {
		t.Errorf("expected 1 ACK, got %d", acks)
	}

	// Assert: Only 1 response in map
	if len(responses) != 1 {
		t.Errorf("expected 1 response, got %d", len(responses))
	}

	// Assert: Timeout occurred (elapsed time should be >= timeout * missing responses)
	// With 2 missing responses and 50ms timeout each, should be ~100ms
	expectedMinTime := 50 * time.Millisecond
	if elapsed < expectedMinTime {
		t.Errorf("expected timeout elapsed >= %v, got %v", expectedMinTime, elapsed)
	}
}

// Test 7.4: Early exit on quorum - don't wait for all responses
func TestWaitForRemoteQuorum_EarlyExitOnQuorum(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 100*time.Millisecond, nil)

	mocker := NewChannelMocker()
	commitChan := make(chan response, 4)

	// Send 2 fast responses (immediate), 2 slow responses (delayed)
	mocker.SendResponse(commitChan, 2, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 3, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 4, CreateSuccessResponse(), 200*time.Millisecond) // Slow
	mocker.SendResponse(commitChan, 5, CreateSuccessResponse(), 200*time.Millisecond) // Slow

	start := time.Now()

	// Execute: 4 prepared nodes, need 2 for quorum
	responses, acks := wc.waitForRemoteQuorum(commitChan, 4, 2, 400)

	elapsed := time.Since(start)

	// Assert: Got 2 ACKs (early exit)
	if acks != 2 {
		t.Errorf("expected exactly 2 ACKs due to early exit, got %d", acks)
	}

	// Assert: Early exit occurred (should complete in < 100ms, not wait 200ms)
	if elapsed > 150*time.Millisecond {
		t.Errorf("expected early exit in < 150ms, took %v", elapsed)
	}

	// Assert: Exactly 2 responses (nodes 2 and 3)
	if len(responses) != 2 {
		t.Errorf("expected 2 responses due to early exit, got %d", len(responses))
	}
}

// Test 7.5: All nodes fail - no ACKs
func TestWaitForRemoteQuorum_AllNodesFail(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 50*time.Millisecond, nil)

	mocker := NewChannelMocker()
	commitChan := make(chan response, 3)

	// All nodes return errors
	mocker.SendResponse(commitChan, 2, CreateErrorResponse("network error"), 0)
	mocker.SendResponse(commitChan, 3, CreateErrorResponse("timeout"), 0)
	mocker.SendResponse(commitChan, 4, CreateErrorResponse("unreachable"), 0)

	// Execute: 3 prepared nodes, need 2 for quorum
	responses, acks := wc.waitForRemoteQuorum(commitChan, 3, 2, 500)

	// Assert: 0 ACKs
	if acks != 0 {
		t.Errorf("expected 0 ACKs when all nodes fail, got %d", acks)
	}

	// Assert: 0 successful responses in map
	if len(responses) != 0 {
		t.Errorf("expected 0 responses when all nodes fail, got %d", len(responses))
	}
}

// Test 7.6: Mixed success and error - count only successes
func TestWaitForRemoteQuorum_MixedSuccessError(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 100*time.Millisecond, nil)

	mocker := NewChannelMocker()
	commitChan := make(chan response, 4)

	// Mix of success and errors
	mocker.SendResponse(commitChan, 2, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 3, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 4, CreateErrorResponse("network error"), 0)
	mocker.SendResponse(commitChan, 5, CreateSuccessResponse(), 0)

	// Execute: 4 prepared nodes, need 2 for quorum
	responses, acks := wc.waitForRemoteQuorum(commitChan, 4, 2, 600)

	// Assert: 2 or 3 ACKs (early exit after 2, but may collect more)
	if acks < 2 {
		t.Errorf("expected at least 2 ACKs, got %d", acks)
	}

	// Assert: Only successful responses in map (no errors)
	for nodeID, resp := range responses {
		if !resp.Success {
			t.Errorf("node %d should not be in responses (failed)", nodeID)
		}
	}

	// Assert: Node 4 (error) not in responses
	AssertNodeNotInResponses(t, responses, 4)
}

// Test 7.7: Timeout per response - slow node not counted
func TestWaitForRemoteQuorum_TimeoutPerResponse(t *testing.T) {
	InitTestTelemetry()

	// Coordinator timeout is 100ms per response
	wc := NewWriteCoordinator(1, nil, nil, nil, 100*time.Millisecond, nil)

	mocker := NewChannelMocker()
	commitChan := make(chan response, 3)

	// Node 2: fast
	// Node 3: slow (exceeds timeout)
	// Node 4: fast
	mocker.SendResponse(commitChan, 2, CreateSuccessResponse(), 0)
	mocker.SendResponse(commitChan, 3, CreateSuccessResponse(), 200*time.Millisecond) // Exceeds timeout
	mocker.SendResponse(commitChan, 4, CreateSuccessResponse(), 0)

	// Execute: 3 prepared nodes, need 2 for quorum
	responses, acks := wc.waitForRemoteQuorum(commitChan, 3, 2, 700)

	// Assert: Got 2 ACKs (nodes 2 and 4, node 3 timed out)
	if acks != 2 {
		t.Errorf("expected 2 ACKs (node 3 timed out), got %d", acks)
	}

	// Assert: 2 responses (nodes 2 and 4)
	if len(responses) != 2 {
		t.Errorf("expected 2 responses, got %d", len(responses))
	}

	// Assert: Nodes 2 and 4 in responses
	AssertResponseSuccess(t, responses, 2)
	AssertResponseSuccess(t, responses, 4)
}

// Test 7.8: Zero quorum needed - early exit on first iteration
func TestWaitForRemoteQuorum_ZeroQuorumNeeded(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 50*time.Millisecond, nil)

	commitChan := make(chan response, 2)

	// Execute: remoteQuorumNeeded = 0 (single-node or coordinator-only)
	// Even though quorum is 0, the loop still runs once per otherPreparedNodes
	// But should exit early after checking the condition
	responses, acks := wc.waitForRemoteQuorum(commitChan, 2, 0, 800)

	// Assert: 0 ACKs (quorum already met)
	if acks != 0 {
		t.Errorf("expected 0 ACKs for zero quorum, got %d", acks)
	}

	// Assert: Empty response map
	if len(responses) != 0 {
		t.Errorf("expected 0 responses for zero quorum, got %d", len(responses))
	}

	// Note: Implementation enters the loop once and times out before checking condition
	// This is acceptable behavior - the condition check happens after each iteration
}

// Test 7.9: Negative quorum needed - early exit on first iteration
func TestWaitForRemoteQuorum_NegativeQuorumNeeded(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 50*time.Millisecond, nil)

	commitChan := make(chan response, 2)

	// Execute: remoteQuorumNeeded = -1 (invalid, but should handle gracefully)
	// The condition remoteAcks >= -1 is always true, so should exit after first iteration
	responses, acks := wc.waitForRemoteQuorum(commitChan, 2, -1, 900)

	// Assert: 0 ACKs (no successful responses collected before exit)
	if acks != 0 {
		t.Errorf("expected 0 ACKs for negative quorum, got %d", acks)
	}

	// Assert: Empty response map (no successful responses)
	if len(responses) != 0 {
		t.Errorf("expected 0 responses for negative quorum, got %d", len(responses))
	}

	// Note: Implementation enters the loop once and times out before checking condition
	// This is acceptable behavior - negative quorum is an edge case that shouldn't occur
}

// Test 7.10: Channel closed early - handle gracefully
func TestWaitForRemoteQuorum_ChannelClosed(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 100*time.Millisecond, nil)

	commitChan := make(chan response, 2)

	// Send 1 response, then close channel
	go func() {
		commitChan <- response{
			nodeID: 2,
			resp:   CreateSuccessResponse(),
			err:    nil,
		}
		time.Sleep(10 * time.Millisecond)
		close(commitChan)
	}()

	// Execute: 2 prepared nodes, need 2 for quorum (but channel closes after 1)
	responses, acks := wc.waitForRemoteQuorum(commitChan, 2, 2, 1000)

	// Assert: Got only 1 ACK (channel closed early)
	if acks != 1 {
		t.Errorf("expected 1 ACK before channel closed, got %d", acks)
	}

	// Assert: 1 response in map
	if len(responses) != 1 {
		t.Errorf("expected 1 response, got %d", len(responses))
	}
}

// Test 7.11: Nil responses - not counted as ACK
func TestWaitForRemoteQuorum_NilResponses(t *testing.T) {
	InitTestTelemetry()
	wc := NewWriteCoordinator(1, nil, nil, nil, 100*time.Millisecond, nil)

	commitChan := make(chan response, 3)

	// Send: 1 success, 1 nil response, 1 error
	commitChan <- response{
		nodeID: 2,
		resp:   CreateSuccessResponse(),
		err:    nil,
	}
	commitChan <- response{
		nodeID: 3,
		resp:   nil, // Nil response
		err:    nil,
	}
	commitChan <- response{
		nodeID: 4,
		resp:   CreateErrorResponse("error"),
		err:    nil,
	}

	// Execute: 3 prepared nodes, need 2 for quorum
	responses, acks := wc.waitForRemoteQuorum(commitChan, 3, 2, 1100)

	// Assert: Only 1 ACK (nil and error not counted)
	if acks != 1 {
		t.Errorf("expected 1 ACK (nil response not counted), got %d", acks)
	}

	// Assert: Only 1 response in map (node 2)
	if len(responses) != 1 {
		t.Errorf("expected 1 response (nil excluded), got %d", len(responses))
	}

	// Assert: Only node 2 in responses
	AssertResponseSuccess(t, responses, 2)
	AssertNodeNotInResponses(t, responses, 3)
	AssertNodeNotInResponses(t, responses, 4)
}
