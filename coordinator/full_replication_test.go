package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// mockNodeProvider implements NodeProvider for testing
type mockNodeProvider struct {
	nodes           []uint64
	totalMembership int // Total membership for split-brain prevention (0 = use len(nodes))
	mu              sync.RWMutex
}

func newMockNodeProvider(nodes []uint64) *mockNodeProvider {
	return &mockNodeProvider{nodes: nodes, totalMembership: len(nodes)}
}

func (m *mockNodeProvider) GetAliveNodes() ([]uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]uint64, len(m.nodes))
	copy(result, m.nodes)
	return result, nil
}

func (m *mockNodeProvider) GetClusterSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}

func (m *mockNodeProvider) GetTotalMembershipSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.totalMembership > 0 {
		return m.totalMembership
	}
	return len(m.nodes)
}

func (m *mockNodeProvider) SetTotalMembership(total int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalMembership = total
}

func (m *mockNodeProvider) RemoveNode(nodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, n := range m.nodes {
		if n == nodeID {
			m.nodes = append(m.nodes[:i], m.nodes[i+1:]...)
			break
		}
	}
}

// TestFullReplication_QuorumWrite tests that writes succeed with quorum
func TestFullReplication_QuorumWrite(t *testing.T) {
	// Setup: 5-node cluster
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1, // node 1 is coordinator
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	// Create a test transaction
	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	// Execute write
	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should succeed (coordinator + 4 nodes, need 3 for quorum)
	if err != nil {
		t.Fatalf("Expected write to succeed, got error: %v", err)
	}

	// Verify replicator was called for all nodes including self
	callCount := replicator.GetCallCount()
	// With quorum optimization:
	// - PREPARE sent to all 5 nodes
	// - Coordinator waits for quorum (3) and exits early
	// - COMMIT sent only to nodes that ACKed PREPARE (3 nodes)
	// - Total: 5 PREPARE + 3 COMMIT = 8 calls (minimum for quorum)
	expectedMinCalls := 8
	expectedMaxCalls := 10 // If all nodes respond before quorum check
	if callCount < expectedMinCalls || callCount > expectedMaxCalls {
		t.Errorf("Expected %d-%d replication calls, got %d", expectedMinCalls, expectedMaxCalls, callCount)
	}
}

// TestFullReplication_QuorumFailure tests that writes fail without quorum
func TestFullReplication_QuorumFailure(t *testing.T) {
	// Setup: 5-node cluster
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Make nodes 2, 3, 4 fail (only node 5 responds)
	// With coordinator (1) + node 5 = 2 nodes, need 3 for quorum
	replicator.SetNodeResponse(2, &ReplicationResponse{Success: false})
	replicator.SetNodeResponse(3, &ReplicationResponse{Success: false})
	replicator.SetNodeResponse(4, &ReplicationResponse{Success: false})

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail (only 2 out of 5 nodes)
	if err == nil {
		t.Fatal("Expected write to fail without quorum, but it succeeded")
	}

	if !contains(err.Error(), "quorum not achieved") {
		t.Errorf("Expected quorum failure error, got: %v", err)
	}
}

// TestFullReplication_AllNodesReceiveWrite tests that all nodes get the write
func TestFullReplication_AllNodesReceiveWrite(t *testing.T) {
	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	if err != nil {
		t.Fatalf("Expected write to succeed, got error: %v", err)
	}

	// Verify all nodes including self received the PREPARE
	// With quorum optimization:
	// - PREPARE sent to all 3 nodes
	// - Coordinator waits for quorum (2) and exits early
	// - COMMIT sent only to nodes that ACKed PREPARE (2 nodes)
	// - Total: 3 PREPARE + 2 COMMIT = 5 calls (minimum for quorum)
	callCount := replicator.GetCallCount()
	expectedMinCalls := 5
	expectedMaxCalls := 6 // If all 3 PREPARE responses arrive before quorum check
	if callCount < expectedMinCalls || callCount > expectedMaxCalls {
		t.Errorf("Expected %d-%d replication calls, got %d", expectedMinCalls, expectedMaxCalls, callCount)
	}
}

// TestFullReplication_NodeFailureDuringWrite tests that dead nodes don't block writes
func TestFullReplication_NodeFailureDuringWrite(t *testing.T) {
	// Setup: 5-node cluster, node 5 is dead
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Node 5 is dead/unresponsive
	replicator.SetNodeResponse(5, &ReplicationResponse{Success: false})

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should still succeed: coordinator (1) + nodes 2,3,4 = 4 nodes, need 3 for quorum
	if err != nil {
		t.Fatalf("Expected write to succeed despite node failure, got error: %v", err)
	}
}

// TestFullReplication_ReadFromAnyNode tests read coordinator with full replication
func TestFullReplication_ReadFromAnyNode(t *testing.T) {
	// Setup: 3-node cluster
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)

	// Mock reader that returns success
	mockReader := &mockReader{
		response: &ReadResponse{
			Success:  true,
			Rows:     []map[string]interface{}{{"id": 1, "name": "Alice"}},
			RowCount: 1,
		},
	}

	coordinator := NewReadCoordinator(
		1,
		nodeProvider,
		mockReader,
		1*time.Second,
	)

	req := &ReadRequest{
		Query:       "SELECT * FROM users WHERE id = 1",
		SnapshotTS:  hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		Consistency: protocol.ConsistencyLocalOne,
		TableName:   "users",
	}

	ctx := context.Background()
	resp, err := coordinator.ReadTransaction(ctx, req)

	if err != nil {
		t.Fatalf("Expected read to succeed, got error: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected successful read response")
	}

	if resp.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", resp.RowCount)
	}
}

// mockReader implements Reader for testing
type mockReader struct {
	response *ReadResponse
	err      error
}

func (m *mockReader) ReadSnapshot(ctx context.Context, nodeID uint64, req *ReadRequest) (*ReadResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestCommitQuorumFailure_CoordinatorDoesNotCommit verifies that if remote commit
// quorum fails, the coordinator does NOT commit locally. This prevents data inconsistency
// where only the coordinator has committed data.
func TestCommitQuorumFailure_CoordinatorDoesNotCommit(t *testing.T) {
	// Setup: 3-node cluster (quorum = 2)
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	clock := hlc.NewClock(1)

	// Create separate replicators for remote and local
	// Remote replicator: PREPARE succeeds, COMMIT fails
	remoteReplicator := newMockReplicator()
	remoteReplicator.SetNodeCommitResponse(2, &ReplicationResponse{Success: false, Error: "commit timeout"})
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "commit timeout"})

	// Local replicator: tracks if local commit was attempted
	localReplicator := newMockReplicator()

	coordinator := NewWriteCoordinator(
		1, // node 1 is coordinator
		nodeProvider,
		remoteReplicator,
		localReplicator,
		1*time.Second,
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should FAIL - remote commit quorum not achieved
	if err == nil {
		t.Fatal("Expected write to fail when remote commit quorum not achieved, but it succeeded")
	}

	// After fix: error message should indicate "partial commit" (more accurate than "quorum not achieved")
	if !contains(err.Error(), "partial commit") && !contains(err.Error(), "commit quorum not achieved") {
		t.Errorf("Expected 'partial commit' or 'commit quorum not achieved' error, got: %v", err)
	}

	// Verify local commit was NOT attempted (critical check)
	localCalls := localReplicator.GetCalls()
	for _, call := range localCalls {
		if call.Request.Phase == PhaseCommit {
			t.Error("CRITICAL: Local commit was attempted despite remote quorum failure - this causes data inconsistency!")
		}
	}

	t.Log("Success: Coordinator did not commit locally when remote quorum failed")
}

// TestCommitQuorumSuccess_PartialRemoteFailure verifies that if at least (quorum-1)
// remote nodes commit, the coordinator commits locally and transaction succeeds.
func TestCommitQuorumSuccess_PartialRemoteFailure(t *testing.T) {
	// Setup: 3-node cluster (quorum = 2)
	// With coordinator, we need (quorum-1) = 1 remote ACK
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	clock := hlc.NewClock(1)

	// Remote replicator: node 2 succeeds, node 3 fails
	remoteReplicator := newMockReplicator()
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "commit timeout"})

	// Local replicator
	localReplicator := newMockReplicator()

	coordinator := NewWriteCoordinator(
		1, // node 1 is coordinator
		nodeProvider,
		remoteReplicator,
		localReplicator,
		1*time.Second,
		clock,
	)

	txn := &Transaction{
		ID:     2,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (2, 'Bob')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 2000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should SUCCEED - node 2 + coordinator = quorum of 2
	if err != nil {
		t.Fatalf("Expected write to succeed with partial remote failure, got error: %v", err)
	}

	// Verify local commit WAS attempted
	localCalls := localReplicator.GetCalls()
	hasLocalCommit := false
	for _, call := range localCalls {
		if call.Request.Phase == PhaseCommit {
			hasLocalCommit = true
			break
		}
	}
	if !hasLocalCommit {
		t.Error("Expected local commit to be attempted after remote quorum achieved")
	}

	t.Log("Success: Coordinator committed locally after remote quorum achieved")
}

// TestSplitBrainPrevention verifies that quorum is calculated from total membership
// to prevent split-brain scenarios. In a 6-node cluster split 3x3, neither partition
// should be able to achieve quorum (which requires 4 acks based on total membership of 6).
func TestSplitBrainPrevention(t *testing.T) {
	clock := hlc.NewClock(1)

	// Simulate partition 1: nodes 1, 2, 3 (can only see each other)
	// Total membership is 6, but only 3 nodes are alive/reachable
	partitionNodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(partitionNodes)
	nodeProvider.SetTotalMembership(6) // Total cluster membership is 6

	replicator := newMockReplicator()
	localReplicator := newMockReplicator()

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		localReplicator,
		1*time.Second,
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{SQL: "INSERT INTO users (id, name) VALUES (1, 'Alice')"},
		},
		StartTS:          clock.Now(),
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail: quorum requires 4 acks (majority of 6), but only 3 nodes are reachable
	if err == nil {
		t.Fatalf("Expected write to fail in partitioned cluster, but it succeeded")
	}

	// Verify error mentions quorum failure
	errStr := err.Error()
	if !contains(errStr, "quorum") {
		t.Errorf("Expected quorum-related error, got: %v", err)
	}

	t.Logf("Split-brain prevention working: %v", err)
}

// TestNoAbortAfterCommitSent verifies that if some nodes commit but quorum fails,
// we don't call abort on nodes that already committed. This is critical because
// abort RPC on an already-committed node returns success without rollback.
func TestNoAbortAfterCommitSent(t *testing.T) {
	// Setup: 5-node cluster (quorum = 3, so remoteQuorumNeeded = 2)
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	clock := hlc.NewClock(1)

	// Remote replicator: only node 2 commits successfully, rest fail during commit
	// This simulates the scenario where COMMIT was sent to all, but only one ACKed
	// remoteQuorumNeeded = 2, but we only get 1 ACK
	remoteReplicator := newMockReplicator()
	remoteReplicator.SetNodeCommitResponse(2, &ReplicationResponse{Success: true})
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "commit timeout"})
	remoteReplicator.SetNodeCommitResponse(4, &ReplicationResponse{Success: false, Error: "commit timeout"})
	remoteReplicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "commit timeout"})

	// Local replicator: tracks calls
	localReplicator := newMockReplicator()

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		remoteReplicator,
		localReplicator,
		100*time.Millisecond, // Short timeout to speed up test
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should FAIL - only 1 remote ACK (node 2), need 2 remote ACKs
	if err == nil {
		t.Fatal("Expected write to fail when remote commit quorum not achieved, but it succeeded")
	}

	// Verify error message indicates partial commit (not "quorum not achieved")
	if !contains(err.Error(), "partial commit") {
		t.Errorf("Expected 'partial commit' error message, got: %v", err)
	}

	// CRITICAL CHECK: Verify that abortTransaction was NOT called after COMMIT phase started
	// Check all replicator calls - there should be NO abort calls
	remoteCalls := remoteReplicator.GetCalls()
	localCalls := localReplicator.GetCalls()

	for _, call := range remoteCalls {
		if call.Request.Phase == PhaseAbort {
			t.Errorf("CRITICAL BUG: Abort was sent to remote node %d after COMMIT phase started - this can cause partial commits!",
				call.NodeID)
		}
	}

	for _, call := range localCalls {
		if call.Request.Phase == PhaseAbort {
			t.Error("CRITICAL BUG: Abort was sent to local node after COMMIT phase started - this can cause partial commits!")
		}
	}

	t.Log("Success: No abort calls after COMMIT phase started (correct behavior for partial commit)")
}

// TestLocalPrepareMustSucceed verifies that if local prepare fails but remote quorum
// succeeds, we abort and return error (not success). The coordinator MUST participate.
func TestLocalPrepareMustSucceed(t *testing.T) {
	// Setup: 3-node cluster (quorum = 2)
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	clock := hlc.NewClock(1)

	// Remote replicator: nodes 2 and 3 prepare successfully
	remoteReplicator := newMockReplicator()

	// Local replicator: PREPARE fails
	localReplicator := newMockReplicator()
	localReplicator.SetNodeResponse(1, &ReplicationResponse{Success: false, Error: "local prepare failed"})

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		remoteReplicator,
		localReplicator,
		1*time.Second,
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should FAIL - coordinator must participate
	if err == nil {
		t.Fatal("Expected write to fail when coordinator prepare fails, but it succeeded")
	}

	// Verify error message indicates coordinator must participate
	if !contains(err.Error(), "coordinator must participate") {
		t.Errorf("Expected 'coordinator must participate' error message, got: %v", err)
	}

	// Wait for abort goroutines to complete (abortTransaction spawns goroutines)
	time.Sleep(50 * time.Millisecond)

	// Verify that abort was called (correct behavior - we're still in PREPARE phase)
	remoteCalls := remoteReplicator.GetCalls()
	hasAbort := false
	for _, call := range remoteCalls {
		if call.Request.Phase == PhaseAbort {
			hasAbort = true
			break
		}
	}

	if !hasAbort {
		t.Error("Expected abort to be called after prepare phase failure")
	}

	// CRITICAL CHECK: Verify NO commit was attempted (locally or remotely)
	for _, call := range remoteCalls {
		if call.Request.Phase == PhaseCommit {
			t.Error("CRITICAL BUG: Commit was attempted despite coordinator prepare failure!")
		}
	}

	localCalls := localReplicator.GetCalls()
	for _, call := range localCalls {
		if call.Request.Phase == PhaseCommit {
			t.Error("CRITICAL BUG: Local commit was attempted despite local prepare failure!")
		}
	}

	t.Log("Success: Coordinator prepare failure aborted transaction before commit phase")
}

// TestPartialCommitErrorMessage verifies error message clearly indicates partial commit scenario
func TestPartialCommitErrorMessage(t *testing.T) {
	// Setup: 5-node cluster (quorum = 3)
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	clock := hlc.NewClock(1)

	// Remote replicator: only node 2 commits, nodes 3,4,5 fail during commit
	remoteReplicator := newMockReplicator()
	remoteReplicator.SetNodeCommitResponse(2, &ReplicationResponse{Success: true})
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "timeout"})
	remoteReplicator.SetNodeCommitResponse(4, &ReplicationResponse{Success: false, Error: "timeout"})
	remoteReplicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "timeout"})

	localReplicator := newMockReplicator()

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		remoteReplicator,
		localReplicator,
		100*time.Millisecond,
		clock,
	)

	txn := &Transaction{
		ID:     1,
		NodeID: 1,
		Statements: []protocol.Statement{
			{
				SQL:       "INSERT INTO users VALUES (1, 'Alice')",
				Type:      protocol.StatementInsert,
				TableName: "users",
			},
		},
		StartTS:          hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
		WriteConsistency: protocol.ConsistencyQuorum,
	}

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should FAIL - need 2 remote ACKs (quorum-1), got only 1
	if err == nil {
		t.Fatal("Expected write to fail, but it succeeded")
	}

	// Verify error message clearly indicates partial commit
	errMsg := err.Error()
	if !contains(errMsg, "partial commit") {
		t.Errorf("Expected 'partial commit' in error message, got: %v", err)
	}

	// Verify error message includes useful details
	if !contains(errMsg, "some nodes may have committed") {
		t.Errorf("Expected 'some nodes may have committed' in error message, got: %v", err)
	}

	// Verify specific numbers are mentioned
	if !contains(errMsg, "got 1") && !contains(errMsg, "needed 2") {
		t.Logf("Warning: Error message doesn't include specific ACK counts: %v", err)
	}

	t.Logf("Success: Error message clearly indicates partial commit scenario: %v", err)
}
