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
