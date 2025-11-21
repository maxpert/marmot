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
	nodes []uint64
	mu    sync.RWMutex
}

func newMockNodeProvider(nodes []uint64) *mockNodeProvider {
	return &mockNodeProvider{nodes: nodes}
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

// mockReplicator implements Replicator for testing
type mockReplicator struct {
	responses      map[uint64]*ReplicationResponse
	callCount      int
	prepareLatency time.Duration
	commitLatency  time.Duration
	mu             sync.Mutex
}

func newMockReplicator() *mockReplicator {
	return &mockReplicator{
		responses:      make(map[uint64]*ReplicationResponse),
		prepareLatency: 10 * time.Millisecond,
		commitLatency:  5 * time.Millisecond,
	}
}

func (m *mockReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *ReplicationRequest) (*ReplicationResponse, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()

	// Simulate network latency
	if req.Phase == PhasePrep {
		time.Sleep(m.prepareLatency)
	} else if req.Phase == PhaseCommit {
		time.Sleep(m.commitLatency)
	}

	// Check if we have a pre-configured response for this node
	m.mu.Lock()
	resp, exists := m.responses[nodeID]
	m.mu.Unlock()

	if exists {
		return resp, nil
	}

	// Default: success
	return &ReplicationResponse{
		Success: true,
	}, nil
}

func (m *mockReplicator) SetNodeResponse(nodeID uint64, resp *ReplicationResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[nodeID] = resp
}

func (m *mockReplicator) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// TestFullReplication_QuorumWrite tests that writes succeed with quorum
func TestFullReplication_QuorumWrite(t *testing.T) {
	// Setup: 5-node cluster
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()

	coordinator := NewWriteCoordinator(
		1, // node 1 is coordinator
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
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
	// Should be called twice per node: PREPARE + COMMIT
	// 5 nodes (including self) × 2 phases = 10 calls
	expectedCalls := 10
	if callCount != expectedCalls {
		t.Errorf("Expected %d replication calls, got %d", expectedCalls, callCount)
	}
}

// TestFullReplication_QuorumFailure tests that writes fail without quorum
func TestFullReplication_QuorumFailure(t *testing.T) {
	// Setup: 5-node cluster
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()

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

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
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

	// Verify all nodes including self received the write
	// 3 nodes (including self) × 2 phases (PREPARE + COMMIT) = 6 calls
	callCount := replicator.GetCallCount()
	expectedCalls := 6
	if callCount != expectedCalls {
		t.Errorf("Expected %d replication calls (all nodes), got %d", expectedCalls, callCount)
	}
}

// TestFullReplication_NodeFailureDuringWrite tests that dead nodes don't block writes
func TestFullReplication_NodeFailureDuringWrite(t *testing.T) {
	// Setup: 5-node cluster, node 5 is dead
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()

	// Node 5 is dead/unresponsive
	replicator.SetNodeResponse(5, &ReplicationResponse{Success: false})

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
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
