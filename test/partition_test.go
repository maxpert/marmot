package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// PartitionSimulator simulates network partitions between nodes
// Inspired by Jepsen and FoundationDB's deterministic simulation testing
type PartitionSimulator struct {
	mu         sync.RWMutex
	partitions map[uint64]map[uint64]bool // from -> to -> blocked
	nodeCount  int
}

// NewPartitionSimulator creates a new partition simulator
func NewPartitionSimulator(nodeCount int) *PartitionSimulator {
	return &PartitionSimulator{
		partitions: make(map[uint64]map[uint64]bool),
		nodeCount:  nodeCount,
	}
}

// PartitionNodes creates a network partition between two sets of nodes
// Nodes in setA cannot communicate with nodes in setB and vice versa
func (ps *PartitionSimulator) PartitionNodes(setA, setB []uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, a := range setA {
		if ps.partitions[a] == nil {
			ps.partitions[a] = make(map[uint64]bool)
		}
		for _, b := range setB {
			ps.partitions[a][b] = true
		}
	}
	for _, b := range setB {
		if ps.partitions[b] == nil {
			ps.partitions[b] = make(map[uint64]bool)
		}
		for _, a := range setA {
			ps.partitions[b][a] = true
		}
	}
}

// HealPartition removes the partition between two sets of nodes
func (ps *PartitionSimulator) HealPartition(setA, setB []uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, a := range setA {
		for _, b := range setB {
			delete(ps.partitions[a], b)
		}
	}
	for _, b := range setB {
		for _, a := range setA {
			delete(ps.partitions[b], a)
		}
	}
}

// IsBlocked returns true if communication from node A to node B is blocked
func (ps *PartitionSimulator) IsBlocked(from, to uint64) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if blocked, ok := ps.partitions[from]; ok {
		return blocked[to]
	}
	return false
}

// Reset clears all partitions
func (ps *PartitionSimulator) Reset() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.partitions = make(map[uint64]map[uint64]bool)
}

// MockNodeState tracks state for testing
type MockNodeState struct {
	mu           sync.RWMutex
	nodeID       uint64
	data         map[string]string // key -> value
	txnLog       []MockTxn         // transaction log
	lastTxnID    uint64
	clock        *hlc.Clock
	isAlive      bool
	partitionSim *PartitionSimulator
}

type MockTxn struct {
	ID        uint64
	Timestamp hlc.Timestamp
	Key       string
	Value     string
	NodeID    uint64
}

// NewMockNodeState creates a new mock node
func NewMockNodeState(nodeID uint64, partitionSim *PartitionSimulator) *MockNodeState {
	return &MockNodeState{
		nodeID:       nodeID,
		data:         make(map[string]string),
		txnLog:       make([]MockTxn, 0),
		clock:        hlc.NewClock(nodeID),
		isAlive:      true,
		partitionSim: partitionSim,
	}
}

// Write performs a write operation (with LWW semantics)
func (n *MockNodeState) Write(key, value string) (MockTxn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.isAlive {
		return MockTxn{}, fmt.Errorf("node %d is down", n.nodeID)
	}

	n.lastTxnID++
	ts := n.clock.Now()

	txn := MockTxn{
		ID:        n.lastTxnID,
		Timestamp: ts,
		Key:       key,
		Value:     value,
		NodeID:    n.nodeID,
	}

	n.data[key] = value
	n.txnLog = append(n.txnLog, txn)
	return txn, nil
}

// Read reads a value
func (n *MockNodeState) Read(key string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	val, ok := n.data[key]
	return val, ok
}

// ApplyTxnWithLWW applies a transaction using LWW conflict resolution
func (n *MockNodeState) ApplyTxnWithLWW(txn MockTxn) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we have a newer version
	for i := len(n.txnLog) - 1; i >= 0; i-- {
		existingTxn := n.txnLog[i]
		if existingTxn.Key == txn.Key {
			// LWW: compare timestamps
			cmp := hlc.Compare(txn.Timestamp, existingTxn.Timestamp)
			if cmp <= 0 {
				// Existing is newer or equal - skip
				return false
			}
			break
		}
	}

	// Apply the transaction
	n.data[txn.Key] = txn.Value
	n.txnLog = append(n.txnLog, txn)
	return true
}

// GetTxnLogSince returns transactions since the given txnID
func (n *MockNodeState) GetTxnLogSince(txnID uint64) []MockTxn {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var result []MockTxn
	for _, txn := range n.txnLog {
		if txn.ID > txnID {
			result = append(result, txn)
		}
	}
	return result
}

// SetAlive sets node availability
func (n *MockNodeState) SetAlive(alive bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isAlive = alive
}

// MockCluster manages multiple mock nodes
type MockCluster struct {
	nodes        []*MockNodeState
	partitionSim *PartitionSimulator
}

// NewMockCluster creates a new mock cluster
func NewMockCluster(nodeCount int) *MockCluster {
	ps := NewPartitionSimulator(nodeCount)
	nodes := make([]*MockNodeState, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewMockNodeState(uint64(i+1), ps)
	}
	return &MockCluster{
		nodes:        nodes,
		partitionSim: ps,
	}
}

// GetNode returns a node by ID (1-indexed)
func (c *MockCluster) GetNode(nodeID uint64) *MockNodeState {
	if nodeID < 1 || int(nodeID) > len(c.nodes) {
		return nil
	}
	return c.nodes[nodeID-1]
}

// ReplicateToQuorum replicates a transaction to quorum of nodes
func (c *MockCluster) ReplicateToQuorum(sourceTxn MockTxn) (int, error) {
	sourceNode := c.GetNode(sourceTxn.NodeID)
	if sourceNode == nil {
		return 0, fmt.Errorf("source node not found")
	}

	quorum := len(c.nodes)/2 + 1
	acks := 1 // source already has it

	for _, node := range c.nodes {
		if node.nodeID == sourceTxn.NodeID {
			continue
		}

		// Check if blocked by partition
		if c.partitionSim.IsBlocked(sourceTxn.NodeID, node.nodeID) {
			continue
		}

		if node.ApplyTxnWithLWW(sourceTxn) {
			acks++
		}
	}

	if acks < quorum {
		return acks, fmt.Errorf("quorum not achieved: got %d, need %d", acks, quorum)
	}
	return acks, nil
}

// HealAndSync simulates partition healing with delta sync
func (c *MockCluster) HealAndSync() {
	c.partitionSim.Reset()

	// Each node syncs with all other nodes using LWW
	for _, srcNode := range c.nodes {
		for _, dstNode := range c.nodes {
			if srcNode.nodeID == dstNode.nodeID {
				continue
			}

			// Get all transactions from source and apply to dest
			txns := srcNode.GetTxnLogSince(0)
			for _, txn := range txns {
				dstNode.ApplyTxnWithLWW(txn)
			}
		}
	}
}

// VerifyConsistency checks if all nodes have consistent data
func (c *MockCluster) VerifyConsistency() (bool, map[string][]string) {
	inconsistencies := make(map[string][]string)

	// Collect all keys
	allKeys := make(map[string]bool)
	for _, node := range c.nodes {
		node.mu.RLock()
		for k := range node.data {
			allKeys[k] = true
		}
		node.mu.RUnlock()
	}

	// Check each key across all nodes
	for key := range allKeys {
		var values []string
		for _, node := range c.nodes {
			val, _ := node.Read(key)
			values = append(values, val)
		}

		// Check if all values are the same
		first := values[0]
		for _, v := range values[1:] {
			if v != first {
				inconsistencies[key] = values
				break
			}
		}
	}

	return len(inconsistencies) == 0, inconsistencies
}

// =======================
// TESTS
// =======================

// TestPartitionAndHeal tests basic partition and healing
func TestPartitionAndHeal(t *testing.T) {
	cluster := NewMockCluster(3)

	// Write to node 1
	node1 := cluster.GetNode(1)
	txn1, _ := node1.Write("key1", "value1")

	// Replicate to quorum
	acks, err := cluster.ReplicateToQuorum(txn1)
	if err != nil {
		t.Fatalf("Failed to replicate: %v", err)
	}
	t.Logf("Replicated to %d nodes", acks)

	// Create partition: node1 isolated
	cluster.partitionSim.PartitionNodes([]uint64{1}, []uint64{2, 3})

	// Try to write from node1 (should fail quorum)
	txn2, _ := node1.Write("key2", "value2")
	_, err = cluster.ReplicateToQuorum(txn2)
	if err == nil {
		t.Fatal("Expected quorum failure during partition")
	}
	t.Logf("Quorum correctly failed: %v", err)

	// Write from node2 (should succeed - majority partition)
	node2 := cluster.GetNode(2)
	txn3, _ := node2.Write("key3", "value3")
	acks, err = cluster.ReplicateToQuorum(txn3)
	if err != nil {
		t.Fatalf("Failed to replicate in majority partition: %v", err)
	}
	t.Logf("Majority partition replicated to %d nodes", acks)

	// Heal partition and sync
	cluster.HealAndSync()

	// Verify consistency
	consistent, inconsistencies := cluster.VerifyConsistency()
	if !consistent {
		t.Fatalf("Inconsistency after healing: %v", inconsistencies)
	}
	t.Log("All nodes consistent after partition healing")
}

// TestConcurrentWritesDuringPartition tests LWW during partition
func TestConcurrentWritesDuringPartition(t *testing.T) {
	cluster := NewMockCluster(3)

	// Partition: node1 vs node2,3
	cluster.partitionSim.PartitionNodes([]uint64{1}, []uint64{2, 3})

	// Both partitions write to the same key
	node1 := cluster.GetNode(1)
	node2 := cluster.GetNode(2)

	// Add delay to ensure different timestamps
	time.Sleep(1 * time.Millisecond)
	txn1, _ := node1.Write("key", "value_from_node1")

	time.Sleep(1 * time.Millisecond)
	txn2, _ := node2.Write("key", "value_from_node2") // This should win (later timestamp)

	// Replicate within partitions (will fail quorum for node1)
	cluster.ReplicateToQuorum(txn1)
	cluster.ReplicateToQuorum(txn2)

	// Heal and sync
	cluster.HealAndSync()

	// The value should be from node2 (later timestamp)
	val1, _ := node1.Read("key")
	val2, _ := node2.Read("key")

	// LWW should pick the later write
	if hlc.Compare(txn2.Timestamp, txn1.Timestamp) > 0 {
		if val1 != "value_from_node2" || val2 != "value_from_node2" {
			t.Fatalf("LWW failed: expected value_from_node2, got node1=%s, node2=%s", val1, val2)
		}
		t.Log("LWW correctly chose later write (node2)")
	} else {
		if val1 != "value_from_node1" || val2 != "value_from_node1" {
			t.Fatalf("LWW failed: expected value_from_node1, got node1=%s, node2=%s", val1, val2)
		}
		t.Log("LWW correctly chose later write (node1)")
	}
}

// TestMajorityPartitionCanProgress tests that majority partition can make progress
func TestMajorityPartitionCanProgress(t *testing.T) {
	cluster := NewMockCluster(5)

	// Partition: nodes 1,2 vs nodes 3,4,5 (majority)
	cluster.partitionSim.PartitionNodes([]uint64{1, 2}, []uint64{3, 4, 5})

	// Minority partition writes should fail
	node1 := cluster.GetNode(1)
	txn1, _ := node1.Write("key", "minority_value")
	_, err := cluster.ReplicateToQuorum(txn1)
	if err == nil {
		t.Fatal("Minority partition should not achieve quorum")
	}
	t.Logf("Minority write correctly failed: %v", err)

	// Majority partition writes should succeed
	node3 := cluster.GetNode(3)
	txn2, _ := node3.Write("key", "majority_value")
	acks, err := cluster.ReplicateToQuorum(txn2)
	if err != nil {
		t.Fatalf("Majority partition should achieve quorum: %v", err)
	}
	t.Logf("Majority write succeeded with %d acks", acks)

	// Heal and verify
	cluster.HealAndSync()

	consistent, _ := cluster.VerifyConsistency()
	if !consistent {
		t.Fatal("Inconsistent after healing")
	}

	// Final value should be majority_value (it was committed)
	val, _ := node1.Read("key")
	if val != "majority_value" {
		t.Fatalf("Expected majority_value, got %s", val)
	}
	t.Log("Majority partition value correctly propagated after healing")
}

// TestConsistencyInvariant verifies the consistency invariant is maintained
func TestConsistencyInvariant(t *testing.T) {
	cluster := NewMockCluster(3)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run concurrent operations
	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// Writer goroutines
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(nodeID uint64) {
			defer wg.Done()
			node := cluster.GetNode(nodeID)
			for j := 0; j < 10; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					key := fmt.Sprintf("key%d", j%3)
					value := fmt.Sprintf("node%d_v%d", nodeID, j)
					txn, _ := node.Write(key, value)
					_, err := cluster.ReplicateToQuorum(txn)
					if err != nil {
						// Quorum failures are expected during partitions
						continue
					}
				}
			}
		}(uint64(i + 1))
	}

	// Partition controller
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				// Create random partition
				cluster.partitionSim.PartitionNodes([]uint64{1}, []uint64{2, 3})
				time.Sleep(50 * time.Millisecond)
				cluster.partitionSim.Reset()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	close(errChan)

	// Heal and verify final consistency
	cluster.HealAndSync()

	consistent, inconsistencies := cluster.VerifyConsistency()
	if !consistent {
		t.Fatalf("Final inconsistency detected: %v", inconsistencies)
	}
	t.Log("Consistency invariant maintained throughout test")
}
