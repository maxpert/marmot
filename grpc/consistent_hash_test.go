package grpc

import (
	"fmt"
	"testing"
)

func TestConsistentHash_AddNode(t *testing.T) {
	ch := NewConsistentHash(3, 150)

	// Add nodes
	ch.AddNode(1)
	ch.AddNode(2)
	ch.AddNode(3)

	// Verify nodes were added
	if ch.Count() != 3 {
		t.Errorf("Expected 3 nodes, got %d", ch.Count())
	}

	// Verify virtual nodes were created (150 per physical node)
	if len(ch.ring) != 450 {
		t.Errorf("Expected 450 virtual nodes, got %d", len(ch.ring))
	}

	// Adding same node again should be idempotent
	ch.AddNode(1)
	if ch.Count() != 3 {
		t.Errorf("Expected 3 nodes after re-adding, got %d", ch.Count())
	}
}

func TestConsistentHash_RemoveNode(t *testing.T) {
	ch := NewConsistentHash(3, 150)

	ch.AddNode(1)
	ch.AddNode(2)
	ch.AddNode(3)

	// Remove node
	ch.RemoveNode(2)

	if ch.Count() != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", ch.Count())
	}

	// Verify virtual nodes were removed (2 nodes * 150 vnodes)
	if len(ch.ring) != 300 {
		t.Errorf("Expected 300 virtual nodes, got %d", len(ch.ring))
	}

	// Removing non-existent node should be safe
	ch.RemoveNode(999)
	if ch.Count() != 2 {
		t.Errorf("Expected 2 nodes after removing non-existent, got %d", ch.Count())
	}
}

func TestConsistentHash_GetNode(t *testing.T) {
	ch := NewConsistentHash(3, 150)

	// Getting node from empty ring should fail
	_, err := ch.GetNode("key1")
	if err == nil {
		t.Error("Expected error for empty ring")
	}

	// Add nodes
	ch.AddNode(1)
	ch.AddNode(2)
	ch.AddNode(3)

	// Get node for key
	node, err := ch.GetNode("test-key")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Node should be one of our added nodes
	if node != 1 && node != 2 && node != 3 {
		t.Errorf("Got unexpected node ID: %d", node)
	}

	// Same key should always return same node (consistency)
	for i := 0; i < 100; i++ {
		n, _ := ch.GetNode("test-key")
		if n != node {
			t.Errorf("Key mapped to different node: expected %d, got %d", node, n)
		}
	}
}

func TestConsistentHash_GetReplicas(t *testing.T) {
	ch := NewConsistentHash(3, 150)

	// Add 5 nodes
	for i := uint64(1); i <= 5; i++ {
		ch.AddNode(i)
	}

	// Get replicas for a key
	replicas := ch.GetReplicas("test-key")

	// Should return 3 replicas (replication factor)
	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(replicas))
	}

	// All replicas should be distinct
	seen := make(map[uint64]bool)
	for _, r := range replicas {
		if seen[r] {
			t.Errorf("Duplicate replica found: %d", r)
		}
		seen[r] = true
	}

	// All replicas should be valid node IDs
	for _, r := range replicas {
		if r < 1 || r > 5 {
			t.Errorf("Invalid replica node ID: %d", r)
		}
	}

	// Same key should always return same replicas in same order
	for i := 0; i < 100; i++ {
		r := ch.GetReplicas("test-key")
		if len(r) != len(replicas) {
			t.Error("Replica count changed")
		}
		for j := range r {
			if r[j] != replicas[j] {
				t.Error("Replica order changed")
			}
		}
	}
}

func TestConsistentHash_GetReplicas_FewerNodesThanReplicas(t *testing.T) {
	ch := NewConsistentHash(5, 150) // Want 5 replicas

	// Add only 3 nodes
	ch.AddNode(1)
	ch.AddNode(2)
	ch.AddNode(3)

	// Should return all 3 nodes (can't have more replicas than nodes)
	replicas := ch.GetReplicas("test-key")
	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(replicas))
	}
}

func TestConsistentHash_Distribution(t *testing.T) {
	ch := NewConsistentHash(3, 150)

	// Add 5 nodes
	for i := uint64(1); i <= 5; i++ {
		ch.AddNode(i)
	}

	// Generate many keys and track distribution
	distribution := make(map[uint64]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := ch.GetNode(key)
		distribution[node]++
	}

	// Check each node got a reasonable share (within 30% of expected)
	expectedPerNode := numKeys / 5
	tolerance := float64(expectedPerNode) * 0.3

	for nodeID := uint64(1); nodeID <= 5; nodeID++ {
		count := distribution[nodeID]
		diff := float64(count - expectedPerNode)
		if diff < 0 {
			diff = -diff
		}

		if diff > tolerance {
			t.Errorf("Poor distribution for node %d: got %d keys, expected ~%d (tolerance ±%.0f)",
				nodeID, count, expectedPerNode, tolerance)
		}
	}

	t.Logf("Distribution across 5 nodes for %d keys:", numKeys)
	for nodeID := uint64(1); nodeID <= 5; nodeID++ {
		percentage := float64(distribution[nodeID]) * 100 / float64(numKeys)
		t.Logf("  Node %d: %d keys (%.1f%%)", nodeID, distribution[nodeID], percentage)
	}
}

func TestConsistentHash_Rebalance(t *testing.T) {
	ch := NewConsistentHash(3, 150)

	// Start with 3 nodes
	ch.AddNode(1)
	ch.AddNode(2)
	ch.AddNode(3)

	// Track initial mappings
	numKeys := 1000
	initialMapping := make(map[string]uint64)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := ch.GetNode(key)
		initialMapping[key] = node
	}

	// Add a 4th node
	ch.AddNode(4)

	// Count how many keys moved
	moved := 0
	for key, oldNode := range initialMapping {
		newNode, _ := ch.GetNode(key)
		if newNode != oldNode {
			moved++
		}
	}

	// Should move roughly 25% of keys (1/4 of load redistributes to new node)
	// Allow 15-35% range due to hash randomness
	movedPct := float64(moved) * 100 / float64(numKeys)

	t.Logf("Added node: %d keys moved (%.1f%%)", moved, movedPct)

	if movedPct < 15 || movedPct > 35 {
		t.Errorf("Expected 15-35%% of keys to move, got %.1f%%", movedPct)
	}
}

func BenchmarkConsistentHash_GetNode(b *testing.B) {
	ch := NewConsistentHash(3, 150)

	// Add 10 nodes
	for i := uint64(1); i <= 10; i++ {
		ch.AddNode(i)
	}

	// Benchmark key lookups
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		ch.GetNode(key)
	}
}

func BenchmarkConsistentHash_GetReplicas(b *testing.B) {
	ch := NewConsistentHash(3, 150)

	// Add 10 nodes
	for i := uint64(1); i <= 10; i++ {
		ch.AddNode(i)
	}

	// Benchmark replica lookups
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		ch.GetReplicas(key)
	}
}
