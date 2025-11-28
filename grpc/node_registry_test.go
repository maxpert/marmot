package grpc

import (
	"testing"
	"time"
)

func TestNodeRegistry_Add(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Should start with self
	if nr.Count() != 1 {
		t.Errorf("Expected 1 node (self), got %d", nr.Count())
	}

	// Add new nodes
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "node2:8080",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	nr.Add(&NodeState{
		NodeId:      3,
		Address:     "node3:8080",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	if nr.Count() != 3 {
		t.Errorf("Expected 3 nodes, got %d", nr.Count())
	}
}

func TestNodeRegistry_Get(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Get self
	node, exists := nr.Get(1)
	if !exists {
		t.Error("Expected to find self")
	}
	if node.NodeId != 1 {
		t.Errorf("Expected node ID 1, got %d", node.NodeId)
	}

	// Get non-existent node
	_, exists = nr.Get(999)
	if exists {
		t.Error("Expected not to find non-existent node")
	}

	// Add and get node
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "node2:8080",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	node, exists = nr.Get(2)
	if !exists {
		t.Error("Expected to find added node")
	}
	if node.Address != "node2:8080" {
		t.Errorf("Expected address node2:8080, got %s", node.Address)
	}
}

func TestNodeRegistry_Update(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add node
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "node2:8080",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	// Update with higher incarnation
	nr.Update(&NodeState{
		NodeId:      2,
		Address:     "node2:8080",
		Status:      NodeStatus_SUSPECT,
		Incarnation: 1,
	})

	node, _ := nr.Get(2)
	if node.Status != NodeStatus_SUSPECT {
		t.Errorf("Expected status SUSPECT, got %v", node.Status)
	}
	if node.Incarnation != 1 {
		t.Errorf("Expected incarnation 1, got %d", node.Incarnation)
	}

	// Update with same incarnation should not change
	nr.Update(&NodeState{
		NodeId:      2,
		Address:     "node2:8080",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
	})

	node, _ = nr.Get(2)
	if node.Status != NodeStatus_SUSPECT {
		t.Error("Status should not change with same incarnation")
	}

	// Update with lower incarnation should not change
	nr.Update(&NodeState{
		NodeId:      2,
		Address:     "node2:8080",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	node, _ = nr.Get(2)
	if node.Status != NodeStatus_SUSPECT {
		t.Error("Status should not change with lower incarnation")
	}
}

func TestNodeRegistry_GetAlive(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add mix of alive and dead nodes
	nr.Add(&NodeState{NodeId: 2, Status: NodeStatus_ALIVE})
	nr.Add(&NodeState{NodeId: 3, Status: NodeStatus_SUSPECT})
	nr.Add(&NodeState{NodeId: 4, Status: NodeStatus_DEAD})
	nr.Add(&NodeState{NodeId: 5, Status: NodeStatus_ALIVE})

	alive := nr.GetAlive()

	// Should have self (1) + nodes 2 and 5
	if len(alive) != 3 {
		t.Errorf("Expected 3 alive nodes, got %d", len(alive))
	}

	// Verify only alive nodes returned
	for _, node := range alive {
		if node.Status != NodeStatus_ALIVE {
			t.Errorf("Node %d is not ALIVE: %v", node.NodeId, node.Status)
		}
	}
}

func TestNodeRegistry_MarkSuspect(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	nr.Add(&NodeState{
		NodeId: 2,
		Status: NodeStatus_ALIVE,
	})

	nr.MarkSuspect(2)

	node, _ := nr.Get(2)
	if node.Status != NodeStatus_SUSPECT {
		t.Errorf("Expected status SUSPECT, got %v", node.Status)
	}
}

func TestNodeRegistry_MarkDead(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	nr.Add(&NodeState{
		NodeId: 2,
		Status: NodeStatus_ALIVE,
	})

	nr.MarkDead(2)

	node, _ := nr.Get(2)
	if node.Status != NodeStatus_DEAD {
		t.Errorf("Expected status DEAD, got %v", node.Status)
	}
}

func TestNodeRegistry_Remove(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	nr.Add(&NodeState{NodeId: 2})
	nr.Add(&NodeState{NodeId: 3})

	if nr.Count() != 3 {
		t.Errorf("Expected 3 nodes, got %d", nr.Count())
	}

	nr.Remove(2)

	if nr.Count() != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", nr.Count())
	}

	_, exists := nr.Get(2)
	if exists {
		t.Error("Node should not exist after removal")
	}
}

func TestNodeRegistry_CheckTimeouts(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add node and record when we saw it
	nr.Add(&NodeState{
		NodeId: 2,
		Status: NodeStatus_ALIVE,
	})

	// Simulate passage of time by manually setting lastSeen to past
	nr.lastSeen[2] = time.Now().Add(-6 * time.Second)

	// Check timeouts with 5 second suspect timeout
	nr.CheckTimeouts(5*time.Second, 10*time.Second)

	node, _ := nr.Get(2)
	if node.Status != NodeStatus_SUSPECT {
		t.Errorf("Node should be SUSPECT after timeout, got %v", node.Status)
	}

	// Simulate more time passing
	nr.lastSeen[2] = time.Now().Add(-11 * time.Second)

	// Check with 10 second dead timeout
	nr.CheckTimeouts(5*time.Second, 10*time.Second)

	node, _ = nr.Get(2)
	if node.Status != NodeStatus_DEAD {
		t.Errorf("Node should be DEAD after timeout, got %v", node.Status)
	}
}

func TestNodeRegistry_CheckTimeouts_SkipsSelf(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Manipulate self's lastSeen
	nr.lastSeen[1] = time.Now().Add(-100 * time.Second)

	// Check timeouts
	nr.CheckTimeouts(5*time.Second, 10*time.Second)

	// Self should still be ALIVE
	node, _ := nr.Get(1)
	if node.Status != NodeStatus_ALIVE {
		t.Error("Self should never timeout")
	}
}

func TestNodeRegistry_CountAlive(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add mix of nodes
	nr.Add(&NodeState{NodeId: 2, Status: NodeStatus_ALIVE})
	nr.Add(&NodeState{NodeId: 3, Status: NodeStatus_SUSPECT})
	nr.Add(&NodeState{NodeId: 4, Status: NodeStatus_DEAD})
	nr.Add(&NodeState{NodeId: 5, Status: NodeStatus_ALIVE})

	aliveCount := nr.CountAlive()

	// Self (1) + nodes 2 and 5 = 3
	if aliveCount != 3 {
		t.Errorf("Expected 3 alive nodes, got %d", aliveCount)
	}
}

func TestNodeRegistry_GetAll(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	nr.Add(&NodeState{NodeId: 2})
	nr.Add(&NodeState{NodeId: 3})
	nr.Add(&NodeState{NodeId: 4})

	all := nr.GetAll()

	if len(all) != 4 {
		t.Errorf("Expected 4 nodes, got %d", len(all))
	}

	// Verify all node IDs present
	ids := make(map[uint64]bool)
	for _, node := range all {
		ids[node.NodeId] = true
	}

	for i := uint64(1); i <= 4; i++ {
		if !ids[i] {
			t.Errorf("Node %d missing from GetAll", i)
		}
	}
}

func BenchmarkNodeRegistry_Add(b *testing.B) {
	nr := NewNodeRegistry(1, "localhost:8081")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nr.Add(&NodeState{
			NodeId: uint64(i + 2),
			Status: NodeStatus_ALIVE,
		})
	}
}

func BenchmarkNodeRegistry_Get(b *testing.B) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Pre-populate
	for i := 2; i <= 1000; i++ {
		nr.Add(&NodeState{
			NodeId: uint64(i),
			Status: NodeStatus_ALIVE,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nr.Get(uint64((i % 999) + 2))
	}
}

func BenchmarkNodeRegistry_GetAlive(b *testing.B) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Pre-populate with mix
	for i := 2; i <= 1000; i++ {
		status := NodeStatus_ALIVE
		if i%3 == 0 {
			status = NodeStatus_SUSPECT
		}
		if i%5 == 0 {
			status = NodeStatus_DEAD
		}
		nr.Add(&NodeState{
			NodeId: uint64(i),
			Status: status,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nr.GetAlive()
	}
}

// TestSWIMRefutation verifies that a node refutes SUSPECT/DEAD claims about itself
func TestSWIMRefutation(t *testing.T) {
	localID := uint64(1)
	nr := NewNodeRegistry(localID, "localhost:8081")

	// Verify local node starts as ALIVE at incarnation 0
	self, exists := nr.Get(localID)
	if !exists {
		t.Fatal("Local node should exist in registry")
	}
	if self.Status != NodeStatus_ALIVE {
		t.Errorf("Local node should be ALIVE, got %v", self.Status)
	}
	if self.Incarnation != 0 {
		t.Errorf("Local node should have incarnation 0, got %d", self.Incarnation)
	}

	// Test 1: Another node claims we're SUSPECT at incarnation 0
	// We should refute by incrementing to incarnation 1
	suspectClaim := &NodeState{
		NodeId:      localID,
		Status:      NodeStatus_SUSPECT,
		Incarnation: 0,
		Address:     "localhost:8081",
	}
	nr.Update(suspectClaim)

	self, _ = nr.Get(localID)
	if self.Status != NodeStatus_ALIVE {
		t.Errorf("Should have refuted SUSPECT claim, status is %v", self.Status)
	}
	if self.Incarnation != 1 {
		t.Errorf("Should have incremented incarnation to 1, got %d", self.Incarnation)
	}

	// Test 2: Another SUSPECT claim at same incarnation (1)
	// We should refute again
	suspectClaim2 := &NodeState{
		NodeId:      localID,
		Status:      NodeStatus_SUSPECT,
		Incarnation: 1,
		Address:     "localhost:8081",
	}
	nr.Update(suspectClaim2)

	self, _ = nr.Get(localID)
	if self.Status != NodeStatus_ALIVE {
		t.Errorf("Should have refuted second SUSPECT claim, status is %v", self.Status)
	}
	if self.Incarnation != 2 {
		t.Errorf("Should have incremented incarnation to 2, got %d", self.Incarnation)
	}

	// Test 3: DEAD claim should also be refuted
	deadClaim := &NodeState{
		NodeId:      localID,
		Status:      NodeStatus_DEAD,
		Incarnation: 2,
		Address:     "localhost:8081",
	}
	nr.Update(deadClaim)

	self, _ = nr.Get(localID)
	if self.Status != NodeStatus_ALIVE {
		t.Errorf("Should have refuted DEAD claim, status is %v", self.Status)
	}
	if self.Incarnation != 3 {
		t.Errorf("Should have incremented incarnation to 3, got %d", self.Incarnation)
	}
}

// TestStateEscalation verifies ALIVE -> SUSPECT -> DEAD escalation rules
func TestStateEscalation(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add a remote node as ALIVE
	remoteID := uint64(2)
	remoteNode := &NodeState{
		NodeId:      remoteID,
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
		Address:     "localhost:8082",
	}
	nr.Add(remoteNode)

	// Test 1: Same incarnation, escalate ALIVE -> SUSPECT
	suspectUpdate := &NodeState{
		NodeId:      remoteID,
		Status:      NodeStatus_SUSPECT,
		Incarnation: 0,
		Address:     "localhost:8082",
	}
	nr.Update(suspectUpdate)

	node, _ := nr.Get(remoteID)
	if node.Status != NodeStatus_SUSPECT {
		t.Errorf("Should escalate to SUSPECT, got %v", node.Status)
	}

	// Test 2: Same incarnation, escalate SUSPECT -> DEAD
	deadUpdate := &NodeState{
		NodeId:      remoteID,
		Status:      NodeStatus_DEAD,
		Incarnation: 0,
		Address:     "localhost:8082",
	}
	nr.Update(deadUpdate)

	node, _ = nr.Get(remoteID)
	if node.Status != NodeStatus_DEAD {
		t.Errorf("Should escalate to DEAD, got %v", node.Status)
	}

	// Test 3: Same incarnation, try to de-escalate DEAD -> ALIVE (should be ignored)
	aliveUpdate := &NodeState{
		NodeId:      remoteID,
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
		Address:     "localhost:8082",
	}
	nr.Update(aliveUpdate)

	node, _ = nr.Get(remoteID)
	if node.Status != NodeStatus_DEAD {
		t.Errorf("Should not de-escalate, status should remain DEAD, got %v", node.Status)
	}
}

// TestIncarnationOrdering verifies higher incarnations always win
func TestIncarnationOrdering(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add node at incarnation 5
	remoteID := uint64(2)
	nr.Add(&NodeState{
		NodeId:      remoteID,
		Status:      NodeStatus_ALIVE,
		Incarnation: 5,
		Address:     "localhost:8082",
	})

	// Test 1: Lower incarnation update should be ignored
	oldUpdate := &NodeState{
		NodeId:      remoteID,
		Status:      NodeStatus_DEAD,
		Incarnation: 3,
		Address:     "localhost:8082",
	}
	nr.Update(oldUpdate)

	node, _ := nr.Get(remoteID)
	if node.Incarnation != 5 {
		t.Errorf("Should ignore old incarnation, have %d", node.Incarnation)
	}
	if node.Status != NodeStatus_ALIVE {
		t.Errorf("Status should remain ALIVE, got %v", node.Status)
	}

	// Test 2: Higher incarnation should always win
	newUpdate := &NodeState{
		NodeId:      remoteID,
		Status:      NodeStatus_SUSPECT,
		Incarnation: 10,
		Address:     "localhost:8082",
	}
	nr.Update(newUpdate)

	node, _ = nr.Get(remoteID)
	if node.Incarnation != 10 {
		t.Errorf("Should accept higher incarnation, have %d", node.Incarnation)
	}
	if node.Status != NodeStatus_SUSPECT {
		t.Errorf("Status should be SUSPECT, got %v", node.Status)
	}
}

// =======================
// MEMBERSHIP MANAGEMENT TESTS
// =======================

func TestNodeRegistry_MarkRemoved(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add a node
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	// Mark node as removed
	err := nr.MarkRemoved(2)
	if err != nil {
		t.Fatalf("MarkRemoved failed: %v", err)
	}

	// Verify status
	node, exists := nr.Get(2)
	if !exists {
		t.Fatal("Node should still exist")
	}
	if node.Status != NodeStatus_REMOVED {
		t.Errorf("Expected REMOVED status, got %v", node.Status)
	}

	// Incarnation should have incremented for gossip propagation
	if node.Incarnation != 1 {
		t.Errorf("Expected incarnation 1, got %d", node.Incarnation)
	}
}

func TestNodeRegistry_MarkRemoved_CannotRemoveSelf(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	err := nr.MarkRemoved(1)
	if err == nil {
		t.Fatal("Should not be able to remove self")
	}
}

func TestNodeRegistry_MarkRemoved_NodeNotFound(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	err := nr.MarkRemoved(999)
	if err == nil {
		t.Fatal("Should fail for non-existent node")
	}
}

func TestNodeRegistry_IsRemoved(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add and remove a node
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	// Not removed yet
	if nr.IsRemoved(2) {
		t.Error("Node should not be removed yet")
	}

	// Mark as removed
	nr.MarkRemoved(2)

	// Now should be removed
	if !nr.IsRemoved(2) {
		t.Error("Node should be marked as removed")
	}
}

func TestNodeRegistry_AllowRejoin(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add and remove a node
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})
	nr.MarkRemoved(2)

	// Allow rejoin
	err := nr.AllowRejoin(2)
	if err != nil {
		t.Fatalf("AllowRejoin failed: %v", err)
	}

	// Status should be DEAD (ready for rejoin)
	node, _ := nr.Get(2)
	if node.Status != NodeStatus_DEAD {
		t.Errorf("Expected DEAD status after AllowRejoin, got %v", node.Status)
	}

	// Node should no longer be considered removed
	if nr.IsRemoved(2) {
		t.Error("Node should not be marked as removed after AllowRejoin")
	}
}

func TestNodeRegistry_AllowRejoin_NotRemoved(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add a node but don't remove it
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	// AllowRejoin should fail since node is not REMOVED
	err := nr.AllowRejoin(2)
	if err == nil {
		t.Fatal("AllowRejoin should fail for non-REMOVED node")
	}
}

func TestNodeRegistry_RemovedExcludedFromCount(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add nodes
	nr.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE})
	nr.Add(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_ALIVE})

	// Count should be 3
	if nr.Count() != 3 {
		t.Errorf("Expected count 3, got %d", nr.Count())
	}

	// Remove one
	nr.MarkRemoved(2)

	// Count should now be 2 (REMOVED excluded)
	if nr.Count() != 2 {
		t.Errorf("Expected count 2 after removal, got %d", nr.Count())
	}
}

func TestNodeRegistry_RemovedIsSticky(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add a node
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	// Mark as removed (incarnation becomes 1)
	nr.MarkRemoved(2)

	// Try to update to ALIVE with higher incarnation via gossip
	// This should be ignored because REMOVED is sticky
	nr.Update(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 10, // Higher incarnation
	})

	// Node should still be REMOVED
	node, _ := nr.Get(2)
	if node.Status != NodeStatus_REMOVED {
		t.Errorf("REMOVED should be sticky, got %v", node.Status)
	}
}

func TestNodeRegistry_RemovedPropagatesViaGossip(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add a node as ALIVE
	nr.Add(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 5,
	})

	// Receive REMOVED status via gossip with higher incarnation
	nr.Update(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_REMOVED,
		Incarnation: 10,
	})

	// Node should now be REMOVED
	node, _ := nr.Get(2)
	if node.Status != NodeStatus_REMOVED {
		t.Errorf("REMOVED should propagate via gossip, got %v", node.Status)
	}
}

func TestNodeRegistry_QuorumInfo(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	// Add nodes
	nr.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE})
	nr.Add(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_ALIVE})
	nr.Add(&NodeState{NodeId: 4, Address: "localhost:8084", Status: NodeStatus_SUSPECT})
	nr.Add(&NodeState{NodeId: 5, Address: "localhost:8085", Status: NodeStatus_DEAD})

	// Total membership = 5 (excludes REMOVED)
	// Alive = 3 (nodes 1, 2, 3)
	// Quorum = 3 (5/2 + 1)
	total, alive, quorum := nr.QuorumInfo()

	if total != 5 {
		t.Errorf("Expected total 5, got %d", total)
	}
	if alive != 3 {
		t.Errorf("Expected alive 3, got %d", alive)
	}
	if quorum != 3 {
		t.Errorf("Expected quorum 3, got %d", quorum)
	}

	// Remove a node
	nr.MarkRemoved(5)

	// Total membership should decrease
	total, _, quorum = nr.QuorumInfo()
	if total != 4 {
		t.Errorf("Expected total 4 after removal, got %d", total)
	}
	if quorum != 3 {
		t.Errorf("Expected quorum 3 after removal, got %d", quorum)
	}
}

func TestNodeRegistry_GetMembershipInfo(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	nr.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE})

	info := nr.GetMembershipInfo()
	if len(info) != 2 {
		t.Fatalf("Expected 2 members, got %d", len(info))
	}

	// Verify info contains expected fields
	found := false
	for _, m := range info {
		if m.NodeID == 2 {
			found = true
			if m.Status != "ALIVE" {
				t.Errorf("Expected ALIVE status, got %s", m.Status)
			}
			if m.Address != "localhost:8082" {
				t.Errorf("Expected localhost:8082, got %s", m.Address)
			}
		}
	}
	if !found {
		t.Error("Node 2 not found in membership info")
	}
}
