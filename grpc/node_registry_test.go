package grpc

import (
	"testing"
	"time"
)

func TestNodeRegistry_Add(t *testing.T) {
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nr.Add(&NodeState{
			NodeId: uint64(i + 2),
			Status: NodeStatus_ALIVE,
		})
	}
}

func BenchmarkNodeRegistry_Get(b *testing.B) {
	nr := NewNodeRegistry(1)

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
	nr := NewNodeRegistry(1)

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
