package grpc

import (
	"testing"
	"time"
)

// TestAliveStatusPropagation verifies that when a node transitions from JOINING to ALIVE,
// the status propagates to other nodes via gossip within a reasonable timeframe
func TestAliveStatusPropagation(t *testing.T) {
	// Setup: Create two node registries (seed and joining node)
	seedRegistry := NewNodeRegistry(1, "localhost:8081")
	joiningRegistry := NewNodeRegistry(2, "localhost:8082")

	// Seed adds the joining node as JOINING
	seedRegistry.Add(&NodeState{
		NodeId:      2,
		Address:     "localhost:8082",
		Status:      NodeStatus_JOINING,
		Incarnation: 0,
	})

	// Mark the joining node as JOINING in its own registry (simulating join process)
	joiningRegistry.MarkJoining(2)

	// Simulate the joining node marking itself ALIVE
	joiningRegistry.MarkAlive(2)

	// Verify the joining node sees itself as ALIVE
	joiningNode, _ := joiningRegistry.Get(2)
	if joiningNode.Status != NodeStatus_ALIVE {
		t.Fatalf("Joining node should be ALIVE after MarkAlive()")
	}
	if joiningNode.Incarnation != 1 {
		t.Fatalf("Incarnation should be 1 after MarkAlive(), got %d", joiningNode.Incarnation)
	}

	// Simulate gossip: joining node sends its state to seed
	allNodes := joiningRegistry.GetAll()
	for _, node := range allNodes {
		if node.NodeId == 2 {
			// Seed receives gossip with Node 2's ALIVE status
			seedRegistry.Update(node)
		}
	}

	// Verify: Seed should now see Node 2 as ALIVE
	node2InSeed, exists := seedRegistry.Get(2)
	if !exists {
		t.Fatal("Seed should have Node 2 in registry")
	}
	if node2InSeed.Status != NodeStatus_ALIVE {
		t.Fatalf("Seed should see Node 2 as ALIVE, got %s", node2InSeed.Status.String())
	}
	if node2InSeed.Incarnation != 1 {
		t.Fatalf("Seed should have Node 2 with incarnation 1, got %d", node2InSeed.Incarnation)
	}

	t.Log("SUCCESS: ALIVE status propagated correctly via simulated gossip")
}

// TestGetReplicationEligibleExcludesJoining verifies that JOINING nodes are excluded from replication
func TestGetReplicationEligibleExcludesJoining(t *testing.T) {
	registry := NewNodeRegistry(1, "localhost:8081")

	// Add nodes in different states
	registry.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE, Incarnation: 0})
	registry.Add(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_JOINING, Incarnation: 0})
	registry.Add(&NodeState{NodeId: 4, Address: "localhost:8084", Status: NodeStatus_SUSPECT, Incarnation: 0})

	eligible := registry.GetReplicationEligible()

	// Should include: Node 1 (ALIVE, self), Node 2 (ALIVE)
	// Should exclude: Node 3 (JOINING), Node 4 (SUSPECT)
	if len(eligible) != 2 {
		t.Fatalf("Expected 2 replication-eligible nodes, got %d", len(eligible))
	}

	hasNode1 := false
	hasNode2 := false
	for _, node := range eligible {
		if node.NodeId == 1 {
			hasNode1 = true
		}
		if node.NodeId == 2 {
			hasNode2 = true
		}
		if node.NodeId == 3 || node.NodeId == 4 {
			t.Fatalf("Node %d should not be replication-eligible (status: %s)", node.NodeId, node.Status.String())
		}
	}

	if !hasNode1 || !hasNode2 {
		t.Fatal("Should include nodes 1 and 2 as replication-eligible")
	}

	t.Log("SUCCESS: GetReplicationEligible correctly excludes JOINING and SUSPECT nodes")
}

// TestRaceConditionSimulation simulates the race condition where CREATE DATABASE
// happens before ALIVE status propagates
func TestRaceConditionSimulation(t *testing.T) {
	// Setup: Seed and joining node
	seedRegistry := NewNodeRegistry(1, "localhost:8081")
	joiningRegistry := NewNodeRegistry(3, "localhost:8083")

	// Initially, seed knows about node 3 as JOINING
	seedRegistry.Add(&NodeState{
		NodeId:      3,
		Address:     "localhost:8083",
		Status:      NodeStatus_JOINING,
		Incarnation: 0,
	})

	t.Log("Initial state: Seed sees Node 3 as JOINING")

	// Simulate: Node 3 marks itself ALIVE
	joiningRegistry.MarkAlive(3)
	t.Log("Node 3 marked itself ALIVE (incarnation=1)")

	// Simulate: CREATE DATABASE happens IMMEDIATELY (before gossip propagates)
	eligibleBefore := seedRegistry.GetReplicationEligible()
	hasNode3Before := false
	for _, node := range eligibleBefore {
		if node.NodeId == 3 {
			hasNode3Before = true
		}
	}

	if hasNode3Before {
		t.Fatal("RACE CONDITION NOT REPRODUCED: Node 3 should NOT be replication-eligible yet")
	}
	t.Log("CREATE DATABASE excludes Node 3 (still JOINING in seed's view)")

	// Simulate: Gossip propagates after a delay
	time.Sleep(100 * time.Millisecond)
	allNodes := joiningRegistry.GetAll()
	for _, node := range allNodes {
		if node.NodeId == 3 {
			seedRegistry.Update(node)
		}
	}

	// Now check if Node 3 is replication-eligible
	eligibleAfter := seedRegistry.GetReplicationEligible()
	hasNode3After := false
	for _, node := range eligibleAfter {
		if node.NodeId == 3 {
			hasNode3After = true
		}
	}

	if !hasNode3After {
		t.Fatal("After gossip propagation, Node 3 should be replication-eligible")
	}

	t.Log("SUCCESS: Race condition reproduced - Node 3 excluded initially, included after gossip")
	t.Log("This explains why Node 3 doesn't receive CREATE DATABASE replication")
}
