package grpc

import (
	"context"
	"testing"
)

// TestGetClusterNodes_ReturnsAllNodes verifies that GetClusterNodes returns all nodes from the registry
func TestGetClusterNodes_ReturnsAllNodes(t *testing.T) {
	// Create server with test config
	config := ServerConfig{
		NodeID:           1,
		Address:          "127.0.0.1",
		Port:             8080,
		AdvertiseAddress: "127.0.0.1:8080",
	}

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Add test nodes to registry
	testNodes := []*NodeState{
		{
			NodeId:      1,
			Address:     "127.0.0.1:8080",
			Status:      NodeStatus_ALIVE,
			Incarnation: 0,
		},
		{
			NodeId:      2,
			Address:     "127.0.0.1:8081",
			Status:      NodeStatus_ALIVE,
			Incarnation: 0,
		},
		{
			NodeId:      3,
			Address:     "127.0.0.1:8082",
			Status:      NodeStatus_JOINING,
			Incarnation: 0,
		},
	}

	for _, node := range testNodes {
		server.registry.Add(node)
	}

	// Call GetClusterNodes
	req := &GetClusterNodesRequest{}
	resp, err := server.GetClusterNodes(context.Background(), req)

	// Verify response
	if err != nil {
		t.Fatalf("GetClusterNodes returned error: %v", err)
	}

	if resp == nil {
		t.Fatal("GetClusterNodes returned nil response")
	}

	if len(resp.Nodes) != len(testNodes) {
		t.Fatalf("Expected %d nodes, got %d", len(testNodes), len(resp.Nodes))
	}

	// Verify all test nodes are in the response
	nodeMap := make(map[uint64]*NodeState)
	for _, node := range resp.Nodes {
		nodeMap[node.NodeId] = node
	}

	for _, expectedNode := range testNodes {
		actualNode, exists := nodeMap[expectedNode.NodeId]
		if !exists {
			t.Errorf("Expected node %d not found in response", expectedNode.NodeId)
			continue
		}

		if actualNode.Address != expectedNode.Address {
			t.Errorf("Node %d: expected address %s, got %s",
				expectedNode.NodeId, expectedNode.Address, actualNode.Address)
		}

		if actualNode.Status != expectedNode.Status {
			t.Errorf("Node %d: expected status %v, got %v",
				expectedNode.NodeId, expectedNode.Status, actualNode.Status)
		}
	}
}

// TestGetClusterNodes_EmptyRegistry verifies that GetClusterNodes works with empty registry
func TestGetClusterNodes_EmptyRegistry(t *testing.T) {
	// Create server with test config
	config := ServerConfig{
		NodeID:           1,
		Address:          "127.0.0.1",
		Port:             8080,
		AdvertiseAddress: "127.0.0.1:8080",
	}

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Call GetClusterNodes with empty registry (only self node)
	req := &GetClusterNodesRequest{}
	resp, err := server.GetClusterNodes(context.Background(), req)

	// Verify response
	if err != nil {
		t.Fatalf("GetClusterNodes returned error: %v", err)
	}

	if resp == nil {
		t.Fatal("GetClusterNodes returned nil response")
	}

	// Registry should contain at least the local node
	if len(resp.Nodes) < 1 {
		t.Fatalf("Expected at least 1 node (self), got %d", len(resp.Nodes))
	}
}
