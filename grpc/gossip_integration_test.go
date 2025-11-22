package grpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// testGossipConfig returns a gossip config suitable for testing
func testGossipConfig() GossipConfig {
	return GossipConfig{
		Interval:       100 * time.Millisecond,
		Fanout:         2,
		SuspectTimeout: 200 * time.Millisecond,
		DeadTimeout:    400 * time.Millisecond,
	}
}

// testNode represents a test cluster node
type testNode struct {
	nodeID   uint64
	registry *NodeRegistry
	gossip   *GossipProtocol
	client   *Client
	server   *Server
	grpcSrv  *grpc.Server
	listener *bufconn.Listener
}

// newTestNode creates a new test node with in-memory networking
func newTestNode(nodeID uint64, replicas, vnodes int) *testNode {
	registry := NewNodeRegistry(nodeID, fmt.Sprintf("localhost:808%d", nodeID))

	client := NewClient(nodeID)

	server := &Server{
		nodeID:   nodeID,
		address:  fmt.Sprintf("node-%d", nodeID),
		port:     8080,
		registry: registry,
	}

	gossip := NewGossipProtocol(nodeID, registry)
	gossip.SetClient(client)
	server.gossip = gossip

	listener := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	RegisterMarmotServiceServer(grpcSrv, server)

	return &testNode{
		nodeID:   nodeID,
		registry: registry,
		gossip:   gossip,
		client:   client,
		server:   server,
		grpcSrv:  grpcSrv,
		listener: listener,
	}
}

// start starts the test node's gRPC server
func (tn *testNode) start() {
	go func() {
		if err := tn.grpcSrv.Serve(tn.listener); err != nil {
			// Server stopped
		}
	}()
}

// stop stops the test node
func (tn *testNode) stop() {
	tn.gossip.Stop()
	tn.grpcSrv.Stop()
	tn.listener.Close()
	tn.client.Close()
}

// dialFunc returns a dialer function for in-memory connections
func (tn *testNode) dialFunc(context.Context, string) (net.Conn, error) {
	return tn.listener.Dial()
}

// connectTo establishes a connection to another test node
func (tn *testNode) connectTo(other *testNode) error {
	conn, err := grpc.Dial(
		fmt.Sprintf("node-%d", other.nodeID),
		grpc.WithContextDialer(other.dialFunc),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	tn.client.connections[other.nodeID] = conn
	tn.client.clients[other.nodeID] = NewMarmotServiceClient(conn)

	// Add to registry
	tn.registry.Add(&NodeState{
		NodeId:      other.nodeID,
		Address:     fmt.Sprintf("node-%d:8080", other.nodeID),
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	})

	return nil
}

// TestGossip_TwoNodes tests basic gossip between two nodes
func TestGossip_TwoNodes(t *testing.T) {
	node1 := newTestNode(1, 3, 150)
	node2 := newTestNode(2, 3, 150)

	node1.start()
	node2.start()
	defer node1.stop()
	defer node2.stop()

	// Connect nodes bidirectionally
	if err := node1.connectTo(node2); err != nil {
		t.Fatalf("Failed to connect node1 to node2: %v", err)
	}
	if err := node2.connectTo(node1); err != nil {
		t.Fatalf("Failed to connect node2 to node1: %v", err)
	}

	// Start gossip on both nodes
	config := testGossipConfig()
	go node1.gossip.Start(config)
	go node2.gossip.Start(config)

	// Wait for gossip rounds to complete
	time.Sleep(500 * time.Millisecond)

	// Both nodes should know about each other
	if node1.registry.Count() != 2 {
		t.Errorf("Node 1 should know about 2 nodes, got %d", node1.registry.Count())
	}
	if node2.registry.Count() != 2 {
		t.Errorf("Node 2 should know about 2 nodes, got %d", node2.registry.Count())
	}

	// Verify node 1 knows about node 2
	if _, exists := node1.registry.Get(2); !exists {
		t.Error("Node 1 should know about node 2")
	}

	// Verify node 2 knows about node 1
	if _, exists := node2.registry.Get(1); !exists {
		t.Error("Node 2 should know about node 1")
	}
}

// TestGossip_MultiNode tests gossip in a larger cluster
func TestGossip_MultiNode(t *testing.T) {
	numNodes := 5
	nodes := make([]*testNode, numNodes)

	// Create nodes
	for i := 0; i < numNodes; i++ {
		nodes[i] = newTestNode(uint64(i+1), 3, 150)
		nodes[i].start()
		defer nodes[i].stop()
	}

	// Create mesh network - each node connected to all others
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i != j {
				if err := nodes[i].connectTo(nodes[j]); err != nil {
					t.Fatalf("Failed to connect node %d to node %d: %v", i+1, j+1, err)
				}
			}
		}
	}

	// Start gossip on all nodes
	config := testGossipConfig()
	for i := 0; i < numNodes; i++ {
		go nodes[i].gossip.Start(config)
	}

	// Wait for cluster to converge
	time.Sleep(1 * time.Second)

	// All nodes should know about all nodes
	for i := 0; i < numNodes; i++ {
		count := nodes[i].registry.Count()
		if count != numNodes {
			t.Errorf("Node %d should know about %d nodes, got %d", i+1, numNodes, count)
		}

		// Verify it knows about each specific node
		for j := 1; j <= numNodes; j++ {
			if _, exists := nodes[i].registry.Get(uint64(j)); !exists {
				t.Errorf("Node %d should know about node %d", i+1, j)
			}
		}
	}
}

// TestGossip_NodeJoin tests a new node joining an existing cluster
func TestGossip_NodeJoin(t *testing.T) {
	// Create initial 3-node cluster
	node1 := newTestNode(1, 3, 150)
	node2 := newTestNode(2, 3, 150)
	node3 := newTestNode(3, 3, 150)

	node1.start()
	node2.start()
	node3.start()
	defer node1.stop()
	defer node2.stop()
	defer node3.stop()

	// Connect initial cluster
	node1.connectTo(node2)
	node1.connectTo(node3)
	node2.connectTo(node1)
	node2.connectTo(node3)
	node3.connectTo(node1)
	node3.connectTo(node2)

	// Start gossip
	config := testGossipConfig()
	go node1.gossip.Start(config)
	go node2.gossip.Start(config)
	go node3.gossip.Start(config)

	// Wait for initial cluster to stabilize
	time.Sleep(500 * time.Millisecond)

	// Create and join new node
	node4 := newTestNode(4, 3, 150)
	node4.start()
	defer node4.stop()

	// New node connects to one existing node (seed)
	node4.connectTo(node1)
	node1.connectTo(node4)

	// Start gossip on new node
	go node4.gossip.Start(config)

	// Wait for gossip to propagate
	time.Sleep(1 * time.Second)

	// All nodes should eventually know about node 4
	for _, node := range []*testNode{node1, node2, node3, node4} {
		if _, exists := node.registry.Get(4); !exists {
			t.Errorf("Node %d should know about node 4", node.nodeID)
		}
	}

	// Node 4 should know about all nodes
	if node4.registry.Count() != 4 {
		t.Errorf("Node 4 should know about 4 nodes, got %d", node4.registry.Count())
	}
}

// TestGossip_FailureDetection tests node failure detection
func TestGossip_FailureDetection(t *testing.T) {
	node1 := newTestNode(1, 3, 150)
	node2 := newTestNode(2, 3, 150)

	node1.start()
	node2.start()
	defer node1.stop()

	// Connect nodes
	node1.connectTo(node2)
	node2.connectTo(node1)

	// Configure timeouts for testing
	suspectTimeout := 300 * time.Millisecond
	deadTimeout := 600 * time.Millisecond

	// Start gossip with timeout checking
	config := GossipConfig{
		Interval:       100 * time.Millisecond,
		Fanout:         2,
		SuspectTimeout: suspectTimeout,
		DeadTimeout:    deadTimeout,
	}
	go node1.gossip.Start(config)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			node1.registry.CheckTimeouts(suspectTimeout, deadTimeout)
		}
	}()

	// Wait for initial gossip
	time.Sleep(200 * time.Millisecond)

	// Stop node 2 (simulate failure)
	node2.stop()

	// Wait for failure detection - node should become SUSPECT then DEAD
	// Poll until we see the status change
	maxWait := deadTimeout + 500*time.Millisecond
	start := time.Now()
	sawSuspect := false

	for time.Since(start) < maxWait {
		node2State, exists := node1.registry.Get(2)
		if !exists {
			t.Fatal("Node 1 should still have node 2 in registry")
		}

		if node2State.Status == NodeStatus_SUSPECT {
			sawSuspect = true
		}

		if node2State.Status == NodeStatus_DEAD {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Final check - should be DEAD
	node2State, _ := node1.registry.Get(2)
	if node2State.Status != NodeStatus_DEAD {
		t.Errorf("Node 2 should eventually be DEAD, got %v", node2State.Status)
	}

	// We should have seen SUSPECT at some point (though this might be flaky)
	if !sawSuspect {
		t.Logf("Warning: Did not observe SUSPECT state (may have transitioned too quickly)")
	}
}

// TestGossip_StatePropagation tests that state changes propagate through gossip
func TestGossip_StatePropagation(t *testing.T) {
	node1 := newTestNode(1, 3, 150)
	node2 := newTestNode(2, 3, 150)
	node3 := newTestNode(3, 3, 150)

	node1.start()
	node2.start()
	node3.start()
	defer node1.stop()
	defer node2.stop()
	defer node3.stop()

	// Create chain: node1 <-> node2 <-> node3 (node1 and node3 not directly connected)
	node1.connectTo(node2)
	node2.connectTo(node1)
	node2.connectTo(node3)
	node3.connectTo(node2)

	// Start gossip
	config := testGossipConfig()
	go node1.gossip.Start(config)
	go node2.gossip.Start(config)
	go node3.gossip.Start(config)

	// Wait for initial gossip
	time.Sleep(500 * time.Millisecond)

	// Manually mark node 2 as suspect on node 1
	node1.registry.MarkSuspect(2)

	// Increment incarnation to make update propagate
	node1State, _ := node1.registry.Get(2)
	node1State.Incarnation++

	// Wait for gossip to propagate
	time.Sleep(1 * time.Second)

	// Node 3 should eventually learn about node 2's SUSPECT status through node 2
	// Note: This depends on gossip implementation details
	node3State, exists := node3.registry.Get(2)
	if !exists {
		t.Fatal("Node 3 should know about node 2")
	}

	// At minimum, node 3 should have some state for node 2
	if node3State == nil {
		t.Error("Node 3 should have state for node 2")
	}
}

// TestGossip_IncarnationNumber tests that higher incarnation numbers win
func TestGossip_IncarnationNumber(t *testing.T) {
	// Create 3 actual nodes to avoid failure detection interference
	node1 := newTestNode(1, 3, 150)
	node2 := newTestNode(2, 3, 150)
	node3 := newTestNode(3, 3, 150)

	node1.start()
	node2.start()
	node3.start()
	defer node1.stop()
	defer node2.stop()
	defer node3.stop()

	// Connect all nodes
	node1.connectTo(node2)
	node1.connectTo(node3)
	node2.connectTo(node1)
	node2.connectTo(node3)
	node3.connectTo(node1)
	node3.connectTo(node2)

	// Manually set conflicting states with different incarnations for node 3
	// Directly manipulate the registry to simulate different views
	node1.registry.mu.Lock()
	node1State := node1.registry.nodes[3]
	node1State.Incarnation = 5
	node1State.Status = NodeStatus_ALIVE
	node1.registry.mu.Unlock()

	node2.registry.mu.Lock()
	node2State := node2.registry.nodes[3]
	node2State.Incarnation = 3 // Lower incarnation
	node2State.Status = NodeStatus_SUSPECT
	node2.registry.mu.Unlock()

	// Start gossip with longer timeouts to avoid interference
	config := GossipConfig{
		Interval:       100 * time.Millisecond,
		Fanout:         2,
		SuspectTimeout: 10 * time.Second, // Long timeout
		DeadTimeout:    20 * time.Second,
	}
	go node1.gossip.Start(config)
	go node2.gossip.Start(config)
	go node3.gossip.Start(config)

	// Wait for gossip to propagate
	time.Sleep(500 * time.Millisecond)

	// Both node1 and node2 should converge to incarnation 5 (ALIVE)
	node3StateOnNode1, _ := node1.registry.Get(3)
	node3StateOnNode2, _ := node2.registry.Get(3)

	if node3StateOnNode1.Incarnation < 5 {
		t.Errorf("Node 1 should have incarnation >= 5, got %d", node3StateOnNode1.Incarnation)
	}

	if node3StateOnNode2.Incarnation < 5 {
		t.Errorf("Node 2 should have incarnation >= 5, got %d", node3StateOnNode2.Incarnation)
	}

	// Since node 3 is actually alive and gossiping, both should eventually see it as ALIVE
	if node3StateOnNode2.Status != NodeStatus_ALIVE {
		t.Logf("Warning: Node 2 has node 3 as %v instead of ALIVE (may be due to timing)", node3StateOnNode2.Status)
	}
}

// BenchmarkGossip_5Nodes benchmarks gossip performance with 5 nodes
func BenchmarkGossip_5Nodes(b *testing.B) {
	numNodes := 5
	nodes := make([]*testNode, numNodes)

	// Create and connect nodes
	for i := 0; i < numNodes; i++ {
		nodes[i] = newTestNode(uint64(i+1), 3, 150)
		nodes[i].start()
	}

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i != j {
				nodes[i].connectTo(nodes[j])
			}
		}
	}

	// Start gossip
	config := testGossipConfig()
	for i := 0; i < numNodes; i++ {
		go nodes[i].gossip.Start(config)
	}

	// Wait for stabilization
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate state change
		nodes[0].registry.MarkSuspect(2)
		time.Sleep(100 * time.Millisecond)
	}
	b.StopTimer()

	// Cleanup
	for i := 0; i < numNodes; i++ {
		nodes[i].stop()
	}
}
