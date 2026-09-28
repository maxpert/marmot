package test

import (
	"fmt"
	"testing"
	"time"
)

// Lagging Node Catchup Integration Test
//
// This test validates that a node which falls far behind can catch up via
// anti-entropy. It simulates a realistic scenario where a node is offline
// for an extended period while the cluster continues processing transactions.
//
// Background:
// - Marmot uses anti-entropy for eventual consistency
// - When a node is down, it misses transactions
// - On restart, anti-entropy detects the lag and syncs missing data
//
// Test Scenario:
// 1. A node goes offline (simulated crash)
// 2. The cluster continues processing many transactions
// 3. When the node restarts, anti-entropy syncs the missing data
//
// Success Criteria:
// - Node 3 successfully catches up after restart
// - Final data consistency across all nodes

// TestLaggingNodeCatchup validates that a lagging node catches up via
// anti-entropy. Row counts and waits are sized to stay well under the 60 s
// cluster test budget: every wait is a poll on a named deadline (WaitForRowCount
// / WaitForAlive), never a flat sleep, so the test finishes as soon as the
// cluster actually converges.
func TestLaggingNodeCatchup(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	t.Log("Starting 3-node cluster...")
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	harness.Phase("start")

	// Create test table
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE test_lagging (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Wait for table to replicate
	allNodes := []int{1, 2, 3}
	if err := harness.WaitForTableExists("test_lagging", allNodes, 10*time.Second); err != nil {
		t.Fatalf("Table did not replicate: %v", err)
	}

	const initialRows = 50
	t.Logf("Inserting %d initial rows...", initialRows)
	for i := 1; i <= initialRows; i++ {
		_, err := harness.ExecNode(1, "INSERT INTO test_lagging (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}
	if err := harness.WaitForRowCount("test_lagging", allNodes, initialRows, 15*time.Second); err != nil {
		t.Fatalf("initial rows did not replicate: %v", err)
	}
	harness.Phase("initial-load")

	// Kill node 3
	t.Logf("Killing node 3...")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("Failed to kill node 3: %v", err)
	}
	harness.Phase("node3-killed")

	// Insert enough rows while node 3 is down that it is meaningfully behind.
	const laggingRows = 500
	total := initialRows + laggingRows
	t.Logf("Inserting %d rows while node 3 is down...", laggingRows)
	for i := initialRows + 1; i <= total; i++ {
		_, err := harness.ExecNode(1, "INSERT INTO test_lagging (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}
	if err := harness.WaitForRowCount("test_lagging", []int{1, 2}, total, 20*time.Second); err != nil {
		t.Fatalf("rows did not replicate to nodes 1&2: %v", err)
	}
	harness.Phase("lagging-load")

	// Restart node 3
	t.Logf("Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, 20*time.Second); err != nil {
		t.Fatalf("Node 3 did not become alive: %v", err)
	}
	harness.Phase("node3-alive")

	if err := harness.WaitForRowCount("test_lagging", []int{3}, total, 20*time.Second); err != nil {
		harness.dumpNodeLogs("lagging_node_fail")
		t.Fatalf("node 3 did not catch up: %v", err)
	}
	harness.Phase("caught-up")

	count3 := harness.getRowCount(3, "test_lagging")
	count1 := harness.getRowCount(1, "test_lagging")
	t.Logf("SUCCESS: Node 3 caught up! Node 3: %d rows, Node 1: %d rows", count3, count1)
}
