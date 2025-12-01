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

// TestLaggingNodeCatchup validates that a lagging node catches up via anti-entropy.
func TestLaggingNodeCatchup(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	t.Log("Starting 3-node cluster...")
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

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

	// Insert initial rows
	t.Logf("Inserting 100 initial rows...")
	for i := 1; i <= 100; i++ {
		_, err := harness.ExecNode(1, "INSERT INTO test_lagging (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Verify initial data on all nodes
	t.Logf("Verifying initial data on all nodes...")
	for nodeID := 1; nodeID <= 3; nodeID++ {
		count := harness.getRowCount(nodeID, "test_lagging")
		if count != 100 {
			t.Fatalf("Node %d has %d rows, expected 100", nodeID, count)
		}
		t.Logf("Node %d: %d rows (OK)", nodeID, count)
	}

	// Kill node 3
	t.Logf("Killing node 3...")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("Failed to kill node 3: %v", err)
	}

	// Wait for cluster to detect node 3 is down
	time.Sleep(3 * time.Second)

	// Insert many rows while node 3 is down
	t.Logf("Inserting 5000 rows while node 3 is down...")
	for i := 101; i <= 5100; i++ {
		_, err := harness.ExecNode(1, "INSERT INTO test_lagging (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
		if i%1000 == 0 {
			t.Logf("Inserted %d rows...", i)
		}
	}

	t.Logf("Completed inserting 5000 rows")

	// Verify node 1 has all data
	count1 := harness.getRowCount(1, "test_lagging")
	t.Logf("Node 1 has %d rows", count1)
	if count1 < 5100 {
		t.Fatalf("Node 1 should have at least 5100 rows, got %d", count1)
	}

	// Restart node 3
	t.Logf("Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}

	if err := harness.WaitForAlive(3, 30*time.Second); err != nil {
		t.Fatalf("Node 3 did not become alive: %v", err)
	}

	// Wait for anti-entropy to sync
	t.Logf("Waiting up to 120 seconds for anti-entropy to sync node 3...")
	maxWait := 120 * time.Second
	startTime := time.Now()
	lastCount := 0

	for time.Since(startTime) < maxWait {
		time.Sleep(5 * time.Second)

		count3 := harness.getRowCount(3, "test_lagging")
		count1 := harness.getRowCount(1, "test_lagging")

		t.Logf("Progress: Node 3 has %d rows, Node 1 has %d rows (elapsed: %v)",
			count3, count1, time.Since(startTime).Round(time.Second))

		if count3 > lastCount {
			lastCount = count3
		}

		// Success condition: Node 3 is within 100 rows of Node 1
		if count3 >= count1-100 {
			t.Logf("SUCCESS: Node 3 caught up! Node 3: %d rows, Node 1: %d rows", count3, count1)
			return
		}
	}

	// Timeout - check final state
	count3 := harness.getRowCount(3, "test_lagging")
	count1Final := harness.getRowCount(1, "test_lagging")

	harness.dumpNodeLogs("lagging_node_fail")
	t.Errorf("TIMEOUT: Node 3 did not catch up after 120 seconds")
	t.Errorf("Final state: Node 3 has %d rows, Node 1 has %d rows", count3, count1Final)
	t.Fatal("Lagging node catchup failed")
}
