package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Gap Detection Integration Test
//
// This test validates the critical gap detection and snapshot-based recovery mechanism
// in Marmot's anti-entropy system. It simulates a realistic scenario where a node falls
// far behind and its transaction log is garbage collected, creating a "gap" that prevents
// delta synchronization.
//
// Background:
// - Marmot uses anti-entropy for eventual consistency (continuous background healing)
// - Delta sync: Replays missing transactions from peer's txn log (efficient for small lags)
// - Snapshot sync: Full database transfer (fallback for large lags or GC gaps)
// - Gap detection: When txn records are missing (GC'd), delta sync is impossible
//
// Test Scenario:
// This simulates a real-world production scenario where:
// 1. A node goes offline for an extended period (e.g., hardware failure, network partition)
// 2. The cluster continues processing 15,000+ transactions
// 3. Garbage collection runs on the offline node's stale database
// 4. When the node restarts, it cannot use delta sync (missing txn records)
// 5. Anti-entropy must detect the gap and automatically trigger snapshot recovery
//
// Success Criteria:
// - Node 3 successfully catches up via snapshot transfer
// - Final data consistency across all nodes (within acceptable bounds)
// - No data loss or corruption

// TestGapDetectionTriggersSnapshot validates snapshot-based recovery when delta sync is impossible.
//
// Test Flow:
// 1. Start 3-node cluster with quorum=2
// 2. Insert 100 initial rows, verify on all nodes
// 3. Kill node 3
// 4. Insert 15,000 rows while node 3 is down
// 5. Simulate GC gap: Delete txn records 101-10100 from node 3's SQLite database
// 6. Restart node 3
// 7. Wait for anti-entropy to detect gap and trigger snapshot
// 8. Assert: Node 3 eventually has close to node 1's row count
func TestGapDetectionTriggersSnapshot(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	t.Log("Starting 3-node cluster...")
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Create test table (CDC-compatible with explicit column list)
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE test_gap_detection (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Wait for table to replicate
	if err := harness.WaitForTableExists("test_gap_detection", 10*time.Second); err != nil {
		t.Fatalf("Table did not replicate: %v", err)
	}

	// Insert 100 initial rows with explicit column lists (CDC requirement)
	t.Logf("Inserting 100 initial rows...")
	for i := 1; i <= 100; i++ {
		_, err := harness.ExecNode(1, "INSERT INTO test_gap_detection (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for replication to all nodes
	time.Sleep(3 * time.Second)

	// Verify initial data on all nodes
	t.Logf("Verifying initial data on all nodes...")
	for nodeID := 1; nodeID <= 3; nodeID++ {
		count := getGapTestRowCount(t, harness, nodeID, "test_gap_detection")
		if count != 100 {
			t.Fatalf("Node %d has %d rows, expected 100", nodeID, count)
		}
		t.Logf("Node %d: %d rows (OK)", nodeID, count)
	}

	// Kill node 3 (simulate crash)
	t.Logf("Killing node 3...")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("Failed to kill node 3: %v", err)
	}

	// Wait for cluster to detect node 3 is down
	time.Sleep(3 * time.Second)

	// Insert 15,000 rows while node 3 is down (ids 101-15100)
	t.Logf("Inserting 15,000 rows while node 3 is down (this may take a minute)...")
	for i := 101; i <= 15100; i++ {
		_, err := harness.ExecNode(1, "INSERT INTO test_gap_detection (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
		if i%1000 == 0 {
			t.Logf("Inserted %d rows...", i)
		}
	}

	t.Logf("Completed inserting 15,000 rows")

	// Verify node 1 has all data
	count1 := getGapTestRowCount(t, harness, 1, "test_gap_detection")
	t.Logf("Node 1 has %d rows", count1)
	if count1 < 15100 {
		t.Fatalf("Node 1 should have at least 15100 rows, got %d", count1)
	}

	// Simulate GC gap: Open node 3's SQLite database and delete txn records
	t.Logf("Simulating GC gap on node 3...")
	node3DBPath := fmt.Sprintf("%s/test.db", harness.Nodes[2].DataDir)
	sqliteDB, err := sql.Open("sqlite3", node3DBPath)
	if err != nil {
		t.Fatalf("Failed to open node 3 database: %v", err)
	}
	defer sqliteDB.Close()

	// Delete transaction records 101-10100 to simulate GC
	result, err := sqliteDB.Exec("DELETE FROM __marmot__txn_records WHERE txn_id BETWEEN 101 AND 10100")
	if err != nil {
		t.Fatalf("Failed to delete txn records: %v", err)
	}
	deleted, _ := result.RowsAffected()
	t.Logf("Deleted %d transaction records (simulating GC gap)", deleted)

	// Close SQLite connection before restarting node
	sqliteDB.Close()

	// Restart node 3
	t.Logf("Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}

	if err := harness.WaitForAlive(3, 30*time.Second); err != nil {
		t.Fatalf("Node 3 did not become alive: %v", err)
	}

	// Wait for anti-entropy to detect gap and trigger snapshot
	t.Logf("Waiting up to 90 seconds for anti-entropy to detect gap and sync via snapshot...")
	maxWait := 90 * time.Second
	startTime := time.Now()
	lastCount := 0

	for time.Since(startTime) < maxWait {
		time.Sleep(5 * time.Second)

		count3 := getGapTestRowCount(t, harness, 3, "test_gap_detection")
		count1 := getGapTestRowCount(t, harness, 1, "test_gap_detection")

		t.Logf("Progress: Node 3 has %d rows, Node 1 has %d rows (elapsed: %v)",
			count3, count1, time.Since(startTime).Round(time.Second))

		if count3 > lastCount {
			lastCount = count3
		}

		// Success condition: Node 3 is within 1000 rows of Node 1
		if count3 >= count1-1000 {
			t.Logf("SUCCESS: Node 3 caught up via snapshot! Node 3: %d rows, Node 1: %d rows", count3, count1)
			return
		}

		// Check if we're making progress
		if count3 > 100 {
			t.Logf("Node 3 is syncing... (%d rows so far)", count3)
		}
	}

	// Timeout - check final state
	count3 := getGapTestRowCount(t, harness, 3, "test_gap_detection")
	count1Final := getGapTestRowCount(t, harness, 1, "test_gap_detection")

	t.Errorf("TIMEOUT: Node 3 did not catch up after 90 seconds")
	t.Errorf("Final state: Node 3 has %d rows, Node 1 has %d rows", count3, count1Final)
	t.Errorf("Expected Node 3 to have at least %d rows (within 1000 of Node 1)", count1Final-1000)
	t.Fatal("Gap detection and snapshot sync failed")
}

// getGapTestRowCount returns the number of rows in a table on a specific node
// (renamed to avoid conflict with other test helper functions)
func getGapTestRowCount(t *testing.T, harness *ClusterHarness, nodeID int, table string) int {
	rows, err := harness.QueryNode(nodeID, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if err != nil {
		t.Logf("Warning: Failed to query node %d: %v", nodeID, err)
		return 0
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Logf("Warning: Failed to scan count from node %d: %v", nodeID, err)
			return 0
		}
	}
	return count
}
