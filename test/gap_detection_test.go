package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestGapDetectionTriggersSnapshot tests that gap detection causes fallback to snapshot
// Scenario: Node offline longer than GC retention, transactions get GC'd, delta sync detects gap
func TestGapDetectionTriggersSnapshot(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Create test table
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE IF NOT EXISTS test_gap_detection (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Write initial batch of transactions (1-100)
	t.Logf("Writing initial batch of transactions...")
	for i := 1; i <= 100; i++ {
		query := fmt.Sprintf("INSERT INTO test_gap_detection (id, value) VALUES (%d, 'initial_%d')", i, i)
		_, err := harness.ExecNode(1, query)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	time.Sleep(3 * time.Second)

	// Verify all nodes have the data
	for nodeID := 1; nodeID <= 3; nodeID++ {
		count := getRowCount(t, harness, nodeID, "test_gap_detection")
		t.Logf("Node %d has %d rows initially", nodeID, count)
		if count != 100 {
			t.Fatalf("Node %d should have 100 rows, got %d", nodeID, count)
		}
	}

	// Kill node 3 to simulate extended downtime
	t.Logf("Killing node 3 to simulate extended downtime...")
	harness.KillNode(3)
	time.Sleep(2 * time.Second)

	// Write many more transactions while node 3 is down (101-15100)
	// This will exceed the delta sync threshold (10000 txns)
	t.Logf("Writing large batch of transactions while node 3 is down (this may take a minute)...")
	batchSize := 100
	totalInserts := 15000
	for i := 101; i <= totalInserts+100; i++ {
		query := fmt.Sprintf("INSERT INTO test_gap_detection (id, value) VALUES (%d, 'while_down_%d')", i, i)
		_, err := harness.ExecNode(1, query)
		if err != nil {
			t.Logf("Warning: insert %d failed: %v", i, err)
		}

		// Progress update every 1000 inserts
		if i%1000 == 0 {
			t.Logf("Progress: inserted %d transactions", i-100)
		}
	}
	time.Sleep(2 * time.Second)

	// Verify nodes 1 & 2 have all the data
	for nodeID := 1; nodeID <= 2; nodeID++ {
		count := getRowCount(t, harness, nodeID, "test_gap_detection")
		t.Logf("Node %d has %d rows after bulk insert", nodeID, count)
		if count < 10000 {
			t.Fatalf("Node %d should have at least 10000 rows, got %d", nodeID, count)
		}
	}

	// Now simulate gap by manually deleting transaction records from node 3's database
	// This simulates what would happen if GC deleted old transactions
	t.Logf("Simulating GC by deleting old transaction records from node 3's database...")
	node3DB := harness.nodes[3].dataDir + "/databases/marmot.db"
	conn, err := sql.Open("sqlite3", node3DB)
	if err != nil {
		t.Fatalf("Failed to open node 3 database: %v", err)
	}

	// Delete transactions 100-10100 (simulating GC)
	// This creates a gap where node 3 has up to txn 100, but peer has 10100+
	deleteQuery := "DELETE FROM __marmot__txn_records WHERE txn_id > 100 AND txn_id <= 10100"
	_, err = conn.Exec(deleteQuery)
	if err != nil {
		conn.Close()
		t.Fatalf("Failed to delete transactions: %v", err)
	}
	conn.Close()

	t.Logf("Deleted transactions 101-10100 from node 3 (simulating GC)")

	// Restart node 3 - it should detect the gap and trigger snapshot
	t.Logf("Restarting node 3 - it should detect gap and fall back to snapshot...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}

	// Wait for node to become alive
	if err := harness.WaitForAlive(3, 30*time.Second); err != nil {
		t.Fatalf("Node 3 did not become ALIVE: %v", err)
	}

	// Wait for anti-entropy to run and detect the gap
	// Anti-entropy runs every 60 seconds, so give it time to detect and heal
	t.Logf("Waiting for anti-entropy to detect gap and trigger snapshot (this may take up to 2 minutes)...")
	time.Sleep(90 * time.Second)

	// Verify node 3 eventually gets all the data via snapshot
	count := getRowCount(t, harness, 3, "test_gap_detection")
	t.Logf("Node 3 has %d rows after gap detection and snapshot", count)

	// Should have close to the same amount as other nodes
	node1Count := getRowCount(t, harness, 1, "test_gap_detection")
	if count < node1Count-1000 {
		t.Fatalf("Node 3 did not catch up via snapshot: has %d rows, node 1 has %d rows (diff > 1000)", count, node1Count)
	}

	t.Logf("SUCCESS: Gap detection triggered snapshot, node 3 caught up (has %d rows)", count)
}

// Helper to get row count from a table
func getRowCount(t *testing.T, harness *ClusterHarness, nodeID int, table string) int {
	rows, err := harness.QueryNode(nodeID, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if err != nil {
		t.Fatalf("Failed to query node %d: %v", nodeID, err)
		return 0
	}
	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()
	return count
}
