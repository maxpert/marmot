package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// waitForRowCount polls until expected row count or timeout
func waitForRowCount(t *testing.T, db *sql.DB, table string, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err == nil && count == expected {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	var actual int
	db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&actual)
	return fmt.Errorf("timeout: expected %d rows, got %d", expected, actual)
}

// waitForRowCountOnNode waits for expected row count on a specific node
func waitForRowCountOnNode(t *testing.T, harness *ClusterHarness, nodeID int, table string, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count := harness.getRowCount(nodeID, table)
		if count == expected {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	actual := harness.getRowCount(nodeID, table)
	return fmt.Errorf("node %d: timeout waiting for %d rows, got %d", nodeID, expected, actual)
}

// verifyRowValue checks if a specific row has expected value
func verifyRowValue(t *testing.T, db *sql.DB, table string, id int, expectedValue string) error {
	var value string
	query := fmt.Sprintf("SELECT value FROM %s WHERE id = ?", table)
	err := db.QueryRow(query, id).Scan(&value)
	if err != nil {
		return fmt.Errorf("failed to query row %d: %v", id, err)
	}
	if value != expectedValue {
		return fmt.Errorf("row %d: expected value '%s', got '%s'", id, expectedValue, value)
	}
	return nil
}

// TestInsertReplication tests single and batch insert replication
func TestInsertReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "insert_test"
	allNodes := []int{1, 2, 3}

	t.Logf("Creating table %s on node 1...", tableName)
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("insert_test_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Test 1: Insert single row on node 1
	t.Logf("Test 1: Inserting single row on node 1...")
	_, err = harness.ExecNode(1, fmt.Sprintf(
		"INSERT INTO %s (id, value) VALUES (1, 'single_row')", tableName))
	if err != nil {
		t.Fatalf("Failed to insert single row: %v", err)
	}

	// Verify it appears on nodes 2 and 3
	for _, nodeID := range []int{2, 3} {
		if err := waitForRowCountOnNode(t, harness, nodeID, tableName, 1, 5*time.Second); err != nil {
			harness.dumpNodeLogs("insert_single_fail")
			t.Fatalf("Single row insert replication failed on node %d: %v", nodeID, err)
		}
		t.Logf("Node %d has single row", nodeID)
	}

	// Test 2: Insert multiple rows in a batch on node 2
	t.Logf("Test 2: Inserting 5 rows in batch on node 2...")
	for i := 2; i <= 6; i++ {
		_, err := harness.ExecNode(2, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'batch_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert batch row %d: %v", i, err)
		}
	}

	// Verify all 6 rows appear on nodes 1 and 3
	for _, nodeID := range []int{1, 3} {
		if err := waitForRowCountOnNode(t, harness, nodeID, tableName, 6, 5*time.Second); err != nil {
			harness.dumpNodeLogs("insert_batch_fail")
			t.Fatalf("Batch insert replication failed on node %d: %v", nodeID, err)
		}
		t.Logf("Node %d has 6 rows", nodeID)
	}

	t.Logf("SUCCESS: Insert replication verified")
}

// TestUpdateReplication tests update replication
func TestUpdateReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "update_test"
	allNodes := []int{1, 2, 3}

	t.Logf("Creating table %s on node 1...", tableName)
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("update_test_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Insert initial rows on node 1
	t.Logf("Inserting 3 initial rows on node 1...")
	for i := 1; i <= 3; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'initial_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for replication
	if err := harness.WaitForRowCount(tableName, allNodes, 3, 5*time.Second); err != nil {
		harness.dumpNodeLogs("update_test_insert")
		t.Fatalf("Insert replication failed: %v", err)
	}
	t.Logf("All nodes have 3 initial rows")

	// Test 1: Update single row on node 2
	t.Logf("Test 1: Updating row id=1 on node 2...")
	_, err = harness.ExecNode(2, fmt.Sprintf(
		"UPDATE %s SET value = 'updated_1' WHERE id = 1", tableName))
	if err != nil {
		t.Fatalf("Failed to update row: %v", err)
	}

	// Verify update propagates to nodes 1 and 3
	time.Sleep(2 * time.Second)
	for _, nodeID := range []int{1, 3} {
		db, err := harness.ConnectToNode(nodeID)
		if err != nil {
			t.Fatalf("Failed to connect to node %d: %v", nodeID, err)
		}
		if err := verifyRowValue(t, db, tableName, 1, "updated_1"); err != nil {
			harness.dumpNodeLogs("update_single_fail")
			t.Fatalf("Update replication failed on node %d: %v", nodeID, err)
		}
		t.Logf("Node %d has updated value for id=1", nodeID)
	}

	// Test 2: Update multiple rows with WHERE clause
	t.Logf("Test 2: Updating multiple rows on node 3...")
	_, err = harness.ExecNode(3, fmt.Sprintf(
		"UPDATE %s SET value = 'batch_updated' WHERE id > 1", tableName))
	if err != nil {
		t.Fatalf("Failed to batch update: %v", err)
	}

	// Verify updates propagate to nodes 1 and 2
	time.Sleep(2 * time.Second)
	for _, nodeID := range []int{1, 2} {
		db, err := harness.ConnectToNode(nodeID)
		if err != nil {
			t.Fatalf("Failed to connect to node %d: %v", nodeID, err)
		}
		for _, id := range []int{2, 3} {
			if err := verifyRowValue(t, db, tableName, id, "batch_updated"); err != nil {
				harness.dumpNodeLogs("update_batch_fail")
				t.Fatalf("Batch update replication failed on node %d: %v", nodeID, err)
			}
		}
		t.Logf("Node %d has batch updated values", nodeID)
	}

	t.Logf("SUCCESS: Update replication verified")
}

// TestDeleteReplication tests delete replication
func TestDeleteReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "delete_test"
	allNodes := []int{1, 2, 3}

	t.Logf("Creating table %s on node 1...", tableName)
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("delete_test_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Insert initial rows on node 1
	t.Logf("Inserting 5 initial rows on node 1...")
	for i := 1; i <= 5; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'row_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for replication
	if err := harness.WaitForRowCount(tableName, allNodes, 5, 5*time.Second); err != nil {
		harness.dumpNodeLogs("delete_test_insert")
		t.Fatalf("Insert replication failed: %v", err)
	}
	t.Logf("All nodes have 5 initial rows")

	// Test 1: Delete single row on node 2
	t.Logf("Test 1: Deleting row id=1 on node 2...")
	_, err = harness.ExecNode(2, fmt.Sprintf(
		"DELETE FROM %s WHERE id = 1", tableName))
	if err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	// Verify delete propagates to nodes 1 and 3
	for _, nodeID := range []int{1, 3} {
		if err := waitForRowCountOnNode(t, harness, nodeID, tableName, 4, 5*time.Second); err != nil {
			harness.dumpNodeLogs("delete_single_fail")
			t.Fatalf("Delete replication failed on node %d: %v", nodeID, err)
		}
		t.Logf("Node %d has 4 rows after delete", nodeID)
	}

	// Test 2: Delete multiple rows with WHERE clause
	t.Logf("Test 2: Deleting multiple rows on node 3...")
	_, err = harness.ExecNode(3, fmt.Sprintf(
		"DELETE FROM %s WHERE id > 3", tableName))
	if err != nil {
		t.Fatalf("Failed to batch delete: %v", err)
	}

	// Verify deletes propagate to nodes 1 and 2
	for _, nodeID := range []int{1, 2} {
		if err := waitForRowCountOnNode(t, harness, nodeID, tableName, 2, 5*time.Second); err != nil {
			harness.dumpNodeLogs("delete_batch_fail")
			t.Fatalf("Batch delete replication failed on node %d: %v", nodeID, err)
		}
		t.Logf("Node %d has 2 rows after batch delete", nodeID)
	}

	t.Logf("SUCCESS: Delete replication verified")
}

// TestInsertUpdateDelete_Sequential tests insert, update, delete on different nodes
func TestInsertUpdateDelete_Sequential(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "sequential_test"
	allNodes := []int{1, 2, 3}

	t.Logf("Creating table %s on node 1...", tableName)
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("sequential_test_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Step 1: Insert row on node 1
	t.Logf("Step 1: Inserting row on node 1...")
	_, err = harness.ExecNode(1, fmt.Sprintf(
		"INSERT INTO %s (id, value) VALUES (1, 'initial')", tableName))
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Wait for insert replication
	if err := harness.WaitForRowCount(tableName, allNodes, 1, 5*time.Second); err != nil {
		harness.dumpNodeLogs("sequential_insert_fail")
		t.Fatalf("Insert replication failed: %v", err)
	}
	t.Logf("All nodes have 1 row")

	// Step 2: Update row on node 2
	t.Logf("Step 2: Updating row on node 2...")
	_, err = harness.ExecNode(2, fmt.Sprintf(
		"UPDATE %s SET value = 'updated' WHERE id = 1", tableName))
	if err != nil {
		t.Fatalf("Failed to update row: %v", err)
	}

	// Wait and verify update
	time.Sleep(2 * time.Second)
	for _, nodeID := range allNodes {
		db, err := harness.ConnectToNode(nodeID)
		if err != nil {
			t.Fatalf("Failed to connect to node %d: %v", nodeID, err)
		}
		if err := verifyRowValue(t, db, tableName, 1, "updated"); err != nil {
			harness.dumpNodeLogs("sequential_update_fail")
			t.Fatalf("Update replication failed on node %d: %v", nodeID, err)
		}
	}
	t.Logf("All nodes have updated value")

	// Step 3: Delete row on node 3
	t.Logf("Step 3: Deleting row on node 3...")
	_, err = harness.ExecNode(3, fmt.Sprintf(
		"DELETE FROM %s WHERE id = 1", tableName))
	if err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	// Verify final state: row deleted on all nodes
	for _, nodeID := range allNodes {
		if err := waitForRowCountOnNode(t, harness, nodeID, tableName, 0, 5*time.Second); err != nil {
			harness.dumpNodeLogs("sequential_delete_fail")
			t.Fatalf("Delete replication failed on node %d: %v", nodeID, err)
		}
	}
	t.Logf("All nodes have 0 rows (final state consistent)")

	t.Logf("SUCCESS: Sequential insert/update/delete verified")
}

// TestBulkInsertReplication tests bulk insert replication
func TestBulkInsertReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "bulk_insert_test"
	allNodes := []int{1, 2, 3}

	t.Logf("Creating table %s on node 1...", tableName)
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("bulk_insert_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Insert 100 rows on node 1
	t.Logf("Inserting 100 rows on node 1...")
	for i := 1; i <= 100; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'bulk_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Verify all 100 appear on nodes 2 and 3
	for _, nodeID := range []int{2, 3} {
		if err := waitForRowCountOnNode(t, harness, nodeID, tableName, 100, 30*time.Second); err != nil {
			harness.dumpNodeLogs("bulk_insert_fail")
			t.Fatalf("Bulk insert replication failed on node %d: %v", nodeID, err)
		}
		t.Logf("Node %d has 100 rows", nodeID)
	}

	t.Logf("SUCCESS: Bulk insert replication verified")
}

// TestBulkUpdateReplication tests bulk update replication
func TestBulkUpdateReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "bulk_update_test"
	allNodes := []int{1, 2, 3}

	t.Logf("Creating table %s on node 1...", tableName)
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("bulk_update_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Insert 50 rows
	t.Logf("Inserting 50 rows on node 1...")
	for i := 1; i <= 50; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'initial_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for insert replication
	if err := harness.WaitForRowCount(tableName, allNodes, 50, 30*time.Second); err != nil {
		harness.dumpNodeLogs("bulk_update_insert")
		t.Fatalf("Insert replication failed: %v", err)
	}
	t.Logf("All nodes have 50 initial rows")

	// Update all 50 rows on node 2
	t.Logf("Updating all 50 rows on node 2...")
	for i := 1; i <= 50; i++ {
		_, err := harness.ExecNode(2, fmt.Sprintf(
			"UPDATE %s SET value = 'updated_%d' WHERE id = %d", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to update row %d: %v", i, err)
		}
	}

	// Wait for updates to propagate
	time.Sleep(5 * time.Second)

	// Verify updates propagated correctly by sampling a few rows
	t.Logf("Verifying updates propagated to nodes 1 and 3...")
	for _, nodeID := range []int{1, 3} {
		db, err := harness.ConnectToNode(nodeID)
		if err != nil {
			t.Fatalf("Failed to connect to node %d: %v", nodeID, err)
		}

		// Sample check: verify first, middle, and last rows
		for _, id := range []int{1, 25, 50} {
			expectedValue := fmt.Sprintf("updated_%d", id)
			if err := verifyRowValue(t, db, tableName, id, expectedValue); err != nil {
				harness.dumpNodeLogs("bulk_update_fail")
				t.Fatalf("Bulk update replication failed on node %d: %v", nodeID, err)
			}
		}
		t.Logf("Node %d has updated values (sampled)", nodeID)
	}

	t.Logf("SUCCESS: Bulk update replication verified")
}

// TestBulkDeleteReplication tests bulk delete replication
func TestBulkDeleteReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "bulk_delete_test"
	allNodes := []int{1, 2, 3}

	t.Logf("Creating table %s on node 1...", tableName)
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("bulk_delete_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Insert 50 rows
	t.Logf("Inserting 50 rows on node 1...")
	for i := 1; i <= 50; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'row_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for insert replication
	if err := harness.WaitForRowCount(tableName, allNodes, 50, 30*time.Second); err != nil {
		harness.dumpNodeLogs("bulk_delete_insert")
		t.Fatalf("Insert replication failed: %v", err)
	}
	t.Logf("All nodes have 50 initial rows")

	// Delete all 50 rows on node 2
	t.Logf("Deleting all 50 rows on node 2...")
	for i := 1; i <= 50; i++ {
		_, err := harness.ExecNode(2, fmt.Sprintf(
			"DELETE FROM %s WHERE id = %d", tableName, i))
		if err != nil {
			t.Fatalf("Failed to delete row %d: %v", i, err)
		}
	}

	// Verify all deletes propagate (count should be 0 on all nodes)
	for _, nodeID := range []int{1, 3} {
		if err := waitForRowCountOnNode(t, harness, nodeID, tableName, 0, 30*time.Second); err != nil {
			harness.dumpNodeLogs("bulk_delete_fail")
			t.Fatalf("Bulk delete replication failed on node %d: %v", nodeID, err)
		}
		t.Logf("Node %d has 0 rows after bulk delete", nodeID)
	}

	t.Logf("SUCCESS: Bulk delete replication verified")
}
