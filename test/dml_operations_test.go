package test

import (
	"fmt"
	"testing"
)

// startKV starts a cluster with an (id, value) table named table.
func startKV(t *testing.T, table string) *cluster {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", table, fmt.Sprintf(kvTable, table))
	return c
}

// TestInsertReplication: a single-row INSERT, then several more through
// another node, reach every node.
func TestInsertReplication(t *testing.T) {
	c := startKV(t, "insert_test")
	want := insertEach(c, 1, "insert_test", 1, 1, "single_row")
	c.waitRows("marmot", kvRows("insert_test"), want, 1, 2, 3)
	want = append(want, insertEach(c, 2, "insert_test", 2, 6, "batch")...)
	c.waitRows("marmot", kvRows("insert_test"), want, 1, 2, 3)
}

// TestUpdateReplication: a single-row UPDATE and a multi-row UPDATE, each
// through a different node, reach every node.
func TestUpdateReplication(t *testing.T) {
	c := startKV(t, "update_test")
	insertEach(c, 1, "update_test", 1, 3, "initial")
	c.mustExec(2, "marmot", "UPDATE update_test SET value = 'updated_1' WHERE id = 1")
	c.waitRows("marmot", kvRows("update_test"), []string{"1|updated_1", "2|initial_2", "3|initial_3"}, 1, 2, 3)
	c.mustExec(3, "marmot", "UPDATE update_test SET value = 'batch_updated' WHERE id > 1")
	c.waitRows("marmot", kvRows("update_test"), []string{"1|updated_1", "2|batch_updated", "3|batch_updated"}, 1, 2, 3)
}

// TestDeleteReplication: a single-row DELETE and a multi-row DELETE, each
// through a different node, reach every node.
func TestDeleteReplication(t *testing.T) {
	c := startKV(t, "delete_test")
	want := insertEach(c, 1, "delete_test", 1, 5, "row")
	c.mustExec(2, "marmot", "DELETE FROM delete_test WHERE id = 1")
	c.waitRows("marmot", kvRows("delete_test"), want[1:], 1, 2, 3)
	c.mustExec(3, "marmot", "DELETE FROM delete_test WHERE id > 3")
	c.waitRows("marmot", kvRows("delete_test"), want[1:3], 1, 2, 3)
}

// TestInsertUpdateDelete_Sequential: one row inserted, updated and deleted,
// each through a different node, is in the same state on every node after
// each step.
func TestInsertUpdateDelete_Sequential(t *testing.T) {
	c := startKV(t, "sequential_test")
	c.mustExec(1, "marmot", "INSERT INTO sequential_test (id, value) VALUES (1, 'initial')")
	c.waitRows("marmot", kvRows("sequential_test"), []string{"1|initial"}, 1, 2, 3)
	c.mustExec(2, "marmot", "UPDATE sequential_test SET value = 'updated' WHERE id = 1")
	c.waitRows("marmot", kvRows("sequential_test"), []string{"1|updated"}, 1, 2, 3)
	c.mustExec(3, "marmot", "DELETE FROM sequential_test WHERE id = 1")
	c.waitRows("marmot", kvRows("sequential_test"), nil, 1, 2, 3)
}

// TestBulkInsertReplication: a hundred single-row INSERTs reach every node.
func TestBulkInsertReplication(t *testing.T) {
	c := startKV(t, "bulk_insert_test")
	want := insertEach(c, 1, "bulk_insert_test", 1, 100, "bulk")
	c.waitRows("marmot", kvRows("bulk_insert_test"), want, 1, 2, 3)
}

// TestBulkUpdateReplication: fifty single-row UPDATEs through one node reach
// every node, each row with its own value.
func TestBulkUpdateReplication(t *testing.T) {
	c := startKV(t, "bulk_update_test")
	insertEach(c, 1, "bulk_update_test", 1, 50, "initial")
	var want []string
	for i := 1; i <= 50; i++ {
		c.mustExec(2, "marmot", "UPDATE bulk_update_test SET value = ? WHERE id = ?", fmt.Sprintf("updated_%d", i), i)
		want = append(want, fmt.Sprintf("%d|updated_%d", i, i))
	}
	c.waitRows("marmot", kvRows("bulk_update_test"), want, 1, 2, 3)
}

// TestBulkDeleteReplication: fifty single-row DELETEs through one node empty
// the table on every node.
func TestBulkDeleteReplication(t *testing.T) {
	c := startKV(t, "bulk_delete_test")
	want := insertEach(c, 1, "bulk_delete_test", 1, 50, "row")
	c.waitRows("marmot", kvRows("bulk_delete_test"), want, 1, 2, 3)
	for i := 1; i <= 50; i++ {
		c.mustExec(2, "marmot", "DELETE FROM bulk_delete_test WHERE id = ?", i)
	}
	c.waitRows("marmot", kvRows("bulk_delete_test"), nil, 1, 2, 3)
}
