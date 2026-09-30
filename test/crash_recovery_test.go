package test

import (
	"fmt"
	"slices"
	"testing"
)

// kvTable is the (id, value) table most crash tests write.
const kvTable = "CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)"

// insertEach inserts ids from..to into table through node id, one
// transaction per row, and returns the rows the table must then hold.
func insertEach(c *cluster, id int, table string, from, to int, tag string) []string {
	c.t.Helper()
	var want []string
	for i := from; i <= to; i++ {
		c.mustExec(id, "marmot", fmt.Sprintf("INSERT INTO %s (id, value) VALUES (?, ?)", table), i, fmt.Sprintf("%s_%d", tag, i))
		want = append(want, fmt.Sprintf("%d|%s_%d", i, tag, i))
	}
	c.progress()
	return want
}

// kvRows is the query that lists an (id, value) table's rows in order.
func kvRows(table string) string {
	return fmt.Sprintf("SELECT id, value FROM %s ORDER BY id", table)
}

// TestDDLReplication: CREATE TABLE through one node reaches every node.
func TestDDLReplication(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "ddl_test_table", fmt.Sprintf(kvTable, "ddl_test_table"))
}

// TestBasicReplication: rows inserted through one node reach every node.
func TestBasicReplication(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "basic_test", fmt.Sprintf(kvTable, "basic_test"))
	want := insertEach(c, 1, "basic_test", 1, 10, "value")
	c.waitRows("marmot", kvRows("basic_test"), want, 1, 2, 3)
}

// TestNodeRestartRecovery: a node killed while rows are written gets them
// after it restarts.
func TestNodeRestartRecovery(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "recovery_test", fmt.Sprintf(kvTable, "recovery_test"))
	want := insertEach(c, 1, "recovery_test", 1, 5, "initial")
	c.waitRows("marmot", kvRows("recovery_test"), want, 1, 2, 3)

	c.kill(3)
	want = append(want, insertEach(c, 1, "recovery_test", 6, 10, "missed")...)
	c.waitRows("marmot", kvRows("recovery_test"), want, 1, 2)

	c.restart(3)
	c.waitRows("marmot", kvRows("recovery_test"), want, 1, 2, 3)
}

// TestCrashMidTransaction: rows an explicit transaction wrote but never
// committed are gone once its coordinator is killed: the other nodes never
// see them, and neither does the coordinator after it restarts and catches
// up. A committed marker row is the barrier that proves each node has
// applied everything the cluster committed before the check.
func TestCrashMidTransaction(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "crash_txn_test", fmt.Sprintf(kvTable, "crash_txn_test"))

	tx, err := c.db(1, "marmot").Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := tx.Exec("INSERT INTO crash_txn_test (id, value) VALUES (?, ?)", i, fmt.Sprintf("uncommitted_%d", i)); err != nil {
			t.Fatalf("insert %d in the open transaction: %v", i, err)
		}
	}
	c.kill(1)
	_ = tx.Rollback()

	marker := insertEach(c, 2, "crash_txn_test", 100, 100, "marker")
	c.waitRows("marmot", kvRows("crash_txn_test"), marker, 2, 3)

	c.restart(1)
	c.waitRows("marmot", kvRows("crash_txn_test"), marker, 1, 2, 3)
}

// TestHighLoadRecovery: a node killed while the cluster keeps committing
// rows has every one of them, and no other, after it restarts.
func TestHighLoadRecovery(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "highload_test", fmt.Sprintf(kvTable, "highload_test"))
	want := insertEach(c, 1, "highload_test", 1, 20, "initial")
	c.waitRows("marmot", kvRows("highload_test"), want, 1, 2, 3)

	c.kill(3)
	want = append(want, insertEach(c, 1, "highload_test", 21, 50, "during_down")...)
	c.waitRows("marmot", kvRows("highload_test"), want, 1, 2)

	c.restart(3)
	c.waitRows("marmot", kvRows("highload_test"), want, 1, 2, 3)
}

// TestRollingRestart: each node in turn is killed and restarted; every write
// through a surviving node while one is down succeeds, and each restarted node
// catches up, so the cluster ends with every row on every node.
func TestRollingRestart(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "rolling_test", fmt.Sprintf(kvTable, "rolling_test"))
	want := insertEach(c, 1, "rolling_test", 1, 20, "baseline")
	c.waitRows("marmot", kvRows("rolling_test"), want, 1, 2, 3)
	c.phase("baseline")

	for down := 1; down <= 3; down++ {
		c.kill(down)
		up := slices.DeleteFunc([]int{1, 2, 3}, func(n int) bool { return n == down })
		from := 20 + (down-1)*5 + 1
		retried := 0
		for id := from; id < from+5; id++ {
			retried += c.execAcrossReconnect(down%3+1, "marmot", "INSERT INTO rolling_test (id, value) VALUES (?, ?)", id, fmt.Sprintf("rolling_%d", id))
			want = append(want, fmt.Sprintf("%d|rolling_%d", id, id))
		}
		t.Logf("node %d down: %d write attempts failed while a restarted peer reconnected", down, retried)
		c.waitRows("marmot", kvRows("rolling_test"), want, up...)
		c.restart(down)
		c.waitRows("marmot", kvRows("rolling_test"), want, down)
		c.phase(fmt.Sprintf("node-%d-restarted", down))
	}
	c.waitRows("marmot", kvRows("rolling_test"), want, 1, 2, 3)
}
