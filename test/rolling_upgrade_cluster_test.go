package test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// restartOnBinary kills node id, switches it to bin ("" for the tree under
// test) and restarts it, waiting until the cluster is ready.
func (c *cluster) restartOnBinary(id int, bin string) {
	c.t.Helper()
	c.kill(id)
	c.node(id).bin = bin
	c.restart(id)
}

// waitDDLRefusedNaming retries ddl through node id until it is refused with
// the rolling-upgrade refusal - the retryable 1213 - naming exactly the
// members in legacy (gossip carries each restarted node's protocol version
// to its peers asynchronously). It fails at once if the DDL commits.
func (c *cluster) waitDDLRefusedNaming(id int, ddl, legacy string) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("node %d refuses %q naming %s", id, ddl, legacy), replicationDeadline, func() (bool, string) {
		_, err := c.exec(id, "marmot", ddl)
		if err == nil {
			c.t.Fatalf("node %d accepted %q while node(s) %s still run the older release", id, ddl, legacy)
		}
		refused := mysqlCode(err) == mysqlcode.ErrCodeDeadlock && strings.Contains(err.Error(), "node(s) "+legacy+" ")
		return refused, err.Error()
	})
}

// TestRollingUpgradeRefusesDDLUntilEveryNodeServesLogPull: a rolling upgrade
// from the release before the log-pull protocol. A cluster started on
// preLogPullCommit gets nodes 1 and 2 upgraded; while node 3 still runs the
// old binary, DDL is refused cluster-wide with the retryable 1213 naming node
// 3 (through an upgraded coordinator), does not commit through the old
// coordinator either, and DML keeps replicating. Once node 3 is upgraded
// too, every node coordinates both writes and DDL, and the schema versions
// agree.
//
// The old release has no readiness endpoint of its own, so readiness asks
// every node over gRPC whether its claim votes are held: a narrow insert
// through the old node 3 before its votes are released returns 1205, which
// is how this test used to flake.
func TestRollingUpgradeRefusesDDLUntilEveryNodeServesLogPull(t *testing.T) {
	c := newCluster(t)
	for _, n := range c.nodes {
		n.bin = preLogPullBin
	}
	c.start()
	createAutoIncTables(c, 1, []target{{"marmot", "t"}})
	c.phase("start-old-release")

	c.restartOnBinary(1, "")
	c.restartOnBinary(2, "")
	c.phase("nodes-1-2-upgraded")

	c.waitDDLRefusedNaming(1, "CREATE TABLE mixed1 (id INT PRIMARY KEY)", "[3]")
	c.waitDDLRefusedNaming(2, "CREATE TABLE mixed2 (id INT PRIMARY KEY)", "[3]")
	if _, err := c.exec(3, "marmot", "CREATE TABLE mixed3 (id INT PRIMARY KEY)"); err == nil {
		t.Fatal("the old coordinator's DDL committed although its upgraded participants must refuse it")
	}
	l := newRowLedger()
	insertFromEveryNode(c, l, "mixed")
	l.waitConverged(c, []target{{"marmot", "t"}})
	c.phase("mixed-checked")

	c.restartOnBinary(3, "")
	c.phase("node-3-upgraded")
	for _, id := range c.all() {
		table := fmt.Sprintf("after%d", id)
		c.createTable(id, "marmot", table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	}
	for _, id := range c.all() {
		for _, mixed := range []string{"mixed1", "mixed2", "mixed3"} {
			if ok, err := c.hasTable(id, "marmot", mixed); err != nil || ok {
				t.Fatalf("node %d: %s present=%v (err %v): a DDL refused while mixed was applied", id, mixed, ok, err)
			}
		}
	}
	insertFromEveryNode(c, l, "upgraded")
	l.waitConverged(c, []target{{"marmot", "t"}})
	c.waitSchemaVersionsEqual("marmot")
}

// insertFromEveryNode inserts one generated row into marmot.t through each
// node and records it in l.
func insertFromEveryNode(c *cluster, l *rowLedger, tag string) {
	c.t.Helper()
	for _, id := range c.all() {
		v := fmt.Sprintf("%s-n%d", tag, id)
		rowID, err := insertNarrow(c, id, "marmot", "INSERT INTO t (v) VALUES (?)", v)
		if err != nil {
			c.t.Fatalf("%s: INSERT through node %d: %v", tag, id, err)
		}
		l.inserted(tag, ledgerKey("marmot", "t", rowID), v)
	}
}
