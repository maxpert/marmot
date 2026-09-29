package test

// Cluster tests for the lifecycle of narrow AUTO_INCREMENT ranges across DDL,
// rebuilt nodes and the client shapes that reach the allocator only through
// bound values or a qualified table.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// idLedger records every generated id a test saw issued, failing the test on
// an id issued twice or outside a 32-bit column.
type idLedger struct {
	t    *testing.T
	seen map[int64]string
}

func newIDLedger(t *testing.T) *idLedger { return &idLedger{t: t, seen: map[int64]string{}} }

// record adds id, issued for who, to the ledger.
func (l *idLedger) record(who string, id int64, err error) {
	l.t.Helper()
	if err != nil {
		l.t.Fatalf("%s: %v", who, err)
	}
	if prev, dup := l.seen[id]; dup || id <= 0 || id > narrowInt32Max {
		l.t.Fatalf("%s: id %d is not a new 32-bit id (already issued to %q)", who, id, prev)
	}
	l.seen[id] = who
}

// stats is the idStats row every node must hold once the ledger's rows
// replicated: every id present once.
func (l *idLedger) stats() []string {
	var highest int64
	for id := range l.seen {
		highest = max(highest, id)
	}
	return []string{fmt.Sprintf("%d|%d|%d", len(l.seen), len(l.seen), highest)}
}

// TestNarrowAutoInc_DropDatabaseRecreateNeverReissues: node 1 issues ids from
// a range, the database is dropped and recreated with the same table, and
// every node inserts again. The claim row survives the drop and every node
// forgets its ranges of the dropped incarnation: no generated insert fails
// and no id repeats (before the fix node 1 kept minting from its dead range
// while the others were granted the same ids afresh).
func TestNarrowAutoInc_DropDatabaseRecreateNeverReissues(t *testing.T) {
	const database, table = "dropdb", "t"
	c := newCluster(t)
	c.start()
	create := func() {
		c.createDatabase(1, database)
		c.createTable(1, database, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	}
	create()
	for i := 0; i < 3; i++ {
		if _, err := insertNarrow(c, 1, database, "INSERT INTO "+table+" (v) VALUES ('before')"); err != nil {
			t.Fatalf("insert before the drop: %v", err)
		}
	}
	c.mustExec(1, "marmot", "DROP DATABASE "+database)
	c.waitDatabase(database, false, 1, 2, 3)
	create()

	// Enough rounds that a node granted the dropped incarnation's range
	// afresh would reach the ids node 1 kept issuing from its old cursor.
	ledger := newIDLedger(t)
	for round := 0; round < 10; round++ {
		for node := 1; node <= 3; node++ {
			who := fmt.Sprintf("node %d round %d", node, round)
			id, err := insertNarrow(c, node, database, "INSERT INTO "+table+" (v) VALUES (?)", who)
			ledger.record(who, id, err)
		}
	}
	c.waitRows(database, idStats(table, "id"), ledger.stats(), 1, 2, 3)
}

// TestNarrowAutoInc_BoundNullAndQualifiedInsertsAreGenerated: a prepared
// INSERT binding NULL to the id, and an INSERT naming its table as db.t from
// a session in another database, text and prepared, all get generated ids.
// Before the fix SQLite assigned such a row's id itself (MAX+1), inside
// another node's unissued range, and the admission refused it with 1235.
// Each round node 1 first issues an id from its range and the row reaches
// the others; nodes 2 and 3 then write every shape, each shape first on its
// node in round one.
func TestNarrowAutoInc_BoundNullAndQualifiedInsertsAreGenerated(t *testing.T) {
	const database, table = "qual", "t"
	c := newCluster(t)
	c.start()
	c.createDatabase(1, database)
	c.createTable(1, database, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")

	ledger := newIDLedger(t)
	boundNull := func(node int) {
		id, err := insertNarrow(c, node, database, "INSERT INTO "+table+" (id, v) VALUES (?, ?)", nil, "bound-null")
		ledger.record(fmt.Sprintf("node %d bound NULL", node), id, err)
	}
	qualified := func(node int) {
		id, err := insertNarrow(c, node, "marmot", "INSERT INTO "+database+"."+table+" (v) VALUES ('qualified')")
		ledger.record(fmt.Sprintf("node %d qualified text", node), id, err)
		id, err = insertNarrow(c, node, "marmot", "INSERT INTO "+database+"."+table+" (v) VALUES (?)", "qualified-bound")
		ledger.record(fmt.Sprintf("node %d qualified bound", node), id, err)
	}
	for round := 0; round < 10; round++ {
		id, err := insertNarrow(c, 1, database, "INSERT INTO "+table+" (v) VALUES ('node1')")
		ledger.record("node 1 generated", id, err)
		c.waitRows(database, idStats(table, "id"), ledger.stats(), 1, 2, 3)
		qualified(2)
		boundNull(2)
		boundNull(3)
		qualified(3)
	}
	c.waitRows(database, idStats(table, "id"), ledger.stats(), 1, 2, 3)
}

// wipe kills node id and deletes everything in its directory but its config
// and log, as a lost disk would.
func (c *cluster) wipe(id int) {
	c.t.Helper()
	c.kill(id)
	n := c.node(id)
	entries, err := os.ReadDir(n.dir)
	if err != nil {
		c.t.Fatal(err)
	}
	for _, e := range entries {
		path := filepath.Join(n.dir, e.Name())
		if path == n.config || path == n.log {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			c.t.Fatalf("wipe node %d: %v", id, err)
		}
	}
}

// TestNarrowAutoInc_RebuiltNodesHoldTheirVotes: a node that loses its data
// directory restarts with its votes held, merges from the other members and
// releases (restart waits for that), restores the table, and inserts on
// every node then succeed with no repeated id. The new cluster itself
// released without help: every node of a new cluster starts held.
func TestNarrowAutoInc_RebuiltNodesHoldTheirVotes(t *testing.T) {
	const table = "rebuilt"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	ledger := newIDLedger(t)
	insertOn := func(phase string) {
		for node := 1; node <= 3; node++ {
			for i := 0; i < 5; i++ {
				id, err := insertNarrow(c, node, "marmot", "INSERT INTO "+table+" (v) VALUES (?)", phase)
				ledger.record(fmt.Sprintf("%s node %d", phase, node), id, err)
			}
		}
	}
	insertOn("before")
	c.wipe(3)
	c.restart(3)
	c.waitTable("marmot", table, 3)
	insertOn("after-node3")
	c.waitRows("marmot", idStats(table, "id"), ledger.stats(), 1, 2, 3)
}
