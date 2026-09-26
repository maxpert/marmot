package test

// Cluster tests for the lifecycle of narrow AUTO_INCREMENT ranges across DDL,
// rebuilt nodes and the client shapes that reach the allocator only through
// bound values or a qualified table, over the real MySQL wire protocol
// against a 3-node ClusterHarness (test/crash_recovery_test.go).

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// openNodeDatabase connects to database on nodeID.
func openNodeDatabase(t *testing.T, harness *ClusterHarness, nodeID int, database string) *sql.DB {
	t.Helper()
	conn, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/%s", harness.Nodes[nodeID-1].MySQLPort, database))
	if err != nil {
		t.Fatalf("open %s on node %d: %v", database, nodeID, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// waitForDatabase waits until database accepts a connection on every node.
func waitForDatabase(t *testing.T, harness *ClusterHarness, database string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for nodeID := 1; nodeID <= numNodes; nodeID++ {
		for {
			conn, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/%s", harness.Nodes[nodeID-1].MySQLPort, database))
			if err == nil {
				err = conn.Ping()
				conn.Close()
			}
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				harness.dumpNodeLogs("wait_database_" + database)
				t.Fatalf("database %s not on node %d: %v", database, nodeID, err)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// waitForTableIn waits until table exists in database on every node.
func waitForTableIn(t *testing.T, harness *ClusterHarness, database, table string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for nodeID := 1; nodeID <= numNodes; nodeID++ {
		conn := openNodeDatabase(t, harness, nodeID, database)
		for {
			var n int
			err := conn.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				harness.dumpNodeLogs("wait_table_" + table)
				t.Fatalf("%s.%s not on node %d: %v", database, table, nodeID, err)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// insertGenerated inserts one row with a generated id through conn, retrying
// the retryable lock-wait code a claim returns while the cluster settles.
func insertGenerated(conn *sql.DB, query string, args ...interface{}) (int64, error) {
	var lastErr error
	for attempt := 0; attempt < 40; attempt++ {
		res, err := conn.Exec(query, args...)
		if err == nil {
			return res.LastInsertId()
		}
		lastErr = err
		if !strings.Contains(err.Error(), "1205") {
			return 0, err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return 0, lastErr
}

// TestNarrowAutoInc_DropDatabaseRecreateNeverReissues is reviewer A's F1:
// node 1 issues ids from a range, the database is dropped and recreated with
// the same table, and every node inserts again. Before, node 1 kept minting
// from its dead range while the others were granted the same ids afresh, and
// generated inserts failed with 1062 (or, unreplicated, overwrote). Now the
// claim row survives the drop and every node forgets its ranges of the
// dropped incarnation: no generated insert fails and no id repeats.
func TestNarrowAutoInc_DropDatabaseRecreateNeverReissues(t *testing.T) {
	const database, table = "dropdb", "t"
	harness := NewClusterHarness(t)
	defer harness.Cleanup()
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	create := func() {
		t.Helper()
		if _, err := harness.ExecNode(1, "CREATE DATABASE "+database); err != nil {
			t.Fatalf("CREATE DATABASE: %v", err)
		}
		waitForDatabase(t, harness, database, 30*time.Second)
		if _, err := openNodeDatabase(t, harness, 1, database).Exec("CREATE TABLE " + table + " (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)"); err != nil {
			t.Fatalf("CREATE TABLE: %v", err)
		}
		waitForTableIn(t, harness, database, table, 30*time.Second)
	}

	create()
	node1 := openNodeDatabase(t, harness, 1, database)
	for i := 0; i < 3; i++ {
		if _, err := insertGenerated(node1, "INSERT INTO "+table+" (v) VALUES ('before')"); err != nil {
			t.Fatalf("insert before the drop: %v", err)
		}
	}

	if _, err := harness.ExecNode(1, "DROP DATABASE "+database); err != nil {
		t.Fatalf("DROP DATABASE: %v", err)
	}
	time.Sleep(2 * time.Second)
	create()

	// Enough rounds that a node granted the dropped incarnation's range
	// afresh would reach the ids node 1 kept issuing from its old cursor.
	seen := make(map[int64]int)
	for round := 0; round < 10; round++ {
		for nodeID := 1; nodeID <= numNodes; nodeID++ {
			id, err := insertGenerated(openNodeDatabase(t, harness, nodeID, database),
				"INSERT INTO "+table+" (v) VALUES (?)", fmt.Sprintf("n%d-%d", nodeID, round))
			if err != nil {
				harness.dumpNodeLogs("dropdb_after")
				t.Fatalf("node %d: generated insert after the recreate failed: %v", nodeID, err)
			}
			if other, dup := seen[id]; dup {
				t.Fatalf("id %d issued by node %d and node %d after the recreate", id, other, nodeID)
			}
			seen[id] = nodeID
		}
	}
	if err := waitForRowCountIn(harness, database, table, len(seen), 30*time.Second); err != nil {
		harness.dumpNodeLogs("dropdb_rows")
		t.Fatal(err)
	}
}

// waitForRowCountIn waits until database.table holds want rows with want
// distinct ids on every node.
func waitForRowCountIn(harness *ClusterHarness, database, table string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for nodeID := 1; nodeID <= numNodes; nodeID++ {
		for {
			conn, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/%s", harness.Nodes[nodeID-1].MySQLPort, database))
			var rows, distinct int
			if err == nil {
				err = conn.QueryRow("SELECT COUNT(*), COUNT(DISTINCT id) FROM "+table).Scan(&rows, &distinct)
				conn.Close()
			}
			if err == nil && rows == want && distinct == want {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("node %d: %s.%s has %d rows, %d distinct ids, want %d (%v)", nodeID, database, table, rows, distinct, want, err)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	return nil
}

// TestNarrowAutoInc_BoundNullAndQualifiedInsertsAreGenerated is reviewer A's
// F3 and F4 over the wire: a prepared INSERT binding NULL to the id, and an
// INSERT naming its table as db.t from a session in another database, text
// and prepared. Before, SQLite assigned such a row's id itself (MAX+1), and
// whenever that fell inside another node's unissued range the admission
// refused it with 1235: about half the bound NULLs and every qualified INSERT
// in reviewer A's runs.
//
// Each round node 1 first issues a generated id from its range and the row
// reaches the others; nodes 2 and 3 then write every shape. A SQLite rowid
// would land inside another node's unissued range and be refused, so every
// write must get a generated, distinct 32-bit id.
func TestNarrowAutoInc_BoundNullAndQualifiedInsertsAreGenerated(t *testing.T) {
	const database, table = "qual", "t"
	harness := NewClusterHarness(t)
	defer harness.Cleanup()
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	if _, err := harness.ExecNode(1, "CREATE DATABASE "+database); err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}
	waitForDatabase(t, harness, database, 30*time.Second)
	if _, err := openNodeDatabase(t, harness, 1, database).Exec("CREATE TABLE " + table + " (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	waitForTableIn(t, harness, database, table, 30*time.Second)

	seen := make(map[int64]bool)
	record := func(nodeID int, shape string, id int64, err error) {
		t.Helper()
		if err != nil {
			harness.dumpNodeLogs("qual_" + shape)
			t.Fatalf("node %d %s: %v", nodeID, shape, err)
		}
		if id <= 0 || id > narrowInt32Max || seen[id] {
			t.Fatalf("node %d %s: id %d is not a new 32-bit id", nodeID, shape, id)
		}
		seen[id] = true
	}
	node1 := openNodeDatabase(t, harness, 1, database)
	const rounds = 10
	for round := 0; round < rounds; round++ {
		id, err := insertGenerated(node1, "INSERT INTO "+table+" (v) VALUES ('node1')")
		record(1, "generated", id, err)
		if err := waitForRowCountIn(harness, database, table, len(seen), 30*time.Second); err != nil {
			t.Fatal(err)
		}
		// Node 2 writes the qualified shapes first and node 3 the bound NULL
		// first, so that in the first round each shape is the first write
		// on its node: a rowid it got from SQLite would be MAX+1, inside
		// another node's unissued range.
		boundNull := func(nodeID int) {
			id, err := insertGenerated(openNodeDatabase(t, harness, nodeID, database),
				"INSERT INTO "+table+" (id, v) VALUES (?, ?)", nil, "bound-null")
			record(nodeID, "bound NULL", id, err)
		}
		qualified := func(nodeID int) {
			inDefault := openNodeDatabase(t, harness, nodeID, "marmot")
			id, err := insertGenerated(inDefault, "INSERT INTO "+database+"."+table+" (v) VALUES ('qualified')")
			record(nodeID, "qualified text", id, err)
			id, err = insertGenerated(inDefault, "INSERT INTO "+database+"."+table+" (v) VALUES (?)", "qualified-bound")
			record(nodeID, "qualified bound", id, err)
		}
		qualified(2)
		boundNull(2)
		boundNull(3)
		qualified(3)
	}
	if err := waitForRowCountIn(harness, database, table, len(seen), 30*time.Second); err != nil {
		harness.dumpNodeLogs("qual_rows")
		t.Fatal(err)
	}
}

// wipeNode stops nodeID and deletes everything in its data directory but its
// config, as a lost disk would.
func wipeNode(t *testing.T, harness *ClusterHarness, nodeID int) {
	t.Helper()
	if err := harness.StopNode(nodeID); err != nil {
		t.Fatalf("StopNode(%d): %v", nodeID, err)
	}
	node := harness.Nodes[nodeID-1]
	entries, err := os.ReadDir(node.DataDir)
	if err != nil {
		t.Fatalf("read node %d data dir: %v", nodeID, err)
	}
	for _, e := range entries {
		if e.Name() == filepath.Base(node.ConfigPath) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(node.DataDir, e.Name())); err != nil {
			t.Fatalf("wipe node %d: %v", nodeID, err)
		}
	}
}

// nodeLogContains reports whether nodeID's current log holds text.
func nodeLogContains(t *testing.T, harness *ClusterHarness, nodeID int, text string) bool {
	t.Helper()
	log, err := os.ReadFile(harness.Nodes[nodeID-1].LogFile)
	if err != nil {
		t.Fatalf("read node %d log: %v", nodeID, err)
	}
	return strings.Contains(string(log), text)
}

const autoIncReleasedLog = "AUTO_INCREMENT claim votes released"

// TestNarrowAutoInc_RebuiltNodesHoldTheirVotes is reviewer B's C2 end to end:
// a node that loses its data directory restarts with its votes held, merges
// from the other members and releases, and inserts on every node then
// succeed with no repeated id. The new cluster itself released without help:
// every node of a new cluster starts held.
//
// The seedless seed node (C2 (a)) is covered in process
// (TestClaimRange_RebuiltNodeHoldsVotesUntilMerged) and its membership-of-one
// belt by grpc.TestAutoIncMergeSafe: in this harness a wiped node 1 comes back
// knowing only itself and stays that way, so the only observable outcome here
// would be that it never releases.
func TestNarrowAutoInc_RebuiltNodesHoldTheirVotes(t *testing.T) {
	const table = "rebuilt"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()

	seen := make(map[int64]bool)
	insertOn := func(phase string, nodes ...int) {
		t.Helper()
		for _, nodeID := range nodes {
			for i := 0; i < 5; i++ {
				id, err := insertNarrow(harness, nodeID, "INSERT INTO "+table+" (v) VALUES (?)", phase)
				if err != nil {
					harness.dumpNodeLogs("rebuilt_" + phase)
					t.Fatalf("%s: node %d insert: %v", phase, nodeID, err)
				}
				if seen[id] || id > narrowInt32Max {
					t.Fatalf("%s: node %d issued %d again or beyond INT", phase, nodeID, id)
				}
				seen[id] = true
			}
		}
	}
	insertOn("before", 1, 2, 3)

	wipeNode(t, harness, 3)
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("StartNode(3): %v", err)
	}
	if err := harness.WaitForAlive(3, 60*time.Second); err != nil {
		harness.dumpNodeLogs("rebuilt_node3")
		t.Fatalf("node 3 did not come back: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for !nodeLogContains(t, harness, 3, autoIncReleasedLog) {
		if time.Now().After(deadline) {
			harness.dumpNodeLogs("rebuilt_node3_release")
			t.Fatal("rebuilt node 3 never released its claim votes")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := harness.WaitForTableExists(table, []int{3}, 60*time.Second); err != nil {
		harness.dumpNodeLogs("rebuilt_node3_table")
		t.Fatalf("node 3 did not restore %s: %v", table, err)
	}
	insertOn("after-node3", 1, 2, 3)
}
