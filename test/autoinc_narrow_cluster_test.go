package test

// Cluster tests for narrow AUTO_INCREMENT columns on the insert path: every
// INSERT into an INT/TINYINT AUTO_INCREMENT table takes its id from a range
// claimed cluster-wide (id.RangeAllocator over coordinator.ClaimRange).

import (
	"database/sql"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

const (
	narrowInt32Max = 2147483647
	// claimRetryDeadline bounds insertNarrow's retries of 1205, the
	// retryable answer to a claim that lost a race for the claim key.
	claimRetryDeadline = 3 * time.Second
)

// startNarrowCluster starts a cluster, configured by opts, and creates table
// with ddl on it.
func startNarrowCluster(t *testing.T, table, ddl string, opts ...func(*clusterConfig)) *cluster {
	t.Helper()
	c := newCluster(t, opts...)
	c.start()
	c.createTable(1, "marmot", table, ddl)
	return c
}

// insertNarrow runs a single-row INSERT on database through node id and
// returns its generated id, retrying 1205 (a claim that lost a race for the
// claim key) for up to claimRetryDeadline. It never fails the test itself,
// so writer goroutines can call it.
func insertNarrow(c *cluster, id int, database, q string, args ...any) (int64, error) {
	var res sql.Result
	var err error
	state, ok := poll(claimRetryDeadline, func() (bool, string) {
		res, err = c.exec(id, database, q, args...)
		return err == nil || mysqlCode(err) != mysqlcode.ErrCodeLockTimeout, fmt.Sprint(err)
	})
	if !ok {
		return 0, fmt.Errorf("node %d: %s: still 1205 after %s: %s", id, q, claimRetryDeadline, state)
	}
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// TestNarrowAutoInc_ConcurrentInsertsFitAndNeverCollide: three nodes insert
// concurrently into one INT AUTO_INCREMENT table. Every id fits 32 bits, no
// id is issued twice, and every node ends with exactly the ids issued - the
// properties the 53-bit ids of GitHub issue #170 broke and node-local rowids
// would break.
func TestNarrowAutoInc_ConcurrentInsertsFitAndNeverCollide(t *testing.T) {
	const table, perNode = "narrow_concurrent", 40
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (group_id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)")

	var mu sync.Mutex
	issued := map[int64]string{}
	var wg sync.WaitGroup
	errs := make(chan error, 3*perNode)
	for node := 1; node <= 3; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for i := 0; i < perNode; i++ {
				name := fmt.Sprintf("n%d-%d", node, i)
				id, err := insertNarrow(c, node, "marmot", "INSERT INTO "+table+" (name) VALUES (?)", name)
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				if prev, dup := issued[id]; dup || id <= 0 || id > narrowInt32Max {
					errs <- fmt.Errorf("node %d issued id %d for %s (already issued for %q, or outside [1, %d])", node, id, name, prev, narrowInt32Max)
				}
				issued[id] = name
				mu.Unlock()
			}
		}(node)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var want []string
	for _, id := range slices.Sorted(maps.Keys(issued)) {
		want = append(want, fmt.Sprintf("%d|%s", id, issued[id]))
	}
	c.waitRows("marmot", "SELECT group_id, name FROM "+table+" ORDER BY group_id", want, 1, 2, 3)
}

// TestNarrowAutoInc_KillNineNeverReissues: a node killed with SIGKILL in the
// middle of a range loses its unissued tail as a gap; after it restarts it
// claims a fresh range and never issues an id at or below one it issued
// before.
func TestNarrowAutoInc_KillNineNeverReissues(t *testing.T) {
	const table = "narrow_kill9"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	var highest int64
	for i := 0; i < 10; i++ {
		id, err := insertNarrow(c, 2, "marmot", "INSERT INTO "+table+" (v) VALUES ('before')")
		if err != nil {
			t.Fatalf("insert before kill: %v", err)
		}
		highest = max(highest, id)
	}
	c.kill(2)
	c.restart(2)
	for i := 0; i < 10; i++ {
		id, err := insertNarrow(c, 2, "marmot", "INSERT INTO "+table+" (v) VALUES ('after')")
		if err != nil {
			t.Fatalf("insert after restart: %v", err)
		}
		if id <= highest || id > narrowInt32Max {
			t.Fatalf("restarted node issued %d; ids up to %d were issued before the kill", id, highest)
		}
	}
	c.waitRows("marmot", idStats(table, "id")+" WHERE v = 'before'", []string{fmt.Sprintf("10|10|%d", highest)}, 1, 2, 3)
	c.waitRows("marmot", "SELECT COUNT(*), COUNT(DISTINCT id) FROM "+table, []string{"20|20"}, 1, 2, 3)
}

// TestNarrowAutoInc_TinyIntExhaustionIsDupEntry: a TINYINT AUTO_INCREMENT
// column filled to 127 refuses the next INSERT with ER_DUP_ENTRY (1062), the
// error MySQL reports, never the retryable 1205.
func TestNarrowAutoInc_TinyIntExhaustionIsDupEntry(t *testing.T) {
	const table = "narrow_tiny"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id TINYINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	for i := 1; i <= 127; i++ {
		if _, err := insertNarrow(c, 1, "marmot", "INSERT INTO "+table+" (v) VALUES ('x')"); err != nil {
			t.Fatalf("insert %d of 127: %v", i, err)
		}
	}
	_, err := c.exec(1, "marmot", "INSERT INTO "+table+" (v) VALUES ('overflow')")
	if code := mysqlCode(err); code != mysqlcode.ErrCodeDupEntry {
		t.Fatalf("insert past 127 into TINYINT: err = %v, want MySQL error %d", err, mysqlcode.ErrCodeDupEntry)
	}
	c.waitRows("marmot", idStats(table, "id"), []string{"127|127|127"}, 1, 2, 3)
}

// TestNarrowAutoInc_FirstWriteInTransactionShape: an ORM-style application
// boots with one transaction inserting into two narrow tables that both need
// their first claim. It commits, and each INSERT's LAST_INSERT_ID is its own
// first id.
func TestNarrowAutoInc_FirstWriteInTransactionShape(t *testing.T) {
	c := startNarrowCluster(t, "users", "CREATE TABLE users (user_id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)")
	c.createTable(1, "marmot", "groups", "CREATE TABLE `groups` (group_id INT AUTO_INCREMENT PRIMARY KEY, display_name TEXT)")

	tx, err := c.db(2, "marmot").Begin()
	if err != nil {
		t.Fatal(err)
	}
	usersRes, err := tx.Exec("INSERT INTO users (name) VALUES (?)", "admin")
	if err != nil {
		t.Fatalf("INSERT users: %v", err)
	}
	groupsRes, err := tx.Exec("INSERT INTO `groups` (display_name) VALUES (?), (?)", "app_admin", "app_editor")
	if err != nil {
		t.Fatalf("INSERT groups: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	userID, _ := usersRes.LastInsertId()
	groupID, _ := groupsRes.LastInsertId()
	if userID <= 0 || groupID <= 0 || groupID+1 > narrowInt32Max || userID > narrowInt32Max {
		t.Fatalf("LAST_INSERT_ID users=%d groups=%d, want ids in [1, %d]", userID, groupID, narrowInt32Max)
	}
	c.waitRows("marmot", "SELECT user_id, name FROM users ORDER BY user_id", []string{fmt.Sprintf("%d|admin", userID)}, 1, 2, 3)
	c.waitRows("marmot", "SELECT group_id, display_name FROM `groups` ORDER BY group_id",
		[]string{fmt.Sprintf("%d|app_admin", groupID), fmt.Sprintf("%d|app_editor", groupID+1)}, 1, 2, 3)
}

// TestNarrowAutoInc_UnmarkedTableKeepsWideIDs: a BIGINT AUTO_INCREMENT table
// carries no width marker and keeps its 53-bit generated ids.
func TestNarrowAutoInc_UnmarkedTableKeepsWideIDs(t *testing.T) {
	const table = "wide_bigint"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id BIGINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	id, err := insertNarrow(c, 1, "marmot", "INSERT INTO "+table+" (v) VALUES ('x')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if id <= 4294967295 || id >= 1<<53 {
		t.Fatalf("BIGINT id %d is not a 53-bit generated id", id)
	}
}

// TestNarrowAutoInc_CopiedIDsRaiseTheBaseOnEveryNode: ids copied into an INT
// AUTO_INCREMENT table with INSERT ... SELECT are admitted from the
// transaction's rows before it replicates, so the first generated id on
// every node lands above the copied ones.
//
// Mutation: skip coordinator.admitNarrowIDs. The first generated id then
// reuses a copied one: the insert fails or lands at or below the copied MAX.
func TestNarrowAutoInc_CopiedIDsRaiseTheBaseOnEveryNode(t *testing.T) {
	const source, table, copied = "copy_source", "copy_target", 300
	c := startNarrowCluster(t, source, "CREATE TABLE "+source+" (id BIGINT PRIMARY KEY, v TEXT)")
	fillExplicitIDs(c, source, copied)
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	c.mustExec(1, "marmot", "INSERT INTO "+table+" (id, v) SELECT id, v FROM "+source)
	c.waitRows("marmot", idStats(table, "id"), []string{fmt.Sprintf("%d|%d|%d", copied, copied, copied)}, 1, 2, 3)
	assertFirstGeneratedAbove(c, table, copied)
}

// fillExplicitIDs inserts rows 1..n into table through node 1.
func fillExplicitIDs(c *cluster, table string, n int) {
	c.t.Helper()
	for start := 1; start <= n; start += 100 {
		values, _ := idValueRows(start, min(start+99, n), "row")
		c.mustExec(1, "marmot", "INSERT INTO "+table+" (id, v) VALUES "+values)
	}
}

// assertFirstGeneratedAbove inserts one generated row through every node and
// requires each id to lie above highest.
func assertFirstGeneratedAbove(c *cluster, table string, highest int64) {
	c.t.Helper()
	for node := 1; node <= 3; node++ {
		id, err := insertNarrow(c, node, "marmot", "INSERT INTO "+table+" (v) VALUES ('generated')")
		if err != nil {
			c.t.Fatalf("node %d: first generated insert: %v", node, err)
		}
		if id <= highest {
			c.t.Fatalf("node %d: generated id %d landed at or below the copied MAX %d", node, id, highest)
		}
	}
}

// TestNarrowAutoInc_SeedFromLegacyRowsOnEveryNode: rows that reached a narrow
// table without passing the allocator - written by a binary that predates
// it, here straight into each stopped node's SQLite file - are covered by the
// DDL-time seed: the next DDL seeds every node's base from its own MAX(id),
// so the first generated id on every node lands above them.
//
// Mutation: make autoIncSeedFloor ignore MAX(id). The first generated id
// then reuses a legacy one: the insert fails or lands at or below the MAX.
func TestNarrowAutoInc_SeedFromLegacyRowsOnEveryNode(t *testing.T) {
	const table, legacy = "legacy_rows", 300
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	values, _ := idValueRows(1, legacy, "legacy")
	for node := 1; node <= 3; node++ {
		c.kill(node)
		path := filepath.Join(c.node(node).dir, "databases", db.DefaultDatabaseName+".db")
		conn, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatalf("open node %d database: %v", node, err)
		}
		_, err = conn.Exec("INSERT INTO " + table + " (id, v) VALUES " + values)
		conn.Close()
		if err != nil {
			t.Fatalf("write legacy rows on node %d: %v", node, err)
		}
	}
	c.start()
	c.mustExec(1, "marmot", "ALTER TABLE "+table+" ADD COLUMN note TEXT")
	c.waitAutoIncBaseAtLeast("marmot", table, legacy, 1, 2, 3)
	assertFirstGeneratedAbove(c, table, legacy)
}

// TestNarrowAutoInc_IDInAnotherNodesRangeIsRefused: node 1 claims a range and
// issues its first id. An explicit id inside node 1's unissued range written
// through node 2 - as a literal, a bound parameter, or copied with INSERT ...
// SELECT - is refused with 1235 before anything is written; were it
// accepted, node 1 would later issue the same id and replace the client's
// row on every node. Node 1 then issues ids past it and no node ever holds a
// client row.
//
// Mutation: admit any id at or below the base. Node 2's writes succeed.
func TestNarrowAutoInc_IDInAnotherNodesRangeIsRefused(t *testing.T) {
	const table, source = "foreign_range", "foreign_source"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	c.createTable(1, "marmot", source, "CREATE TABLE "+source+" (id BIGINT PRIMARY KEY, v TEXT)")
	first, err := insertNarrow(c, 1, "marmot", "INSERT INTO "+table+" (v) VALUES ('node1')")
	if err != nil {
		t.Fatalf("node 1 insert: %v", err)
	}
	c.mustExec(2, "marmot", "INSERT INTO "+source+" (id, v) VALUES (?, 'copied')", first+3)
	c.waitRows("marmot", "SELECT id FROM "+source, []string{fmt.Sprint(first + 3)}, 2)
	for name, w := range map[string]struct {
		q    string
		args []any
	}{
		"literal":         {q: fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, 'client')", table, first+1)},
		"bound parameter": {q: "INSERT INTO " + table + " (id, v) VALUES (?, 'client')", args: []any{first + 2}},
		"insert select":   {q: "INSERT INTO " + table + " (id, v) SELECT id, v FROM " + source},
	} {
		if _, err := c.exec(2, "marmot", w.q, w.args...); mysqlCode(err) != mysqlcode.ErrCodeNotSupportedYet {
			t.Fatalf("%s: an id inside node 1's unissued range was admitted through node 2: err = %v", name, err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := insertNarrow(c, 1, "marmot", "INSERT INTO "+table+" (v) VALUES ('node1')"); err != nil {
			t.Fatalf("node 1 insert %d: %v", i, err)
		}
	}
	c.waitRows("marmot", "SELECT COUNT(*), COUNT(DISTINCT id), SUM(v = 'node1') FROM "+table, []string{"6|6|6"}, 1, 2, 3)
}

// idStats is "COUNT(*)|COUNT(DISTINCT col)|MAX(col)" of table.
func idStats(table, col string) string {
	return fmt.Sprintf("SELECT COUNT(*), COUNT(DISTINCT %s), MAX(%s) FROM %s", col, col, table)
}
