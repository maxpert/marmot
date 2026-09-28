package test

// Cluster tests for narrow AUTO_INCREMENT columns on the insert path: every
// INSERT into an INT/TINYINT AUTO_INCREMENT table takes its id from a range
// claimed cluster-wide (id.RangeAllocator over coordinator.ClaimRange),
// driven over the real MySQL wire protocol against a 3-node ClusterHarness
// (test/crash_recovery_test.go).

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

const narrowInt32Max = 2147483647

// startNarrowCluster starts a 3-node cluster and creates table on it. opts
// configure the harness (e.g. GCIntervalSeconds) before any node config is
// written.
func startNarrowCluster(t *testing.T, table, ddl string, opts ...func(*ClusterHarness)) *ClusterHarness {
	t.Helper()
	harness := NewClusterHarness(t, opts...)
	if err := harness.StartCluster(); err != nil {
		harness.Cleanup()
		t.Fatalf("StartCluster: %v", err)
	}
	if _, err := harness.ExecNode(1, ddl); err != nil {
		harness.Cleanup()
		t.Fatalf("CREATE TABLE %s: %v", table, err)
	}
	if err := harness.WaitForTableExists(table, []int{1, 2, 3}, 15*time.Second); err != nil {
		harness.dumpNodeLogs("narrow_" + table)
		harness.Cleanup()
		t.Fatalf("DDL did not replicate: %v", err)
	}
	return harness
}

// insertNarrow inserts one row on nodeID, retrying the retryable lock-wait
// timeout a claim returns while the cluster settles, and returns the id.
func insertNarrow(harness *ClusterHarness, nodeID int, query string, args ...interface{}) (int64, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		res, err := harness.ExecNode(nodeID, query, args...)
		if err == nil {
			return res.LastInsertId()
		}
		lastErr = err
		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != mysqlcode.ErrCodeLockTimeout {
			return 0, err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return 0, lastErr
}

// narrowIDStats returns COUNT(*), COUNT(DISTINCT id) and MAX(id) on nodeID.
func narrowIDStats(harness *ClusterHarness, nodeID int, table, col string) (count, distinct, maxID int64, err error) {
	rows, err := harness.QueryNode(nodeID, fmt.Sprintf("SELECT COUNT(*), COUNT(DISTINCT %s), MAX(%s) FROM %s", col, col, table))
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, 0, 0, fmt.Errorf("no row from %s", table)
	}
	err = rows.Scan(&count, &distinct, &maxID)
	return count, distinct, maxID, err
}

// TestNarrowAutoInc_ConcurrentInsertsFitAndNeverCollide: three nodes insert
// concurrently into one INT AUTO_INCREMENT table. Every id must fit 32 bits
// and no id may be issued twice - the two properties the 53-bit ids of
// GitHub issue #170 broke and node-local rowids would break.
func TestNarrowAutoInc_ConcurrentInsertsFitAndNeverCollide(t *testing.T) {
	const table = "narrow_concurrent"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (group_id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)")
	defer harness.Cleanup()

	const perNode = 40
	var wg sync.WaitGroup
	errs := make(chan error, 3*perNode)
	for nodeID := 1; nodeID <= 3; nodeID++ {
		wg.Add(1)
		go func(nodeID int) {
			defer wg.Done()
			for i := 0; i < perNode; i++ {
				id, err := insertNarrow(harness, nodeID, "INSERT INTO "+table+" (name) VALUES (?)", fmt.Sprintf("n%d-%d", nodeID, i))
				if err != nil {
					errs <- fmt.Errorf("node %d insert %d: %w", nodeID, i, err)
					return
				}
				if id <= 0 || id > narrowInt32Max {
					errs <- fmt.Errorf("node %d issued id %d, outside [1, %d]", nodeID, id, narrowInt32Max)
				}
			}
		}(nodeID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		harness.dumpNodeLogs("narrow_concurrent")
		t.FailNow()
	}

	if err := harness.WaitForRowCount(table, []int{1, 2, 3}, 3*perNode, 30*time.Second); err != nil {
		harness.dumpNodeLogs("narrow_concurrent_rows")
		t.Fatalf("rows did not replicate: %v", err)
	}
	for nodeID := 1; nodeID <= 3; nodeID++ {
		count, distinct, maxID, err := narrowIDStats(harness, nodeID, table, "group_id")
		if err != nil {
			t.Fatalf("node %d: %v", nodeID, err)
		}
		if count != 3*perNode || distinct != count {
			t.Fatalf("node %d: COUNT(*)=%d COUNT(DISTINCT group_id)=%d, want both %d", nodeID, count, distinct, 3*perNode)
		}
		if maxID > narrowInt32Max {
			t.Fatalf("node %d: MAX(group_id)=%d does not fit INT", nodeID, maxID)
		}
	}
}

// TestNarrowAutoInc_KillNineNeverReissues: a node killed with SIGKILL in the
// middle of a range loses its unissued tail as a gap; after restart it
// claims a fresh range and never issues an id it issued before.
func TestNarrowAutoInc_KillNineNeverReissues(t *testing.T) {
	const table = "narrow_kill9"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()

	issued := make(map[int64]bool)
	var highest int64
	for i := 0; i < 10; i++ {
		id, err := insertNarrow(harness, 2, "INSERT INTO "+table+" (v) VALUES ('before')")
		if err != nil {
			t.Fatalf("insert before kill: %v", err)
		}
		issued[id] = true
		highest = max(highest, id)
	}

	if err := harness.KillNode(2); err != nil {
		t.Fatalf("KillNode: %v", err)
	}
	if err := harness.StartNode(2); err != nil {
		t.Fatalf("StartNode: %v", err)
	}
	if err := harness.WaitForAlive(2, 60*time.Second); err != nil {
		harness.dumpNodeLogs("narrow_kill9_restart")
		t.Fatalf("node 2 did not come back: %v", err)
	}

	for i := 0; i < 10; i++ {
		id, err := insertNarrow(harness, 2, "INSERT INTO "+table+" (v) VALUES ('after')")
		if err != nil {
			harness.dumpNodeLogs("narrow_kill9_after")
			t.Fatalf("insert after restart: %v", err)
		}
		if issued[id] || id <= highest {
			t.Fatalf("restarted node issued %d; ids up to %d were issued before the kill", id, highest)
		}
		if id > narrowInt32Max {
			t.Fatalf("id %d does not fit INT", id)
		}
		issued[id] = true
	}
}

// TestNarrowAutoInc_TinyIntExhaustionIsDupEntry: a TINYINT AUTO_INCREMENT
// column filled to 127 refuses the next INSERT with ER_DUP_ENTRY (1062), the
// error MySQL reports, never the retryable 1205.
func TestNarrowAutoInc_TinyIntExhaustionIsDupEntry(t *testing.T) {
	const table = "narrow_tiny"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id TINYINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()

	for i := 1; i <= 127; i++ {
		if _, err := insertNarrow(harness, 1, "INSERT INTO "+table+" (v) VALUES ('x')"); err != nil {
			harness.dumpNodeLogs("narrow_tiny_fill")
			t.Fatalf("insert %d of 127: %v", i, err)
		}
	}

	_, err := harness.ExecNode(1, "INSERT INTO "+table+" (v) VALUES ('overflow')")
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != mysqlcode.ErrCodeDupEntry {
		t.Fatalf("insert past 127 into TINYINT: err = %v, want MySQL error %d", err, mysqlcode.ErrCodeDupEntry)
	}
}

// TestNarrowAutoInc_LLDAPTransactionShape: LLDAP boots with one transaction
// inserting into two narrow tables that both need their first claim. It must
// complete, and each INSERT's LAST_INSERT_ID must be its own first id.
func TestNarrowAutoInc_LLDAPTransactionShape(t *testing.T) {
	harness := startNarrowCluster(t, "users", "CREATE TABLE users (user_id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)")
	defer harness.Cleanup()
	if _, err := harness.ExecNode(1, "CREATE TABLE `groups` (group_id INT AUTO_INCREMENT PRIMARY KEY, display_name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE groups: %v", err)
	}
	if err := harness.WaitForTableExists("groups", []int{1, 2, 3}, 15*time.Second); err != nil {
		t.Fatalf("groups did not replicate: %v", err)
	}

	conn, err := harness.ConnectToNode(2)
	if err != nil {
		t.Fatalf("ConnectToNode: %v", err)
	}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	usersRes, err := tx.Exec("INSERT INTO users (name) VALUES (?)", "admin")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("INSERT users: %v", err)
	}
	groupsRes, err := tx.Exec("INSERT INTO `groups` (display_name) VALUES (?), (?)", "lldap_admin", "lldap_password_manager")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("INSERT groups: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}

	userID, _ := usersRes.LastInsertId()
	groupID, _ := groupsRes.LastInsertId()
	var storedUser, firstGroup, groupCount int64
	if err := conn.QueryRow("SELECT user_id FROM users WHERE name = 'admin'").Scan(&storedUser); err != nil {
		t.Fatalf("read users: %v", err)
	}
	if err := conn.QueryRow("SELECT MIN(group_id), COUNT(*) FROM `groups`").Scan(&firstGroup, &groupCount); err != nil {
		t.Fatalf("read groups: %v", err)
	}
	if userID != storedUser || groupID != firstGroup || groupCount != 2 {
		t.Fatalf("LAST_INSERT_ID users=%d (stored %d), groups=%d (first stored %d, %d rows)", userID, storedUser, groupID, firstGroup, groupCount)
	}
	if storedUser > narrowInt32Max || firstGroup+1 > narrowInt32Max {
		t.Fatalf("ids do not fit INT: user %d, groups from %d", storedUser, firstGroup)
	}
}

// TestNarrowAutoInc_UnmarkedTableKeepsWideIDs: a BIGINT AUTO_INCREMENT table
// carries no width marker and keeps the 53-bit ids it always had.
func TestNarrowAutoInc_UnmarkedTableKeepsWideIDs(t *testing.T) {
	const table = "wide_bigint"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id BIGINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()

	id, err := insertNarrow(harness, 1, "INSERT INTO "+table+" (v) VALUES ('x')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if id <= 4294967295 || id >= 1<<53 {
		t.Fatalf("BIGINT id %d is not a 53-bit generated id", id)
	}
}

// TestNarrowAutoInc_CopiedIDsRaiseTheBaseOnEveryNode: ids copied into an INT
// AUTO_INCREMENT table with INSERT ... SELECT - MySQL's usual way to
// re-declare a column's width, whose ids are known only once it has run - are
// admitted from the transaction's rows before it replicates, so the first
// generated id on EVERY node lands above the copied ones.
//
// Mutation: skip the admission of the transaction's rows
// (coordinator.admitNarrowIDs). The first generated id then reuses a copied
// one and the insert fails or "landed at or below the copied MAX" fires.
func TestNarrowAutoInc_CopiedIDsRaiseTheBaseOnEveryNode(t *testing.T) {
	const source, table, copied = "copy_source", "copy_target", 300
	harness := startNarrowCluster(t, source, "CREATE TABLE "+source+" (id BIGINT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()

	fillExplicitIDs(t, harness, source, copied)
	if _, err := harness.ExecNode(1, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE %s: %v", table, err)
	}
	if err := harness.WaitForTableExists(table, []int{1, 2, 3}, 15*time.Second); err != nil {
		t.Fatalf("%s did not replicate: %v", table, err)
	}
	if _, err := harness.ExecNode(1, "INSERT INTO "+table+" (id, v) SELECT id, v FROM "+source); err != nil {
		t.Fatalf("INSERT ... SELECT: %v", err)
	}
	if err := harness.WaitForRowCount(table, []int{1, 2, 3}, copied, 30*time.Second); err != nil {
		t.Fatalf("copied rows did not replicate: %v", err)
	}
	assertFirstGeneratedAbove(t, harness, table, copied)
}

// TestNarrowAutoInc_SeedFromLegacyRowsOnEveryNode: rows that reached a narrow
// table without ever passing the allocator - written by a binary that
// predates it - are covered by the DDL-time seed. The next DDL on the table
// seeds every node's base from that node's own MAX(id), so the first
// generated id on EVERY node lands above them. The legacy rows are written
// straight into each stopped node's SQLite file, the only way such rows can
// arise now that every client write is admitted.
//
// Mutation: make autoIncSeedFloor ignore MAX(id) and seed only the declared
// floor. The first generated id then reuses a legacy one, and the insert
// fails or "landed at or below the copied MAX" fires.
func TestNarrowAutoInc_SeedFromLegacyRowsOnEveryNode(t *testing.T) {
	const table, legacy = "legacy_rows", 300
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()

	harness.StopCluster()
	for nodeID := 1; nodeID <= 3; nodeID++ {
		path := filepath.Join(harness.Nodes[nodeID-1].DataDir, "databases", db.DefaultDatabaseName+".db")
		conn, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatalf("open node %d database: %v", nodeID, err)
		}
		for id := 1; id <= legacy; id++ {
			if _, err := conn.Exec("INSERT INTO "+table+" (id, v) VALUES (?, 'legacy')", id); err != nil {
				conn.Close()
				t.Fatalf("write legacy row %d on node %d: %v", id, nodeID, err)
			}
		}
		conn.Close()
	}
	for nodeID := 1; nodeID <= 3; nodeID++ {
		if err := harness.StartNode(nodeID); err != nil {
			t.Fatalf("StartNode %d: %v", nodeID, err)
		}
	}
	for nodeID := 1; nodeID <= 3; nodeID++ {
		if err := harness.WaitForAlive(nodeID, 60*time.Second); err != nil {
			harness.dumpNodeLogs("narrow_legacy_restart")
			t.Fatalf("node %d did not come back: %v", nodeID, err)
		}
	}

	if _, err := harness.ExecNode(1, "ALTER TABLE "+table+" ADD COLUMN note TEXT"); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN: %v", err)
	}
	for nodeID := 1; nodeID <= 3; nodeID++ {
		waitForAutoIncBaseAtLeast(t, harness.Nodes[nodeID-1].DataDir, db.DefaultDatabaseName, table, legacy, 15*time.Second)
	}
	assertFirstGeneratedAbove(t, harness, table, legacy)
}

// TestNarrowAutoInc_IDInAnotherNodesRangeIsRefused: node 1 claims a range
// and issues its first id. An explicit id inside node 1's unissued range,
// written through node 2 - as a literal, as a bound parameter, or copied
// with INSERT ... SELECT - is refused with 1235 before anything is written;
// were it accepted, node 1 would later issue the same id and the replicated
// row would replace the client's on every node. Node 1 then issues ids past
// it and the table never holds the client's row.
//
// Mutation: admit any id at or below the base. Node 2's writes succeed and
// "was admitted" fires.
func TestNarrowAutoInc_IDInAnotherNodesRangeIsRefused(t *testing.T) {
	const table, source = "foreign_range", "foreign_source"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()
	if _, err := harness.ExecNode(1, "CREATE TABLE "+source+" (id BIGINT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE %s: %v", source, err)
	}
	if err := harness.WaitForTableExists(source, []int{1, 2, 3}, 15*time.Second); err != nil {
		t.Fatalf("%s did not replicate: %v", source, err)
	}

	first, err := insertNarrow(harness, 1, "INSERT INTO "+table+" (v) VALUES ('node1')")
	if err != nil {
		t.Fatalf("node 1 insert: %v", err)
	}
	if _, err := harness.ExecNode(2, "INSERT INTO "+source+" (id, v) VALUES (?, 'copied')", first+3); err != nil {
		t.Fatalf("fill %s: %v", source, err)
	}
	if err := harness.WaitForRowCount(source, []int{2}, 1, 15*time.Second); err != nil {
		t.Fatalf("%s row did not reach node 2: %v", source, err)
	}

	for name, write := range map[string]func() error{
		"literal": func() error {
			_, err := harness.ExecNode(2, fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, 'client')", table, first+1))
			return err
		},
		"bound parameter": func() error {
			_, err := harness.ExecNode(2, "INSERT INTO "+table+" (id, v) VALUES (?, 'client')", first+2)
			return err
		},
		"insert select": func() error {
			_, err := harness.ExecNode(2, "INSERT INTO "+table+" (id, v) SELECT id, v FROM "+source)
			return err
		},
	} {
		err := write()
		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != mysqlcode.ErrCodeNotSupportedYet {
			t.Fatalf("%s: an id inside node 1's unissued range was admitted through node 2: err = %v", name, err)
		}
	}

	for i := 0; i < 5; i++ {
		if _, err := insertNarrow(harness, 1, "INSERT INTO "+table+" (v) VALUES ('node1')"); err != nil {
			t.Fatalf("node 1 insert %d: %v", i, err)
		}
	}
	if err := harness.WaitForRowCount(table, []int{1, 2, 3}, 6, 15*time.Second); err != nil {
		t.Fatalf("rows did not replicate: %v", err)
	}
	for nodeID := 1; nodeID <= 3; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT COUNT(*) FROM "+table+" WHERE v <> 'node1'")
		if err != nil {
			t.Fatalf("node %d: %v", nodeID, err)
		}
		var foreign int
		rows.Next()
		_ = rows.Scan(&foreign)
		rows.Close()
		if foreign != 0 {
			t.Fatalf("node %d holds %d rows written through node 2 in node 1's range", nodeID, foreign)
		}
	}
}

// fillExplicitIDs inserts rows 1..n into table through node 1.
func fillExplicitIDs(t *testing.T, harness *ClusterHarness, table string, n int) {
	t.Helper()
	for start := 1; start <= n; start += 100 {
		var values []string
		for id := start; id < start+100 && id <= n; id++ {
			values = append(values, fmt.Sprintf("(%d, 'row_%d')", id, id))
		}
		if _, err := harness.ExecNode(1, "INSERT INTO "+table+" (id, v) VALUES "+strings.Join(values, ", ")); err != nil {
			t.Fatalf("fill %s: %v", table, err)
		}
	}
}

// assertFirstGeneratedAbove inserts one generated row through every node and
// requires each id to lie above highest.
func assertFirstGeneratedAbove(t *testing.T, harness *ClusterHarness, table string, highest int64) {
	t.Helper()
	for nodeID := 1; nodeID <= 3; nodeID++ {
		id, err := insertNarrow(harness, nodeID, "INSERT INTO "+table+" (v) VALUES ('generated')")
		if err != nil {
			harness.dumpNodeLogs("narrow_generated_above")
			t.Fatalf("node %d: first generated insert: %v", nodeID, err)
		}
		if id <= highest {
			t.Fatalf("node %d: generated id %d landed at or below the copied MAX %d", nodeID, id, highest)
		}
	}
}
