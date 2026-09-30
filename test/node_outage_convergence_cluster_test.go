package test

// Cluster tests for convergence across node outages: every ACKed write
// across two databases, an explicit transaction held open across a peer's
// outage, database create/drop while a node is down, a node killed mid
// catch-up, convergence with GC actually running, and transactions a killed
// node had prepared but never committed.
//
// A transaction a node holds locally PENDING (its own coordinator killed
// before its local commit, or a participant COMMIT that failed against a
// pinned session) must not block that database's log pull until the
// stale-transaction GC ends it: the puller resolves such a record in the same
// round (grpc.LogPuller's resolveLocalPending). grpc/log_puller_test.go pins
// each resolution deterministically; these tests the end-to-end outcome.

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// createAutoIncTables creates each database of targets (other than marmot)
// and each table as (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT) through node
// id, waiting until every running node has them.
func createAutoIncTables(c *cluster, id int, targets []target) {
	c.t.Helper()
	for _, tg := range targets {
		if tg.database != "marmot" {
			if ok, err := c.hasDatabase(id, tg.database); err != nil || !ok {
				c.createDatabase(id, tg.database)
			}
		}
		c.createTable(id, tg.database, tg.table, "CREATE TABLE "+tg.table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	}
}

// TestNodeDownTwoDatabasesConverges: a node killed under insert and update
// load across two databases, then restarted: every node holds every ACKed
// row with its ACKed value.
func TestNodeDownTwoDatabasesConverges(t *testing.T) {
	c := newCluster(t)
	c.start()
	targets := []target{{"outagea", "t"}, {"outageb", "t"}}
	createAutoIncTables(c, 1, targets)
	l := newRowLedger()
	stop := runWriters(c, l, []int{1, 2, 3}, targets, true)
	l.waitForMore(c, ledgerStep, "before the outage")
	c.kill(3)
	l.waitForMore(c, ledgerStep, "while node 3 is down")
	c.restart(3)
	l.waitForMore(c, ledgerStep, "after node 3 returned")
	stop()
	l.waitConverged(c, targets)
}

// TestExplicitTxnAcrossNodeDownConverges: an explicit transaction (BEGIN;
// several DML; COMMIT) held open on node 1 while node 3 is killed during it
// is delivered everywhere, along with the load nodes 2 and 3 wrote around it.
// Its first inserts into xa and xb claim node 1's id ranges inside BEGIN
// while nodes 2 and 3 claim theirs concurrently.
//
// Mutation: flush the batch committer before a claim-only commit
// (TransactionManager.CommitTransactionAfter). The xb claim waits on DML
// queued behind the writer the transaction's own session holds, and the
// transaction fails.
func TestExplicitTxnAcrossNodeDownConverges(t *testing.T) {
	c := newCluster(t)
	c.start()
	targets := []target{{"marmot", "xa"}, {"marmot", "xb"}}
	createAutoIncTables(c, 1, targets)
	l := newRowLedger()
	stop := runWriters(c, l, []int{2, 3}, targets, true)
	l.waitForMore(c, ledgerStep, "before the transaction")
	// A COMMIT refused with 1213 (a write-write conflict with the concurrent
	// writers) committed nothing and is documented retryable: the client runs
	// the transaction again. Only the first attempt kills node 3. A 1213 on a
	// statement before COMMIT is not retried: there it means the transaction's
	// pinned session outlived the lock wait, which is the stall this test
	// guards.
	var err error
	for attempt := 1; attempt <= explicitTxnAttempts; attempt++ {
		err = runExplicitTxnAcrossOutage(c, l, attempt == 1)
		var refused *commitRefusedError
		if !errors.As(err, &refused) || mysqlCode(err) != mysqlcode.ErrCodeDeadlock {
			break
		}
	}
	if err != nil {
		t.Fatalf("explicit transaction: %v", err)
	}
	c.restart(3)
	l.waitForMore(c, ledgerStep, "after node 3 returned")
	stop()
	l.waitConverged(c, targets)
}

// commitRefusedError is an explicit transaction's COMMIT failing, as opposed
// to one of its statements.
type commitRefusedError struct{ err error }

func (e *commitRefusedError) Error() string { return "COMMIT: " + e.err.Error() }
func (e *commitRefusedError) Unwrap() error { return e.err }

// explicitTxnAttempts caps TestExplicitTxnAcrossNodeDownConverges' runs of
// its explicit transaction on a COMMIT refused with 1213.
const explicitTxnAttempts = 5

// runExplicitTxnAcrossOutage runs, on one connection to node 1: BEGIN, three
// inserts, (first attempt only) a kill of node 3, three more inserts and an
// UPDATE of a row node 2 committed, then COMMIT, recording the rows once the
// COMMIT is ACKed. Any failure rolls the transaction back.
func runExplicitTxnAcrossOutage(c *cluster, l *rowLedger, killNode3 bool) error {
	tx, err := c.db(1, "marmot").Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	type row struct{ key, v string }
	var rows []row
	insert := func(table, v string) error {
		res, err := tx.Exec("INSERT INTO "+table+" (v) VALUES (?)", v)
		if err != nil {
			return fmt.Errorf("INSERT %s %s: %w", table, v, err)
		}
		id, _ := res.LastInsertId()
		rows = append(rows, row{ledgerKey("marmot", table, id), v})
		return nil
	}
	for i, table := range []string{"xa", "xb", "xa"} {
		if err := insert(table, fmt.Sprintf("txn-pre-%d", i)); err != nil {
			return err
		}
		if i == 0 {
			// The transaction now holds node 1's SQLite writer: let nodes 2
			// and 3 commit while it does, so their commits queue on node 1
			// before the xb insert claims a range.
			l.waitForMore(c, 3, "while node 1's transaction holds its writer")
		}
	}
	if killNode3 {
		c.kill(3)
	}
	for i := 0; i < 3; i++ {
		if err := insert("xa", fmt.Sprintf("txn-post-%d", i)); err != nil {
			return err
		}
	}
	updated, ok := l.pick("n2", 1)
	if ok && strings.HasPrefix(updated, "marmot.xa/") {
		if _, err := tx.Exec("UPDATE xa SET v = 'txn-upd' WHERE id = " + strings.TrimPrefix(updated, "marmot.xa/")); err != nil {
			return fmt.Errorf("UPDATE %s: %w", updated, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return &commitRefusedError{err: err}
	}
	for _, r := range rows {
		l.inserted("txn", r.key, r.v)
	}
	if ok {
		// Node 2's writer may update the same row concurrently: only its
		// presence is certain.
		l.updated(updated, "txn-upd", false)
	}
	return nil
}

// TestDatabaseOpsWhileNodeDownConverge: DROP DATABASE, CREATE DATABASE and a
// drop and recreate while a node is down: the returning node ends with the
// same databases and rows as the others, and no dropped database comes back.
func TestDatabaseOpsWhileNodeDownConverge(t *testing.T) {
	c := newCluster(t)
	c.start()
	createAutoIncTables(c, 1, []target{{"holddb", "t"}, {"recreatedb", "t"}})
	for i := 0; i < 5; i++ {
		for _, database := range []string{"holddb", "recreatedb"} {
			c.mustExec(1, database, "INSERT INTO t (v) VALUES (?)", fmt.Sprintf("old-%d", i))
		}
	}
	for _, database := range []string{"holddb", "recreatedb"} {
		c.waitRows(database, "SELECT COUNT(*) FROM t", []string{"5"}, 1, 2, 3)
	}

	c.kill(3)
	c.mustExec(2, "marmot", "DROP DATABASE holddb")
	c.mustExec(1, "marmot", "DROP DATABASE recreatedb")
	c.waitDatabase("recreatedb", false, 1, 2)
	c.waitDatabase("holddb", false, 1, 2)
	targets := []target{{"newdb", "t"}, {"recreatedb", "t"}}
	createAutoIncTables(c, 1, targets)
	l := newRowLedger()
	for i := 0; i < 20; i++ {
		for _, tg := range targets {
			v := fmt.Sprintf("new-%d", i)
			res := c.mustExec(1+i%2, tg.database, "INSERT INTO t (v) VALUES (?)", v)
			id, _ := res.LastInsertId()
			l.inserted("w", ledgerKey(tg.database, "t", id), v)
		}
	}

	c.restart(3)
	c.waitDatabase("holddb", false, 3)
	c.waitDatabase("newdb", true, 3)
	l.waitConverged(c, targets)
	for _, id := range c.all() {
		if ok, err := c.hasDatabase(id, "holddb"); err != nil || ok {
			t.Fatalf("node %d: holddb present=%v (err %v) after the drop", id, ok, err)
		}
	}
}

// pulledSeqs sums node id's pull cursors over every peer and database, as its
// admin API reports them: it grows as the node applies peers' commit logs.
func (c *cluster) pulledSeqs(id int) (uint64, error) {
	var state struct {
		Peers []struct {
			Databases []struct {
				Seq uint64 `json:"local_pull_cursor_seq"`
			} `json:"databases"`
		} `json:"peers"`
	}
	if err := c.admin(id, http.MethodGet, "/cluster/replication", clientTimeout, &state); err != nil {
		return 0, err
	}
	var total uint64
	for _, p := range state.Peers {
		for _, d := range p.Databases {
			total += d.Seq
		}
	}
	return total, nil
}

// TestKillMidCatchUpConverges: a node that missed DML and DDL while down is
// SIGKILLed again as soon as its restart has begun pulling its peers' logs,
// then restarted: every node converges on every ACKed row, applies every DDL
// once, and reports the same schema versions.
func TestKillMidCatchUpConverges(t *testing.T) {
	c := newCluster(t)
	c.start()
	targets := []target{{"marmot", "catchupa"}, {"catchup", "catchupb"}}
	createAutoIncTables(c, 1, targets)
	l := newRowLedger()
	stop := runWriters(c, l, []int{1, 2}, targets, true)
	l.waitForMore(c, ledgerStep, "before the outage")
	before, err := c.pulledSeqs(3)
	if err != nil {
		t.Fatal(err)
	}
	c.kill(3)
	l.waitForMore(c, 3*ledgerStep, "while node 3 is down")
	c.mustExec(1, "marmot", "ALTER TABLE catchupa ADD COLUMN c1 INT")
	c.mustExec(1, "marmot", "CREATE TABLE catchupd (id INT PRIMARY KEY)")
	c.mustExec(2, "catchup", "ALTER TABLE catchupb ADD COLUMN c2 INT")
	l.waitForMore(c, ledgerStep, "after the DDL")

	c.startNode(3)
	waitFor(t, "node 3 begins pulling its peers' logs", readyDeadline, func() (bool, string) {
		pulled, err := c.pulledSeqs(3)
		return err == nil && pulled > before, fmt.Sprintf("pulled %d (before %d), err %v", pulled, before, err)
	})
	c.kill(3)
	c.restart(3)
	l.waitForMore(c, ledgerStep, "after node 3 returned")
	stop()
	l.waitConverged(c, targets)
	c.waitSchemaVersionsEqual("marmot", "catchup")
	for _, id := range c.all() {
		for _, q := range []struct{ database, query string }{
			{"marmot", "SELECT COUNT(*) FROM catchupd"},
			{"marmot", "SELECT c1 FROM catchupa LIMIT 1"},
			{"catchup", "SELECT c2 FROM catchupb LIMIT 1"},
		} {
			if _, err := c.rows(id, q.database, q.query); err != nil {
				t.Errorf("node %d: %s: %v", id, q.query, err)
			}
		}
	}
}

// committedTxnCount is node id's count of committed transaction records in
// database, which GC lowers as it deletes them.
func (c *cluster) committedTxnCount(id int, database string) (int64, error) {
	var counters struct {
		Count int64 `json:"committed_txn_count"`
	}
	err := c.admin(id, http.MethodGet, "/"+database+"/metadata/counters", clientTimeout, &counters)
	return counters.Count, err
}

// TestConvergesWithLiveGC runs the cluster with GC deleting log records every
// second (both retention floors at 0, so a record is eligible once every
// peer's watermark passed it). Once GC has deleted records on every node, a
// node killed and restarted still converges to every ACKed row: GC must not
// remove anything a node that is behind needs to catch up.
func TestConvergesWithLiveGC(t *testing.T) {
	c := newCluster(t, func(cfg *clusterConfig) {
		cfg.gcIntervalSeconds = 1
		cfg.gcMinRetentionHours = 0
		cfg.deltaSyncThresholdSecs = 0
	})
	c.start()
	targets := []target{{"marmot", "livegc"}}
	createAutoIncTables(c, 1, targets)
	l := newRowLedger()
	stop := runWriters(c, l, []int{1, 2, 3}, targets, false)
	l.waitForMore(c, ledgerStep, "before the GC pass")

	peak := map[int]int64{}
	waitFor(t, "GC deleted committed records on every node", readyDeadline, func() (bool, string) {
		c.progress()
		deleted := 0
		for _, id := range c.all() {
			n, err := c.committedTxnCount(id, "marmot")
			if err != nil {
				return false, err.Error()
			}
			peak[id] = max(peak[id], n)
			if n < peak[id] {
				deleted++
			}
		}
		return deleted == len(c.nodes), fmt.Sprintf("peaks %v", peak)
	})
	c.kill(3)
	l.waitForMore(c, ledgerStep, "while node 3 is down")
	c.restart(3)
	l.waitForMore(c, ledgerStep, "after node 3 returned")
	stop()
	l.waitConverged(c, targets)
}

// TestPreparedOnKilledNodeCommittedByPeersConverges: transactions node 3
// durably prepared (strict_prepare_sync: a PREPARE is synced before it is
// ACKed) but never committed, which its peers did commit. An
// explicit transaction's pinned session holds node 3's SQLite writer while
// nodes 1 and 2 commit writes, so every COMMIT to node 3 waits on that
// writer; node 3 is then SIGKILLed and restarted with those transactions
// recovered PENDING. The stale-transaction GC is kept out of the test
// (heartbeat_timeout_seconds far above its budget), so only the log pull
// resolving those records can deliver the rows. Node 3 must hold every
// ACKed row the moment it reports itself ALIVE (JOINING -> ALIVE only once
// caught up), and every node must converge.
func TestPreparedOnKilledNodeCommittedByPeersConverges(t *testing.T) {
	c := newCluster(t, func(cfg *clusterConfig) {
		cfg.heartbeatTimeoutSecs = 600
		cfg.strictPrepareSync = true
	})
	c.start()
	targets := []target{{"marmot", "pa"}, {"marmot", "pb"}}
	createAutoIncTables(c, 1, targets)
	l := newRowLedger()
	stop := runWriters(c, l, []int{1, 2}, targets[:1], false)
	l.waitForMore(c, ledgerStep, "before the pin")

	pin, err := c.db(3, "marmot").Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pin.Exec("INSERT INTO pb (v) VALUES ('pinned')"); err != nil {
		t.Fatalf("pin node 3's writer: %v", err)
	}
	l.waitForMore(c, ledgerStep, "while node 3's writer is pinned")
	waitFor(t, "node 3 holds transactions its peers committed PENDING", readyDeadline, func() (bool, string) {
		var pending []struct {
			NodeID uint64 `json:"node_id"`
		}
		if err := c.admin(3, http.MethodGet, "/marmot/metadata/transactions/pending", clientTimeout, &pending); err != nil {
			return false, err.Error()
		}
		peers := 0
		for _, p := range pending {
			if p.NodeID != 3 {
				peers++
			}
		}
		return peers > 0, fmt.Sprintf("%d pending, %d from peers", len(pending), peers)
	})
	c.kill(3)
	_ = pin.Rollback()
	stop()

	c.startNode(3)
	waitFor(t, "node 3 ALIVE", readyDeadline, func() (bool, string) {
		statuses, err := c.statuses(3)
		if err != nil || statuses[3] != "ALIVE" || c.ping(3) != nil {
			return false, fmt.Sprint(statuses, err)
		}
		if bad := l.mismatchesOn(c, 3, targets[:1]); len(bad) > 0 {
			t.Fatalf("node 3 reported ALIVE while missing %d ACKed rows, first: %s", len(bad), bad[0])
		}
		return true, ""
	})
	c.waitReady()
	l.waitConverged(c, targets[:1])
}

// schemaVersion is database's schema version as node id's admin API reports
// it.
func (c *cluster) schemaVersion(id int, database string) (string, error) {
	var v any
	err := c.admin(id, http.MethodGet, "/"+database+"/metadata/schema/version", clientTimeout, &v)
	return fmt.Sprint(v), err
}

// waitSchemaVersionsEqual waits until every node reports the same schema
// version for every database.
func (c *cluster) waitSchemaVersionsEqual(databases ...string) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("equal schema versions of %v", databases), replicationDeadline, func() (bool, string) {
		for _, database := range databases {
			var versions []string
			for _, id := range c.all() {
				v, err := c.schemaVersion(id, database)
				if err != nil {
					return false, fmt.Sprintf("node %d %s: %v", id, database, err)
				}
				versions = append(versions, v)
			}
			for _, v := range versions[1:] {
				if v != versions[0] {
					return false, fmt.Sprintf("%s: %v", database, versions)
				}
			}
		}
		return true, ""
	})
}
