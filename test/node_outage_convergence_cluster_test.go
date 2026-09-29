package test

// Cluster tests for convergence across node outages: every ACKed write
// across two databases, an explicit transaction held open across a peer's
// outage, database create/drop while a node is down, a node killed mid
// catch-up, convergence with GC actually running, and transactions a killed
// node had prepared but never committed.
//
// These check row presence (missing=0) and duplicate ids only. Row values
// converging across nodes is checked separately
// (row_version_convergence_cluster_test.go).
//
// A transaction a node holds locally PENDING (its own coordinator killed
// before its local commit, or a participant COMMIT that failed against a
// pinned session) must not block that database's log pull until the
// stale-transaction GC ends it, or an ACKed row stays missing well past
// convergenceDeadline: the puller resolves such a record in the same round
// (grpc.LogPuller's resolveLocalPending). grpc/log_puller_test.go pins each
// resolution deterministically, these tests the end-to-end outcome.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// keyLedger records every ACKed row key (database.table/id) across possibly
// several databases, and each writer's own keys so a writer can UPDATE one of
// its own rows. Unlike rowLedger (f2_convergence_cluster_test.go, one
// database) it does not compare values across nodes - see the file comment.
type keyLedger struct {
	harness *ClusterHarness
	mu      sync.Mutex
	keys    map[string]bool
	own     map[string][]string
	dups    []string
}

func newKeyLedger(h *ClusterHarness) *keyLedger {
	return &keyLedger{harness: h, keys: map[string]bool{}, own: map[string][]string{}}
}

func ledgerKey(database, table string, id int64) string {
	return fmt.Sprintf("%s.%s/%d", database, table, id)
}

// acked records key as ACKed by writer. A duplicate key (the same id
// generated twice) is recorded, never silently overwritten.
func (l *keyLedger) acked(writer, key string) {
	l.harness.Progress()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.keys[key] {
		l.dups = append(l.dups, key)
	}
	l.keys[key] = true
	l.own[writer] = append(l.own[writer], key)
}

// pick returns one of writer's own ACKed keys, deterministically spread by i.
func (l *keyLedger) pick(writer string, i int) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := l.own[writer]
	if len(keys) == 0 {
		return "", false
	}
	return keys[(i*7919)%len(keys)], true
}

func (l *keyLedger) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.keys)
}

// rowIDsOn reads every row id (not value: see file comment) of every
// database.table on node.
func rowIDsOn(t *testing.T, h *ClusterHarness, node int, tables map[string][]string) (map[string]bool, error) {
	got := map[string]bool{}
	for database, ts := range tables {
		conn := openTimedNodeDatabase(t, h, node, database)
		for _, table := range ts {
			a, err := queryAnswer(conn, "SELECT id FROM "+table)
			if err != nil {
				return nil, fmt.Errorf("node %d %s.%s: %v", node, database, table, err)
			}
			for _, r := range a.rows {
				got[database+"."+table+"/"+r[0]] = true
			}
		}
	}
	return got, nil
}

// waitKeysConverged polls until every node holds every ACKed row's id, within
// deadline.
func waitKeysConverged(t *testing.T, h *ClusterHarness, l *keyLedger, tables map[string][]string, deadline time.Duration) {
	t.Helper()
	start := time.Now()
	for {
		h.Progress()
		var missing []string
		for node := 1; node <= numNodes; node++ {
			got, err := rowIDsOn(t, h, node, tables)
			if err != nil {
				missing = append(missing, err.Error())
				continue
			}
			l.mu.Lock()
			for k := range l.keys {
				if !got[k] {
					missing = append(missing, fmt.Sprintf("node %d %s absent", node, k))
				}
			}
			l.mu.Unlock()
		}
		if len(missing) == 0 {
			t.Logf("converged: %d ACKed rows on every node, %s after load stopped", l.size(), time.Since(start).Round(time.Millisecond))
			return
		}
		if time.Since(start) > deadline {
			sort.Strings(missing)
			if len(missing) > 20 {
				missing = append(missing[:20], fmt.Sprintf("... %d more", len(missing)-20))
			}
			h.dumpNodeLogs("cluster_" + strings.ReplaceAll(t.Name(), "/", "_"))
			t.Fatalf("ACKed rows still missing %s after load stopped:\n%s", deadline, strings.Join(missing, "\n"))
		}
		time.Sleep(convergencePoll)
	}
}

// runKeyWriters runs one writer per node in nodes: it inserts round-robin into
// every database.table and every third statement updates one of its own
// ACKed rows (the id, and so the ledger key, does not change). A statement
// that fails is simply retried on the next iteration.
func runKeyWriters(t *testing.T, h *ClusterHarness, l *keyLedger, nodes []int, targets [][2]string, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			conns := map[string]*sql.DB{}
			for _, tg := range targets {
				if conns[tg[0]] == nil {
					conns[tg[0]] = openTimedNodeDatabase(t, h, n, tg[0])
				}
			}
			writer := fmt.Sprintf("n%d", n)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				tg := targets[i%len(targets)]
				if i%3 == 2 {
					if key, ok := l.pick(writer, i); ok {
						parts := strings.SplitN(key, "/", 2)
						dt := strings.SplitN(parts[0], ".", 2)
						v := fmt.Sprintf("n%d#%d-u", n, i)
						if _, err := execTimed(conns[dt[0]], "UPDATE "+dt[1]+" SET v = ? WHERE id = "+parts[1], v); err != nil {
							time.Sleep(convergencePoll)
						}
						continue
					}
				}
				conn := conns[tg[0]]
				v := fmt.Sprintf("n%d#%d", n, i)
				res, err := execTimed(conn, "INSERT INTO "+tg[1]+" (v) VALUES (?)", v)
				if err != nil {
					time.Sleep(convergencePoll)
					continue
				}
				if id, err := res.LastInsertId(); err == nil {
					l.acked(writer, ledgerKey(tg[0], tg[1], id))
				}
			}
		}(n)
	}
	return &wg
}

// sleepWithProgress sleeps d while reporting progress to the stall watchdog, so an
// intentionally idle phase (e.g. "node stopped, about to restart") is never
// mistaken for a stall.
func sleepWithProgress(h *ClusterHarness, d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		h.Progress()
		time.Sleep(convergencePoll)
	}
}

// startWithTables starts a cluster and creates each database (unless "marmot") and
// each database.table (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT).
func startWithTables(t *testing.T, h *ClusterHarness, tables map[string][]string) {
	t.Helper()
	if err := h.StartCluster(); err != nil {
		h.Cleanup()
		t.Fatalf("StartCluster: %v", err)
	}
	createTables(t, h, []int{1, 2, 3}, tables)
}

func createTables(t *testing.T, h *ClusterHarness, nodes []int, tables map[string][]string) {
	t.Helper()
	for database, ts := range tables {
		if database != "marmot" {
			if _, err := h.ExecNode(1, "CREATE DATABASE "+database); err != nil {
				t.Fatalf("CREATE DATABASE %s: %v", database, err)
			}
			waitDatabasePresence(t, h, nodes, database, true, 30*time.Second)
		}
		for _, table := range ts {
			if _, err := execTimed(openTimedNodeDatabase(t, h, nodes[0], database), "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)"); err != nil {
				t.Fatalf("CREATE TABLE %s.%s: %v", database, table, err)
			}
			for _, n := range nodes {
				waitTableOn(t, h, n, database, table, 30*time.Second)
			}
		}
	}
}

func hasDatabase(t *testing.T, h *ClusterHarness, node int, database string) (bool, error) {
	a, err := queryAnswer(openTimedNodeDatabase(t, h, node, "marmot"), "SHOW DATABASES")
	if err != nil {
		return false, err
	}
	for _, r := range a.rows {
		if strings.EqualFold(r[0], database) {
			return true, nil
		}
	}
	return false, nil
}

func waitDatabasePresence(t *testing.T, h *ClusterHarness, nodes []int, database string, present bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for _, n := range nodes {
		for {
			h.Progress()
			ok, err := hasDatabase(t, h, n, database)
			if err == nil && ok == present {
				break
			}
			if time.Now().After(deadline) {
				h.dumpNodeLogs("cluster_db_" + database)
				t.Fatalf("database %s present=%v not reached on node %d within %s (err=%v)", database, present, n, timeout, err)
			}
			time.Sleep(convergencePoll)
		}
	}
}

func waitTableOn(t *testing.T, h *ClusterHarness, node int, database, table string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	conn := openTimedNodeDatabase(t, h, node, database)
	for {
		h.Progress()
		if _, err := queryAnswer(conn, "SELECT COUNT(*) FROM "+table); err == nil {
			return
		}
		if time.Now().After(deadline) {
			h.dumpNodeLogs("cluster_table_" + table)
			t.Fatalf("%s.%s not on node %d within %s", database, table, node, timeout)
		}
		time.Sleep(convergencePoll)
	}
}

// schemaVersionOf reads database's schema version through node's admin API.
func schemaVersionOf(h *ClusterHarness, node int, database string) (string, error) {
	url := fmt.Sprintf("http://localhost:%d/admin/%s/metadata/schema/version", h.Nodes[node-1].GRPCPort, database)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Marmot-Secret", "test-secret")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	noteClientCall()
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var v interface{}
	if err := json.Unmarshal(body, &v); err != nil {
		return "", fmt.Errorf("status %d body %s", resp.StatusCode, body)
	}
	return fmt.Sprintf("%d:%s", resp.StatusCode, strings.TrimSpace(string(body))), nil
}

// waitSchemaVersionsEqual waits until every node reports the same schema
// version for every database.
func waitSchemaVersionsEqual(t *testing.T, h *ClusterHarness, databases []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		h.Progress()
		var diff []string
		for _, database := range databases {
			var vs []string
			for n := 1; n <= numNodes; n++ {
				v, err := schemaVersionOf(h, n, database)
				if err != nil {
					v = "err:" + err.Error()
				}
				vs = append(vs, v)
			}
			if vs[0] != vs[1] || vs[0] != vs[2] {
				diff = append(diff, fmt.Sprintf("%s: %v", database, vs))
			}
		}
		if len(diff) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("schema versions differ after %s: %v", timeout, diff)
		}
		time.Sleep(time.Second)
	}
}

// waitLogLine waits for the n-th occurrence of line in node's log.
func waitLogLine(h *ClusterHarness, node int, line string, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(h.Nodes[node-1].LogFile)
		if strings.Count(string(data), line) >= n {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// TestNodeDownTwoDatabasesConverges: a node stopped for 15s under
// insert+update load across two databases, then returned: every ACKed row on
// every node within convergenceDeadline.
func TestNodeDownTwoDatabasesConverges(t *testing.T) {
	h := NewClusterHarness(t)
	defer h.Cleanup()
	tables := map[string][]string{"outagea": {"t"}, "outageb": {"t"}}
	startWithTables(t, h, tables)
	h.Phase("start")

	l := newKeyLedger(h)
	stop := make(chan struct{})
	wg := runKeyWriters(t, h, l, []int{1, 2, 3}, [][2]string{{"outagea", "t"}, {"outageb", "t"}}, stop)
	sleepWithProgress(h, 3*time.Second)
	h.Phase("load")

	if err := h.StopNode(3); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-stopped")
	sleepWithProgress(h, 15*time.Second)

	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-alive")
	sleepWithProgress(h, 3*time.Second)
	close(stop)
	wg.Wait()
	h.Phase("load-stopped")

	if len(l.dups) > 0 {
		t.Fatalf("duplicate ids: %v", l.dups)
	}
	waitKeysConverged(t, h, l, tables, convergenceDeadline)
	h.Phase("converged")
}

// TestExplicitTxnAcrossNodeDownConverges: an explicit transaction (BEGIN;
// several DML; COMMIT) held open on node 1, with node 3 stopped during it:
// delivered everywhere.
func TestExplicitTxnAcrossNodeDownConverges(t *testing.T) {
	h := NewClusterHarness(t)
	defer h.Cleanup()
	tables := map[string][]string{"marmot": {"xa", "xb"}}
	startWithTables(t, h, tables)
	h.Phase("start")

	l := newKeyLedger(h)
	stop := make(chan struct{})
	wg := runKeyWriters(t, h, l, []int{2, 3}, [][2]string{{"marmot", "xa"}, {"marmot", "xb"}}, stop)
	sleepWithProgress(h, 3*time.Second)
	h.Phase("load")

	db1 := openTimedNodeDatabase(t, h, 1, "marmot")
	var txnKeys []string
	for attempt := 1; ; attempt++ {
		keys, err := runExplicitTxnAcrossOutage(t, h, l, db1, attempt == 1)
		if err == nil {
			txnKeys = keys
			break
		}
		if !isDeadlockRetry(err) || attempt == explicitTxnAttempts {
			t.Fatalf("explicit txn attempt %d: %v", attempt, err)
		}
		// 1213 is documented retryable: nothing of the transaction was
		// committed, and its ids were already returned, so the product cannot
		// re-claim them at COMMIT - the client retries the whole transaction.
		t.Logf("explicit txn attempt %d refused with retryable 1213, retrying: %v", attempt, err)
		sleepWithProgress(h, explicitTxnBackoff)
	}
	h.Phase("commit")
	for _, k := range txnKeys {
		l.acked("txn", k)
	}
	t.Logf("explicit txn committed %d rows, ACKed=%d", len(txnKeys), l.size())
	sleepWithProgress(h, 3*time.Second)

	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-alive")
	sleepWithProgress(h, 2*time.Second)
	close(stop)
	wg.Wait()
	h.Phase("load-stopped")

	if len(l.dups) > 0 {
		t.Fatalf("duplicate ids: %v", l.dups)
	}
	waitKeysConverged(t, h, l, tables, convergenceDeadline)
	h.Phase("converged")
}

// explicitTxnAttempts caps how often TestExplicitTxnAcrossNodeDownConverges
// runs its explicit transaction on the documented-retryable 1213, and
// explicitTxnBackoff is the pause before each retry.
const (
	explicitTxnAttempts = 5
	explicitTxnBackoff  = time.Second
)

// isDeadlockRetry reports whether err is MySQL 1213, the retryable refusal
// of a whole transaction.
func isDeadlockRetry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlcode.ErrCodeDeadlock
}

// runExplicitTxnAcrossOutage runs the explicit transaction of
// TestExplicitTxnAcrossNodeDownConverges on node 1 - three
// inserts, node 3 stopped mid-transaction, three more inserts and an UPDATE
// of a row node 2 committed, held open a while before COMMIT - and returns
// the keys it inserted once COMMIT succeeds. Only the first attempt stops
// node 3 and holds the transaction open; a retry runs straight through. Any
// failure rolls it back.
func runExplicitTxnAcrossOutage(t *testing.T, h *ClusterHarness, l *keyLedger, db1 *sql.DB, firstAttempt bool) ([]string, error) {
	conn, err := db1.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	committed := false
	defer func() {
		if !committed {
			_, _ = connExecTimed(conn, "ROLLBACK")
		}
	}()
	var keys []string
	insert := func(table, v string) error {
		res, err := connExecTimed(conn, "INSERT INTO "+table+" (v) VALUES (?)", v)
		if err != nil {
			return err
		}
		id, _ := res.LastInsertId()
		keys = append(keys, ledgerKey("marmot", table, id))
		return nil
	}
	if _, err := connExecTimed(conn, "BEGIN"); err != nil {
		return nil, err
	}
	h.Phase("begin")
	for i, table := range []string{"xa", "xb", "xa"} {
		if err := insert(table, fmt.Sprintf("txn-pre-%d", i)); err != nil {
			return nil, err
		}
	}
	if firstAttempt {
		sleepWithProgress(h, 2*time.Second)
		if err := h.StopNode(3); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		// xa: node 1 already holds an id range for it, so no claim (which
		// needs a quorum while node 3 is not yet suspected) is needed.
		if err := insert("xa", fmt.Sprintf("txn-post-%d", i)); err != nil {
			return nil, err
		}
	}
	// Update a row node 2 committed before this transaction.
	if key, ok := l.pick("n2", 1); ok && strings.HasPrefix(key, "marmot.xa/") {
		if _, err := connExecTimed(conn, "UPDATE xa SET v = ? WHERE id = "+strings.TrimPrefix(key, "marmot.xa/"), "txn-upd"); err != nil {
			return nil, err
		}
	}
	if firstAttempt {
		sleepWithProgress(h, 3*time.Second)
	}
	if _, err := connExecTimed(conn, "COMMIT"); err != nil {
		return nil, err
	}
	committed = true
	return keys, nil
}

// connExecTimed runs one statement on an already-open *sql.Conn under
// clusterQueryTimeout (execTimed takes a *sql.DB, which an explicit
// transaction's pinned connection is not).
func connExecTimed(conn *sql.Conn, q string, args ...interface{}) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterQueryTimeout)
	defer cancel()
	defer noteClientCall()
	return conn.ExecContext(ctx, q, args...)
}

// TestDatabaseOpsWhileNodeDownConverge: CREATE DATABASE and DROP DATABASE
// (and a drop+recreate) while a node is down: the returning node converges
// and no dropped database is resurrected.
func TestDatabaseOpsWhileNodeDownConverge(t *testing.T) {
	h := NewClusterHarness(t)
	defer h.Cleanup()
	startWithTables(t, h, map[string][]string{"holddb": {"t"}, "recreatedb": {"t"}})
	h.Phase("start")

	l := newKeyLedger(h)
	for i := 0; i < 5; i++ {
		for _, database := range []string{"holddb", "recreatedb"} {
			if _, err := execTimed(openTimedNodeDatabase(t, h, 1, database), "INSERT INTO t (v) VALUES (?)", fmt.Sprintf("old-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for n := 1; n <= numNodes; n++ {
		for _, database := range []string{"holddb", "recreatedb"} {
			for {
				a, err := queryAnswer(openTimedNodeDatabase(t, h, n, database), "SELECT COUNT(*) FROM t")
				if err == nil && a.rows[0][0] == "5" {
					break
				}
				sleepWithProgress(h, convergencePoll)
			}
		}
	}
	h.Phase("baseline-load")

	if err := h.StopNode(3); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-stopped")
	up := []int{1, 2}
	if _, err := h.ExecNode(2, "DROP DATABASE holddb"); err != nil {
		t.Fatalf("DROP holddb: %v", err)
	}
	if _, err := h.ExecNode(1, "DROP DATABASE recreatedb"); err != nil {
		t.Fatalf("DROP recreatedb: %v", err)
	}
	waitDatabasePresence(t, h, up, "recreatedb", false, 30*time.Second)
	createTables(t, h, up, map[string][]string{"newdb": {"t"}, "recreatedb": {"t"}})
	for i := 0; i < 20; i++ {
		for _, database := range []string{"newdb", "recreatedb"} {
			v := fmt.Sprintf("new-%d", i)
			res, err := execTimed(openTimedNodeDatabase(t, h, up[i%2], database), "INSERT INTO t (v) VALUES (?)", v)
			if err != nil {
				t.Fatalf("insert %s: %v", database, err)
			}
			id, _ := res.LastInsertId()
			l.acked("w", ledgerKey(database, "t", id))
		}
	}
	h.Phase("ops-done-node3-down")

	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-alive")
	waitDatabasePresence(t, h, []int{3}, "holddb", false, convergenceDeadline)
	waitDatabasePresence(t, h, []int{3}, "newdb", true, convergenceDeadline)
	waitKeysConverged(t, h, l, map[string][]string{"newdb": {"t"}, "recreatedb": {"t"}}, convergenceDeadline)
	h.Phase("converged")

	// One more anti-entropy round: an old peer must not bring holddb back.
	sleepWithProgress(h, 6*time.Second)
	for n := 1; n <= numNodes; n++ {
		ok, err := hasDatabase(t, h, n, "holddb")
		if err != nil || ok {
			t.Fatalf("node %d: holddb present=%v err=%v after the drop", n, ok, err)
		}
	}
	h.Phase("no-resurrection-checked")
}

// TestKillMidCatchUpConverges: kill -9 of a node mid catch-up (DDL and DML
// missed while down), then restart: converges, no duplicate apply, equal
// schema versions. The kill lands 150ms after the startup pull began, inside
// the 20ms-300ms range over which this was seen to hold.
func TestKillMidCatchUpConverges(t *testing.T) {
	h := NewClusterHarness(t)
	defer h.Cleanup()
	tables := map[string][]string{"marmot": {"catchupa"}, "catchup": {"catchupb"}}
	startWithTables(t, h, tables)
	h.Phase("start")

	l := newKeyLedger(h)
	stop := make(chan struct{})
	targets := [][2]string{{"marmot", "catchupa"}, {"catchup", "catchupb"}}
	wg := runKeyWriters(t, h, l, []int{1, 2}, targets, stop)
	sleepWithProgress(h, 3*time.Second)
	h.Phase("load")

	if err := h.StopNode(3); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-stopped")
	sleepWithProgress(h, 4*time.Second)

	c1 := openTimedNodeDatabase(t, h, 1, "marmot")
	for _, q := range []string{"ALTER TABLE catchupa ADD COLUMN c1 INT", "CREATE TABLE catchupd (id INT PRIMARY KEY)"} {
		if _, err := execTimed(c1, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if _, err := execTimed(openTimedNodeDatabase(t, h, 2, "catchup"), "ALTER TABLE catchupb ADD COLUMN c2 INT"); err != nil {
		t.Fatalf("alter catchupb: %v", err)
	}
	sleepWithProgress(h, 5*time.Second)
	h.Phase("missed-ddl")

	const pullLine = "Starting startup log pull"
	before, _ := os.ReadFile(h.Nodes[2].LogFile)
	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	if !waitLogLine(h, 3, pullLine, strings.Count(string(before), pullLine)+1, 20*time.Second) {
		t.Logf("startup pull line not seen; killing anyway")
	}
	time.Sleep(150 * time.Millisecond)
	if err := h.KillNode(3); err != nil {
		t.Fatal(err)
	}
	h.Phase("killed-mid-catchup")
	sleepWithProgress(h, 2*time.Second)

	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-alive")
	sleepWithProgress(h, 2*time.Second)
	close(stop)
	wg.Wait()
	h.Phase("load-stopped")

	if len(l.dups) > 0 {
		t.Fatalf("duplicate ids: %v", l.dups)
	}
	waitKeysConverged(t, h, l, tables, convergenceDeadline)
	h.Phase("converged")
	waitSchemaVersionsEqual(t, h, []string{"marmot", "catchup"}, 15*time.Second)
	h.Phase("schema-versions-equal")

	for n := 1; n <= numNodes; n++ {
		for _, q := range []struct{ db, q string }{
			{"marmot", "SELECT COUNT(*) FROM catchupd"},
			{"marmot", "SELECT c1 FROM catchupa LIMIT 1"},
			{"catchup", "SELECT c2 FROM catchupb LIMIT 1"},
		} {
			if _, err := queryAnswer(openTimedNodeDatabase(t, h, n, q.db), q.q); err != nil {
				t.Errorf("node %d %s: %v", n, q.q, err)
			}
		}
	}
}

// TestConvergesWithLiveGC runs the cluster with GC actually deleting log
// entries (gc_interval_seconds lives in [replication]; a harness that wrote
// it elsewhere would silently run the 60s default and never see GC delete
// anything). With GC on and running every 5s, writes settle
// enough to be GC'd (seen via the log line GC logs when it deletes committed
// records, db/transaction.go cleanupOldTransactionRecords), and a node killed
// and restarted afterward still converges to every ACKed row - GC must not
// remove anything a still-behind node needs to catch up on.
func TestConvergesWithLiveGC(t *testing.T) {
	const table = "livegc"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)", func(h *ClusterHarness) {
		// gc_interval_seconds must stay >= anti_entropy_interval_seconds (the
		// harness's 5s) and gc_min_retention_hours must stay >=
		// delta_sync_threshold_seconds in hours (cfg.Validate enforces
		// both), so both retention knobs are dropped to 0: a committed
		// record becomes GC-eligible as soon as every peer's watermark has
		// passed it, instead of after 2h.
		h.GCIntervalSeconds = 5
		h.GCMinRetentionHours = 0
		h.DeltaSyncThresholdSeconds = 0
	})
	defer harness.Cleanup()
	harness.Phase("start")

	tables := []string{table}
	ledger := newRowLedger(harness)
	stop := make(chan struct{})
	wg := runLedgerWriters(t, harness, ledger, []int{1, 2, 3}, tables, stop)
	ledger.waitForMore(t, 150, "before the GC pass")
	harness.Phase("load")

	const gcLine = "GC: Cleaned up old transaction records"
	deadline := time.Now().Add(20 * time.Second)
	for n := 1; n <= numNodes; n++ {
		for {
			data, _ := os.ReadFile(harness.Nodes[n-1].LogFile)
			if strings.Contains(string(data), gcLine) {
				break
			}
			if time.Now().After(deadline) {
				close(stop)
				wg.Wait()
				harness.dumpNodeLogs("live_gc_no_pass")
				t.Fatalf("node %d: no %q within %s of live GC running", n, gcLine, 20*time.Second)
			}
			harness.Progress()
			time.Sleep(convergencePoll)
		}
	}
	harness.Phase("gc-pass-seen-on-every-node")

	ledger.waitForMore(t, 50, "after the GC pass")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("kill node 3: %v", err)
	}
	harness.Phase("node3-killed")
	ledger.waitForMore(t, 50, "while node 3 is down")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatalf("node 3 did not come back: %v", err)
	}
	harness.Phase("node3-alive")
	ledger.waitForMore(t, 50, "after node 3 returned")
	close(stop)
	wg.Wait()
	harness.Phase("load-stopped")

	if len(ledger.dups) > 0 {
		t.Fatalf("duplicate ids: %v", ledger.dups)
	}
	waitLedgerConverged(t, harness, ledger, tables)
	harness.Phase("converged")
}

// missingKeysOn lists every ACKed row of tables that node lacks.
func missingKeysOn(t *testing.T, h *ClusterHarness, l *keyLedger, node int, tables map[string][]string) ([]string, error) {
	got, err := rowIDsOn(t, h, node, tables)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var missing []string
	for k := range l.keys {
		if !got[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	return missing, nil
}

// TestPreparedOnKilledNodeCommittedByPeersConverges: transactions node 3
// durably prepared but never committed, which its
// peers did commit: an explicit transaction's pinned session holds node 3's
// SQLite writer while nodes 1 and 2 commit writes, so every COMMIT to node 3
// waits on that writer; node 3 is then killed (kill -9) and restarted with
// those transactions recovered PENDING. The stale-transaction GC is kept out
// of the test's window (heartbeat_timeout_seconds far above it), so only the
// log pull resolving those records - committing them through the local
// commit path - can deliver the rows. Node 3 must hold every ACKed row the
// moment it reports itself ALIVE (JOINING -> ALIVE only once caught up), and
// every node must converge within convergenceDeadline.
func TestPreparedOnKilledNodeCommittedByPeersConverges(t *testing.T) {
	h := NewClusterHarness(t, func(h *ClusterHarness) { h.HeartbeatTimeoutSeconds = 600 })
	defer h.Cleanup()
	tables := map[string][]string{"marmot": {"pa", "pb"}}
	startWithTables(t, h, tables)
	h.Phase("start")

	l := newKeyLedger(h)
	stop := make(chan struct{})
	wg := runKeyWriters(t, h, l, []int{1, 2}, [][2]string{{"marmot", "pa"}}, stop)
	sleepWithProgress(h, 2*time.Second)
	h.Phase("load")

	conn, err := openTimedNodeDatabase(t, h, 3, "marmot").Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range []string{"BEGIN", "INSERT INTO pb (v) VALUES ('pinned')"} {
		if _, err := connExecTimed(conn, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	h.Phase("writer-pinned")
	before := l.size()
	sleepWithProgress(h, 4*time.Second)
	if l.size() == before {
		t.Fatal("no write committed while node 3's writer was pinned")
	}
	if err := h.KillNode(3); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	h.Phase("node3-killed")

	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(convergenceDeadline)
	for {
		h.Progress()
		status, err := h.selfStatus(3)
		if err == nil && status == "ALIVE" {
			// Read right after observing ALIVE; a read error only means
			// node 3's MySQL listener is not up yet.
			missing, readErr := missingKeysOn(t, h, l, 3, tables)
			if readErr == nil && len(missing) > 0 {
				t.Fatalf("node 3 promoted ALIVE while missing %d ACKed rows: %v", len(missing), missing)
			}
			if readErr == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			missing, readErr := missingKeysOn(t, h, l, 3, tables)
			h.dumpNodeLogs("cluster_prepared_killed")
			t.Fatalf("node 3 not ALIVE within %s (status=%q err=%v, read err=%v); missing %d ACKed rows: %v",
				convergenceDeadline, status, err, readErr, len(missing), missing)
		}
		time.Sleep(convergencePoll)
	}
	h.Phase("node3-alive")

	waitKeysConverged(t, h, l, tables, convergenceDeadline)
	h.Phase("converged")
}
