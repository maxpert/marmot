package test

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// convergenceDeadline bounds how long after the write load stops every node
// must hold every ACKed row: the harness's 5 s anti-entropy interval, a
// second round for a pair held back by a transaction still settling, and
// margin for the per-pair pulls themselves.
const convergenceDeadline = 30 * time.Second

const (
	// loadWindow is how long the ClaimsUnderDDL load runs.
	loadWindow = 20 * time.Second
	// rowLedgerStep is how many further ACKed rows each phase of the
	// kill-and-restart test waits for, within phaseDeadline.
	rowLedgerStep = 200
	phaseDeadline = 20 * time.Second
	// restartDeadline bounds the restarted node's return.
	restartDeadline = 30 * time.Second
	// convergencePoll is the polling interval of every wait here.
	convergencePoll = 250 * time.Millisecond
)

// rowLedger records every ACKed row as table/id -> value, and reports each
// one to the harness's stall watchdog as progress.
type rowLedger struct {
	harness *ClusterHarness
	mu      sync.Mutex
	rows    map[string]string
	dups    []string
}

func newRowLedger(harness *ClusterHarness) *rowLedger {
	return &rowLedger{harness: harness, rows: map[string]string{}}
}

// size returns the number of ACKed rows recorded.
func (l *rowLedger) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.rows)
}

// waitForMore waits until n more rows than now are ACKed, or fails.
func (l *rowLedger) waitForMore(t *testing.T, n int, phase string) {
	t.Helper()
	want := l.size() + n
	deadline := time.Now().Add(phaseDeadline)
	for l.size() < want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: only %d of %d ACKed rows within %s", phase, l.size(), want, phaseDeadline)
		}
		time.Sleep(convergencePoll)
	}
}

func (l *rowLedger) record(table string, id int64, v string) {
	l.harness.Progress()
	l.mu.Lock()
	defer l.mu.Unlock()
	k := fmt.Sprintf("%s/%d", table, id)
	if prev, ok := l.rows[k]; ok {
		l.dups = append(l.dups, fmt.Sprintf("%s: %s and %s", k, prev, v))
	}
	l.rows[k] = v
}

// missingOn returns the ledger rows node does not hold with their ACKed value.
func (l *rowLedger) missingOn(t *testing.T, harness *ClusterHarness, node int, tables []string) []string {
	t.Helper()
	conn := openTimedNodeDatabase(t, harness, node, "marmot")
	got := map[string]string{}
	for _, table := range tables {
		a, err := queryAnswer(conn, "SELECT id, v FROM "+table)
		if err != nil {
			return []string{fmt.Sprintf("node %d %s unreadable: %v", node, table, err)}
		}
		for _, r := range a.rows {
			got[table+"/"+r[0]] = r[1]
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var missing []string
	for k, v := range l.rows {
		if got[k] != v {
			missing = append(missing, fmt.Sprintf("node %d %s want %q got %q", node, k, v, got[k]))
		}
	}
	sort.Strings(missing)
	return missing
}

// waitLedgerConverged polls until every node holds every ACKed row or
// convergenceDeadline passes, and fails with the rows still missing.
func waitLedgerConverged(t *testing.T, harness *ClusterHarness, ledger *rowLedger, tables []string) {
	t.Helper()
	start := time.Now()
	for {
		var missing []string
		for node := 1; node <= numNodes; node++ {
			missing = append(missing, ledger.missingOn(t, harness, node, tables)...)
		}
		if len(missing) == 0 {
			t.Logf("converged: %d ACKed rows on every node %s after load stopped", len(ledger.rows), time.Since(start).Round(time.Millisecond))
			return
		}
		if time.Since(start) > convergenceDeadline {
			if len(missing) > 20 {
				missing = append(missing[:20], fmt.Sprintf("... %d more", len(missing)-20))
			}
			t.Fatalf("ACKed rows still missing %s after load stopped:\n%s", convergenceDeadline, strings.Join(missing, "\n"))
		}
		time.Sleep(convergencePoll)
	}
}

// runLedgerWriters inserts into tables round-robin from every node in nodes until
// stop closes, recording each ACKed row. A node may be down part of the time;
// its failed statements are simply not ACKed.
func runLedgerWriters(t *testing.T, harness *ClusterHarness, ledger *rowLedger, nodes []int, tables []string, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			conn := openTimedNodeDatabase(t, harness, n, "marmot")
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				table := tables[i%len(tables)]
				v := fmt.Sprintf("n%d#%d", n, i)
				res, err := execTimed(conn, "INSERT INTO "+table+" (v) VALUES (?)", v)
				if err != nil {
					time.Sleep(convergencePoll)
					continue
				}
				if id, err := res.LastInsertId(); err == nil {
					ledger.record(table, id, v)
				}
			}
		}(n)
	}
	return &wg
}

// TestClaimsUnderDDLConvergesOnEveryNode: narrow inserts on all three nodes
// while DDL commits from every node. A node that declines a PREPARE (its
// schema version is briefly behind) misses those transactions; anti-entropy
// must deliver every ACKed row to every node within convergenceDeadline of
// the load stopping. On 7a66596 30-50 rows per table stayed missing on every
// node forever.
func TestClaimsUnderDDLConvergesOnEveryNode(t *testing.T) {
	harness := startNarrowCluster(t, "s1", "CREATE TABLE s1 (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()
	c1 := openTimedNodeDatabase(t, harness, 1, "marmot")
	if _, err := execTimed(c1, "CREATE TABLE s2 (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	waitForTableIn(t, harness, "marmot", "s2", 30*time.Second)
	harness.Phase("start")

	tables := []string{"s1", "s2"}
	ledger := newRowLedger(harness)
	stop := make(chan struct{})
	wg := runLedgerWriters(t, harness, ledger, []int{1, 2, 3}, tables, stop)

	deadline := time.Now().Add(loadWindow)
	for i := 0; time.Now().Before(deadline); i++ {
		creator, alterer := i%3+1, (i+1)%3+1
		cc := openTimedNodeDatabase(t, harness, creator, "marmot")
		if _, err := execTimed(cc, fmt.Sprintf("CREATE TABLE d%d (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)", i)); err != nil {
			t.Logf("create d%d on node %d: %v", i, creator, err)
			continue
		}
		if i > 0 {
			ca := openTimedNodeDatabase(t, harness, alterer, "marmot")
			if _, err := execTimed(ca, fmt.Sprintf("ALTER TABLE d%d ADD COLUMN c%d INT", i-1, i)); err != nil {
				t.Logf("alter d%d on node %d: %v", i-1, alterer, err)
			}
		}
	}
	close(stop)
	wg.Wait()
	harness.Phase("load-stopped")

	if len(ledger.dups) > 0 {
		t.Fatalf("duplicate ids: %v", ledger.dups)
	}
	if len(ledger.rows) == 0 {
		t.Fatal("no row was ACKed")
	}
	waitLedgerConverged(t, harness, ledger, tables)
	harness.Phase("converged")
}

// TestKilledNodeConvergesAfterRestart: a node SIGKILLed under write load
// and restarted converges to every row the cluster ACKed, whether it
// coordinated the row, missed it while down, or held it only in its own log.
func TestKilledNodeConvergesAfterRestart(t *testing.T) {
	harness := startNarrowCluster(t, "kr", "CREATE TABLE kr (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()

	tables := []string{"kr"}
	ledger := newRowLedger(harness)
	stop := make(chan struct{})
	wg := runLedgerWriters(t, harness, ledger, []int{1, 2, 3}, tables, stop)
	harness.Phase("start")

	ledger.waitForMore(t, rowLedgerStep, "before the kill")
	harness.Phase("load")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("kill node 3: %v", err)
	}
	harness.Phase("node3-killed")
	ledger.waitForMore(t, rowLedgerStep, "while node 3 is down")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatalf("node 3 did not come back: %v", err)
	}
	harness.Phase("node3-alive")
	ledger.waitForMore(t, rowLedgerStep, "after node 3 returned")
	close(stop)
	wg.Wait()
	harness.Phase("load-stopped")

	if len(ledger.dups) > 0 {
		t.Fatalf("duplicate ids: %v", ledger.dups)
	}
	waitLedgerConverged(t, harness, ledger, tables)
	harness.Phase("converged")
}
