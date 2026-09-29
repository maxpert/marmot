package test

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// ledgerStep is how many further ACKed rows each phase of a load test
	// waits for (rowLedger.waitForMore).
	ledgerStep = 30
	// writeRetryDeadline bounds how long a writer retries one statement
	// through its node before it moves on to the next.
	writeRetryDeadline = time.Second
)

// target is one database.table a writer writes.
type target struct{ database, table string }

// rowLedger records every ACKed row as "database.table/id" -> value. A row
// whose last write failed with an unknown outcome keeps only its presence
// checked (uncertain).
type rowLedger struct {
	mu        sync.Mutex
	rows      map[string]string
	uncertain map[string]bool
	own       map[string][]string
	dups      []string
}

func newRowLedger() *rowLedger {
	return &rowLedger{rows: map[string]string{}, uncertain: map[string]bool{}, own: map[string][]string{}}
}

func ledgerKey(database, table string, id int64) string {
	return fmt.Sprintf("%s.%s/%d", database, table, id)
}

// inserted records a row writer inserted and was ACKed. The same key twice
// (an id generated twice) is recorded as a duplicate.
func (l *rowLedger) inserted(writer, key, v string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prev, dup := l.rows[key]; dup {
		l.dups = append(l.dups, fmt.Sprintf("%s: %s and %s", key, prev, v))
	}
	l.rows[key] = v
	l.own[writer] = append(l.own[writer], key)
}

// updated records the outcome of an UPDATE of key to v: ACKed sets the
// value; a failure leaves the row's value unknown.
func (l *rowLedger) updated(key, v string, acked bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if acked {
		l.rows[key] = v
		delete(l.uncertain, key)
		return
	}
	l.uncertain[key] = true
}

// size is the number of ACKed rows.
func (l *rowLedger) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.rows)
}

// pick returns one of writer's own ACKed keys, spread by i.
func (l *rowLedger) pick(writer string, i int) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := l.own[writer]
	if len(keys) == 0 {
		return "", false
	}
	return keys[(i*7919)%len(keys)], true
}

// waitForMore waits until n more rows than now are ACKed.
func (l *rowLedger) waitForMore(c *cluster, n int, phase string) {
	c.t.Helper()
	want := l.size() + n
	waitFor(c.t, fmt.Sprintf("%s: %d ACKed rows", phase, want), replicationDeadline, func() (bool, string) {
		c.progress()
		return l.size() >= want, fmt.Sprintf("%d ACKed", l.size())
	})
}

// mismatchesOn lists every ACKed row node id does not hold with its ACKed
// value (only its presence, for an uncertain row).
func (l *rowLedger) mismatchesOn(c *cluster, id int, targets []target) []string {
	got := map[string]string{}
	for _, tg := range targets {
		rows, err := c.rows(id, tg.database, "SELECT id, v FROM "+tg.table)
		if err != nil {
			return []string{fmt.Sprintf("node %d %s.%s: %v", id, tg.database, tg.table, err)}
		}
		for _, r := range rows {
			idText, v, _ := strings.Cut(r, "|")
			got[tg.database+"."+tg.table+"/"+idText] = v
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var bad []string
	for key, v := range l.rows {
		gotV, present := got[key]
		if !present || (!l.uncertain[key] && gotV != v) {
			bad = append(bad, fmt.Sprintf("node %d %s: want %q, got %q (present %v)", id, key, v, gotV, present))
		}
	}
	slices.Sort(bad)
	return bad
}

// runWriters starts one writer per node in nodes: it inserts into targets
// round-robin and, with updates, makes every third statement an UPDATE of
// one of its own ACKed rows. A statement that fails (its node down, a quorum
// out of reach) is retried for up to writeRetryDeadline and then given up.
// The returned stop ends every writer and waits for them.
func runWriters(c *cluster, l *rowLedger, nodes []int, targets []target, updates bool) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			writer := fmt.Sprintf("n%d", n)
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				tg, v := targets[i%len(targets)], fmt.Sprintf("n%d#%d", n, i)
				if key, ok := l.pick(writer, i); updates && i%3 == 2 && ok {
					database, rest, _ := strings.Cut(key, ".")
					table, id, _ := strings.Cut(rest, "/")
					_, acked, _ := writeRetrying(c, n, database, done, "UPDATE "+table+" SET v = ? WHERE id = "+id, v)
					l.updated(key, v, acked)
					continue
				}
				if id, acked, _ := writeRetrying(c, n, tg.database, done, "INSERT INTO "+tg.table+" (v) VALUES (?)", v); acked {
					l.inserted(writer, ledgerKey(tg.database, tg.table, id), v)
				}
			}
		}(n)
	}
	return func() {
		close(done)
		wg.Wait()
	}
}

// writeRetrying runs q through node n, retrying a failure with a real
// client's jittered backoff (retryBackoff) until it succeeds,
// writeRetryDeadline passes or done closes. It reports the insert id, whether
// the statement was ACKed, and the last error when it was not.
func writeRetrying(c *cluster, n int, database string, done <-chan struct{}, q string, args ...any) (int64, bool, string) {
	var id int64
	var acked bool
	lastErr, _ := retryBackoff(writeRetryDeadline, func() (bool, string) {
		res, err := c.exec(n, database, q, args...)
		if err == nil {
			acked = true
			id, _ = res.LastInsertId()
			return true, ""
		}
		select {
		case <-done:
			return true, ""
		default:
			return false, err.Error()
		}
	})
	return id, acked, lastErr
}

// TestClaimsUnderDDLConvergesOnEveryNode: narrow inserts on all three nodes
// while DDL commits from every node. A node that declines a PREPARE (its
// schema version briefly behind) misses those transactions; anti-entropy
// must deliver every ACKed row to every node. On 7a66596 30-50 rows per
// table stayed missing on every node forever.
func TestClaimsUnderDDLConvergesOnEveryNode(t *testing.T) {
	c := startNarrowCluster(t, "s1", "CREATE TABLE s1 (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	c.createTable(1, "marmot", "s2", "CREATE TABLE s2 (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	targets := []target{{"marmot", "s1"}, {"marmot", "s2"}}
	l := newRowLedger()
	stop := runWriters(c, l, []int{1, 2, 3}, targets, false)
	const ddlRounds = 12
	committed := 0
	for i := 0; i < ddlRounds; i++ {
		creator, alterer := i%3+1, (i+1)%3+1
		// A DDL may be refused while claims hold its table's schema (1213,
		// retryable); only the rows' convergence is under test.
		if _, err := c.exec(creator, "marmot", fmt.Sprintf("CREATE TABLE d%d (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)", i)); err == nil {
			committed++
		}
		if i > 0 {
			if _, err := c.exec(alterer, "marmot", fmt.Sprintf("ALTER TABLE d%d ADD COLUMN c%d INT", i-1, i)); err == nil {
				committed++
			}
		}
		l.waitForMore(c, 5, fmt.Sprintf("DDL round %d", i))
	}
	stop()
	if committed == 0 {
		t.Fatal("no DDL committed during the load")
	}
	t.Logf("%d DDL committed, %d rows ACKed", committed, l.size())
	l.waitConverged(c, targets)
}

// TestKilledNodeConvergesAfterRestart: a node SIGKILLed under write load and
// restarted converges to every row the cluster ACKed, whether it coordinated
// the row, missed it while down, or held it only in its own log.
func TestKilledNodeConvergesAfterRestart(t *testing.T) {
	c := startNarrowCluster(t, "kr", "CREATE TABLE kr (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	targets := []target{{"marmot", "kr"}}
	l := newRowLedger()
	stop := runWriters(c, l, []int{1, 2, 3}, targets, false)
	l.waitForMore(c, ledgerStep, "before the kill")
	c.kill(3)
	l.waitForMore(c, ledgerStep, "while node 3 is down")
	c.restart(3)
	l.waitForMore(c, ledgerStep, "after node 3 returned")
	stop()
	l.waitConverged(c, targets)
}

// convergenceDeadline bounds how long after the load stops every node must
// hold every ACKed row: a few anti-entropy rounds.
const convergenceDeadline = 5 * time.Second

// waitConverged waits until every node holds every ACKed row with its ACKed
// value and all nodes hold the same rows in every target, and fails on any
// id generated twice.
func (l *rowLedger) waitConverged(c *cluster, targets []target) {
	c.t.Helper()
	if len(l.dups) > 0 {
		c.t.Fatalf("duplicate ids: %v", l.dups)
	}
	if l.size() == 0 {
		c.t.Fatal("no row was ACKed")
	}
	waitFor(c.t, fmt.Sprintf("%d ACKed rows on every node", l.size()), convergenceDeadline, func() (bool, string) {
		var bad []string
		for _, id := range c.all() {
			bad = append(bad, l.mismatchesOn(c, id, targets)...)
		}
		if len(bad) > 0 {
			return false, fmt.Sprintf("%d mismatches, first: %s", len(bad), strings.Join(bad[:min(len(bad), 5)], "; "))
		}
		return true, ""
	})
	for _, tg := range targets {
		c.waitSameRows(tg.database, "SELECT id, v FROM "+tg.table+" ORDER BY id", c.all()...)
	}
}
