package test

// Cluster test for row-value convergence: nodes 1 and 2 keep overwriting the
// same rows while node 3 is down and for a moment after it returns, then
// node 3 writes each row once more. Every node must end with exactly node
// 3's last value for every row. The order in which a node meets older and
// newer images of a row is pinned deterministically by grpc's log puller
// tests; this checks the end-to-end outcome.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// hotRows is how many rows the hot writers keep overwriting.
const hotRows = 10

// hotWriters counts the ACKed updates of runHotWriters and keeps the last
// failure, for a wait that times out to print.
type hotWriters struct {
	acked   atomic.Int64
	lastErr atomic.Value
}

// runHotWriters has each node in nodes overwrite rows 1..hotRows in turn
// until the returned stop is called, counting ACKed updates in w.
func runHotWriters(c *cluster, database, table string, nodes []int, w *hotWriters) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				q := "UPDATE " + table + " SET v = ? WHERE id = ?"
				if _, ok, lastErr := writeRetrying(c, n, database, done, q, fmt.Sprintf("n%d#%d", n, i), i%hotRows+1); ok {
					w.acked.Add(1)
				} else if lastErr != "" {
					w.lastErr.Store(fmt.Sprintf("node %d: %s", n, lastErr))
				}
			}
		}(n)
	}
	return func() {
		close(done)
		wg.Wait()
	}
}

// waitAcked waits until w's ACKed updates have grown by n.
func waitAcked(c *cluster, w *hotWriters, n int64, phase string) {
	c.t.Helper()
	want := w.acked.Load() + n
	waitFor(c.t, fmt.Sprintf("%s: %d ACKed updates", phase, want), replicationDeadline, func() (bool, string) {
		c.progress()
		return w.acked.Load() >= want, fmt.Sprintf("%d ACKed, last error: %v", w.acked.Load(), w.lastErr.Load())
	})
}

// TestSameRowsOverwrittenAcrossOutageConvergeByValue: rows overwritten on
// nodes 1 and 2 while node 3 is down, and just after it returns, then
// written once more by node 3 before it may have pulled what it missed, end
// with node 3's last value on every node.
func TestSameRowsOverwrittenAcrossOutageConvergeByValue(t *testing.T) {
	const database, table = "hotrows", "t"
	// Two writers overwriting the same rows block on each other's intents; a
	// short lock wait turns each such conflict into a quick retryable 1205.
	c := newCluster(t, func(cfg *clusterConfig) { cfg.lockWaitTimeoutSecs = 1 })
	c.start()
	c.createDatabase(1, database)
	c.createTable(1, database, table, "CREATE TABLE "+table+" (id INT PRIMARY KEY, v TEXT)")
	values, _ := idValueRows(1, hotRows, "seed")
	c.mustExec(1, database, "INSERT INTO "+table+" (id, v) VALUES "+values)
	c.waitSameRows(database, "SELECT id, v FROM "+table+" ORDER BY id", 1, 2, 3)

	c.phase("seeded")
	c.kill(3)
	var w hotWriters
	stop := runHotWriters(c, database, table, []int{1, 2}, &w)
	waitAcked(c, &w, 3*ledgerStep, "while node 3 is down")
	c.phase("outage-load")
	c.startNode(3)
	c.waitMySQL(3)
	c.phase("restart")
	waitAcked(c, &w, ledgerStep, "after node 3 returned")
	stop()
	c.phase("after-load")

	var want []string
	for id := 1; id <= hotRows; id++ {
		c.mustExec(3, database, "UPDATE "+table+" SET v = ? WHERE id = ?", fmt.Sprintf("final#%d", id), id)
		want = append(want, fmt.Sprintf("%d|final#%d", id, id))
	}
	c.phase("final-writes")
	c.waitRows(database, "SELECT id, v FROM "+table+" ORDER BY id", want, 1, 2, 3)
	c.phase("converged")
}
