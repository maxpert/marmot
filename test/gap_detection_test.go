package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestLaggingNodeCatchup: a node killed while the cluster commits a hundred
// transactions catches up through anti-entropy after it restarts, ending with
// exactly the rows every other node has.
func TestLaggingNodeCatchup(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.mustExec(1, "marmot", "CREATE TABLE lagging (id INT PRIMARY KEY, v TEXT)")
	c.phase("start")

	const rowsPerTxn = 5
	var want []string
	next := 1
	commit := func(txns int) {
		for i := 0; i < txns; i++ {
			values := make([]string, rowsPerTxn)
			for r := range values {
				values[r] = fmt.Sprintf("(%d, 'v%d')", next, next)
				want = append(want, fmt.Sprintf("%d|v%d", next, next))
				next++
			}
			c.mustExec(1, "marmot", "INSERT INTO lagging (id, v) VALUES "+strings.Join(values, ", "))
		}
		c.progress()
	}
	const q = "SELECT id, v FROM lagging ORDER BY id"
	commit(10)
	c.waitRows("marmot", q, want, 1, 2, 3)
	c.phase("initial-load")

	c.kill(3)
	commit(100)
	c.waitRows("marmot", q, want, 1, 2)
	c.phase("lagging-load")

	c.restart(3)
	c.waitRows("marmot", q, want, 1, 2, 3)
	c.phase("caught-up")
}
