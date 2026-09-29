package test

import (
	"fmt"
	"testing"
)

// TestGracefulStopThenRestartRejoins: a node stopped with SIGTERM announces
// that it is leaving, and its peers keep that record. Restarted, it must
// rejoin as ALIVE in its peers' view and its own and serve writes, not read
// its own departure as a decommission and shut down again.
func TestGracefulStopThenRestartRejoins(t *testing.T) {
	c := newCluster(t)
	c.start()
	createAutoIncTables(c, 1, []target{{"marmot", "t"}})
	c.stop(3)
	waitFor(t, "node 1 records node 3's departure", readyDeadline, func() (bool, string) {
		statuses, err := c.statuses(1)
		return err == nil && statuses[3] != "" && statuses[3] != "ALIVE", fmt.Sprint(statuses, err)
	})

	c.restart(3)
	l := newRowLedger()
	for _, id := range c.all() {
		v := fmt.Sprintf("after-restart-n%d", id)
		res := c.mustExec(id, "marmot", "INSERT INTO t (v) VALUES (?)", v)
		rowID, _ := res.LastInsertId()
		l.inserted("w", ledgerKey("marmot", "t", rowID), v)
	}
	l.waitConverged(c, []target{{"marmot", "t"}})
}
