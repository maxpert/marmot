package test

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// backstopLoad is how long TestNarrowAutoInc_ClaimsCommitUnderConstantBaseSync
// inserts while the sync command runs back to back.
const backstopLoad = 3 * time.Second

// TestNarrowAutoInc_ClaimsCommitUnderConstantBaseSync amplifies the base sync:
// while every node inserts into two SMALLINT tables (a claim every 31 ids),
// the operator's sync command runs back to back on every node, so each node
// pulls its peers' bases many times a second. A sync landing between a
// claim's PREPARE and COMMIT used to raise the stored base past the claim's
// newBase and refuse the COMMIT, leaving the claim key locked until the
// pending-transaction GC: that node's inserts then stall on 1205 far past
// claimRetryDeadline. Every insert must succeed, no id may be issued twice,
// and every node must end with exactly the rows acknowledged.
//
// The interleaving itself is pinned deterministically by
// db/autoinc_floors_test.go TestCommitAppliesAClaimAMergeRaiseOvertook and
// TestCommitAppliesAClaimAMergeRaisePassed.
func TestNarrowAutoInc_ClaimsCommitUnderConstantBaseSync(t *testing.T) {
	tables := []string{"bs1", "bs2"}
	c := startNarrowCluster(t, tables[0], "CREATE TABLE "+tables[0]+" (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	c.createTable(1, "marmot", tables[1], "CREATE TABLE "+tables[1]+" (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")

	stop := time.Now().Add(backstopLoad)
	var syncs atomic.Int64
	var mu sync.Mutex
	ledgers := map[string]map[int64]string{tables[0]: {}, tables[1]: {}}
	errs := make(chan error, 6)
	var wg sync.WaitGroup
	for n := 1; n <= 3; n++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			for time.Now().Before(stop) {
				var answer autoIncSyncAnswer
				if err := c.admin(n, http.MethodPost, "/cluster/autoinc/sync", syncTimeout, &answer); err != nil {
					errs <- fmt.Errorf("sync on node %d: %w", n, err)
					return
				}
				syncs.Add(1)
			}
		}(n)
		go func(n int) {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				table, who := tables[i%len(tables)], fmt.Sprintf("node %d #%d", n, i)
				id, err := insertNarrow(c, n, "marmot", "INSERT INTO "+table+" (v) VALUES (?)", who)
				if err != nil {
					errs <- fmt.Errorf("%s: %w", who, err)
					return
				}
				mu.Lock()
				prev, dup := ledgers[table][id]
				ledgers[table][id] = who
				mu.Unlock()
				if dup {
					errs <- fmt.Errorf("DUPLICATE id %s/%d: %s and %s", table, id, prev, who)
					return
				}
			}
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	if syncs.Load() < 1000 {
		t.Fatalf("only %d syncs ran in %s; the test did not amplify the backstop", syncs.Load(), backstopLoad)
	}
	for _, table := range tables {
		ledger := &idLedger{t: t, seen: ledgers[table]}
		c.waitRows("marmot", idStats(table, "id"), ledger.stats(), 1, 2, 3)
	}
	t.Logf("%d syncs, %d + %d rows", syncs.Load(), len(ledgers[tables[0]]), len(ledgers[tables[1]]))
}
