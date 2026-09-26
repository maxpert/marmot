package test

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Log lines a claim COMMIT that could not be applied leaves: the participant's
// refusal (db ReplicationEngine.Commit) and, on the claimant, a local commit
// that failed after the remote quorum committed (coordinator
// commitLocalAfterRemoteQuorum).
const (
	claimCommitRefusedLog = "COMMIT REFUSED: auto-increment claim could not be applied"
	localCommitFailedLog  = "Local commit failed after remote quorum achieved"
)

// TestNarrowAutoInc_ClaimsCommitUnderConstantBaseSync is reviewer rr-s3cf2's
// H1 hunt, amplified without touching the backstop's interval: while every
// node inserts into two SMALLINT tables (a claim every 31 ids), the test runs
// the operator's sync command back to back on every node, so each node pulls
// its peers' bases many times a second. A peer that committed a claim first
// reports exactly its end; a sync landing between this node's PREPARE and
// COMMIT of that claim used to raise the stored base past the claim's newBase
// and refuse the COMMIT, leaving the claim key locked until the
// pending-transaction GC. Every claim COMMIT must now apply, and no id may be
// acknowledged twice.
//
// Mutation: test the merged floor in applyClaimTx's condition (or write merges
// to the committed floor). "claim COMMITs were refused" fires.
func TestNarrowAutoInc_ClaimsCommitUnderConstantBaseSync(t *testing.T) {
	tables := []string{"bs1", "bs2"}
	harness := startNarrowCluster(t, tables[0], "CREATE TABLE "+tables[0]+" (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()
	if _, err := execTimed(openTimedNodeDatabase(t, harness, 1, "marmot"),
		"CREATE TABLE "+tables[1]+" (id SMALLINT AUTO_INCREMENT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE %s: %v", tables[1], err)
	}
	if err := harness.WaitForTableExists(tables[1], []int{1, 2, 3}, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	const load = 45 * time.Second
	stop := time.Now().Add(load)
	var syncs, syncErrs atomic.Int64
	var wg sync.WaitGroup
	for n := 1; n <= 3; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for time.Now().Before(stop) {
				if _, err := runAutoIncSync(harness, n); err != nil {
					syncErrs.Add(1)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				syncs.Add(1)
			}
		}(n)
	}

	var mu sync.Mutex
	ledger := map[string]string{}
	var dups []string
	inserted := make([]int, 4)
	failures := make([]error, 4)
	for n := 1; n <= 3; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			conn := openTimedNodeDatabase(t, harness, n, "marmot")
			for i := 0; time.Now().Before(stop); i++ {
				table := tables[i%len(tables)]
				who := fmt.Sprintf("node %d #%d", n, i)
				id, err := insertNarrowWithin(conn, 30*time.Second, "INSERT INTO "+table+" (v) VALUES (?)", who)
				mu.Lock()
				if err != nil {
					failures[n] = err
					mu.Unlock()
					return
				}
				inserted[n]++
				key := fmt.Sprintf("%s/%d", table, id)
				if prev, dup := ledger[key]; dup {
					dups = append(dups, fmt.Sprintf("%s: %s and %s", key, prev, who))
				}
				ledger[key] = who
				mu.Unlock()
			}
		}(n)
	}
	wg.Wait()

	t.Logf("syncs=%d sync errors=%d inserted per node=%v ids=%d", syncs.Load(), syncErrs.Load(), inserted[1:], len(ledger))
	if len(dups) > 0 {
		t.Fatalf("DUPLICATE ids: %v", dups)
	}
	var refusals []string
	for n := 1; n <= 3; n++ {
		logText, err := os.ReadFile(harness.Nodes[n-1].LogFile)
		if err != nil {
			t.Fatalf("read node %d log: %v", n, err)
		}
		refused := strings.Count(string(logText), claimCommitRefusedLog)
		failed := strings.Count(string(logText), localCommitFailedLog)
		t.Logf("node %d: commit-refused=%d local-commit-failed=%d", n, refused, failed)
		if refused+failed > 0 {
			refusals = append(refusals, fmt.Sprintf("node %d: %d refused, %d local commits failed", n, refused, failed))
		}
	}
	if len(refusals) > 0 {
		harness.dumpNodeLogs("base_sync_refusals")
		t.Fatalf("claim COMMITs were refused under constant base syncs: %v", refusals)
	}
	for n := 1; n <= 3; n++ {
		if failures[n] != nil {
			harness.dumpNodeLogs("base_sync_inserts")
			t.Fatalf("node %d stopped inserting after %d rows: %v", n, inserted[n], failures[n])
		}
	}
	if syncs.Load() < 100 {
		t.Fatalf("only %d syncs ran in %s; the hunt did not amplify the backstop", syncs.Load(), load)
	}
}
