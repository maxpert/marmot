package test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// preLogPullCommit is the last commit before nodes pulled each other's commit
// logs: the release a rolling upgrade to the log-pull protocol starts from.
const preLogPullCommit = "7a66596"

// upgradeGossipDeadline bounds each wait for gossip to carry a node's new log
// protocol version to its peers after that node restarts.
const upgradeGossipDeadline = 20 * time.Second

var (
	preLogPullBinOnce sync.Once
	preLogPullBinPath string
	preLogPullBinErr  error
)

// preLogPullBinary builds the preLogPullCommit binary once per test process, from a
// read-only `git archive` of that commit, and reuses a binary a previous run
// left under testDataRoot. Call it before NewClusterHarness: the build is not
// part of the cluster's time budget.
func preLogPullBinary(t *testing.T) string {
	t.Helper()
	preLogPullBinOnce.Do(func() {
		dir := filepath.Join(testDataRoot, "marmot_pref2_"+preLogPullCommit)
		bin := filepath.Join(dir, "marmot")
		if _, err := os.Stat(bin); err == nil {
			preLogPullBinPath = bin
			return
		}
		src := filepath.Join(dir, "src")
		if err := os.MkdirAll(src, 0o755); err != nil {
			preLogPullBinErr = err
			return
		}
		archive := exec.Command("sh", "-c", fmt.Sprintf("git archive %s | tar -x -C %s", preLogPullCommit, src))
		archive.Dir = repoRoot()
		if out, err := archive.CombinedOutput(); err != nil {
			preLogPullBinErr = fmt.Errorf("git archive %s: %v\n%s", preLogPullCommit, err, out)
			return
		}
		build := exec.Command("go", "build", "-tags", "sqlite_preupdate_hook sqlite_fts5 sqlite_json sqlite_math_functions sqlite_foreign_keys sqlite_stat4 sqlite_vacuum_incr", "-o", bin, ".")
		build.Dir = src
		if out, err := build.CombinedOutput(); err != nil {
			preLogPullBinErr = fmt.Errorf("build %s: %v\n%s", preLogPullCommit, err, out)
			return
		}
		preLogPullBinPath = bin
	})
	if preLogPullBinErr != nil {
		t.Fatalf("pre-log-pull binary: %v", preLogPullBinErr)
	}
	return preLogPullBinPath
}

// restartOnBinary stops node, switches it to bin ("" for the harness's own
// binary) and starts it again, waiting until it answers.
func restartOnBinary(t *testing.T, h *ClusterHarness, node int, bin string) {
	t.Helper()
	if err := h.StopNode(node); err != nil {
		t.Fatal(err)
	}
	if bin == "" {
		delete(h.NodeBin, node)
	} else {
		h.NodeBin[node] = bin
	}
	if err := h.StartNode(node); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForAlive(node, restartDeadline); err != nil {
		t.Fatal(err)
	}
}

// ddlRefusedNaming reports whether err is the rolling-upgrade refusal (the
// retryable 1213) naming exactly the members in legacy.
func ddlRefusedNaming(err error, legacy string) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlcode.ErrCodeDeadlock &&
		strings.Contains(mysqlErr.Message, "node(s) "+legacy+" ")
}

// waitDDLRefusedNaming retries ddl on node until it is refused naming exactly
// legacy (gossip carries each restarted node's protocol version to its peers
// asynchronously), failing after upgradeGossipDeadline.
func waitDDLRefusedNaming(t *testing.T, h *ClusterHarness, node int, ddl, legacy string) {
	t.Helper()
	deadline := time.Now().Add(upgradeGossipDeadline)
	for {
		h.Progress()
		_, err := execTimed(openTimedNodeDatabase(t, h, node, "marmot"), ddl)
		if ddlRefusedNaming(err, legacy) {
			return
		}
		if err == nil {
			t.Fatalf("node %d accepted %q while node(s) %s still run the older release", node, ddl, legacy)
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d never refused %q naming %s within %s; last error: %v", node, ddl, legacy, upgradeGossipDeadline, err)
		}
		time.Sleep(convergencePoll)
	}
}

// waitDDLAccepted retries ddl on node until it succeeds, failing after
// upgradeGossipDeadline.
func waitDDLAccepted(t *testing.T, h *ClusterHarness, node int, ddl string) {
	t.Helper()
	deadline := time.Now().Add(upgradeGossipDeadline)
	for {
		h.Progress()
		_, err := execTimed(openTimedNodeDatabase(t, h, node, "marmot"), ddl)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d still refuses %q %s after the upgrade finished: %v", node, ddl, upgradeGossipDeadline, err)
		}
		time.Sleep(convergencePoll)
	}
}

// insertFromEveryNode inserts one row into marmot.t through each node and
// records it in l.
func insertFromEveryNode(t *testing.T, h *ClusterHarness, l *keyLedger, tag string) {
	t.Helper()
	for n := 1; n <= numNodes; n++ {
		res, err := execTimed(openTimedNodeDatabase(t, h, n, "marmot"), "INSERT INTO t (v) VALUES (?)", fmt.Sprintf("%s-n%d", tag, n))
		if err != nil {
			t.Fatalf("%s: INSERT through node %d: %v", tag, n, err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		l.acked(fmt.Sprintf("n%d", n), ledgerKey("marmot", "t", id))
	}
}

// TestRollingUpgradeRefusesDDLUntilEveryNodeServesLogPull: a rolling upgrade
// from the release before the log-pull protocol. A cluster started on
// preLogPullCommit gets nodes 1 and 2 upgraded; while node 3 still runs the old
// binary, DDL is refused cluster-wide with the retryable error naming node 3
// (through an upgraded coordinator) and does not commit through the old
// coordinator either, while DML keeps replicating. Once node 3 is upgraded
// too, every node coordinates both writes and DDL, and the schema versions
// agree.
func TestRollingUpgradeRefusesDDLUntilEveryNodeServesLogPull(t *testing.T) {
	oldBin := preLogPullBinary(t)
	h := NewClusterHarness(t, func(h *ClusterHarness) {
		h.NodeBin = map[int]string{1: oldBin, 2: oldBin, 3: oldBin}
	})
	defer h.Cleanup()
	tables := map[string][]string{"marmot": {"t"}}
	startWithTables(t, h, tables)
	h.Phase("start-old-release")

	restartOnBinary(t, h, 1, "")
	restartOnBinary(t, h, 2, "")
	// The old release started the cluster without the readiness endpoint
	// StartCluster waits on; the upgraded nodes answer it.
	for _, n := range []int{1, 2} {
		if err := h.WaitForVotesReleased(n, upgradeGossipDeadline); err != nil {
			t.Fatal(err)
		}
	}
	h.Phase("nodes-1-2-upgraded")

	waitDDLRefusedNaming(t, h, 1, "CREATE TABLE mixed1 (id INT PRIMARY KEY)", "[3]")
	waitDDLRefusedNaming(t, h, 2, "CREATE TABLE mixed2 (id INT PRIMARY KEY)", "[3]")
	if _, err := execTimed(openTimedNodeDatabase(t, h, 3, "marmot"), "CREATE TABLE mixed3 (id INT PRIMARY KEY)"); err == nil {
		t.Fatal("the old coordinator's DDL committed although its upgraded participants must refuse it")
	}
	l := newKeyLedger(h)
	insertFromEveryNode(t, h, l, "mixed")
	waitKeysConverged(t, h, l, tables, convergenceDeadline)
	h.Phase("mixed-checked")

	restartOnBinary(t, h, 3, "")
	h.Phase("node-3-upgraded")

	for n := 1; n <= numNodes; n++ {
		table := fmt.Sprintf("after%d", n)
		waitDDLAccepted(t, h, n, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
		tables["marmot"] = append(tables["marmot"], table)
		for m := 1; m <= numNodes; m++ {
			waitTableOn(t, h, m, "marmot", table, upgradeGossipDeadline)
		}
	}
	for n := 1; n <= numNodes; n++ {
		for m := 1; m <= numNodes; m++ {
			if h.tableExistsOnNode(m, fmt.Sprintf("mixed%d", n)) {
				t.Fatalf("DDL refused while mixed left table mixed%d on node %d", n, m)
			}
		}
	}
	insertFromEveryNode(t, h, l, "upgraded")
	waitKeysConverged(t, h, l, map[string][]string{"marmot": {"t"}}, convergenceDeadline)
	waitSchemaVersionsEqual(t, h, []string{"marmot"}, upgradeGossipDeadline)
	h.Phase("upgraded-checked")
}
