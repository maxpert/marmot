package test

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestLoneRestartedNodeRefusesToBeItsOwnQuorum is F2: a node restarted while its
// peers are unreachable must not commit alone.
//
// Before this step the registry started with self only and was never persisted,
// so a restarted node computed a total membership of 1, was its own majority,
// and committed writes its peers never saw. Two mechanisms now prevent it: the
// registry restores its last known membership from disk, so the quorum
// denominator is 3 rather than 1; and, when no snapshot is readable, the
// coordinator refuses to act as a quorum of one on any node configured with
// seeds.
//
// Mutation: drop restoreMembershipLocked in the registry constructor AND the
// belt in GetClusterState. The lone node accepts the write and the row-count
// assertion below fires.
func TestLoneRestartedNodeRefusesToBeItsOwnQuorum(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	const table = "f2_quorum"
	allNodes := []int{1, 2, 3}

	if _, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, v TEXT)", table)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := harness.WaitForTableExists(table, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("f2_ddl")
		t.Fatalf("DDL did not replicate: %v", err)
	}

	if _, err := harness.ExecNode(1, fmt.Sprintf(
		"INSERT INTO %s (id, v) VALUES (1, 'before')", table)); err != nil {
		t.Fatalf("initial INSERT: %v", err)
	}
	if err := harness.WaitForRowCount(table, allNodes, 1, 15*time.Second); err != nil {
		harness.dumpNodeLogs("f2_initial")
		t.Fatalf("initial row did not replicate: %v", err)
	}

	// Node 3 dies abruptly, then its peers go away. This is the window the
	// defect lived in: node 3 comes back knowing nothing and reaching no one.
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("KillNode(3): %v", err)
	}
	if err := harness.StopNode(1); err != nil {
		t.Fatalf("StopNode(1): %v", err)
	}
	if err := harness.StopNode(2); err != nil {
		t.Fatalf("StopNode(2): %v", err)
	}

	if err := harness.StartNode(3); err != nil {
		t.Fatalf("StartNode(3): %v", err)
	}
	if err := harness.WaitForAlive(3, 30*time.Second); err != nil {
		harness.dumpNodeLogs("f2_restart")
		t.Fatalf("node 3 did not come up alone: %v", err)
	}

	// The invariant. Whichever mechanism fires - a restored denominator of 3
	// that cannot be met, or the seed-node belt - the write must be refused.
	_, err := harness.ExecNode(3, fmt.Sprintf(
		"INSERT INTO %s (id, v) VALUES (2, 'alone')", table))
	if err == nil {
		harness.dumpNodeLogs("f2_lone_accepted")
		t.Fatal("a lone restarted node accepted a write: it acted as its own quorum")
	}
	t.Logf("lone node refused the write, as required: %v", err)

	// And it did not write locally either. An error returned after a local
	// commit would still have diverged from the peers that were down.
	// Mutation: refuse only after applying locally.
	if got := harness.getRowCount(3, table); got != 1 {
		t.Errorf("row count on the lone node = %d, want 1; the refused write must not have been applied", got)
	}

	// Once the cluster is back, the same statement must succeed and replicate:
	// the refusal is transient, not a node that has wedged itself.
	// Mutation: make the refusal permanent (e.g. latch it); this fires.
	if err := harness.StartNode(1); err != nil {
		t.Fatalf("StartNode(1): %v", err)
	}
	if err := harness.StartNode(2); err != nil {
		t.Fatalf("StartNode(2): %v", err)
	}
	if err := harness.WaitForClusterConvergence(60 * time.Second); err != nil {
		harness.dumpNodeLogs("f2_reconverge")
		t.Fatalf("cluster did not reconverge: %v", err)
	}

	var insertErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, insertErr = harness.ExecNode(3, fmt.Sprintf(
			"INSERT INTO %s (id, v) VALUES (2, 'after')", table))
		if insertErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if insertErr != nil {
		harness.dumpNodeLogs("f2_after_reconverge")
		t.Fatalf("INSERT still refused after the cluster reconverged: %v", insertErr)
	}
	if err := harness.WaitForRowCount(table, allNodes, 2, 30*time.Second); err != nil {
		harness.dumpNodeLogs("f2_after_replicate")
		t.Fatalf("the write did not replicate after reconvergence: %v", err)
	}
}

// TestFreshNodeWithUnreachableSeedsRefusesWrites covers the belt on its own: a
// node that has never known any membership, so there is no snapshot to restore,
// and whose seeds are down. Its membership is genuinely one, and the only thing
// distinguishing it from a legitimate single-node deployment is that it was
// configured with seeds.
//
// Mutation: drop the HasSeedNodes() condition and this passes for the wrong
// reason; drop the belt entirely and the write is accepted.
func TestFreshNodeWithUnreachableSeedsRefusesWrites(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start only node 2, whose configured seeds (node 1) are never started.
	if err := harness.StartNode(2); err != nil {
		t.Fatalf("StartNode(2): %v", err)
	}
	if err := harness.WaitForAlive(2, 30*time.Second); err != nil {
		harness.dumpNodeLogs("f2_fresh_boot")
		t.Fatalf("node 2 did not come up: %v", err)
	}

	_, err := harness.ExecNode(2, "CREATE TABLE f2_fresh (id INT PRIMARY KEY, v TEXT)")
	if err == nil {
		// A DDL is a quorum write too; if it is accepted the belt did not fire.
		harness.dumpNodeLogs("f2_fresh_accepted")
		t.Fatal("a fresh node with unreachable seeds accepted a write")
	}

	// The client must be told to retry: the seeds may come up at any moment.
	// Mutation: return a non-retryable code from ErrMembershipNotLearned.
	if !strings.Contains(err.Error(), "1205") && !strings.Contains(strings.ToLower(err.Error()), "membership") {
		t.Errorf("error %q names neither the retryable code 1205 nor the membership condition", err)
	}
	t.Logf("fresh node with unreachable seeds refused the write: %v", err)
}
