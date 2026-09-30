package test

import (
	"slices"
	"testing"

	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// TestLoneRestartedNodeRefusesToBeItsOwnQuorum: a node restarted while its
// peers are unreachable must not commit alone. Its registry restores its
// last known membership from disk, so the quorum denominator is 3 rather
// than 1 (the write fails with 1105, quorum not achieved); and when no
// membership is readable, the coordinator refuses to act as a quorum of one
// on a node configured with seeds (1205). Either way nothing is written
// locally, and once the peers are back the same write succeeds everywhere.
//
// Mutation: drop restoreMembershipLocked in the registry constructor AND the
// belt in GetClusterState. The lone node accepts the write.
func TestLoneRestartedNodeRefusesToBeItsOwnQuorum(t *testing.T) {
	c := newCluster(t)
	c.start()
	const table = "f2_quorum"
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id INT PRIMARY KEY, v TEXT)")
	c.mustExec(1, "marmot", "INSERT INTO "+table+" (id, v) VALUES (1, 'before')")
	c.waitRows("marmot", "SELECT id, v FROM "+table+" ORDER BY id", []string{"1|before"}, 1, 2, 3)

	// Node 3 dies abruptly, then its peers go away: it comes back knowing
	// nothing and reaching no one.
	c.kill(3)
	c.kill(1)
	c.kill(2)
	c.startNode(3)
	c.waitMySQL(3)
	_, err := c.exec(3, "marmot", "INSERT INTO "+table+" (id, v) VALUES (2, 'alone')")
	if code := mysqlCode(err); !slices.Contains([]uint16{mysqlcode.ErrCodeUnknown, mysqlcode.ErrCodeLockTimeout}, code) {
		t.Fatalf("a lone restarted node answered the write with %v, want refusal 1105 or 1205", err)
	}
	// Mutation: refuse only after applying locally.
	c.waitRows("marmot", "SELECT id, v FROM "+table+" ORDER BY id", []string{"1|before"}, 3)

	// Mutation: make the refusal permanent (latch it). The write below fails.
	c.startNode(1)
	c.startNode(2)
	c.waitReady()
	c.execAcrossReconnect(3, "marmot", "INSERT INTO "+table+" (id, v) VALUES (2, 'after')")
	c.waitRows("marmot", "SELECT id, v FROM "+table+" ORDER BY id", []string{"1|before", "2|after"}, 1, 2, 3)
}

// TestFreshNodeWithUnreachableSeedsRefusesWrites covers the belt on its own:
// a node that has never known any membership, so there is none to restore,
// and whose seeds are down. Only its configured seeds tell it apart from a
// single-node deployment; it must refuse writes with the retryable 1205, as
// the seeds may come up at any moment.
//
// Mutations: drop the belt - the write is accepted; return a non-retryable
// code from ErrMembershipNotLearned - the code check fires.
func TestFreshNodeWithUnreachableSeedsRefusesWrites(t *testing.T) {
	c := newCluster(t)
	// Only node 2 starts; its seed, node 1, never does.
	c.startNode(2)
	c.waitMySQL(2)
	// A DDL is a quorum write too.
	_, err := c.exec(2, "marmot", "CREATE TABLE f2_fresh (id INT PRIMARY KEY, v TEXT)")
	if code := mysqlCode(err); code != mysqlcode.ErrCodeLockTimeout {
		t.Fatalf("a fresh node with unreachable seeds answered a write with %v, want 1205", err)
	}
}
