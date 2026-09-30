package test

// Cluster tests for how a node that lost its claim state, or joined, comes
// back into the AUTO_INCREMENT claim protocol: the vote hold on every path
// that loses claim state, the release once the node can reach enough
// members, and the operator's sync command for a membership change.

import (
	"fmt"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// syncTimeout bounds one run of the sync command: it asks every member, each
// within the merge's own per-peer timeout.
const syncTimeout = 6 * time.Second

// autoIncSyncNode is one member's entry in the sync command's answer.
type autoIncSyncNode struct {
	NodeID             uint64   `json:"node_id"`
	Answered           bool     `json:"answered"`
	VotesHeld          bool     `json:"votes_held"`
	Members            []uint64 `json:"members"`
	Reached            []uint64 `json:"reached"`
	ReachedEveryMember bool     `json:"reached_every_member"`
}

// autoIncSyncAnswer is the sync command's answer.
type autoIncSyncAnswer struct {
	Complete bool              `json:"complete"`
	Nodes    []autoIncSyncNode `json:"nodes"`
}

// node returns id's entry.
func (a autoIncSyncAnswer) node(id uint64) (autoIncSyncNode, bool) {
	for _, n := range a.Nodes {
		if n.NodeID == id {
			return n, true
		}
	}
	return autoIncSyncNode{}, false
}

// autoIncSync runs the sync command (POST /admin/cluster/autoinc/sync) on
// node id.
func (c *cluster) autoIncSync(id int) autoIncSyncAnswer {
	c.t.Helper()
	var answer autoIncSyncAnswer
	if err := c.admin(id, http.MethodPost, "/cluster/autoinc/sync", syncTimeout, &answer); err != nil {
		c.t.Fatal(err)
	}
	return answer
}

// waitCompleteSync re-runs the sync command on node id until it reports
// complete, as the membership-change procedure does. A sync gathers only
// from the peers a node sees ALIVE, so a report before that may miss only
// restarted, whose promotion has not reached every peer yet; any other
// unanswered, held or unreached member fails the test, and the complete
// report must show every member reached by every node.
func (c *cluster) waitCompleteSync(id int, restarted ...uint64) autoIncSyncAnswer {
	c.t.Helper()
	var answer autoIncSyncAnswer
	waitFor(c.t, fmt.Sprintf("sync on node %d complete", id), replicationDeadline, func() (bool, string) {
		answer = c.autoIncSync(id)
		for _, n := range answer.Nodes {
			if !answer.Complete && slices.Contains(restarted, n.NodeID) {
				continue
			}
			if !n.Answered || n.VotesHeld {
				c.t.Fatalf("node %d did not answer the sync or is held: %+v", n.NodeID, answer)
			}
			for _, member := range n.Members {
				if !slices.Contains(n.Reached, member) && (answer.Complete || !slices.Contains(restarted, member)) {
					c.t.Fatalf("node %d did not reach member %d in a sync that reported complete=%v: %+v", n.NodeID, member, answer.Complete, answer)
				}
			}
		}
		return answer.Complete, fmt.Sprintf("%+v", answer)
	})
	return answer
}

// TestNarrowAutoInc_HeldNodeReleasesAfterItsSeedReturns: a wiped node whose
// only seed is down - so its join fails and its catch-up cannot use the seed
// - is held while it cannot reach enough members: the sync reports it held
// and incomplete, and a narrow insert through it waits out the lock wait and
// returns 1205. It releases once its seed is back, which needs it to connect
// to the returned seed it learned of as ALIVE from another member.
//
// The lock wait is 1s, inside the client's read timeout, so the insert's
// bounded wait ends in 1205 rather than a dropped connection.
//
// Mutations: never hold (both hold sites) - "was not held while its seed was
// down" fires; drop the ALIVE callback on discovery (node_registry.go) - the
// cluster never becomes ready after the seed returns; an unbounded hold wait
// - the insert never answers 1205.
func TestNarrowAutoInc_HeldNodeReleasesAfterItsSeedReturns(t *testing.T) {
	const table = "seedback"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
		func(cfg *clusterConfig) { cfg.lockWaitTimeoutSecs = 1 })
	ledger := newIDLedger(t)
	insertOnEvery(c, ledger, table, "before", 3, 1, 2, 3)
	c.wipe(3)
	c.kill(1)
	c.startNode(3)
	waitFor(t, "node 3 answers MySQL", readyDeadline, func() (bool, string) {
		err := c.ping(3)
		return err == nil, fmt.Sprint(err)
	})
	c.waitTable("marmot", table, 3)
	c.phase("node-3-rebuilt-seed-down")

	answer := c.autoIncSync(3)
	if self, ok := answer.node(3); !ok || !self.VotesHeld || answer.Complete {
		t.Fatalf("node 3 was not held while its seed was down, or the sync reported complete: %+v", answer)
	}
	if _, err := c.exec(3, "marmot", "INSERT INTO "+table+" (v) VALUES ('held')"); mysqlCode(err) != mysqlcode.ErrCodeLockTimeout {
		t.Fatalf("a narrow insert on held node 3 returned %v, want 1205", err)
	}

	c.restart(1)
	c.phase("seed-returned")
	insertOnEvery(c, ledger, table, "after", 3, 1, 2, 3)
	c.waitCompleteSync(2, 1)
	c.waitRows("marmot", idStats(table, "id"), ledger.stats(), 1, 2, 3)
}

// highestID is the largest id l recorded.
func (l *idLedger) highestID() int64 {
	var highest int64
	for id := range l.seen {
		highest = max(highest, id)
	}
	return highest
}

// TestNarrowAutoInc_WipedSeedHoldsUntilMerged: the seed node, which has no
// seeds of its own, loses its data directory - one lost member, inside the
// failure model autoIncMergeSafe assumes. It comes back knowing only itself
// and reports its votes held (over gRPC) while no other member answers it;
// once the members that kept their bases return, it merges and releases, the
// first id it then issues lies above every id committed before the wipe, and
// it catches up every row from their commit logs. The other two stop
// gracefully (SIGTERM): a SIGKILLed node may lose the unsynced tail of its
// commit log, which peers then cannot pull from it, and three failures at
// once are outside the model this test is about.
//
// Mutation: never hold (both hold sites). "not held" fires.
func TestNarrowAutoInc_WipedSeedHoldsUntilMerged(t *testing.T) {
	const table = "wipedseed"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	ledger := newIDLedger(t)
	insertOnEvery(c, ledger, table, "before", 5, 1, 2, 3)
	c.stop(2)
	c.stop(3)
	c.wipe(1)
	c.startNode(1)
	waitFor(t, "wiped node 1 answers", readyDeadline, func() (bool, string) {
		_, err := c.votesHeld(1)
		return err == nil, fmt.Sprint(err)
	})
	if held, err := c.votesHeld(1); err != nil || !held {
		t.Fatalf("wiped seed node 1 is not held while no other member answers it (held %v, err %v)", held, err)
	}
	c.phase("wiped-seed-held")

	c.startNode(2)
	c.startNode(3)
	c.waitReady()
	c.waitTable("marmot", table, 1)
	id, err := insertNarrow(c, 1, "marmot", "INSERT INTO "+table+" (v) VALUES ('after')")
	ledger.record("rebuilt node 1", id, err)
	if highest := ledger.highestID(); id != highest {
		t.Fatalf("rebuilt node 1 issued %d, not above the ids committed before the wipe", id)
	}
	c.waitRows("marmot", idStats(table, "id"), ledger.stats(), 1, 2, 3)
}

// TestNarrowAutoInc_TwoWipedNodesNeverDuplicate: the seed node and a joiner
// whose only seed it is both lose their data directories. Two of three
// members lost claim state, outside the failure model autoIncMergeSafe
// assumes: the two may count each other as the whole membership and release
// before they hear from the member that kept the bases. What is owed there
// is the backstop: an id they reissue is refused with 1062 (the row it would
// collide with is already in the table), never committed twice, and once
// the base sync has raised their bases, inserts through them succeed.
func TestNarrowAutoInc_TwoWipedNodesNeverDuplicate(t *testing.T) {
	const table = "twowiped"
	c := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	ledger := newIDLedger(t)
	insertOnEvery(c, ledger, table, "before", 5, 1, 2, 3)
	c.wipe(1)
	c.wipe(3)
	c.startNode(1)
	c.startNode(3)
	c.waitReady()
	c.waitTable("marmot", table, 1, 3)
	c.phase("rebuilt")

	const wanted = 3
	for _, node := range []int{1, 3} {
		refused, done := 0, 0
		waitFor(t, fmt.Sprintf("%d inserts through rebuilt node %d", wanted, node), replicationDeadline, func() (bool, string) {
			who := fmt.Sprintf("rebuilt node %d #%d", node, done)
			id, err := insertNarrow(c, node, "marmot", "INSERT INTO "+table+" (v) VALUES (?)", who)
			if code := mysqlCode(err); err != nil && code != mysqlcode.ErrCodeDupEntry {
				t.Fatalf("%s: %v, want success or 1062", who, err)
			}
			if err != nil {
				refused++
				return false, fmt.Sprintf("%d refused with 1062", refused)
			}
			ledger.record(who, id, nil)
			done++
			return done == wanted, fmt.Sprintf("%d inserted, %d refused with 1062", done, refused)
		})
		t.Logf("rebuilt node %d: %d inserts refused with 1062 before the base sync", node, refused)
	}
	var want []string
	for _, id := range slices.Sorted(maps.Keys(ledger.seen)) {
		want = append(want, fmt.Sprintf("%d|%s", id, ledger.seen[id]))
	}
	c.waitRows("marmot", "SELECT id, v FROM "+table+" ORDER BY id", want, 1, 2, 3)
}

// TestNarrowAutoInc_SyncAfterAddingAMember is the membership-change
// procedure on a growing cluster: a fourth node joins, becomes ALIVE and
// releases, the operator runs the sync command, and it reports all four
// members answered, unheld and reaching every member. Ids stay unique across
// all four.
//
// Mutation: syncAutoIncBasesEverywhere asks no other member. The sync never
// reports four members reaching every member.
func TestNarrowAutoInc_SyncAfterAddingAMember(t *testing.T) {
	const table = "grow"
	c := newCluster(t, func(cfg *clusterConfig) { cfg.size = 4 })
	c.startNodes(1, 2, 3)
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	ledger := newIDLedger(t)
	insertOnEvery(c, ledger, table, "three", 3, 1, 2, 3)

	c.restart(4)
	c.waitTable("marmot", table, 4)
	answer := c.waitCompleteSync(1, 4)
	if len(answer.Nodes) != 4 {
		t.Fatalf("the complete sync covers %d members, want 4: %+v", len(answer.Nodes), answer)
	}
	for _, n := range answer.Nodes {
		if len(n.Members) != 4 || !n.ReachedEveryMember {
			t.Fatalf("node %d: complete, yet reached %v of %v", n.NodeID, n.Reached, n.Members)
		}
	}
	insertOnEvery(c, ledger, table, "four", 3, 1, 2, 3, 4)
	c.waitRows("marmot", idStats(table, "id"), ledger.stats(), 1, 2, 3, 4)
}

// insertOnEvery inserts k generated rows through each of nodes and records
// their ids in ledger.
func insertOnEvery(c *cluster, ledger *idLedger, table, phase string, k int, nodes ...int) {
	c.t.Helper()
	for _, node := range nodes {
		for i := 0; i < k; i++ {
			who := fmt.Sprintf("%s node %d #%d", phase, node, i)
			id, err := insertNarrow(c, node, "marmot", "INSERT INTO "+table+" (v) VALUES (?)", who)
			ledger.record(who, id, err)
		}
	}
}
