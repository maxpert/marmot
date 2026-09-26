package test

// Cluster tests for how a node that lost its claim state, or joined, comes
// back into the AUTO_INCREMENT claim protocol: the vote hold on every path
// that loses claim state, the release once the node can reach enough members,
// and the operator's sync command for a membership change. Every client call
// here carries its own timeout, so a hang fails the test instead of blocking
// it.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// clusterQueryTimeout bounds every single client query in these tests.
const clusterQueryTimeout = 20 * time.Second

// openTimedNodeDatabase connects to database on nodeID with I/O timeouts on
// the connection itself, on top of each query's own context.
func openTimedNodeDatabase(t *testing.T, harness *ClusterHarness, nodeID int, database string) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("root:@tcp(localhost:%d)/%s?timeout=5s&readTimeout=%s&writeTimeout=%s",
		harness.Nodes[nodeID-1].MySQLPort, database, clusterQueryTimeout, clusterQueryTimeout)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s on node %d: %v", database, nodeID, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// execTimed runs one statement under clusterQueryTimeout.
func execTimed(conn *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterQueryTimeout)
	defer cancel()
	return conn.ExecContext(ctx, query, args...)
}

// isLockTimeout reports whether err is the retryable 1205 a held or
// quorum-less claim returns.
func isLockTimeout(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlcode.ErrCodeLockTimeout
}

// insertNarrowWithin inserts one row with a generated id, retrying 1205 until
// budget is spent.
func insertNarrowWithin(conn *sql.DB, budget time.Duration, query string, args ...interface{}) (int64, error) {
	deadline := time.Now().Add(budget)
	for {
		res, err := execTimed(conn, query, args...)
		if err == nil {
			return res.LastInsertId()
		}
		if !isLockTimeout(err) || time.Now().After(deadline) {
			return 0, err
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// autoIncSyncNode is one member's entry in the sync command's answer.
type autoIncSyncNode struct {
	NodeID             uint64   `json:"node_id"`
	Answered           bool     `json:"answered"`
	Complete           bool     `json:"complete"`
	VotesHeld          bool     `json:"votes_held"`
	Members            []uint64 `json:"members"`
	Reached            []uint64 `json:"reached"`
	ReachedEveryMember bool     `json:"reached_every_member"`
	Error              string   `json:"error"`
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

// runAutoIncSync runs POST /admin/cluster/autoinc/sync on nodeID with the
// harness's cluster secret.
func runAutoIncSync(harness *ClusterHarness, nodeID int) (autoIncSyncAnswer, error) {
	url := fmt.Sprintf("http://localhost:%d/admin/cluster/autoinc/sync", harness.Nodes[nodeID-1].GRPCPort)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return autoIncSyncAnswer{}, err
	}
	req.Header.Set("X-Marmot-Secret", "test-secret")
	resp, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
	if err != nil {
		return autoIncSyncAnswer{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return autoIncSyncAnswer{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return autoIncSyncAnswer{}, fmt.Errorf("sync on node %d: %d %s", nodeID, resp.StatusCode, body)
	}
	var wrapped struct {
		Data autoIncSyncAnswer `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return autoIncSyncAnswer{}, fmt.Errorf("sync on node %d: %w: %s", nodeID, err, body)
	}
	return wrapped.Data, nil
}

// waitForRelease waits until nodeID's current log says its held votes were
// released. The log starts afresh with each start, so the line proves this
// run of the node was held.
func waitForRelease(t *testing.T, harness *ClusterHarness, nodeID int, timeout time.Duration, why string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !nodeLogContains(t, harness, nodeID, autoIncReleasedLog) {
		if time.Now().After(deadline) {
			harness.dumpNodeLogs(fmt.Sprintf("release_node%d", nodeID))
			t.Fatalf("node %d: %s", nodeID, why)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// idLedger records every acknowledged generated id and fails on a repeat.
type idLedger map[int64]string

func (l idLedger) record(t *testing.T, id int64, who string) {
	t.Helper()
	if id <= 0 || id > narrowInt32Max {
		t.Fatalf("%s: id %d is not a 32-bit id", who, id)
	}
	if prev, dup := l[id]; dup {
		t.Fatalf("DUPLICATE id %d: acknowledged to %s and to %s", id, prev, who)
	}
	l[id] = who
}

// quorumProbeTable is a wide table every test here creates, so
// waitForQuorumWrites can tell when each node can commit writes again.
const quorumProbeTable = "quorum_probe"

// createQuorumProbe creates quorumProbeTable on every node.
func createQuorumProbe(t *testing.T, harness *ClusterHarness) {
	t.Helper()
	if _, err := execTimed(openTimedNodeDatabase(t, harness, 1, "marmot"),
		"CREATE TABLE "+quorumProbeTable+" (k INT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE %s: %v", quorumProbeTable, err)
	}
	if err := harness.WaitForTableExists(quorumProbeTable, []int{1, 2, 3}, 30*time.Second); err != nil {
		t.Fatal(err)
	}
}

// waitForQuorumWrites waits until every node in nodes commits an ordinary
// replicated write. A node's connections to peers that restarted come back
// on gRPC's reconnect backoff, and until they do its writes cannot reach a
// quorum; that is not what these tests are about.
func waitForQuorumWrites(t *testing.T, harness *ClusterHarness, timeout time.Duration, nodes ...int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for _, nodeID := range nodes {
		conn := openTimedNodeDatabase(t, harness, nodeID, "marmot")
		for {
			_, err := execTimed(conn, "REPLACE INTO "+quorumProbeTable+" (k, v) VALUES (?, 'probe')", nodeID)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				harness.dumpNodeLogs("quorum_writes")
				t.Fatalf("node %d cannot commit a replicated write: %v", nodeID, err)
			}
			time.Sleep(time.Second)
		}
	}
}

// insertOnEvery inserts k rows on each node and records their ids.
func insertOnEvery(t *testing.T, harness *ClusterHarness, ledger idLedger, table, phase string, k int, nodes ...int) {
	t.Helper()
	for _, nodeID := range nodes {
		conn := openTimedNodeDatabase(t, harness, nodeID, "marmot")
		for i := 0; i < k; i++ {
			who := fmt.Sprintf("%s node %d #%d", phase, nodeID, i)
			id, err := insertNarrowWithin(conn, 60*time.Second, "INSERT INTO "+table+" (v) VALUES (?)", who)
			if err != nil {
				harness.dumpNodeLogs(phase)
				t.Fatalf("%s: %v", who, err)
			}
			ledger.record(t, id, who)
		}
	}
}

// TestNarrowAutoInc_HeldNodeReleasesAfterItsSeedReturns is reviewer B's C2
// (b) and (d) - a wiped node whose only seed is down, so its join fails and
// its catch-up strategy cannot use the seed - and reviewer A's F-L1: the node
// is held while it cannot reach enough members, and releases once its seed
// is back. Before the R3c-14 fix it never connected to the returned seed,
// which it had learned of as ALIVE from another member, and stayed held.
//
// Mutations: never hold (both hold sites) - "was not held while its seed was
// down" fires; drop the ALIVE callback on discovery (node_registry.go) - "did
// not release after its seed returned" fires.
func TestNarrowAutoInc_HeldNodeReleasesAfterItsSeedReturns(t *testing.T) {
	const table = "seedback"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()
	createQuorumProbe(t, harness)
	ledger := idLedger{}
	insertOnEvery(t, harness, ledger, table, "before", 3, 1, 2, 3)

	wipeNode(t, harness, 3)
	if err := harness.StopNode(1); err != nil {
		t.Fatalf("StopNode(1): %v", err)
	}
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("StartNode(3): %v", err)
	}
	if err := harness.WaitForAlive(3, 90*time.Second); err != nil {
		t.Fatalf("node 3 did not come back: %v", err)
	}
	if err := harness.WaitForTableExists(table, []int{3}, 60*time.Second); err != nil {
		harness.dumpNodeLogs("seedback_table")
		t.Fatalf("node 3 did not restore %s: %v", table, err)
	}

	// With node 1 down node 3 hears one unheld member of three: it must stay
	// held, and its narrow inserts wait.
	answer, err := runAutoIncSync(harness, 3)
	if err != nil {
		t.Fatal(err)
	}
	self, ok := answer.node(3)
	if !ok || !self.VotesHeld {
		harness.dumpNodeLogs("seedback_held")
		t.Fatalf("node 3 was not held while its seed was down: %+v", answer)
	}
	if answer.Complete {
		t.Fatalf("the sync reported complete with node 1 down: %+v", answer)
	}
	if _, err := execTimed(openTimedNodeDatabase(t, harness, 3, "marmot"), "INSERT INTO "+table+" (v) VALUES ('held')"); !isLockTimeout(err) {
		t.Fatalf("a narrow insert on held node 3 returned %v, want 1205", err)
	}

	if err := harness.StartNode(1); err != nil {
		t.Fatalf("StartNode(1): %v", err)
	}
	if err := harness.WaitForAlive(1, 90*time.Second); err != nil {
		t.Fatalf("node 1 did not come back: %v", err)
	}
	waitForRelease(t, harness, 3, 90*time.Second, "did not release after its seed returned")

	waitForQuorumWrites(t, harness, 120*time.Second, 1, 2, 3)
	insertOnEvery(t, harness, ledger, table, "after", 3, 1, 2, 3)
	waitForCompleteSync(t, harness, 2, 3)
}

// syncCompletePromotions is how many JOINING-to-ALIVE promotion windows a
// complete sync may take after a restart: a sync gathers only from the peers
// a node sees ALIVE, and a restarted node is ALIVE to its peers once it has
// been promoted and gossip has carried that.
const syncCompletePromotions = 6

// syncRetryInterval spaces the sync command's re-runs.
const syncRetryInterval = time.Second

// syncCompleteDeadline bounds waitForCompleteSync: syncCompletePromotions
// promotion windows (check interval plus minimum healthy time) of the
// configuration the harness's nodes run with.
func syncCompleteDeadline() time.Duration {
	p := cfg.Config.Cluster.Promotion
	return syncCompletePromotions * time.Duration(p.CheckIntervalSeconds+p.MinHealthyDurationSec) * time.Second
}

// waitForCompleteSync runs the sync command on nodeID until it reports
// complete, as the membership-change procedure does: re-run until the report
// is complete. Every report before that may miss only restarted, the member
// whose promotion to ALIVE has not reached every peer yet; any other
// unanswered, held or unreached member fails the test, and a complete report
// must show every member reached by every node.
func waitForCompleteSync(t *testing.T, harness *ClusterHarness, nodeID int, restarted uint64) {
	t.Helper()
	deadline := time.Now().Add(syncCompleteDeadline())
	for {
		answer, err := runAutoIncSync(harness, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range answer.Nodes {
			if !n.Answered || n.VotesHeld {
				t.Fatalf("node %d did not answer the sync or is held: %+v", n.NodeID, answer)
			}
			reached := make(map[uint64]bool, len(n.Reached))
			for _, id := range n.Reached {
				reached[id] = true
			}
			for _, member := range n.Members {
				if reached[member] {
					continue
				}
				if answer.Complete || member != restarted {
					t.Fatalf("node %d did not reach member %d in a sync that reported complete=%v: %+v",
						n.NodeID, member, answer.Complete, answer)
				}
			}
		}
		if answer.Complete {
			return
		}
		if time.Now().After(deadline) {
			harness.dumpNodeLogs("sync_incomplete")
			t.Fatalf("the sync with every member back is not complete after %s: %+v", syncCompleteDeadline(), answer)
		}
		time.Sleep(syncRetryInterval)
	}
}

// TestNarrowAutoInc_WipedSeedAndItsJoinerHold is reviewer B's C2 (a) and
// (c): the seed node, which has no seeds of its own, loses its data
// directory, and so does a node whose only seed it is, which therefore
// catches up from an empty seed. Both restart held and release only by
// merging claim bases from the member that kept them, and the sync command
// then finds every member unheld and reachable.
//
// It stops there: with two of three nodes rebuilt, a new write needs one of
// them, and a rebuilt node refuses writes until anti-entropy restores its
// schema version (the pre-existing catch-up gap, a separate step).
//
// Mutation: never hold (both hold sites). "node 1 was never held" fires.
func TestNarrowAutoInc_WipedSeedAndItsJoinerHold(t *testing.T) {
	const table = "wipedseed"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()
	insertOnEvery(t, harness, idLedger{}, table, "before", 5, 1, 2, 3)

	wipeNode(t, harness, 1)
	wipeNode(t, harness, 3)
	for _, nodeID := range []int{1, 3} {
		if err := harness.StartNode(nodeID); err != nil {
			t.Fatalf("StartNode(%d): %v", nodeID, err)
		}
		if err := harness.WaitForAlive(nodeID, 90*time.Second); err != nil {
			t.Fatalf("node %d did not come back: %v", nodeID, err)
		}
	}
	waitForRelease(t, harness, 1, 90*time.Second, "node 1 was never held, or never released")
	waitForRelease(t, harness, 3, 90*time.Second, "node 3 was never held, or never released")

	deadline := time.Now().Add(60 * time.Second)
	for {
		answer, err := runAutoIncSync(harness, 2)
		if err != nil {
			t.Fatal(err)
		}
		if answer.Complete {
			break
		}
		if time.Now().After(deadline) {
			harness.dumpNodeLogs("wipedseed_sync")
			t.Fatalf("the sync never found every member unheld and reachable: %+v", answer)
		}
		time.Sleep(2 * time.Second)
	}
}

// TestNarrowAutoInc_SyncAfterAddingAMember is the membership-change procedure
// (R3c-8b) on a growing cluster: a fourth node joins, becomes ALIVE and
// releases, the operator runs the sync command, and it reports every member
// answered, unheld and reaching every member. Ids stay unique across all four.
//
// Mutation: syncAutoIncBasesEverywhere asks no other member. "the sync did
// not reach every member" fires.
func TestNarrowAutoInc_SyncAfterAddingAMember(t *testing.T) {
	const table = "grow"
	harness := startNarrowCluster(t, table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()
	ledger := idLedger{}
	insertOnEvery(t, harness, ledger, table, "three", 3, 1, 2, 3)

	harness.Nodes = append(harness.Nodes, harness.createNode(4))
	if err := harness.StartNode(4); err != nil {
		t.Fatalf("StartNode(4): %v", err)
	}
	if err := harness.WaitForAlive(4, 90*time.Second); err != nil {
		t.Fatalf("node 4 did not come up: %v", err)
	}
	waitForRelease(t, harness, 4, 90*time.Second, "the new member never released its votes")
	if err := harness.WaitForTableExists(table, []int{4}, 90*time.Second); err != nil {
		harness.dumpNodeLogs("grow_table")
		t.Fatalf("node 4 did not restore %s: %v", table, err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		answer, err := runAutoIncSync(harness, 1)
		if err != nil {
			t.Fatal(err)
		}
		if answer.Complete && len(answer.Nodes) == 4 {
			for _, n := range answer.Nodes {
				if len(n.Members) != 4 || !n.ReachedEveryMember {
					t.Fatalf("node %d: complete, yet reached %v of %v", n.NodeID, n.Reached, n.Members)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			harness.dumpNodeLogs("grow_sync")
			t.Fatalf("the sync did not reach every member: %+v", answer)
		}
		time.Sleep(2 * time.Second)
	}
	insertOnEvery(t, harness, ledger, table, "four", 3, 1, 2, 3, 4)
}
