package test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// TestNarrowAutoInc_HeldInsertInTransactionDoesNotStallJoin: on a held node
// alone, an explicit transaction writes a row - pinning the user database's
// SQLite writer - and then inserts into a narrow table while its peers start.
// The release needs their answers, they join from this node's snapshot, and
// the snapshot's checkpoint waits on the pinned writer; so the insert answers
// 1205 at once instead of waiting for a release it blocks. After the client
// rolls back, the peers join, the node releases, and a narrow insert
// succeeds.
//
// The merge interval is a minute, so only peers turning ALIVE trigger the
// release; the lock wait is 20s, far above the fail-fast bound.
//
// Mutation: wait for the release inside a pinned transaction. The insert
// blocks until the claim's deadline and "waited for the release" fires.
func TestNarrowAutoInc_HeldInsertInTransactionDoesNotStallJoin(t *testing.T) {
	c := newCluster(t, func(cfg *clusterConfig) { cfg.autoIncMergeMS = 60000; cfg.lockWaitTimeoutSecs = 20 })
	c.startNode(1)
	c.waitMySQL(1)
	c.mustExec(1, "marmot", "CREATE TABLE plain (k INT PRIMARY KEY, v TEXT)")
	c.mustExec(1, "marmot", "CREATE TABLE narrow (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	if held, err := c.votesHeld(1); err != nil || !held {
		t.Fatalf("node 1 alone must hold its votes: held=%v err=%v", held, err)
	}

	// A read timeout above the lock wait, so a wait shows as its duration
	// rather than a dropped connection.
	pool, err := sql.Open("mysql", strings.ReplaceAll(c.dsn(1, "marmot"), clientTimeout.String(), "30s"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO plain (k, v) VALUES (1, 'pin')"); err != nil {
		t.Fatal(err)
	}
	c.startNode(2)
	c.startNode(3)
	start := time.Now()
	_, err = tx.Exec("INSERT INTO narrow (v) VALUES ('in-txn')")
	elapsed := time.Since(start)
	if mysqlCode(err) != mysqlcode.ErrCodeLockTimeout {
		t.Fatalf("narrow insert in a pinned transaction on a held node returned %v, want 1205", err)
	}
	if elapsed > time.Second {
		t.Fatalf("narrow insert in a pinned transaction waited for the release: 1205 after %s", elapsed)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	c.phase("in-transaction insert failed fast")

	if _, err := pool.Exec("INSERT INTO narrow (v) VALUES ('after')"); err != nil {
		t.Fatalf("autocommit narrow insert after the rollback: %v", err)
	}
	if held, err := c.votesHeld(1); err != nil || held {
		t.Fatalf("node 1 still held after a granted claim: held=%v err=%v", held, err)
	}
	c.phase("released and inserted")
}

// TestNarrowAutoInc_InsertWaitsOutStartupVoteHold: a client that writes to a
// narrow AUTO_INCREMENT table the moment a new cluster's first node answers
// MySQL - LLDAP creating its first group - gets its row, not 1205. Every
// node of a new cluster starts with its claim votes held (db.AutoIncHoldTable)
// and releases them only after merging claim bases from its peers; a narrow
// insert waits for that release, up to the lock wait.
//
// The merge interval is the production default, so the hold outlasts the
// insert's arrival unless a peer turning ALIVE triggers the merge.
//
// Mutation: ClaimRange not waiting on ErrLocalVotesHeld fails the first
// insert with 1205.
func TestNarrowAutoInc_InsertWaitsOutStartupVoteHold(t *testing.T) {
	c := newCluster(t, func(cfg *clusterConfig) { cfg.autoIncMergeMS = 2000 })
	c.startNode(1)
	waitFor(t, "seed node 1 ALIVE", readyDeadline, func() (bool, string) {
		statuses, err := c.statuses(1)
		if err != nil {
			return false, err.Error()
		}
		return statuses[1] == "ALIVE", fmt.Sprint(statuses)
	})
	c.startNode(2)
	c.startNode(3)
	c.waitMySQL(1)

	const table = "startup_groups"
	c.mustExec(1, "marmot", "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	held, err := c.votesHeld(1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("node 1 votes held before the first insert: %v", held)
	ids := map[int64]int{}
	record := func(nodeID int) {
		res := c.mustExec(nodeID, "marmot", "INSERT INTO "+table+" (v) VALUES (?)", fmt.Sprintf("n%d", nodeID))
		got, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if prev, dup := ids[got]; dup {
			t.Fatalf("id %d issued by node %d and node %d", got, prev, nodeID)
		}
		ids[got] = nodeID
	}
	record(1)
	c.phase("first insert")

	c.waitTable("marmot", table, 2, 3)
	record(2)
	record(3)
	c.phase("inserts on every node")
}
