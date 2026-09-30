//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package test

import (
	"errors"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// holdWaitTableWidthMax is the ceiling of markedTableDDL's 32-bit signed column.
const holdWaitTableWidthMax = 1<<31 - 1

// setupHoldWaitCluster builds a released 3-node in-process cluster with
// database.table created through node 1 and seeded on every node.
func setupHoldWaitCluster(t *testing.T, database, table string) *inprocCluster {
	t.Helper()
	c := newInprocCluster(t, []uint64{1, 2, 3})
	for _, nodeID := range []uint64{1, 2, 3} {
		require.NoError(t, c.nodes[nodeID].dm.CreateDatabase(database))
	}
	driveDDL(t, c.nodes[1], database, table, sprintfDDL(table))
	for _, nodeID := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[nodeID], database, table, 0)
	}
	return c
}

// holdVotes holds node's claim votes, as a node that initialised its system
// database does until it merges claim bases (db.AutoIncHoldTable).
func holdVotes(t *testing.T, node *inprocNode) {
	t.Helper()
	_, err := node.dm.GetSystemDatabase().GetWriteDB().Exec(
		"INSERT INTO "+db.AutoIncHoldTable+" (id, since) VALUES (1, ?)", time.Now().UnixNano())
	require.NoError(t, err)
}

// setLockWait sets transaction.lock_wait_timeout_seconds for one test.
func setLockWait(t *testing.T, seconds int) {
	prev := cfg.Config.Transaction.LockWaitTimeoutSeconds
	cfg.Config.Transaction.LockWaitTimeoutSeconds = seconds
	t.Cleanup(func() { cfg.Config.Transaction.LockWaitTimeoutSeconds = prev })
}

// allocateResult is one narrow id allocation's outcome.
type allocateResult struct {
	id  uint64
	err error
}

// allocateAsync allocates one id for database.table through allocator.
func allocateAsync(allocator *id.RangeAllocator, database, table string) <-chan allocateResult {
	done := make(chan allocateResult, 1)
	go func() {
		next, err := allocator.Allocate(database, table, holdWaitTableWidthMax, 1)
		done <- allocateResult{id: next, err: err}
	}()
	return done
}

// isLockWaitTimeout reports whether err is MySQL 1205.
func isLockWaitTimeout(err error) bool {
	var mysqlErr *protocol.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Code == protocol.ErrCodeLockTimeout
}

// TestClaimRange_HeldCoordinatorWaitsForRelease: a narrow id allocated on a
// node whose claim votes are held waits for the release, then claims a fresh
// range - the LLDAP startup failure, where the other two nodes had released
// and node 1 answered 1205 at once.
//
// Mutations: ClaimRange not waiting on ErrLocalVotesHeld returns 1205 within
// milliseconds, before the release; a release that does not wake waiters
// (no Notify in MergeAutoIncBasesAndReleaseVotes) misses the 3s guard.
func TestClaimRange_HeldCoordinatorWaitsForRelease(t *testing.T) {
	setLockWait(t, 5)
	const database, table = "holdwait", "items"
	c := setupHoldWaitCluster(t, database, table)

	first, err := id.NewRangeAllocator(c.nodes[2].wc, inprocCoordinatorTimeout).Allocate(database, table, holdWaitTableWidthMax, 1)
	require.NoError(t, err)

	holdVotes(t, c.nodes[1])
	const releaseAfter = 300 * time.Millisecond
	start := time.Now()
	done := allocateAsync(id.NewRangeAllocator(c.nodes[1].wc, inprocCoordinatorTimeout), database, table)

	select {
	case res := <-done:
		t.Fatalf("allocation on a held node returned before the release after %s: id %d, err %v", time.Since(start), res.id, res.err)
	case <-time.After(releaseAfter):
	}
	require.NoError(t, c.nodes[1].dm.MergeAutoIncBasesAndReleaseVotes(nil))

	select {
	case res := <-done:
		require.NoError(t, res.err, "a held node's allocation must wait out the hold, not return 1205")
		require.NotZero(t, res.id)
		require.NotEqual(t, first, res.id, "the id must come from a fresh range")
		require.Greater(t, res.id, first, "node 1's range must lie above node 2's")
	case <-time.After(3 * time.Second):
		t.Fatal("allocation did not complete within 3s of the release")
	}
}

// setWriteTimeout sets replication.write_timeout_ms for one test.
func setWriteTimeout(t *testing.T, ms int) {
	prev := cfg.Config.Replication.WriteTimeoutMS
	cfg.Config.Replication.WriteTimeoutMS = ms
	t.Cleanup(func() { cfg.Config.Replication.WriteTimeoutMS = prev })
}

// setupHandlerCluster builds a released 3-node in-process cluster and a
// MySQL-facing handler over node 1, as marmot.go wires one, with a narrow
// table and a plain table created through it in database.
func setupHandlerCluster(t *testing.T, database, table string) (*inprocCluster, *coordinator.CoordinatorHandler) {
	t.Helper()
	c := newInprocCluster(t, []uint64{1, 2, 3})
	for _, nodeID := range []uint64{1, 2, 3} {
		require.NoError(t, c.nodes[nodeID].dm.CreateDatabase(database))
	}
	node := c.nodes[1]
	reader := coordinator.NewReadCoordinator(node.id, c.provider, db.NewLocalReader(node.dm), inprocCoordinatorTimeout)
	handler := coordinator.NewCoordinatorHandler(node.id, node.wc, reader, node.clock, node.dm, nil, nil, nil)
	session := newSession(100, database)
	for _, ddl := range []string{
		"CREATE TABLE " + table + " (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
		"CREATE TABLE plain (k INT PRIMARY KEY, v TEXT)",
	} {
		_, err := handler.HandleQuery(session, ddl, nil)
		require.NoError(t, err)
	}
	for _, nodeID := range []uint64{1, 2, 3} {
		waitForBase(t, c.nodes[nodeID], database, table, 0)
	}
	return c, handler
}

// newSession is a client connection to database.
func newSession(connID uint64, database string) *protocol.ConnectionSession {
	return &protocol.ConnectionSession{ConnID: connID, CurrentDatabase: database, TranspilationEnabled: true}
}

// timedQuery runs q through handler and reports how long it took.
func timedQuery(handler *coordinator.CoordinatorHandler, session *protocol.ConnectionSession, q string) (time.Duration, error) {
	start := time.Now()
	_, err := handler.HandleQuery(session, q, nil)
	return time.Since(start), err
}

// TestNarrowInsert_HeldWaitIsBoundedByTheLockWait: a narrow insert on a node
// whose votes are never released waits the lock wait - the MySQL-facing
// promise - even when the write timeout is shorter, then answers 1205.
//
// Mutation: bound the claim by the write timeout alone (the allocator's
// budget without the lock wait). The insert answers 1205 after 0.5s.
func TestNarrowInsert_HeldWaitIsBoundedByTheLockWait(t *testing.T) {
	setLockWait(t, 2)
	setWriteTimeout(t, 500)
	const database, table = "holdlockwait", "items"
	c, handler := setupHandlerCluster(t, database, table)

	holdVotes(t, c.nodes[1])
	elapsed, err := timedQuery(handler, newSession(1, database), "INSERT INTO "+table+" (v) VALUES ('held')")
	require.True(t, isLockWaitTimeout(err), "want 1205, got %v", err)
	require.GreaterOrEqual(t, elapsed, 1800*time.Millisecond, "the hold wait ended before the lock wait")
	require.Less(t, elapsed, 4*time.Second, "the hold wait outlasted the lock wait")
}

// TestNarrowInsert_HeldInsidePinnedTransactionFailsFast: inside an explicit
// transaction that already wrote a row - so its connection holds the user
// database's SQLite writer - a narrow insert on a held node answers 1205 at
// once instead of waiting. The release it would wait for needs the joining
// peers' snapshot, whose checkpoint waits on that very writer.
//
// Mutation: wait regardless of a pinned session. The insert waits out the
// 5s lock wait.
func TestNarrowInsert_HeldInsidePinnedTransactionFailsFast(t *testing.T) {
	setLockWait(t, 5)
	const database, table = "holdpinned", "items"
	c, handler := setupHandlerCluster(t, database, table)
	session := newSession(1, database)

	holdVotes(t, c.nodes[1])
	_, err := handler.HandleQuery(session, "BEGIN", nil)
	require.NoError(t, err)
	_, err = handler.HandleQuery(session, "INSERT INTO plain (k, v) VALUES (1, 'pin')", nil)
	require.NoError(t, err)
	elapsed, err := timedQuery(handler, session, "INSERT INTO "+table+" (v) VALUES ('in-txn')")
	require.True(t, isLockWaitTimeout(err), "want 1205, got %v", err)
	require.Less(t, elapsed, time.Second, "a claim inside a pinned transaction waited for the release")
	_, err = handler.HandleQuery(session, "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestNarrowInsert_FirstWriteOfTransactionWaitsForRelease: LLDAP creates each
// startup group in its own transaction whose first write is the narrow
// INSERT. No writer is pinned yet when it claims, so it waits for the release
// and succeeds - the LLDAP startup failure stays fixed.
//
// Mutation: fail fast in every explicit transaction. The INSERT answers 1205
// before the release.
func TestNarrowInsert_FirstWriteOfTransactionWaitsForRelease(t *testing.T) {
	setLockWait(t, 5)
	const database, table = "holdfirstwrite", "items"
	c, handler := setupHandlerCluster(t, database, table)
	session := newSession(1, database)

	holdVotes(t, c.nodes[1])
	_, err := handler.HandleQuery(session, "BEGIN", nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := handler.HandleQuery(session, "INSERT INTO "+table+" (v) VALUES ('lldap_admin')", nil)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the first write of a transaction returned before the release: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, c.nodes[1].dm.MergeAutoIncBasesAndReleaseVotes(nil))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("the insert did not complete within 3s of the release")
	}
	_, err = handler.HandleQuery(session, "COMMIT", nil)
	require.NoError(t, err)
}

// TestClaimRange_HeldCoordinatorWaitIsBounded: a node whose votes are never
// released answers a narrow allocation with 1205 once the lock wait expires,
// not at once and not never.
//
// Mutation: an unbounded wait (no lock-wait deadline) never returns within
// the 3s guard; no wait at all returns before the lock wait.
func TestClaimRange_HeldCoordinatorWaitIsBounded(t *testing.T) {
	setLockWait(t, 1)
	const database, table = "holdbound", "items"
	c := setupHoldWaitCluster(t, database, table)

	holdVotes(t, c.nodes[1])
	start := time.Now()
	done := allocateAsync(id.NewRangeAllocator(c.nodes[1].wc, inprocCoordinatorTimeout), database, table)

	select {
	case res := <-done:
		elapsed := time.Since(start)
		require.True(t, isLockWaitTimeout(res.err), "want 1205, got id %d, err %v", res.id, res.err)
		require.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "1205 came before the lock wait expired")
	case <-time.After(3 * time.Second):
		t.Fatal("a never-released hold was waited on past the lock wait")
	}
}
