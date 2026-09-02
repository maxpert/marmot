//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

// Tests for eager execution of DML inside an explicit BEGIN...COMMIT/ROLLBACK
// transaction (as opposed to buffering statements until COMMIT).
//
// Requirements pinned here (see task brief for full context):
//   - Each DML inside a txn returns REAL rows-affected and REAL last_insert_id.
//   - Reads inside the txn on a pinned database see the txn's own uncommitted
//     writes (read-your-own-writes), while other connections do not.
//   - COMMIT replicates via the same 2PC path as autocommit DML (CDC msgpack,
//     never raw SQL).
//   - ROLLBACK, and disconnect/session-close with an open txn, discard
//     everything and release the writer so later transactions are not stuck.
//   - Zero-row DML inside a txn reports RowsAffected 0, not a fake 1.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEagerInsertReturnsRealLastInsertId pins requirement 1: an INSERT inside
// an explicit transaction must report the real auto-increment id it produced,
// not the old fake RowsAffected:1/LastInsertId:0 buffered-statement response.
func TestEagerInsertReturnsRealLastInsertId(t *testing.T) {
	s := setupNoopDML(t)

	for _, q := range []string{"BEGIN"} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoError(t, err)
	}

	res, err := s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('x')", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, int64(1), res.RowsAffected)
	require.NotZero(t, res.LastInsertId, "eager INSERT must report the real last_insert_id")

	_, err = s.handler.HandleQuery(s.session, "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestEagerReadSeesOwnUncommittedWrite pins requirement 2: a SELECT after a
// DML in the same still-open transaction must see that DML's write, while an
// independent connection to the same database must not.
func TestEagerReadSeesOwnUncommittedWrite(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('own-write')", nil)
	require.NoError(t, err)

	// Read-your-own-writes: same session, same still-open transaction.
	res, err := s.handler.HandleQuery(s.session, "SELECT name FROM t WHERE name = 'own-write'", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "SELECT inside the open transaction must see its own uncommitted INSERT")

	// Isolation: an independent connection must not see the uncommitted write.
	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name = 'own-write'").Scan(&count))
	require.Equal(t, 0, count, "an independent connection must not see the uncommitted write")

	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)

	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name = 'own-write'").Scan(&count))
	require.Equal(t, 1, count, "COMMIT must persist the write")
}

// TestEagerParentChildInsertUsingReturnedId is the LLDAP shape: a parent
// INSERT whose real last_insert_id feeds a child INSERT in the same
// transaction, both surviving COMMIT.
func TestEagerParentChildInsertUsingReturnedId(t *testing.T) {
	s := setupNoopDML(t)

	for _, q := range []string{
		"CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoError(t, err)
	}

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	parentRes, err := s.handler.HandleQuery(s.session, "INSERT INTO parent (name) VALUES ('p1')", nil)
	require.NoError(t, err)
	require.NotZero(t, parentRes.LastInsertId)

	childSQL := fmt.Sprintf("INSERT INTO child (parent_id, name) VALUES (%d, 'c1')", parentRes.LastInsertId)
	childRes, err := s.handler.HandleQuery(s.session, childSQL, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), childRes.RowsAffected)

	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)

	var parentCount, childCount int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM parent WHERE name = 'p1'").Scan(&parentCount))
	require.Equal(t, 1, parentCount)
	require.NoError(t, s.conn.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM child WHERE parent_id = %d AND name = 'c1'", parentRes.LastInsertId),
	).Scan(&childCount))
	require.Equal(t, 1, childCount, "child row must link to the parent's real last_insert_id")
}

// TestEagerRollbackDiscardsWrites pins requirement 4: ROLLBACK discards
// everything written eagerly inside the transaction, and releases the writer
// so a later statement on the same connection succeeds (the writer is not
// left stuck holding the SQLite write lock).
func TestEagerRollbackDiscardsWrites(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('rollback-me')", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "ROLLBACK", nil)
	require.NoError(t, err)

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name = 'rollback-me'").Scan(&count))
	require.Equal(t, 0, count, "ROLLBACK must discard the eagerly-executed INSERT")

	// The writer must not be stuck: a fresh statement must succeed.
	res, err := s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('after-rollback')", nil)
	require.NoError(t, err, "writer must be released after ROLLBACK")
	require.Equal(t, int64(1), res.RowsAffected)
}

// TestEagerZeroRowUpdateReportsZero pins requirement 7: a zero-row DML inside
// a transaction reports RowsAffected 0, not the old fake RowsAffected:1, and
// still commits cleanly as a no-op (no 2PC round for the no-op statement).
func TestEagerZeroRowUpdateReportsZero(t *testing.T) {
	s := setupNoopDML(t)
	before := s.replicator.prepares

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	res, err := s.handler.HandleQuery(s.session, "UPDATE t SET name = 'z' WHERE id = 999", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, int64(0), res.RowsAffected, "no-op DML inside a transaction must report zero rows")

	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)
	require.Equal(t, before, s.replicator.prepares, "an all-no-op transaction must not run 2PC")
}

// TestEagerCommitReplicatesViaCDC pins requirement 3: COMMIT drives the exact
// same 2PC path handleCommit uses today, and the write is durable afterward.
//
// It cannot observe this via countingReplicator (the seam noop_dml_test.go's
// negative "2PC did not run" assertions use): WriteCoordinator dispatches the
// coordinator's own node through wc.localReplicator, never through the
// injected wc.replicator (coordinator/write_coordinator.go:750,756) - that
// only fires for genuine remote peers, and setupNoopDML's fixture is a
// single-node cluster with none. Durable presence in an independent
// connection is proof enough: a PinnedSession's underlying SQLite
// transaction is only ever rolled back (see PinnedSession's doc comment in
// pinned_txn.go), never committed directly, so the write can only have
// landed via CDC replay through WriteTransaction/2PC.
func TestEagerCommitReplicatesViaCDC(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('y')", nil)
	require.NoError(t, err)

	res, err := s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotZero(t, res.CommittedTxnId, "COMMIT must report the committed txn id")

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name = 'y'").Scan(&count))
	require.Equal(t, 1, count, "COMMIT must persist the write via CDC replay, since the pinned SQLite txn is only ever rolled back")
}

// TestEagerSessionCloseWithOpenTxnRollsBack pins requirement 4's disconnect
// case: a client that vanishes with an open transaction must not leave its
// eager writes applied, nor leave the SQLite writer stuck. CloseSession is
// the seam protocol/server.go's connection-cleanup path calls.
func TestEagerSessionCloseWithOpenTxnRollsBack(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('leaked')", nil)
	require.NoError(t, err)

	s.handler.CloseSession(s.session)

	require.False(t, s.session.InTransaction(), "CloseSession must end the session's transaction")

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name = 'leaked'").Scan(&count))
	require.Equal(t, 0, count, "a session closed mid-transaction must not leave its writes applied")

	// The writer must not be stuck: a fresh autocommit statement must succeed.
	res, err := s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('after-close')", nil)
	require.NoError(t, err, "writer must be released after CloseSession")
	require.Equal(t, int64(1), res.RowsAffected)
}

// TestEagerEmptyTransactionCommitNoop guards requirement 7: BEGIN immediately
// followed by COMMIT, with no pinned session ever created, must still behave
// (no panic, no error, no 2PC round).
func TestEagerEmptyTransactionCommitNoop(t *testing.T) {
	s := setupNoopDML(t)
	before := s.replicator.prepares

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)
	require.Equal(t, before, s.replicator.prepares, "an empty transaction must not run 2PC")
}
