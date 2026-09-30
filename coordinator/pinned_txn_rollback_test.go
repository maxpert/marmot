//go:build sqlite_preupdate_hook

package coordinator_test

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// TestRolledBackExplicitTransactionLeavesNothingToCommit: an explicit
// transaction's pinned session outlives the lock wait, so its SQLite
// transaction is rolled back and the next statement is answered 1213 -
// MySQL's "the transaction was rolled back, run it again". The transaction
// must then be over, as in MySQL: a later COMMIT applies and replicates
// nothing of it, and a later BEGIN starts a fresh transaction that commits.
//
// Mutation: answer the ended pinned session with 1213 without ending the
// transaction. The COMMIT applies 'first' and "applied the rolled-back
// statement" fires.
func TestRolledBackExplicitTransactionLeavesNothingToCommit(t *testing.T) {
	old := cfg.Config.Transaction.LockWaitTimeoutSeconds
	cfg.Config.Transaction.LockWaitTimeoutSeconds = 1
	t.Cleanup(func() { cfg.Config.Transaction.LockWaitTimeoutSeconds = old })

	s := setupNoopDML(t)
	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)
	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('first')", nil)
	require.NoError(t, err)
	// Outlive the pinned session's lock wait.
	time.Sleep(1500 * time.Millisecond)
	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('second')", nil)
	require.Error(t, err)
	me := protocol.ConvertToMySQLError(err)
	require.Equal(t, uint16(protocol.ErrCodeDeadlock), me.Code, "statement after the rollback: %s", me.Message)
	require.False(t, s.session.InTransaction(), "a 1213 must end the transaction")

	commitsBefore := s.replicator.commits
	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err, "COMMIT outside a transaction is a no-op in MySQL")
	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name IN ('first', 'second')").Scan(&count))
	require.Zero(t, count, "COMMIT after 1213 applied the rolled-back statement")
	require.Equal(t, commitsBefore, s.replicator.commits, "COMMIT after 1213 replicated the rolled-back statement")

	_, err = s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)
	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('retry')", nil)
	require.NoError(t, err, "a fresh BEGIN must not reuse the ended session")
	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name = 'retry'").Scan(&count))
	require.Equal(t, 1, count, "the retried transaction must commit")
}
