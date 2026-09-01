//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"testing"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// withDDLImplicitCommit sets the flag for one test and restores it after.
func withDDLImplicitCommit(t *testing.T, enabled bool) {
	t.Helper()
	prev := cfg.Config.Transaction.DDLImplicitCommit
	cfg.Config.Transaction.DDLImplicitCommit = enabled
	t.Cleanup(func() { cfg.Config.Transaction.DDLImplicitCommit = prev })
}

// TestDDLInTransactionSeesSchemaChange is the LLDAP migration shape: DDL and
// dependent DML in one transaction. On MySQL the DDL commits first, so the
// UPDATE sees the new column.
func TestDDLInTransactionSeesSchemaChange(t *testing.T) {
	withDDLImplicitCommit(t, true)
	s := setupNoopDML(t)

	for _, q := range []string{
		"BEGIN",
		"ALTER TABLE t ADD COLUMN temp_name TEXT",
		"UPDATE t SET temp_name = name",
		"COMMIT",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoErrorf(t, err, "query %q", q)
	}

	var got string
	require.NoError(t, s.conn.QueryRow("SELECT temp_name FROM t WHERE id = 1").Scan(&got))
	require.Equal(t, "a", got, "DML must see the column added earlier in the transaction")
}

// TestDDLImplicitCommitPersistsPriorWrites pins that statements buffered before
// the DDL are committed by it, rather than discarded or deferred.
func TestDDLImplicitCommitPersistsPriorWrites(t *testing.T) {
	withDDLImplicitCommit(t, true)
	s := setupNoopDML(t)

	for _, q := range []string{
		"BEGIN",
		"INSERT INTO t (id, name) VALUES (2, 'b')",
		"ALTER TABLE t ADD COLUMN note TEXT",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoErrorf(t, err, "query %q", q)
	}

	// The INSERT is durable before any COMMIT is sent, as on MySQL.
	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE id = 2").Scan(&count))
	require.Equal(t, 1, count, "DDL must commit the statements buffered before it")

	require.False(t, s.session.InTransaction(),
		"the transaction is ended by the DDL, not left open")
}

// TestCommitAfterImplicitCommitIsNoop covers the trailing COMMIT every ORM
// sends after its transaction body; MySQL accepts it silently.
func TestCommitAfterImplicitCommitIsNoop(t *testing.T) {
	withDDLImplicitCommit(t, true)
	s := setupNoopDML(t)

	for _, q := range []string{
		"BEGIN",
		"ALTER TABLE t ADD COLUMN note TEXT",
		"COMMIT",
		"ROLLBACK",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoErrorf(t, err, "query %q", q)
	}
}

// TestDDLInTransactionBufferedWhenDisabled pins the opt-out: DDL stays inside
// the transaction and is applied at COMMIT, so nothing lands before then.
func TestDDLInTransactionBufferedWhenDisabled(t *testing.T) {
	withDDLImplicitCommit(t, false)
	s := setupNoopDML(t)

	for _, q := range []string{
		"BEGIN",
		"INSERT INTO t (id, name) VALUES (2, 'b')",
		"ALTER TABLE t ADD COLUMN note TEXT",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoErrorf(t, err, "query %q", q)
	}

	require.True(t, s.session.InTransaction(),
		"with the flag off, DDL must not end the transaction")

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE id = 2").Scan(&count))
	require.Equal(t, 0, count, "buffered statements must not land before COMMIT")

	_, err := s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)

	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE id = 2").Scan(&count))
	require.Equal(t, 1, count, "COMMIT applies the buffered statements")
}

// TestDDLInTransactionFailsWithoutImplicitCommit documents the cost of opting
// out: DDL stays in the transaction and is applied at COMMIT, so DML in that
// same transaction cannot see the schema change. This is the failure that makes
// MySQL-written migrations (LLDAP's, for one) unable to run.
func TestDDLInTransactionFailsWithoutImplicitCommit(t *testing.T) {
	withDDLImplicitCommit(t, false)
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)
	_, err = s.handler.HandleQuery(s.session, "ALTER TABLE t ADD COLUMN temp_name TEXT", nil)
	require.NoError(t, err, "the DDL itself buffers fine")

	_, err = s.handler.HandleQuery(s.session, "UPDATE t SET temp_name = name", nil)
	if err == nil {
		_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	}
	require.Error(t, err, "DML cannot see a column added earlier in the same transaction")
	require.Contains(t, err.Error(), "temp_name")
}

// TestDMLInTransactionStillBuffers guards the change from widening: only schema
// changes trigger an implicit commit.
func TestDMLInTransactionStillBuffers(t *testing.T) {
	withDDLImplicitCommit(t, true)
	s := setupNoopDML(t)

	for _, q := range []string{
		"BEGIN",
		"INSERT INTO t (id, name) VALUES (2, 'b')",
		"INSERT INTO t (id, name) VALUES (3, 'c')",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoErrorf(t, err, "query %q", q)
	}

	require.True(t, s.session.InTransaction(), "DML must not end the transaction")

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t").Scan(&count))
	require.Equal(t, 1, count, "DML stays buffered until COMMIT")
}

// TestCausesImplicitCommitClassification pins which statements end a
// transaction, matching MySQL's implicit-commit list.
func TestCausesImplicitCommitClassification(t *testing.T) {
	ends := []protocol.StatementCode{
		protocol.StatementDDL,
		protocol.StatementCreateDatabase,
		protocol.StatementDropDatabase,
		protocol.StatementCreateVectorIndex,
		protocol.StatementDropVectorIndex,
		protocol.StatementReindexVectorIndex,
	}
	keeps := []protocol.StatementCode{
		protocol.StatementInsert,
		protocol.StatementUpdate,
		protocol.StatementDelete,
		protocol.StatementReplace,
		protocol.StatementSelect,
	}
	for _, code := range ends {
		require.Truef(t, coordinator.CausesImplicitCommit(protocol.Statement{Type: code}),
			"statement type %d should end a transaction", code)
	}
	for _, code := range keeps {
		require.Falsef(t, coordinator.CausesImplicitCommit(protocol.Statement{Type: code}),
			"statement type %d should not end a transaction", code)
	}
}
