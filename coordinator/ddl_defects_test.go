//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"database/sql"
	"testing"

	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// TestUnparseableDDLReturnsCleanError pins the LLDAP 0.6.3 regression: a DDL
// statement with an unquoted, hyphenated constraint name is not valid MySQL
// syntax (unquoted identifiers cannot contain '-'). Vitess's DDL fallback used
// to swallow the resulting syntax error and hand back a partially-parsed AST
// (e.g. "ALTER TABLE t ADD CONSTRAINT unique-user-email UNIQUE (email)"
// degraded to just "ALTER TABLE t"), which Marmot then forwarded into 2PC,
// where SQLite's PREPARE failed with a confusing "incomplete input" error.
// The statement must instead be rejected immediately with a clean MySQL
// syntax error, and the table must be left completely untouched.
func TestUnparseableDDLReturnsCleanError(t *testing.T) {
	s := setupNoopDML(t)

	badDDL := "alter table t add CONSTRAINT unique-user-email UNIQUE (email)"
	rs, err := s.handler.HandleQuery(s.session, badDDL, nil)
	require.Nil(t, rs, "no result set for a rejected statement")
	require.Error(t, err, "unquoted hyphenated identifier is not valid MySQL syntax")

	mysqlErr, ok := err.(*protocol.MySQLError)
	require.Truef(t, ok, "expected *protocol.MySQLError, got %T: %v", err, err)
	require.Equal(t, protocol.ErrCodeParseError, mysqlErr.Code)

	// The table must be exactly what setupNoopDML created - no truncated DDL
	// ("ALTER TABLE t") must have been silently applied.
	rows, err := s.conn.Query("PRAGMA table_info(t)")
	require.NoError(t, err)
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk))
		cols = append(cols, name)
	}
	require.Equal(t, []string{"id", "name"}, cols, "the malformed DDL must not have altered the table")
}

// TestAddConstraintUniqueUsesGeneratedIndex verifies the properly-quoted
// equivalent of the LLDAP statement (a well-formed MySQL "ADD CONSTRAINT ...
// UNIQUE" with a hyphenated name) transpiles to a real SQLite UNIQUE INDEX
// that actually enforces uniqueness, end to end through HandleQuery.
func TestAddConstraintUniqueUsesGeneratedIndex(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "ALTER TABLE t ADD COLUMN email TEXT", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session,
		"ALTER TABLE t ADD CONSTRAINT `unique-user-email` UNIQUE (email)", nil)
	require.NoError(t, err, "well-formed ADD CONSTRAINT ... UNIQUE must transpile and apply")

	_, err = s.handler.HandleQuery(s.session,
		"UPDATE t SET email = 'a@example.com' WHERE id = 1", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session,
		"INSERT INTO t (id, name, email) VALUES (2, 'dup', 'a@example.com')", nil)
	require.Error(t, err, "the generated unique index must reject a duplicate email")
}

// TestSubqueryHavingSurvivesTranspilation pins the second LLDAP regression: an
// IN-subquery with GROUP BY/HAVING was mangled by transpilation because the
// serializer treated every *sqlparser.Where node as a WHERE clause, including
// ones that were actually HAVING (Vitess represents both with the same Where
// struct, distinguished only by its Type field). That turned "GROUP BY email
// HAVING COUNT(email) > ?" into "GROUP BY email WHERE COUNT(email) > ?",
// which SQLite's PREPARE rejected with "near \"WHERE\": syntax error".
func TestSubqueryHavingSurvivesTranspilation(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "ALTER TABLE t ADD COLUMN email TEXT", nil)
	require.NoError(t, err)

	for i, row := range []struct {
		id    int
		name  string
		email string
	}{
		{2, "b", "dup@example.com"},
		{3, "c", "dup@example.com"},
		{4, "d", "unique@example.com"},
	} {
		_, err := s.handler.HandleQuery(s.session,
			"INSERT INTO t (id, name, email) VALUES (?, ?, ?)",
			[]interface{}{row.id, row.name, row.email})
		require.NoErrorf(t, err, "insert row %d", i)
	}

	sql := "SELECT `id`, `email` FROM `t` WHERE `email` IN " +
		"(SELECT `email` FROM `t` GROUP BY `email` HAVING COUNT(`email`) > ?) " +
		"ORDER BY `id` ASC"
	rs, err := s.handler.HandleQuery(s.session, sql, []interface{}{1})
	require.NoError(t, err, "GROUP BY/HAVING subquery must survive transpilation")
	require.Len(t, rs.Rows, 2, "only the two duplicate-email rows qualify")
	require.Equal(t, "dup@example.com", rs.Rows[0][1])
	require.Equal(t, "dup@example.com", rs.Rows[1][1])
}
