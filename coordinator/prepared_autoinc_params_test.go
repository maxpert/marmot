//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

// Reproduces the LLDAP boot failure: a prepared INSERT into a table with an
// auto-increment PK, executed with wire-supplied params, failed with
// "not enough args to execute query: want 6 got 5". Root cause:
// transform.ExtractLiterals rewrites the auto-increment id the pipeline
// injects into the AST (a *sqlparser.Literal) into its own `?` placeholder,
// so the final SQL carries 6 placeholders (5 caller + 1 injected) while the
// coordinator's execParams selection picked only ONE of the two param
// sources (wire params XOR stmt.ExtractedParams) instead of merging them per
// stmt.ParamOrder. See protocol.Statement.MergeExecParams.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPreparedAutoIncrementInsertWithWireParams pins the exact LLDAP shape:
// a prepared INSERT naming every non-PK column, with 5 wire params, inside an
// explicit transaction, against a table with an auto-increment PK.
func TestPreparedAutoIncrementInsertWithWireParams(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE groups ("+
		"group_id INTEGER PRIMARY KEY, "+
		"display_name TEXT, "+
		"lowercase_display_name TEXT, "+
		"creation_date TEXT, "+
		"uuid TEXT, "+
		"modified_date TEXT)", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	sql := "INSERT INTO groups (display_name, lowercase_display_name, creation_date, uuid, modified_date) " +
		"VALUES (?, ?, ?, ?, ?)"
	params := []interface{}{"Admins", "admins", "2026-01-01", "uuid-1", "2026-01-01"}

	res, err := s.handler.HandleQuery(s.session, sql, params)
	require.NoError(t, err, "prepared INSERT with wire params must not fail on placeholder/arg count mismatch")
	require.NotNil(t, res)
	require.Equal(t, int64(1), res.RowsAffected)
	require.NotZero(t, res.LastInsertId, "auto-increment id must still be generated and returned")

	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)

	var displayName, uuid string
	var groupID int64
	require.NoError(t, s.conn.QueryRow(
		"SELECT group_id, display_name, uuid FROM groups WHERE display_name = 'Admins'",
	).Scan(&groupID, &displayName, &uuid))
	require.Equal(t, "Admins", displayName)
	require.Equal(t, "uuid-1", uuid)
	require.NotZero(t, groupID)
	require.Equal(t, groupID, res.LastInsertId)
}

// TestPreparedAutoIncrementInsertAutocommit pins the same shape outside an
// explicit transaction, exercising the autocommit DML path.
func TestPreparedAutoIncrementInsertAutocommit(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE groups ("+
		"group_id INTEGER PRIMARY KEY, "+
		"display_name TEXT, "+
		"lowercase_display_name TEXT, "+
		"creation_date TEXT, "+
		"uuid TEXT, "+
		"modified_date TEXT)", nil)
	require.NoError(t, err)

	sql := "INSERT INTO groups (display_name, lowercase_display_name, creation_date, uuid, modified_date) " +
		"VALUES (?, ?, ?, ?, ?)"
	params := []interface{}{"Users", "users", "2026-01-01", "uuid-2", "2026-01-01"}

	res, err := s.handler.HandleQuery(s.session, sql, params)
	require.NoError(t, err, "autocommit prepared INSERT with wire params must not fail")
	require.NotNil(t, res)
	require.Equal(t, int64(1), res.RowsAffected)
	require.NotZero(t, res.LastInsertId)

	var displayName string
	require.NoError(t, s.conn.QueryRow(
		"SELECT display_name FROM groups WHERE uuid = 'uuid-2'",
	).Scan(&displayName))
	require.Equal(t, "Users", displayName)
}

// TestUpdateLiteralBeforeWireParamOrdering proves values land in the right
// columns when a literal precedes a wire placeholder in the serialized SQL,
// not merely that execution succeeds.
func TestUpdateLiteralBeforeWireParamOrdering(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE items ("+
		"id INTEGER PRIMARY KEY, "+
		"status TEXT, "+
		"name TEXT)", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "INSERT INTO items (id, status, name) VALUES (1, 'pending', 'orig')", nil)
	require.NoError(t, err)

	// The auto-increment rule doesn't apply here (id is supplied), but this
	// still exercises the general literal-before-wire-param serialization
	// order that transform.ExtractLiterals/MergeExecParams must preserve
	// whenever ExtractLiterals runs alongside caller-supplied `?` marks.
	sql := "UPDATE items SET status = 'active', name = ? WHERE id = ?"
	params := []interface{}{"renamed", int64(1)}

	res, err := s.handler.HandleQuery(s.session, sql, params)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.RowsAffected)

	var status, name string
	require.NoError(t, s.conn.QueryRow("SELECT status, name FROM items WHERE id = 1").Scan(&status, &name))
	require.Equal(t, "active", status, "the literal-valued column must get the literal, not a wire param")
	require.Equal(t, "renamed", name, "the wire-param column must get the wire param, not the literal")
}

// TestTextProtocolInsertAllLiterals is a regression check: an INSERT with no
// wire params at all (text protocol, everything a literal) must still work.
func TestTextProtocolInsertAllLiterals(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE plain ("+
		"id INTEGER PRIMARY KEY, "+
		"name TEXT)", nil)
	require.NoError(t, err)

	res, err := s.handler.HandleQuery(s.session, "INSERT INTO plain (name) VALUES ('literal-only')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.RowsAffected)
	require.NotZero(t, res.LastInsertId)

	var name string
	require.NoError(t, s.conn.QueryRow("SELECT name FROM plain WHERE id = ?", res.LastInsertId).Scan(&name))
	require.Equal(t, "literal-only", name)
}

// TestPreparedInsertNoAutoIncrementColumn pins that a wire-param INSERT into
// a table with no auto-increment column (no id injection, so no
// ExtractedParams/ParamOrder in play at all) is unaffected by the merge.
func TestPreparedInsertNoAutoIncrementColumn(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE logs ("+
		"seq INTEGER, "+
		"message TEXT)", nil)
	require.NoError(t, err)

	sql := "INSERT INTO logs (seq, message) VALUES (?, ?)"
	params := []interface{}{int64(42), "hello"}

	res, err := s.handler.HandleQuery(s.session, sql, params)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.RowsAffected)

	var seq int64
	var message string
	require.NoError(t, s.conn.QueryRow("SELECT seq, message FROM logs").Scan(&seq, &message))
	require.Equal(t, int64(42), seq)
	require.Equal(t, "hello", message)
}
