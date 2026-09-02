//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVirtualColumnCDC_CaptureRefusedNotCrashed reproduces (in a safe, fixed
// form) a confirmed go-sqlite3 v1.14.24 crash: its preupdate hook segfaults
// reading a GENERATED ALWAYS AS (...) VIRTUAL column's value, because
// sqlite3_preupdate_new/old return a NULL sqlite3_value* for a virtual
// column's index and row() dereferences it unconditionally via
// sqlite3_value_type. This was verified directly against the vendored
// go-sqlite3@v1.14.24 source (sqlite3_opt_preupdate_hook.go) and reproduced
// out of band in a throwaway program that registers a real preupdate hook on
// such a table and crashes the process with SIGSEGV in
// _Cfunc_sqlite3_value_type on INSERT - before hookCallback's guard existed.
// Embedding an actual crash in `go test` would take down the whole binary, so
// this test instead proves the FIX: hookCallback detects VIRTUAL columns via
// schema.VirtualColumns (populated from PRAGMA table_xinfo's hidden=2) and
// refuses capture with a clear conflict error before ever calling
// data.Old()/data.New() - so the crash can no longer be reached at all.
func TestVirtualColumnCDC_CaptureRefusedNotCrashed(t *testing.T) {
	source := newRowidTestDatabase(t, 1)

	const ddl = `CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		a TEXT,
		b INTEGER GENERATED ALWAYS AS (id + 1) VIRTUAL
	)`
	_, err := source.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())

	schema, err := source.schemaCache.GetSchemaFor("t")
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, schema.VirtualColumns, "schema must detect the VIRTUAL column via PRAGMA table_xinfo")

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 8001)
	require.NoError(t, err)
	defer session.Rollback()
	require.NoError(t, session.BeginTx(ctx))

	// This INSERT is exactly what crashed the vendored go-sqlite3 preupdate
	// hook before the fix (New() reads the virtual column's index). It must
	// now fail cleanly instead of segfaulting the process.
	_, execErr := session.ExecContext(ctx, "INSERT INTO t (id, a) VALUES (1, 'hi')")
	require.Error(t, execErr)
	assert.Contains(t, execErr.Error(), "VIRTUAL")
	assert.Contains(t, execErr.Error(), "b")

	// The process is still alive and the session's conflict state is set -
	// proof there was no crash and the guard fired before Old()/New().
	assert.Error(t, session.GetConflictError())
}

// TestVirtualColumnCDC_UpdateAndDeleteAlsoRefused verifies the guard applies
// uniformly to UPDATE and DELETE, not just INSERT - both New() (UPDATE) and
// Old() (UPDATE/DELETE) can hit the same NULL sqlite3_value* for a virtual
// column's index.
func TestVirtualColumnCDC_UpdateAndDeleteAlsoRefused(t *testing.T) {
	source := newRowidTestDatabase(t, 1)

	const ddl = `CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		a TEXT,
		b INTEGER GENERATED ALWAYS AS (id + 1) VIRTUAL
	)`
	// Insert the row directly (bypassing the hook) so UPDATE/DELETE have
	// something to act on.
	_, err := source.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	_, err = source.GetWriteDB().Exec("INSERT INTO t (id, a) VALUES (1, 'hi')")
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())

	ctx := context.Background()

	updateSession, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 8002)
	require.NoError(t, err)
	defer updateSession.Rollback()
	require.NoError(t, updateSession.BeginTx(ctx))
	_, updateErr := updateSession.ExecContext(ctx, "UPDATE t SET a = 'bye' WHERE id = 1")
	require.Error(t, updateErr)
	assert.Contains(t, updateErr.Error(), "VIRTUAL")
	require.NoError(t, updateSession.Rollback())

	deleteSession, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 8003)
	require.NoError(t, err)
	defer deleteSession.Rollback()
	require.NoError(t, deleteSession.BeginTx(ctx))
	_, deleteErr := deleteSession.ExecContext(ctx, "DELETE FROM t WHERE id = 1")
	require.Error(t, deleteErr)
	assert.Contains(t, deleteErr.Error(), "VIRTUAL")
}

// TestVirtualColumnCDC_StoredGeneratedColumnIsFine verifies that a STORED
// generated column (the audit's other claim: unaffected) does not crash
// capture like VIRTUAL does. Unlike VIRTUAL, SQLite computes and stores a
// real value for it, so the preupdate hook can safely read it - but that
// value is intentionally NOT captured or replicated: SQLite rejects an
// explicit INSERT/UPDATE of a generated column ("cannot INSERT into
// generated column", verified directly), and the value is deterministically
// recomputed from the table's other captured columns by SQLite itself on
// every replica once the DDL (which carries the GENERATED ALWAYS AS
// expression) has replicated. The other real columns must still capture and
// apply normally, at their correct positions.
func TestVirtualColumnCDC_StoredGeneratedColumnIsFine(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		a TEXT,
		b INTEGER GENERATED ALWAYS AS (id + 1) STORED,
		e TEXT
	)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	schema, err := source.schemaCache.GetSchemaFor("t")
	require.NoError(t, err)
	assert.Empty(t, schema.VirtualColumns, "STORED generated columns must not be treated as VIRTUAL")
	assert.NotContains(t, schema.Columns, "b", "generated columns must be excluded from CDC capture")
	assert.Contains(t, schema.Columns, "e", "a real column declared after the generated column must still be tracked")

	entries := captureEntries(t, source, 8004, "INSERT INTO t (id, a, e) VALUES (1, 'hi', 'world')")
	require.Len(t, entries, 1)
	assert.NotContains(t, entries[0].NewValues, "b", "generated column value must not be captured")
	require.Contains(t, entries[0].NewValues, "e", "column declared after the generated column must still capture at its correct position")

	applyEntries(t, replica, entries)

	var a, e string
	var b int64
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT a, b, e FROM t WHERE id = 1`).Scan(&a, &b, &e))
	assert.Equal(t, "hi", a)
	assert.Equal(t, int64(2), b, "SQLite must recompute the STORED generated column locally on the replica")
	assert.Equal(t, "world", e)
}

// TestSchemaLoad_FTS5HiddenPseudocolumnsExcluded is a regression guard for a
// finding made while fixing the VIRTUAL-column segfault: PRAGMA table_xinfo
// (needed to get generated columns' true positions correctly - see
// loadSchema) also exposes hidden == 1 rows for virtual tables' own
// pseudocolumns, which table_info silently excluded. Verified directly: an
// FTS5 table declared with 2 real columns reports 4 rows from table_xinfo
// (the 2 real columns plus a "docs"-named and a "rank" hidden == 1
// pseudocolumn). These must stay excluded from Columns/FullColumns exactly
// as table_info excluded them, or CDC/publisher schema would gain two fake
// columns for every FTS5 table. Also verified directly that the preupdate
// hook never actually fires for an FTS5 virtual table's own name (it fires,
// if at all, for its real b-tree shadow tables), so this is a schema-loading
// correctness issue rather than a capture-path one.
func TestSchemaLoad_FTS5HiddenPseudocolumnsExcluded(t *testing.T) {
	source := newRowidTestDatabase(t, 1)

	_, err := source.GetWriteDB().Exec(`CREATE VIRTUAL TABLE docs USING fts5(title, body)`)
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())

	schema, err := source.schemaCache.GetSchemaFor("docs")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"title", "body"}, schema.Columns,
		"FTS5's hidden table-name/rank pseudocolumns must not appear as real columns")
	assert.Empty(t, schema.VirtualColumns, "FTS5 hidden columns are not GENERATED ALWAYS VIRTUAL columns")
}
