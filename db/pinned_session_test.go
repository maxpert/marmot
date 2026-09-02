//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPinnedSession_SequentialInsertsReturnRealResults verifies two sequential
// ExecuteStatement INSERTs on the same pinned session each report real,
// distinct lastInsertId and rowsAffected == 1 - not the fake buffered-
// statement response the old defer-to-COMMIT flow returned.
func TestPinnedSession_SequentialInsertsReturnRealResults(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`))

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 20001)
	require.NoError(t, err)
	defer session.Release()

	rowsAffected1, lastInsertID1, err := session.ExecuteStatement(ctx, "INSERT INTO test (name) VALUES (?)", []interface{}{"alice"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected1)
	assert.NotZero(t, lastInsertID1)

	rowsAffected2, lastInsertID2, err := session.ExecuteStatement(ctx, "INSERT INTO test (name) VALUES (?)", []interface{}{"bob"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected2)
	assert.NotZero(t, lastInsertID2)

	assert.NotEqual(t, lastInsertID1, lastInsertID2)
}

// TestPinnedSession_QuerySeesOwnUncommittedWrites verifies a Query on the
// pinned session observes a row written earlier in the same session, while a
// query from a separate *sql.DB connection to the same file does not - the
// core "eager execution" isolation guarantee.
func TestPinnedSession_QuerySeesOwnUncommittedWrites(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`))

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 20002)
	require.NoError(t, err)
	defer session.Release()

	_, _, err = session.ExecuteStatement(ctx, "INSERT INTO test (id, name) VALUES (1, 'alice')", nil)
	require.NoError(t, err)

	// Own session sees the uncommitted row.
	cols, rows, err := session.Query(ctx, "SELECT id, name FROM test WHERE id = 1", nil)
	require.NoError(t, err)
	assert.Contains(t, cols, "name")
	require.Len(t, rows, 1)
	assert.Equal(t, "alice", rows[0]["name"])

	// A separate connection (the read pool) to the same file does not see it,
	// since the pinned session's SQLite transaction is still open and
	// uncommitted (writer isolation).
	var count int
	require.NoError(t, source.GetReadDB().QueryRow("SELECT COUNT(*) FROM test WHERE id = 1").Scan(&count))
	assert.Equal(t, 0, count, "uncommitted write on the pinned session must not be visible to another connection")
}

// TestPinnedSession_CDCEntriesAccumulateInOrder verifies CDCEntries() after
// two statements returns entries for both, in order, with correct
// Table/Operation/NewValues.
func TestPinnedSession_CDCEntriesAccumulateInOrder(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`))

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 20003)
	require.NoError(t, err)
	defer session.Release()

	_, _, err = session.ExecuteStatement(ctx, "INSERT INTO test (id, name) VALUES (1, 'alice')", nil)
	require.NoError(t, err)
	_, _, err = session.ExecuteStatement(ctx, "INSERT INTO test (id, name) VALUES (2, 'bob')", nil)
	require.NoError(t, err)

	entries := session.CDCEntries()
	require.Len(t, entries, 2)

	assert.Equal(t, "test", entries[0].Table)
	assert.Equal(t, uint8(OpTypeInsert), entries[0].Operation)
	require.NotNil(t, entries[0].NewValues)

	assert.Equal(t, "test", entries[1].Table)
	assert.Equal(t, uint8(OpTypeInsert), entries[1].Operation)
	require.NotNil(t, entries[1].NewValues)

	var name0, name1 string
	require.NoError(t, encoding.Unmarshal(entries[0].NewValues["name"], &name0))
	require.NoError(t, encoding.Unmarshal(entries[1].NewValues["name"], &name1))
	assert.Equal(t, "alice", name0)
	assert.Equal(t, "bob", name1)
}

// TestPinnedSession_ReleaseAlwaysRollsBack verifies Release never commits,
// even when CDCEntries() was read first - the "never double-apply" invariant.
func TestPinnedSession_ReleaseAlwaysRollsBack(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`))

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 20004)
	require.NoError(t, err)

	_, _, err = session.ExecuteStatement(ctx, "INSERT INTO test (id, name) VALUES (1, 'alice')", nil)
	require.NoError(t, err)

	entries := session.CDCEntries()
	require.Len(t, entries, 1)

	require.NoError(t, session.Release())

	var count int
	require.NoError(t, source.GetWriteDB().QueryRow("SELECT COUNT(*) FROM test WHERE id = 1").Scan(&count))
	assert.Equal(t, 0, count, "Release must always roll back, never commit, regardless of CDCEntries having been read")
}

// TestPinnedSession_ZeroRowUpdateReturnsNoRowsNoEntries verifies a zero-row
// UPDATE returns rowsAffected == 0, err == nil, and contributes no
// CDCEntries().
func TestPinnedSession_ZeroRowUpdateReturnsNoRowsNoEntries(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`))

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 20005)
	require.NoError(t, err)
	defer session.Release()

	rowsAffected, _, err := session.ExecuteStatement(ctx, "UPDATE test SET name = 'nobody' WHERE id = 999", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rowsAffected)
	assert.Empty(t, session.CDCEntries())
}

// TestPinnedSession_RowLockHeldFromStatementTime proves the pinned session's
// row lock is acquired immediately at ExecuteStatement time, not deferred to
// Release: after one INSERT on a not-yet-released pinned session, a
// concurrent AcquireCDCRowLock for the same table+intent key from a
// different txnID must fail.
func TestPinnedSession_RowLockHeldFromStatementTime(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`))

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 20006)
	require.NoError(t, err)
	defer session.Release()

	_, _, err = session.ExecuteStatement(ctx, "INSERT INTO test (id, name) VALUES (1, 'alice')", nil)
	require.NoError(t, err)

	entries := session.CDCEntries()
	require.Len(t, entries, 1)

	conflictErr := source.metaStore.AcquireCDCRowLock(99999, entries[0].Table, string(entries[0].IntentKey))
	assert.Error(t, conflictErr, "row lock for a row written by an unreleased pinned session must already be held")
}
