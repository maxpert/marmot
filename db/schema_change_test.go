package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// withinDeadline fails the test when f has not returned in time: every path
// here runs on a database whose write handle has one connection, and a read
// that waits on it never returns.
func withinDeadline(t *testing.T, what string, f func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not finish: it is waiting on the write connection its own transaction holds", what)
	}
}

// replayDDL applies statements the way anti-entropy replay does: in one
// transaction on the database's write handle, finished before the commit.
func replayDDL(t *testing.T, mdb *ReplicatedDatabase, table string, statements ...string) {
	t.Helper()
	withinDeadline(t, "replay", func() error {
		ctx := context.Background()
		tx, err := mdb.GetDB().BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var change SchemaChange
		for _, stmt := range statements {
			if err := mdb.ApplyReplayedDDL(ctx, tx, stmt, table, 2, &change); err != nil {
				return err
			}
		}
		if err := mdb.FinishReplayedSchemaChange(tx, &change); err != nil {
			return err
		}
		return tx.Commit()
	})
}

// TestReplayedDDLChangesIncarnationsLikeACommit pins R3c-10 on the
// anti-entropy replay path (grpc handleReplay -> ApplyReplayedDDL): a node
// that catches up on a DDL it missed ends the same incarnations, inherits
// the same bases and seeds the same tables as one that applied its COMMIT.
// Reviewer B's H1: a lagging n1 keeps a cursor for t while it misses
// "RENAME t TO t2"; without the forget it would go on issuing from it.
//
// Mutations: FinishReplayedSchemaChange returns nil at once - "a replayed
// rename did not end both names' incarnations" fires; it seeds through the
// database's write handle instead of the replay's transaction - the replay
// misses its deadline.
func TestReplayedDDLChangesIncarnationsLikeACommit(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	_, err := mdb.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (90, 'x')")
	require.NoError(t, err)
	seedClaimBase(t, dm, "testdb", "t", 500, 1)

	rec := &incarnationRecorder{}
	dm.SetAutoIncIncarnationListener(rec)

	replayDDL(t, mdb, "t", "ALTER TABLE t RENAME TO t2")
	require.Equal(t, []string{"testdb.t", "testdb.t2"}, rec.takeTables(), "a replayed rename did not end both names' incarnations")
	require.Equal(t, uint64(500), readClaimBase(t, dm, "testdb", "t2"), "a replayed rename did not inherit t's base")

	replayDDL(t, mdb, "u", "CREATE TABLE u (id INTEGER /*M:16a*/ PRIMARY KEY, v TEXT)", "INSERT INTO u (id, v) VALUES (700, 'y')")
	require.Equal(t, []string{"testdb.u"}, rec.takeTables(), "a replayed CREATE did not end u's incarnation")
	// The seed reads inside the replay's transaction, so it sees the row the
	// same replay wrote.
	require.Equal(t, uint64(700), readClaimBase(t, dm, "testdb", "u"), "a replayed CREATE was not seeded from its rows")

	// Replayed DDL is idempotent: replaying the CREATE again changes nothing.
	replayDDL(t, mdb, "u", "CREATE TABLE u (id INTEGER /*M:16a*/ PRIMARY KEY, v TEXT)")
	require.Empty(t, rec.takeTables(), "an idempotent replay ended an incarnation")
}

// TestAttachAfterRestoreEndsIncarnationsAndSeeds pins R3c-10 on the restore
// path (grpc CatchUpFromPeer -> DetachDatabase, file replaced,
// AttachDatabase): every table in the restored file may be a new
// incarnation, so the node forgets the whole database's ranges before it is
// back in service, a table that took the place of one the old file had
// inherits its base, and every table is seeded from its own MAX(id).
//
// Mutation: skip restoredSchemaChange in AttachDatabase. "a restore did not
// end the database's incarnations" fires.
func TestAttachAfterRestoreEndsIncarnationsAndSeeds(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "t", 500, 1)

	rec := &incarnationRecorder{}
	dm.SetAutoIncIncarnationListener(rec)

	var relPath string
	require.NoError(t, dm.GetSystemDatabase().GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", "testdb").Scan(&relPath))

	require.NoError(t, dm.DetachDatabase(context.Background(), "testdb"))
	// The restored file: t was renamed to t2 on the peer, which also holds
	// rows this node never saw, and a table this node never had.
	restored, err := sql.Open("sqlite3", filepath.Join(dm.dataDir, relPath))
	require.NoError(t, err)
	for _, stmt := range []string{
		"ALTER TABLE t RENAME TO t2",
		"INSERT INTO t2 (id, v) VALUES (300, 'peer')",
		"CREATE TABLE w (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)",
		"INSERT INTO w (id, v) VALUES (800, 'peer')",
	} {
		_, err := restored.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	require.NoError(t, restored.Close())

	require.NoError(t, dm.AttachDatabase("testdb"))
	require.Equal(t, []string{"testdb"}, rec.databases, "a restore did not end the database's incarnations")
	require.Equal(t, uint64(500), readClaimBase(t, dm, "testdb", "t2"), "the restored t2 did not inherit t's base")
	require.Equal(t, uint64(800), readClaimBase(t, dm, "testdb", "w"), "the restored w was not seeded from its rows")
	require.Equal(t, uint64(500), readClaimBase(t, dm, "testdb", "t"), "the restore changed the replaced name's row")
}
