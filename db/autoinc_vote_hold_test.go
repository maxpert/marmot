package db

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// TestUninitialisedSystemDatabaseHoldsVotes pins which starts hold the
// node's claim votes. Any start that initialises its system database holds,
// whatever the node's seeds or catch-up strategy: that is a new node, or one
// that lost its data directory (reviewer B's C2 (a) seedless seed node, (b)
// strategy error, (c) NO_CATCHUP), and they look the same. A crash during that
// first initialisation holds again. A node restarting over its own claim
// history, and a system database written by a binary that predates claims
// (it never voted), are not held.
//
// Mutation: hold only when the claim table is new, ignoring the registry
// (the old rule). "a system database from a binary without claims was held"
// fires.
func TestUninitialisedSystemDatabaseHoldsVotes(t *testing.T) {
	requireHeld := func(t *testing.T, dm *DatabaseManager, want bool, msg string) {
		t.Helper()
		held, err := dm.AutoIncVotesHeld()
		require.NoError(t, err)
		require.Equal(t, want, held, msg)
	}

	dir := t.TempDir()
	dm, err := NewDatabaseManager(dir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	requireHeld(t, dm, true, "a node that initialised its system database may vote before merging claim bases")
	require.NoError(t, dm.MergeAutoIncBasesAndReleaseVotes([]AutoIncBase{{Database: "d", Table: "t", Base: 40}}))
	requireHeld(t, dm, false, "the merge did not release the hold")
	require.NoError(t, dm.Close())

	restarted, err := NewDatabaseManager(dir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	requireHeld(t, restarted, false, "a node restarting over its own claim history was held again")
	require.NoError(t, restarted.Close())

	// A crash after the system database file was created but before its
	// first initialisation committed: no claim table, nothing registered.
	interrupted := t.TempDir()
	writeSystemDatabase(t, interrupted, false)
	dm, err = NewDatabaseManager(interrupted, 1, hlc.NewClock(1))
	require.NoError(t, err)
	requireHeld(t, dm, true, "an interrupted first initialisation was not held")
	require.NoError(t, dm.Close())

	upgraded := t.TempDir()
	writeSystemDatabase(t, upgraded, true)
	dm, err = NewDatabaseManager(upgraded, 1, hlc.NewClock(1))
	require.NoError(t, err)
	requireHeld(t, dm, false, "a system database from a binary without claims was held")
	require.NoError(t, dm.Close())
}

// writeSystemDatabase writes the system database a binary without claim
// tables leaves behind: the database registry, holding the default database
// when registered is set, and no claim table.
func writeSystemDatabase(t *testing.T, dataDir string, registered bool) {
	t.Helper()
	conn, err := sql.Open(SQLiteDriverName, filepath.Join(dataDir, SystemDatabaseName+".db"))
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Exec(`CREATE TABLE __marmot_databases (name TEXT PRIMARY KEY, created_at INTEGER NOT NULL, path TEXT NOT NULL)`)
	require.NoError(t, err)
	if registered {
		_, err = conn.Exec(`INSERT INTO __marmot_databases (name, created_at, path) VALUES (?, 1, ?)`,
			DefaultDatabaseName, filepath.Join("databases", DefaultDatabaseName+".db"))
		require.NoError(t, err)
	}
}

// TestMergeAndReleaseVotesOnlyRaises pins the merge: every base ends at the
// maximum of this node's and the merged one, a table only the merge names is
// added, and nothing is lowered - the merged floor an earlier merge raised
// included.
//
// Mutation: drop "WHERE excluded.merged > merged" from the merge's upsert.
// "the merge lowered a base" fires.
func TestMergeAndReleaseVotesOnlyRaises(t *testing.T) {
	dm, err := NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dm.Close()
	store := NewAutoIncClaimStore(dm.GetSystemDatabase())
	require.NoError(t, dm.RaiseAutoIncBases([]AutoIncBase{{Database: "d", Table: "ahead_here", Base: 900}}))
	require.NoError(t, store.Seed("d", "behind_here", 100, 1))

	require.NoError(t, dm.MergeAutoIncBasesAndReleaseVotes([]AutoIncBase{
		{Database: "d", Table: "ahead_here", Base: 500},
		{Database: "d", Table: "behind_here", Base: 700},
		{Database: "d", Table: "only_merged", Base: 60},
	}))

	bases, held, err := dm.AutoIncBases()
	require.NoError(t, err)
	require.False(t, held)
	got := map[string]uint64{}
	for _, b := range bases {
		got[b.Table] = b.Base
	}
	require.Equal(t, uint64(900), got["ahead_here"], "the merge lowered a base")
	require.Equal(t, uint64(700), got["behind_here"], "the merge did not raise a base")
	require.Equal(t, uint64(60), got["only_merged"], "a table only a peer held was not added")
}

// TestRaiseAutoIncBasesNeverTouchesTheHold pins the membership backstop's
// store operation (R3c-8a): it raises bases exactly as the merge does, in one
// transaction, and leaves the vote hold as it found it - a held node stays
// held until its own merge, an unheld node stays unheld.
//
// Mutation: raiseBases deletes the hold row whatever release says. "the
// backstop released a held node's votes" fires.
func TestRaiseAutoIncBasesNeverTouchesTheHold(t *testing.T) {
	dm, err := NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dm.Close()
	store := NewAutoIncClaimStore(dm.GetSystemDatabase())
	require.NoError(t, store.Seed("d", "ahead_here", 900, 1))

	raise := []AutoIncBase{
		{Database: "d", Table: "ahead_here", Base: 500},
		{Database: "d", Table: "only_raised", Base: 60},
	}
	require.NoError(t, dm.RaiseAutoIncBases(raise))
	bases, held, err := dm.AutoIncBases()
	require.NoError(t, err)
	require.True(t, held, "the backstop released a held node's votes")
	got := map[string]uint64{}
	for _, b := range bases {
		got[b.Table] = b.Base
	}
	require.Equal(t, uint64(900), got["ahead_here"], "the backstop lowered a base")
	require.Equal(t, uint64(60), got["only_raised"], "a table only a peer held was not added")

	require.NoError(t, dm.MergeAutoIncBasesAndReleaseVotes(nil))
	require.NoError(t, dm.RaiseAutoIncBases([]AutoIncBase{{Database: "d", Table: "only_raised", Base: 90}}))
	_, held, err = dm.AutoIncBases()
	require.NoError(t, err)
	require.False(t, held, "the backstop placed a hold")
	base, err := store.ReadBase("d", "only_raised")
	require.NoError(t, err)
	require.Equal(t, uint64(90), base)

	// One transaction: a batch with one out-of-range base changes nothing.
	err = dm.RaiseAutoIncBases([]AutoIncBase{
		{Database: "d", Table: "only_raised", Base: 95},
		{Database: "d", Table: "broken", Base: 1 << 63},
	})
	require.Error(t, err)
	base, err = store.ReadBase("d", "only_raised")
	require.NoError(t, err)
	require.Equal(t, uint64(90), base, "a failed backstop round applied part of its bases")
}
