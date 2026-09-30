//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// readFloors reads a claim row's three floors from dm's system database.
func readFloors(t *testing.T, dm *DatabaseManager, database, table string) claimFloors {
	t.Helper()
	var f claimFloors
	require.NoError(t, dm.GetSystemDatabase().GetReadDB().QueryRow(
		"SELECT committed, seed, merged FROM "+AutoIncClaimTable+" WHERE db = ? AND tbl = ?", database, table).
		Scan(&f.committed, &f.seed, &f.merged))
	return f
}

// TestCommitAppliesAClaimAMergeRaiseOvertook: a peer of the same claim
// committed first, and this node's membership backstop (or the operator's
// sync command) pulled that peer's base, newBase+size, between this node's
// PREPARE and its COMMIT. The COMMIT must apply: refusing it left the claim's
// transaction prepared, holding the claim key until the pending-transaction
// GC, and this node could neither claim for the table nor vote on it.
//
// Mutation: test the merged floor in applyClaimTx's condition (or write merges
// to the committed floor). "a merge raise between PREPARE and COMMIT refused
// the claim" fires.
func TestCommitAppliesAClaimAMergeRaiseOvertook(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 1000, 1)

	const txnID = 9100
	require.True(t, prepareClaim(engine, t, txnID, 7, 1000, 64).Success)
	require.NoError(t, dm.RaiseAutoIncBases([]AutoIncBase{{Database: "testdb", Table: "users", Base: 1064}}))

	res := commitClaim(engine, txnID)
	require.True(t, res.Success, "a merge raise between PREPARE and COMMIT refused the claim: %s", res.Error)
	require.Nil(t, mdb.GetTransactionManager().GetTransaction(txnID), "the claim's transaction is still pending after its COMMIT")
	require.GreaterOrEqual(t, readClaimBase(t, dm, "testdb", "users"), uint64(1064))
	require.Equal(t, claimFloors{committed: 1064, seed: 1000, merged: 1064}, readFloors(t, dm, "testdb", "users"))

	next := prepareClaim(engine, t, txnID+1, 8, 1064, 64)
	require.True(t, next.Success, "the next claim on the table could not PREPARE: %s", next.Error)
	require.True(t, commitClaim(engine, txnID+1).Success)
	require.Equal(t, uint64(1128), readClaimBase(t, dm, "testdb", "users"))
}

// TestCommitAppliesAClaimAMergeRaisePassed: a merge may report a base past
// this claim's end, from later claims this node has not seen yet. The COMMIT
// still applies, and the base stays at the merged floor: the merged floor
// only ever refuses at PREPARE.
func TestCommitAppliesAClaimAMergeRaisePassed(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 1000, 1)

	const txnID = 9110
	require.True(t, prepareClaim(engine, t, txnID, 7, 1000, 64).Success)
	require.NoError(t, dm.RaiseAutoIncBases([]AutoIncBase{{Database: "testdb", Table: "users", Base: 5000}}))

	res := commitClaim(engine, txnID)
	require.True(t, res.Success, "a merge raise between PREPARE and COMMIT refused the claim: %s", res.Error)
	require.Equal(t, uint64(5000), readClaimBase(t, dm, "testdb", "users"))

	stale := prepareClaim(engine, t, txnID+1, 8, 1064, 64)
	require.False(t, stale.Success, "a claim below the merged floor was voted for")
	require.Equal(t, uint64(5000), stale.AutoIDStoredBase)
}

// TestClaimFloorsHaveOneWriterEach pins which operation writes which floor:
// a claim's apply the committed floor, the DDL/restore seed and a rename's
// inheritance the seed floor, and a merge the merged floor. Each is a raise.
func TestClaimFloorsHaveOneWriterEach(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	store := autoIncClaimStoreForTest(dm)

	require.NoError(t, store.Seed("testdb", "users", 100, 1))
	require.Equal(t, claimFloors{seed: 100}, readFloors(t, dm, "testdb", "users"), "Seed")

	require.NoError(t, dm.RaiseAutoIncBases([]AutoIncBase{{Database: "testdb", Table: "users", Base: 150}}))
	require.Equal(t, claimFloors{seed: 100, merged: 150}, readFloors(t, dm, "testdb", "users"), "RaiseAutoIncBases")

	require.True(t, prepareClaim(engine, t, 9120, 7, 150, 10).Success)
	require.True(t, commitClaim(engine, 9120).Success)
	require.Equal(t, claimFloors{committed: 160, seed: 100, merged: 150}, readFloors(t, dm, "testdb", "users"), "ApplyClaims")

	require.NoError(t, store.Inherit("testdb", "renamed", []string{"users"}, 2))
	require.Equal(t, claimFloors{seed: 160}, readFloors(t, dm, "testdb", "renamed"), "Inherit")

	require.NoError(t, dm.MergeAutoIncBasesAndReleaseVotes([]AutoIncBase{{Database: "testdb", Table: "users", Base: 120}}))
	require.NoError(t, store.Seed("testdb", "users", 50, 1))
	require.Equal(t, claimFloors{committed: 160, seed: 100, merged: 150}, readFloors(t, dm, "testdb", "users"), "a floor was lowered")
}

// TestPreSplitSystemDatabaseIsSplitOnOpen opens a system database written by
// the binary at 541fc0f - one base column, no vote-hold table - and checks
// that the split keeps every check exactly as strict: all three floors start
// at the old base, the node is not held, PREPARE refuses below it, and a
// claim at it commits.
//
// Mutation: skip migrateAutoIncClaimTable in createAutoIncTables. The database
// still opens, as the claim DDL is IF NOT EXISTS, but the first floor read
// fails with "no such column: committed".
func TestPreSplitSystemDatabaseIsSplitOnOpen(t *testing.T) {
	dir := t.TempDir()
	clock := hlc.NewClock(1)
	dm, err := NewDatabaseManager(dir, 1, clock)
	require.NoError(t, err)
	require.NoError(t, dm.MergeAutoIncBasesAndReleaseVotes(nil))
	markedTableDB(t, NewReplicationEngine(1, dm, clock), dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	require.NoError(t, dm.Close())

	sys, err := sql.Open(SQLiteDriverName, filepath.Join(dir, SystemDatabaseName+".db"))
	require.NoError(t, err)
	for _, stmt := range []string{
		"DROP TABLE " + AutoIncClaimTable,
		"DROP TABLE " + AutoIncHoldTable,
		preSplitClaimDDL,
		"INSERT INTO " + AutoIncClaimTable + " (db, tbl, base, owner, granted_at) VALUES ('testdb', 'users', 1000, 3, 1)",
	} {
		_, err = sys.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	require.NoError(t, sys.Close())

	dm, err = NewDatabaseManager(dir, 1, clock)
	require.NoError(t, err, "a 541fc0f system database did not open")
	defer dm.Close()
	dm.SetClusterMembership(func() int { return testClaimMembership })
	engine := NewReplicationEngine(1, dm, clock)

	require.Equal(t, claimFloors{committed: 1000, seed: 1000, merged: 1000}, readFloors(t, dm, "testdb", "users"))
	held, err := dm.AutoIncVotesHeld()
	require.NoError(t, err)
	require.False(t, held, "a system database from a binary that granted claims was held")

	stale := prepareClaim(engine, t, 9130, 7, 500, 64)
	require.False(t, stale.Success, "a claim below the pre-split base was voted for")
	require.Equal(t, uint64(1000), stale.AutoIDStoredBase)

	require.True(t, prepareClaim(engine, t, 9131, 7, 1000, 64).Success)
	require.True(t, commitClaim(engine, 9131).Success)
	require.Equal(t, claimFloors{committed: 1064, seed: 1000, merged: 1000}, readFloors(t, dm, "testdb", "users"))
}

// TestRaiseAutoIncBasesFromPlacesEachFloor pins where a restore puts each
// floor: the peer's committed and merged floors become merged, the peer's
// seed stays seed, and this node keeps its own three floors, each as the
// larger of the two - so committed is exactly this node's.
//
// Mutation: carry the peer's committed floor into committed. "the peer's
// committed floor became this node's" fires.
func TestRaiseAutoIncBasesFromPlacesEachFloor(t *testing.T) {
	dir := t.TempDir()
	incoming := filepath.Join(dir, "incoming.db")
	local := filepath.Join(dir, "local.db")
	newClaimFileFloors(t, incoming, true, map[string]claimFloors{
		"both":      {committed: 100, seed: 300, merged: 200},
		"peer_only": {committed: 700, seed: 5},
	})
	newClaimFileFloors(t, local, true, map[string]claimFloors{
		"both":       {committed: 50, seed: 10, merged: 20},
		"local_only": {committed: 40, seed: 30, merged: 60},
	})

	require.NoError(t, RaiseAutoIncBasesFrom(incoming, local))

	got := claimFloorsIn(t, incoming)
	require.Equal(t, claimFloors{committed: 50, seed: 300, merged: 200}, got["both"], "the peer's committed floor became this node's")
	require.Equal(t, claimFloors{seed: 5, merged: 700}, got["peer_only"], "the peer's committed floor became this node's")
	require.Equal(t, claimFloors{committed: 40, seed: 30, merged: 60}, got["local_only"], "a row only this node held was changed")
}

// TestRaiseAutoIncBasesFromPreSplitFiles: either side of a restore may come
// from the binary at 541fc0f. The peer's file is split before it is adopted;
// this node's file is read as it is, one base standing for all three floors,
// and is never written.
func TestRaiseAutoIncBasesFromPreSplitFiles(t *testing.T) {
	dir := t.TempDir()
	incoming := filepath.Join(dir, "incoming.db")
	local := filepath.Join(dir, "local.db")
	newPreSplitClaimFile(t, incoming, map[string]int64{"users": 500, "peer_only": 300})
	newPreSplitClaimFile(t, local, map[string]int64{"users": 900, "local_only": 50})

	require.NoError(t, RaiseAutoIncBasesFrom(incoming, local))

	got := claimFloorsIn(t, incoming)
	require.Equal(t, claimFloors{committed: 900, seed: 900, merged: 900}, got["users"])
	require.Equal(t, claimFloors{seed: 300, merged: 300}, got["peer_only"])
	require.Equal(t, claimFloors{committed: 50, seed: 50, merged: 50}, got["local_only"])

	var preSplit int
	require.NoError(t, loneCopy(t, local).QueryRow(
		"SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = 'base'", AutoIncClaimTable).Scan(&preSplit))
	require.Equal(t, 1, preSplit, "the restore rewrote this node's own system database")
}
