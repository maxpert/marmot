package db

import (
	"database/sql"
	"database/sql/driver"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// newClaimFile creates a system-database file holding the given claim rows
// for database "d", in WAL mode as the system database runs.
func newClaimFile(t *testing.T, path string, withTable bool, bases map[string]int64) {
	t.Helper()
	conn, err := sql.Open(SQLiteDriverName, path)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = conn.Exec("CREATE TABLE registry (name TEXT)")
	require.NoError(t, err)
	if !withTable {
		return
	}
	_, err = conn.Exec(autoIncClaimDDL)
	require.NoError(t, err)
	for tbl, base := range bases {
		_, err = conn.Exec("INSERT INTO "+AutoIncClaimTable+" (db, tbl, base, owner, granted_at) VALUES ('d', ?, ?, 1, 1)", tbl, base)
		require.NoError(t, err)
	}
}

// claimBasesIn reads every claim base for database "d" from a lone .db file,
// copied away from any -wal/-shm beside it, as the restorer moves it.
func claimBasesIn(t *testing.T, path string) map[string]int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lone := filepath.Join(t.TempDir(), "lone.db")
	require.NoError(t, os.WriteFile(lone, data, 0o644))

	conn, err := sql.Open(SQLiteDriverName, lone)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	rows, err := conn.Query("SELECT tbl, base FROM " + AutoIncClaimTable + " WHERE db = 'd'")
	require.NoError(t, err)
	defer rows.Close()
	bases := map[string]int64{}
	for rows.Next() {
		var tbl string
		var base int64
		require.NoError(t, rows.Scan(&tbl, &base))
		bases[tbl] = base
	}
	require.NoError(t, rows.Err())
	return bases
}

// TestRaiseAutoIncBasesFromNeverLowersABase: a restore installs a peer's system
// database. Every base must end at max(local, incoming): the peer may have
// missed a claim this node committed as part of a majority, and a lowered base
// would let this node accept that range again.
//
// Mutation: drop "WHERE excluded.base > base" from the merge's upsert. The
// local copy then overwrites a higher incoming base and "a restore lowered a
// base" fires.
func TestRaiseAutoIncBasesFromNeverLowersABase(t *testing.T) {
	dir := t.TempDir()
	incoming := filepath.Join(dir, "incoming.db")
	local := filepath.Join(dir, "local.db")
	newClaimFile(t, incoming, true, map[string]int64{"stale_on_peer": 500, "ahead_on_peer": 700})
	newClaimFile(t, local, true, map[string]int64{"stale_on_peer": 900, "ahead_on_peer": 100, "only_local": 50})

	require.NoError(t, RaiseAutoIncBasesFrom(incoming, local))

	got := claimBasesIn(t, incoming)
	require.Equal(t, int64(900), got["stale_on_peer"], "the peer's lower base replaced this node's committed one")
	require.Equal(t, int64(700), got["ahead_on_peer"], "a restore lowered a base")
	require.Equal(t, int64(50), got["only_local"], "a row only this node held was lost")
}

// TestRaiseAutoIncBasesFromAPeerWithoutTheClaimTable: a peer running a binary
// that predates the claim table ships a system database without it; this
// node's rows must still land.
func TestRaiseAutoIncBasesFromAPeerWithoutTheClaimTable(t *testing.T) {
	dir := t.TempDir()
	incoming := filepath.Join(dir, "incoming.db")
	local := filepath.Join(dir, "local.db")
	newClaimFile(t, incoming, false, nil)
	newClaimFile(t, local, true, map[string]int64{"users": 42})

	require.NoError(t, RaiseAutoIncBasesFrom(incoming, local))
	require.Equal(t, map[string]int64{"users": 42}, claimBasesIn(t, incoming))
}

// TestRaiseAutoIncBasesFromNoLocalState: a node with no system database of
// its own takes the peer's as it is.
func TestRaiseAutoIncBasesFromNoLocalState(t *testing.T) {
	dir := t.TempDir()
	incoming := filepath.Join(dir, "incoming.db")
	newClaimFile(t, incoming, true, map[string]int64{"users": 7})

	require.NoError(t, RaiseAutoIncBasesFrom(incoming, filepath.Join(dir, "absent.db")))
	require.Equal(t, map[string]int64{"users": 7}, claimBasesIn(t, incoming))
}

// TestRaiseAutoIncBasesFromMergesDurably: the restorer moves the incoming
// system database into place after the merge, so the connection that commits
// the merged bases must be as durable as every other system-database
// connection (WithDurableCommits): synchronous=FULL and, on darwin,
// fullfsync.
//
// Mutation: open the merge connection with SQLiteDriverName, or drop its
// "_sync=FULL". "the claim-base merge opened no durable connection" or "the
// claim-base merge ran on a connection that does not sync commits" fires.
func TestRaiseAutoIncBasesFromMergesDurably(t *testing.T) {
	dir := t.TempDir()
	incoming := filepath.Join(dir, "incoming.db")
	local := filepath.Join(dir, "local.db")
	newClaimFile(t, incoming, true, map[string]int64{"users": 7})
	newClaimFile(t, local, true, map[string]int64{"users": 9})

	durable := sqliteDrivers[SQLiteDurableDriverName]
	connectHook := durable.ConnectHook
	t.Cleanup(func() { durable.ConnectHook = connectHook })
	type settings struct{ sync, fullfsync int64 }
	var opened []settings
	durable.ConnectHook = func(conn *sqlite3.SQLiteConn) error {
		if err := connectHook(conn); err != nil {
			return err
		}
		opened = append(opened, settings{sync: pragmaInt(t, conn, "synchronous"), fullfsync: pragmaInt(t, conn, "fullfsync")})
		return nil
	}

	require.NoError(t, RaiseAutoIncBasesFrom(incoming, local))
	require.NotEmpty(t, opened, "the claim-base merge opened no durable connection")
	wantFullfsync := int64(0)
	if runtime.GOOS == "darwin" {
		wantFullfsync = 1
	}
	for _, s := range opened {
		require.Equal(t, int64(2), s.sync, "the claim-base merge ran on a connection that does not sync commits")
		require.Equal(t, wantFullfsync, s.fullfsync, "the claim-base merge ran on a connection that does not flush the drive cache")
	}
	require.Equal(t, map[string]int64{"users": 9}, claimBasesIn(t, incoming))
}

// pragmaInt reads an integer PRAGMA on a raw driver connection.
func pragmaInt(t *testing.T, conn *sqlite3.SQLiteConn, pragma string) int64 {
	t.Helper()
	rows, err := conn.Query("PRAGMA "+pragma, nil)
	require.NoError(t, err)
	defer rows.Close()
	dest := make([]driver.Value, 1)
	require.NoError(t, rows.Next(dest))
	v, ok := dest[0].(int64)
	require.True(t, ok, "PRAGMA %s returned %T", pragma, dest[0])
	return v
}
