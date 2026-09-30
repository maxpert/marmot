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

// claimFloors is one claim row's three floors (autoIncClaimDDL).
type claimFloors struct {
	committed, seed, merged int64
}

// newClaimFile creates a system-database file holding the given claim rows
// for database "d", each base as the row's committed floor, in WAL mode as
// the system database runs.
func newClaimFile(t *testing.T, path string, withTable bool, bases map[string]int64) {
	t.Helper()
	rows := make(map[string]claimFloors, len(bases))
	for tbl, base := range bases {
		rows[tbl] = claimFloors{committed: base}
	}
	newClaimFileFloors(t, path, withTable, rows)
}

// newClaimFileFloors is newClaimFile with every floor of every row given.
func newClaimFileFloors(t *testing.T, path string, withTable bool, rows map[string]claimFloors) {
	t.Helper()
	conn := newSystemFile(t, path)
	defer func() { require.NoError(t, conn.Close()) }()
	if !withTable {
		return
	}
	_, err := conn.Exec(autoIncClaimDDL)
	require.NoError(t, err)
	for tbl, f := range rows {
		_, err = conn.Exec("INSERT INTO "+AutoIncClaimTable+" (db, tbl, committed, seed, merged, owner, granted_at) "+
			"VALUES ('d', ?, ?, ?, ?, 1, 1)", tbl, f.committed, f.seed, f.merged)
		require.NoError(t, err)
	}
}

// preSplitClaimDDL is the claim table as the binary at 541fc0f created it,
// before the base was split into committed, seed and merged floors.
const preSplitClaimDDL = `CREATE TABLE IF NOT EXISTS ` + AutoIncClaimTable + ` (
	db         TEXT    NOT NULL,
	tbl        TEXT    NOT NULL,
	base       INTEGER NOT NULL,
	owner      INTEGER NOT NULL,
	granted_at INTEGER NOT NULL,
	PRIMARY KEY (db, tbl)
) WITHOUT ROWID`

// newPreSplitClaimFile creates a system-database file whose claim table has
// the 541fc0f shape, holding the given bases for database "d".
func newPreSplitClaimFile(t *testing.T, path string, bases map[string]int64) {
	t.Helper()
	conn := newSystemFile(t, path)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err := conn.Exec(preSplitClaimDDL)
	require.NoError(t, err)
	for tbl, base := range bases {
		_, err = conn.Exec("INSERT INTO "+AutoIncClaimTable+" (db, tbl, base, owner, granted_at) VALUES ('d', ?, ?, 1, 1)", tbl, base)
		require.NoError(t, err)
	}
}

// newSystemFile creates a system-database file in WAL mode, as the system
// database runs, with a table beside the claim table.
func newSystemFile(t *testing.T, path string) *sql.DB {
	t.Helper()
	conn, err := sql.Open(SQLiteDriverName, path)
	require.NoError(t, err)
	_, err = conn.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = conn.Exec("CREATE TABLE registry (name TEXT)")
	require.NoError(t, err)
	return conn
}

// loneCopy opens a copy of the .db file at path, away from any -wal/-shm
// beside it, as the restorer moves it.
func loneCopy(t *testing.T, path string) *sql.DB {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lone := filepath.Join(t.TempDir(), "lone.db")
	require.NoError(t, os.WriteFile(lone, data, 0o644))
	conn, err := sql.Open(SQLiteDriverName, lone)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

// claimBasesIn reads every claim base for database "d" from a lone copy of
// the .db file at path.
func claimBasesIn(t *testing.T, path string) map[string]int64 {
	t.Helper()
	bases := map[string]int64{}
	for tbl, f := range claimFloorsIn(t, path) {
		bases[tbl] = max(f.committed, f.seed, f.merged)
	}
	return bases
}

// claimFloorsIn reads every claim row's floors for database "d" from a lone
// copy of the .db file at path.
func claimFloorsIn(t *testing.T, path string) map[string]claimFloors {
	t.Helper()
	rows, err := loneCopy(t, path).Query("SELECT tbl, committed, seed, merged FROM " + AutoIncClaimTable + " WHERE db = 'd'")
	require.NoError(t, err)
	defer rows.Close()
	floors := map[string]claimFloors{}
	for rows.Next() {
		var tbl string
		var f claimFloors
		require.NoError(t, rows.Scan(&tbl, &f.committed, &f.seed, &f.merged))
		floors[tbl] = f
	}
	require.NoError(t, rows.Err())
	return floors
}

// TestRaiseAutoIncBasesFromNeverLowersABase: a restore installs a peer's system
// database. Every base must end at max(local, incoming): the peer may have
// missed a claim this node committed as part of a majority, and a lowered base
// would let this node accept that range again.
//
// Mutation: set merged to excluded.merged instead of the larger of the two in
// the merge's upsert. The local copy then overwrites a higher incoming base
// and "a restore lowered a base" fires.
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

// holdIn reports whether a lone copy of the .db file at path carries the
// AUTO_INCREMENT vote hold.
func holdIn(t *testing.T, path string) bool {
	t.Helper()
	conn := loneCopy(t, path)
	var tables, rows int
	require.NoError(t, conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = ?", AutoIncHoldTable).Scan(&tables))
	if tables == 0 {
		return false
	}
	require.NoError(t, conn.QueryRow("SELECT COUNT(*) FROM "+AutoIncHoldTable).Scan(&rows))
	return rows > 0
}

// placeHold writes the vote hold into the system-database file at path.
func placeHold(t *testing.T, path string) {
	t.Helper()
	conn, err := sql.Open(SQLiteDriverName, path)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.Exec(autoIncHoldDDL)
	require.NoError(t, err)
	_, err = conn.Exec(holdVotesSQL, 1)
	require.NoError(t, err)
}

// TestRaiseAutoIncBasesFromNoLocalStateHoldsVotes: a node installing a peer's
// system database with none of its own may have ACKed claims whose rows the
// peer lacks (a node rebuilt from one lagging peer). It must not vote until it
// has merged bases from a majority.
//
// Mutation: skip the hold when there is no local file. "a node rebuilt from
// one peer's system database may vote at once" fires.
func TestRaiseAutoIncBasesFromNoLocalStateHoldsVotes(t *testing.T) {
	dir := t.TempDir()
	incoming := filepath.Join(dir, "incoming.db")
	newClaimFile(t, incoming, true, map[string]int64{"users": 7})

	require.NoError(t, RaiseAutoIncBasesFrom(incoming, filepath.Join(dir, "absent.db")))
	require.True(t, holdIn(t, incoming), "a node rebuilt from one peer's system database may vote at once")
}

// TestRaiseAutoIncBasesFromKeepsThisNodesHold: the hold belongs to the node,
// not to the file a peer sent. A peer's hold must not travel to this node, and
// this node's own hold must survive the install.
//
// Mutation: skip clearing the incoming hold, or skip copying the local one.
// One of the two assertions fires.
func TestRaiseAutoIncBasesFromKeepsThisNodesHold(t *testing.T) {
	dir := t.TempDir()

	incoming := filepath.Join(dir, "held-peer.db")
	local := filepath.Join(dir, "local.db")
	newClaimFile(t, incoming, true, map[string]int64{"users": 7})
	placeHold(t, incoming)
	newClaimFile(t, local, true, map[string]int64{"users": 9})
	require.NoError(t, RaiseAutoIncBasesFrom(incoming, local))
	require.False(t, holdIn(t, incoming), "a peer's vote hold travelled to this node through its snapshot")

	incoming = filepath.Join(dir, "peer.db")
	heldLocal := filepath.Join(dir, "held-local.db")
	newClaimFile(t, incoming, true, map[string]int64{"users": 7})
	newClaimFile(t, heldLocal, true, map[string]int64{"users": 9})
	placeHold(t, heldLocal)
	require.NoError(t, RaiseAutoIncBasesFrom(incoming, heldLocal))
	require.True(t, holdIn(t, incoming), "this node's vote hold was lost when a peer's system database was installed")
}
