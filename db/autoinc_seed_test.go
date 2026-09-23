//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

func newSeedTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// newSeedTestStore builds an AutoIncClaimStore backed by its own standalone
// ReplicatedDatabase, standing in for the system database the claim table now
// lives in. It is deliberately a separate database from newSeedTestDB's
// in-memory *sql.DB: seedAutoIncBasesForDDL derives its floor from the USER
// database (tm.db) but writes through the injected store, which is always
// backed by the system database - the same split that
// TransactionManager.autoIncClaimStoreAndDatabaseName exists to serve.
func newSeedTestStore(t *testing.T) *AutoIncClaimStore {
	t.Helper()
	sys := newRowidTestDatabase(t, 1)
	// The system database's initialisation creates the claim table
	// (DatabaseManager's system-database setup); this fixture stands in for it.
	_, err := sys.GetWriteDB().Exec(autoIncClaimDDL)
	require.NoError(t, err)
	return NewAutoIncClaimStore(sys)
}

// readClaimRow reads a (database, table) claim row directly through the
// store's own system database, keyed by both database and table - the claim
// table now serves every user database from one system-database table, so a
// lookup by table name alone could match another database's row.
func readClaimRow(t *testing.T, store *AutoIncClaimStore, database, table string) (base, owner int64, ok bool) {
	t.Helper()
	base64, err := store.ReadBase(database, table)
	if errors.Is(err, ErrAutoIncBaseAbsent) {
		return 0, 0, false
	}
	require.NoError(t, err)

	err = store.sys.GetReadDB().QueryRow(
		`SELECT owner FROM `+AutoIncClaimTable+` WHERE db = ? AND tbl = ?`, database, table).Scan(&owner)
	require.NoError(t, err, "base was readable but owner was not for %s.%s", database, table)
	return int64(base64), owner, true
}

// TestSeedAutoIncBasesForDDL_NewMarkedTableSeedsZero pins the empty-table
// case: a brand-new table with a marked, explicitly declared AUTO_INCREMENT
// column and no rows seeds base 0.
func TestSeedAutoIncBasesForDDL_NewMarkedTableSeedsZero(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)

	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 7}}))

	base, owner, ok := readClaimRow(t, store, "testdb", "t")
	require.True(t, ok, "expected a claim row to be created")
	require.Equal(t, int64(0), base)
	require.Equal(t, int64(7), owner)
}

// TestSeedAutoIncBasesForDDL_ExistingRowsRaiseTheFloor pins the tagged-table
// case: a table already holding ids 1..1000 must seed a base at or above
// 1000, or the allocator would hand out ids that collide with live rows.
func TestSeedAutoIncBasesForDDL_ExistingRowsRaiseTheFloor(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	for i := 1; i <= 1000; i++ {
		_, err := db.Exec(`INSERT INTO t (id, v) VALUES (?, ?)`, i, "x")
		require.NoError(t, err)
	}

	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}}))

	base, _, ok := readClaimRow(t, store, "testdb", "t")
	require.True(t, ok)
	require.GreaterOrEqual(t, base, int64(1000))
}

// TestSeedAutoIncBasesForDDL_DeclaredFloorWins pins the other input to the
// floor: max(MAX(existing rows), the marker's own AutoIncFloor). A client
// that declares AUTO_INCREMENT=5000 on a table holding only a handful of low
// ids must still seed at the declared floor (4999), not at the smaller
// MAX(id).
func TestSeedAutoIncBasesForDDL_DeclaredFloorWins(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER /*M:32a:4999*/ PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		_, err := db.Exec(`INSERT INTO t (id, v) VALUES (?, ?)`, i, "x")
		require.NoError(t, err)
	}

	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}}))

	base, _, ok := readClaimRow(t, store, "testdb", "t")
	require.True(t, ok)
	require.Equal(t, int64(4999), base)
}

// TestSeedAutoIncBasesForDDL_OnlyRaises pins the raise-only invariant end to
// end through the DDL-seeding path: seeding again with a lower current
// max/floor must never lower an already-stored base.
func TestSeedAutoIncBasesForDDL_OnlyRaises(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	for i := 1; i <= 1000; i++ {
		_, err := db.Exec(`INSERT INTO t (id, v) VALUES (?, ?)`, i, "x")
		require.NoError(t, err)
	}

	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}}))
	base1, _, ok := readClaimRow(t, store, "testdb", "t")
	require.True(t, ok)
	require.GreaterOrEqual(t, base1, int64(1000))

	// Second DDL touching the same table sees a much smaller MAX(id) (e.g. most
	// rows were since deleted) and no declared floor; the stored base must not
	// move down to it.
	_, err = db.Exec(`DELETE FROM t WHERE id > 5`)
	require.NoError(t, err)
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}}))

	base2, _, ok := readClaimRow(t, store, "testdb", "t")
	require.True(t, ok)
	require.Equal(t, base1, base2, "seeding must only ever raise the base")
}

// TestSeedAutoIncBasesForDDL_NoMarkerSeedsNothing pins the "this rule does
// not apply" path: a table with no explicitly declared AUTO_INCREMENT marker
// must get no claim row at all.
func TestSeedAutoIncBasesForDDL_NoMarkerSeedsNothing(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)

	// A store IS wired here, deliberately: the point is that a table with no
	// marker never reaches the store gate at all, not merely that a missing
	// store happens not to matter.
	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}}))

	_, _, ok := readClaimRow(t, store, "testdb", "t")
	require.False(t, ok, "a table with no AUTO_INCREMENT marker must get no claim row")
}

// TestSeedAutoIncBasesForDDL_MarkedButNotExplicitAutoIncSeedsNothing pins the
// narrower condition: a merely-narrow column (e.g. TINYINT with no
// AUTO_INCREMENT) carries a width marker but is not the seeding target.
func TestSeedAutoIncBasesForDDL_MarkedButNotExplicitAutoIncSeedsNothing(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER /*M:8*/)`)
	require.NoError(t, err)

	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}}))

	_, _, ok := readClaimRow(t, store, "testdb", "t")
	require.False(t, ok)
}

// TestSeedAutoIncBasesForDDL_MissingTableIsANoOp covers a DROP TABLE intent
// naming a table that no longer exists by the time seeding runs: absent is
// not an error.
func TestSeedAutoIncBasesForDDL_MissingTableIsANoOp(t *testing.T) {
	db := newSeedTestDB(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "gone", owner: 1}}))
}

// TestSeedAutoIncBasesForDDL_DedupesRepeatedTable pins that a table named by
// more than one DDL intent in the same transaction is only seeded once, using
// the first intent's owner.
func TestSeedAutoIncBasesForDDL_DedupesRepeatedTable(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY)`)
	require.NoError(t, err)

	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{
		{table: "t", owner: 42},
		{table: "t", owner: 99},
	}))

	_, owner, ok := readClaimRow(t, store, "testdb", "t")
	require.True(t, ok)
	require.Equal(t, int64(42), owner)
}

// TestApplyNonDMLIntents_SeedsAutoIncBaseAfterCreateTable is the wiring
// test: a CREATE TABLE DDL intent applied through the real applyNonDMLIntents
// path (DDL exec, schema cache reload, then seeding) leaves a claim row for
// its marked column, owned by the intent's own NodeID.
func TestApplyNonDMLIntents_SeedsAutoIncBaseAfterCreateTable(t *testing.T) {
	db := newSeedTestDB(t)
	schemaCache := NewSchemaCache()
	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), schemaCache)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")

	intents := []*WriteIntentRecord{
		{
			IntentType:   IntentTypeDDL,
			TableName:    "orders",
			NodeID:       3,
			SQLStatement: `CREATE TABLE orders (id INTEGER /*M:16a*/ PRIMARY KEY, total INTEGER)`,
		},
	}
	require.NoError(t, tm.applyNonDMLIntents(1, intents))

	base, owner, ok := readClaimRow(t, store, "testdb", "orders")
	require.True(t, ok, "expected applyNonDMLIntents to seed a claim row for the tagged table")
	require.Equal(t, int64(0), base)
	require.Equal(t, int64(3), owner)
}

// TestApplyNonDMLIntents_UnmarkedDDLSeedsNothing is the negative wiring case:
// ordinary DDL with no AUTO_INCREMENT marker must not create a claim row.
func TestApplyNonDMLIntents_UnmarkedDDLSeedsNothing(t *testing.T) {
	db := newSeedTestDB(t)
	schemaCache := NewSchemaCache()
	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), schemaCache)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")

	intents := []*WriteIntentRecord{
		{
			IntentType:   IntentTypeDDL,
			TableName:    "plain",
			NodeID:       3,
			SQLStatement: `CREATE TABLE plain (id INTEGER PRIMARY KEY, name TEXT)`,
		},
	}
	require.NoError(t, tm.applyNonDMLIntents(1, intents))

	_, _, ok := readClaimRow(t, store, "testdb", "plain")
	require.False(t, ok)
}

// TestSeedAutoIncBasesForDDL_NoStoreWiredFailsFast pins the fail-fast
// contract: a marked table found with no claim store wired is a
// configuration error, not a "nothing to do" case. The natural mistake is to
// treat a nil store like an absent marker and silently skip the seed; that
// would leave a table live under AUTO_INCREMENT with no claim row, and the
// very next INSERT's PREPARE would then reject on ErrAutoIncBaseAbsent with no
// record of why.
//
// Mutation: fall through when claimStore == nil instead of returning the
// "no auto-increment claim store wired" error.
func TestSeedAutoIncBasesForDDL_NoStoreWiredFailsFast(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)

	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetDatabaseName("testdb")
	// Deliberately no SetAutoIncClaimStore call.

	err = tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}})
	require.Error(t, err, "seeding a marked table with no store wired must fail, not silently skip")
	require.Contains(t, err.Error(), "no auto-increment claim store wired")
	require.Contains(t, err.Error(), "t", "the error should name the table it failed to seed")
}

// TestSeedAutoIncBasesForDDL_NegativeIdsSeedZero: a table whose ids are all
// negative is tagged. A negative MAX(id) contributes nothing to the floor, as
// in MySQL, whose counter for such a table starts at 1; the base is 0 and
// readable.
//
// Mutation: cast a negative MAX(id) to uint64 as the floor (drop the
// "max.Int64 > 0" guard in autoIncSeedFloor). The stored base is then negative,
// ReadBase refuses it, every claim on the table is rejected, and "a table with
// only negative ids must seed base 0" fires.
func TestSeedAutoIncBasesForDDL_NegativeIdsSeedZero(t *testing.T) {
	db := newSeedTestDB(t)
	_, err := db.Exec(`CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	for _, id := range []int{-5, -1} {
		_, err := db.Exec(`INSERT INTO t (id, v) VALUES (?, ?)`, id, "x")
		require.NoError(t, err)
	}

	store := newSeedTestStore(t)
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), nil)
	tm.SetAutoIncClaimStore(store)
	tm.SetDatabaseName("testdb")
	require.NoError(t, tm.seedAutoIncBasesForDDL([]ddlTableOwner{{table: "t", owner: 1}}))

	base, err := store.ReadBase("testdb", "t")
	require.NoError(t, err, "a table with only negative ids must seed base 0")
	require.Equal(t, uint64(0), base, "a table with only negative ids must seed base 0")
}
