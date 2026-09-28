package db

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// seedLegacySchemaVersionForTest writes directly into store's pebble
// representation, standing in for what an older release's now-removed
// UpdateSchemaVersion write path would have left behind. There is no
// production writer of this key any more, so a test simulating a
// pre-upgrade database must poke it directly.
func seedLegacySchemaVersionForTest(t *testing.T, store *PebbleMetaStore, dbName string, version int64) {
	t.Helper()
	rec := pebbleSchemaVersionRecord{Version: version, UpdatedAt: time.Now().UnixNano()}
	data, err := encoding.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, store.db.Set(pebbleSchemaKey(dbName), data, store.syncWrite))
}

// TestSchemaVersionTable_PersistsAcrossCloseReopen pins that
// __marmot_schema_version, not any in-memory cache, is the durable source:
// a bumped version must survive a full close and reopen of the database.
func TestSchemaVersionTable_PersistsAcrossCloseReopen(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "appdb.db")

	metaStore, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	mdb, err := NewReplicatedDatabase(dbPath, 1, hlc.NewClock(1), metaStore, WithDatabaseName("appdb"))
	require.NoError(t, err)
	require.Equal(t, uint64(0), mdb.SchemaVersion())

	tx, err := mdb.GetWriteDB().Begin()
	require.NoError(t, err)
	v, err := bumpSchemaVersionInTx(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), v)
	require.NoError(t, tx.Commit())
	mdb.schemaVersion.Store(v)
	require.NoError(t, mdb.Close())

	metaStore2, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	mdb2, err := NewReplicatedDatabase(dbPath, 1, hlc.NewClock(1), metaStore2, WithDatabaseName("appdb"))
	require.NoError(t, err)
	defer mdb2.Close()
	require.Equal(t, uint64(1), mdb2.SchemaVersion(), "schema version must persist across close/reopen")
}

// TestSchemaVersionTable_MigratesLegacyOnlyForPreexistingFile pins the
// migration rule precisely: a SQLite file that already carried
// __marmot_applied_txn before this open (it predates the table) seeds its new
// __marmot_schema_version from the retiring pebble-stored counter.
func TestSchemaVersionTable_MigratesLegacyOnlyForPreexistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	legacy, err := NewPebbleMetaStore(filepath.Join(tmpDir, "legacy_meta.pebble"), DefaultPebbleOptions())
	require.NoError(t, err)
	defer legacy.Close()
	seedLegacySchemaVersionForTest(t, legacy, "appdb", 7)

	dbPath := filepath.Join(tmpDir, "appdb.db")
	raw, err := sql.Open(SQLiteDriverName, dbPath)
	require.NoError(t, err)
	require.NoError(t, ensureAppliedTxnTable(raw))
	require.NoError(t, raw.Close())

	metaStore, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	mdb, err := NewReplicatedDatabase(dbPath, 1, hlc.NewClock(1), metaStore,
		WithDatabaseName("appdb"), WithLegacySchemaVersionSource(legacy.GetSchemaVersion))
	require.NoError(t, err)
	defer mdb.Close()

	require.Equal(t, uint64(7), mdb.SchemaVersion(), "a pre-existing file must migrate the legacy pebble version")
}

// TestSchemaVersionTable_FreshDatabaseIgnoresStaleLegacyEntry pins the other
// half of the migration rule: a database file that did not exist before this
// open (a fresh CREATE DATABASE) starts at 0 even when a legacy pebble entry
// exists under the same name, left over from an earlier, dropped incarnation.
func TestSchemaVersionTable_FreshDatabaseIgnoresStaleLegacyEntry(t *testing.T) {
	tmpDir := t.TempDir()
	legacy, err := NewPebbleMetaStore(filepath.Join(tmpDir, "legacy_meta.pebble"), DefaultPebbleOptions())
	require.NoError(t, err)
	defer legacy.Close()
	seedLegacySchemaVersionForTest(t, legacy, "appdb", 7)

	dbPath := filepath.Join(tmpDir, "appdb.db") // never opened before: genuinely fresh

	metaStore, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	mdb, err := NewReplicatedDatabase(dbPath, 1, hlc.NewClock(1), metaStore,
		WithDatabaseName("appdb"), WithLegacySchemaVersionSource(legacy.GetSchemaVersion))
	require.NoError(t, err)
	defer mdb.Close()

	require.Equal(t, uint64(0), mdb.SchemaVersion(), "a fresh database must not inherit a stale legacy value")
}

// TestSchemaVersionTable_EveryOpenPathCreatesTheTable: a database file
// opened without a name - the system database, or any caller of
// NewReplicatedDatabase that passes no options - still gets the table, at
// version 0, so a DDL commit on it bumps the version instead of failing
// after its peers committed (a partial commit).
//
// Mutation: create the table only for a named database. "a database opened
// without a name has no schema version table" fires.
func TestSchemaVersionTable_EveryOpenPathCreatesTheTable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "unnamed.db")

	metaStore, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	mdb, err := NewReplicatedDatabase(dbPath, 1, hlc.NewClock(1), metaStore)
	require.NoError(t, err)
	defer mdb.Close()

	existed, err := sqliteTableExists(mdb.GetWriteDB(), "__marmot_schema_version")
	require.NoError(t, err)
	require.True(t, existed, "a database opened without a name has no schema version table")
	require.Equal(t, uint64(0), mdb.SchemaVersion())

	tx, err := mdb.GetWriteDB().Begin()
	require.NoError(t, err)
	v, err := bumpSchemaVersionInTx(tx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Equal(t, uint64(1), v)
}

// TestSchemaVersionTable_ManagerPathsCreateTheTable: every DatabaseManager
// path that opens a user database - CreateDatabase, a restart's registry
// load, and ImportExistingDatabases - leaves the file with the table.
func TestSchemaVersionTable_ManagerPathsCreateTheTable(t *testing.T) {
	dir := t.TempDir()
	dm, err := NewDatabaseManager(dir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	require.NoError(t, dm.CreateDatabase("created"))
	requireSchemaVersionTable(t, dm, "created", "CreateDatabase")
	sys, err := dm.GetDatabase(SystemDatabaseName)
	require.NoError(t, err)
	existed, err := sqliteTableExists(sys.GetWriteDB(), "__marmot_schema_version")
	require.NoError(t, err)
	require.True(t, existed, "the system database has no schema version table")
	require.NoError(t, dm.Close())

	dm, err = NewDatabaseManager(dir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dm.Close()
	requireSchemaVersionTable(t, dm, "created", "registry load on restart")
	requireSchemaVersionTable(t, dm, DefaultDatabaseName, "default database")
}

// TestSchemaVersionTable_RowlessTableIsRepairedAtOpen: a crash between the CREATE TABLE and the seeding INSERT left
// __marmot_schema_version present but empty. Before the fix, opening such a
// file failed every time with "read schema version: sql: no rows in result
// set"; ensureSchemaVersionTable must instead repair the row and open
// normally.
func TestSchemaVersionTable_RowlessTableIsRepairedAtOpen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "x.db")
	sqlDB, err := sql.Open(SQLiteDriverName, dbPath)
	require.NoError(t, err)
	defer sqlDB.Close()

	// State after a crash right after CREATE TABLE, before the INSERT.
	_, err = sqlDB.Exec(schemaVersionTableDDL)
	require.NoError(t, err)

	v, err := ensureSchemaVersionTable(sqlDB, "x", true, nil)
	require.NoError(t, err, "open must survive a crash between CREATE TABLE and the seeding INSERT")
	require.Equal(t, uint64(0), v)

	// A second open must also see the repaired row, not repair it again.
	v, err = ensureSchemaVersionTable(sqlDB, "x", true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), v)
}

// TestAdvanceSchemaVersion_NeverLowersCache pins that two post-commit
// stores of the cached schema version finishing out of order (replay versus
// the 2PC non-DML commit callback, both outside the writer lock) must never
// regress the cache below a value it already holds.
func TestAdvanceSchemaVersion_NeverLowersCache(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore, err := NewMetaStore(filepath.Join(tmpDir, "appdb.db"))
	require.NoError(t, err)
	mdb, err := NewReplicatedDatabase(filepath.Join(tmpDir, "appdb.db"), 1, hlc.NewClock(1), metaStore, WithDatabaseName("appdb"))
	require.NoError(t, err)
	defer mdb.Close()

	mdb.advanceSchemaVersion(5)
	require.Equal(t, uint64(5), mdb.SchemaVersion())

	mdb.advanceSchemaVersion(3) // out-of-order, lower: must not regress
	require.Equal(t, uint64(5), mdb.SchemaVersion())

	mdb.advanceSchemaVersion(7)
	require.Equal(t, uint64(7), mdb.SchemaVersion())
}

func requireSchemaVersionTable(t *testing.T, dm *DatabaseManager, name, path string) {
	t.Helper()
	mdb, err := dm.GetDatabase(name)
	require.NoError(t, err)
	existed, err := sqliteTableExists(mdb.GetWriteDB(), "__marmot_schema_version")
	require.NoError(t, err)
	require.True(t, existed, "%s: %s has no schema version table", path, name)
}
