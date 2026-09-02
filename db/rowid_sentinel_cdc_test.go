//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRowidTestDatabase creates a ReplicatedDatabase backed by its own temp dir
// and Pebble meta store, for use as either the capture ("source") or apply
// ("replica") side of a CDC round trip.
func newRowidTestDatabase(t *testing.T, nodeID uint64) *ReplicatedDatabase {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")
	require.NoError(t, os.MkdirAll(metaPath, 0755))

	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)

	clock := hlc.NewClock(nodeID)
	replicatedDB, err := NewReplicatedDatabase(dbPath, nodeID, clock, metaStore)
	require.NoError(t, err)

	t.Cleanup(func() {
		replicatedDB.Close()
		metaStore.Close()
	})
	return replicatedDB
}

// captureEntries runs execStatements inside a single CDC capture transaction
// against source and returns the resulting intent entries in sequence order.
func captureEntries(t *testing.T, source *ReplicatedDatabase, txnID uint64, execStatements ...string) []*IntentEntry {
	t.Helper()

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))
	for _, stmt := range execStatements {
		_, err = session.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// Intent entries must be read before Commit(): Commit() calls cleanup(),
	// which clears the captured-row buffer (see EphemeralHookSession.cleanup).
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	return entries
}

// applyEntries applies each entry to replica's write DB using its own schema cache.
func applyEntries(t *testing.T, replica *ReplicatedDatabase, entries []*IntentEntry) {
	t.Helper()
	adapter := &schemaCacheAdapter{cache: replica.schemaCache}
	for _, entry := range entries {
		require.NoError(t, ApplyCDCEntry(replica.GetWriteDB(), adapter, entry))
	}
}

// TestRowidSentinelCDC_CaptureIncludesRowidKey verifies hookCallback embeds the
// SQLite rowid under the "rowid" CDC key for tables with no explicit PRIMARY KEY.
func TestRowidSentinelCDC_CaptureIncludesRowidKey(t *testing.T) {
	source := newRowidTestDatabase(t, 1)

	_, err := source.GetWriteDB().Exec(`CREATE TABLE metadata (version SMALLINT)`)
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())

	entries := captureEntries(t, source, 2001, `INSERT INTO metadata (version) VALUES (1)`)
	require.Len(t, entries, 1)
	require.Equal(t, uint8(OpTypeInsert), entries[0].Operation)
	require.Contains(t, entries[0].NewValues, "rowid")

	var rowid int64
	require.NoError(t, encoding.Unmarshal(entries[0].NewValues["rowid"], &rowid))
	assert.Equal(t, int64(1), rowid)

	entries = captureEntries(t, source, 2002, `UPDATE metadata SET version = 2 WHERE rowid = 1`)
	require.Len(t, entries, 1)
	require.Equal(t, uint8(OpTypeUpdate), entries[0].Operation)
	require.Contains(t, entries[0].OldValues, "rowid")
	require.Contains(t, entries[0].NewValues, "rowid")

	entries = captureEntries(t, source, 2003, `DELETE FROM metadata WHERE rowid = 1`)
	require.Len(t, entries, 1)
	require.Equal(t, uint8(OpTypeDelete), entries[0].Operation)
	require.Contains(t, entries[0].OldValues, "rowid")
	require.Nil(t, entries[0].NewValues)
}

// TestRowidSentinelCDC_ApplyRoundTrip_Insert reproduces the LLDAP bug report:
// `CREATE TABLE metadata (version SMALLINT)` followed by writes must replicate.
// It also asserts the replica lands on the SAME rowid as the origin.
func TestRowidSentinelCDC_ApplyRoundTrip_Insert(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE metadata (version SMALLINT)`
	_, err := source.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())

	_, err = replica.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, replica.ReloadSchema())

	entries := captureEntries(t, source, 3001, `INSERT INTO metadata (version) VALUES (1)`)
	require.Len(t, entries, 1)

	applyEntries(t, replica, entries)

	var rowid int64
	var version int64
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT rowid, version FROM metadata`).Scan(&rowid, &version))
	assert.Equal(t, int64(1), rowid, "replica must land on origin's rowid")
	assert.Equal(t, int64(1), version)
}

// TestRowidSentinelCDC_ApplyRoundTrip_UpdateDelete verifies the exact failing
// sequence from the bug report - UPDATE and DELETE on a no-PK table - now
// replicates without the "primary key column rowid not found" error.
func TestRowidSentinelCDC_ApplyRoundTrip_UpdateDelete(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE metadata (version SMALLINT)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	insertEntries := captureEntries(t, source, 4001, `INSERT INTO metadata (version) VALUES (1)`)
	applyEntries(t, replica, insertEntries)

	updateEntries := captureEntries(t, source, 4002, `UPDATE metadata SET version = 2`)
	require.Len(t, updateEntries, 1)
	applyEntries(t, replica, updateEntries)

	var version int64
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT version FROM metadata`).Scan(&version))
	assert.Equal(t, int64(2), version)

	deleteEntries := captureEntries(t, source, 4003, `DELETE FROM metadata`)
	require.Len(t, deleteEntries, 1)
	applyEntries(t, replica, deleteEntries)

	var count int
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM metadata`).Scan(&count))
	assert.Equal(t, 0, count)
}

// TestRowidSentinelCDC_ApplyRoundTrip_MultiRow verifies multiple no-PK rows
// each preserve their own distinct origin rowid on the replica.
func TestRowidSentinelCDC_ApplyRoundTrip_MultiRow(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE events (label TEXT)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	entries := captureEntries(t, source, 5001,
		`INSERT INTO events (label) VALUES ('a')`,
		`INSERT INTO events (label) VALUES ('b')`,
		`INSERT INTO events (label) VALUES ('c')`,
	)
	require.Len(t, entries, 3)
	applyEntries(t, replica, entries)

	rows, err := replica.GetWriteDB().Query(`SELECT rowid, label FROM events ORDER BY rowid`)
	require.NoError(t, err)
	defer rows.Close()

	var got []struct {
		rowid int64
		label string
	}
	for rows.Next() {
		var r int64
		var l string
		require.NoError(t, rows.Scan(&r, &l))
		got = append(got, struct {
			rowid int64
			label string
		}{r, l})
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 3)
	assert.Equal(t, int64(1), got[0].rowid)
	assert.Equal(t, "a", got[0].label)
	assert.Equal(t, int64(2), got[1].rowid)
	assert.Equal(t, "b", got[1].label)
	assert.Equal(t, int64(3), got[2].rowid)
	assert.Equal(t, "c", got[2].label)

	// Delete the middle row and verify only it disappears on the replica.
	deleteEntries := captureEntries(t, source, 5002, `DELETE FROM events WHERE rowid = 2`)
	require.Len(t, deleteEntries, 1)
	applyEntries(t, replica, deleteEntries)

	var remaining int
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM events`).Scan(&remaining))
	assert.Equal(t, 2, remaining)
	var stillThere int
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT COUNT(*) FROM events WHERE rowid = 2`).Scan(&stillThere))
	assert.Equal(t, 0, stillThere)
}

// TestRowidSentinelCDC_ShadowedRowidColumn_CaptureFails verifies capture is
// refused with a clear error when a no-PK table declares a column named
// rowid, oid, or _rowid_, since it would collide with the CDC rowid key.
func TestRowidSentinelCDC_ShadowedRowidColumn_CaptureFails(t *testing.T) {
	for _, shadowCol := range []string{"rowid", "oid", "_rowid_", "RowId"} {
		t.Run(shadowCol, func(t *testing.T) {
			source := newRowidTestDatabase(t, 1)

			ddl := `CREATE TABLE bad (` + quoteSQLiteIdent(shadowCol) + ` TEXT, name TEXT)`
			_, err := source.GetWriteDB().Exec(ddl)
			require.NoError(t, err)
			require.NoError(t, source.ReloadSchema())

			ctx := context.Background()
			session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 6001)
			require.NoError(t, err)
			defer session.Rollback()

			require.NoError(t, session.BeginTx(ctx))
			_, execErr := session.ExecContext(ctx, `INSERT INTO bad (`+quoteSQLiteIdent(shadowCol)+`, name) VALUES ('x', 'y')`)
			require.Error(t, execErr)
			assert.Contains(t, execErr.Error(), "shadows SQLite's rowid alias")
		})
	}
}

// TestRowidSentinelCDC_WithoutRowidTable_NeverUsesSentinel confirms WITHOUT
// ROWID tables - which SQLite requires to declare an explicit PRIMARY KEY -
// never fall into the rowid-sentinel path, so the rowid CDC key is never
// synthesized for them.
func TestRowidSentinelCDC_WithoutRowidTable_NeverUsesSentinel(t *testing.T) {
	source := newRowidTestDatabase(t, 1)

	_, err := source.GetWriteDB().Exec(
		`CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID`)
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())

	schema, err := source.schemaCache.GetSchemaFor("kv")
	require.NoError(t, err)
	assert.False(t, isRowidSentinelSchema(schema), "WITHOUT ROWID table must use its declared PK, not the rowid sentinel")
	assert.Equal(t, []string{"k"}, schema.PrimaryKeys)

	entries := captureEntries(t, source, 7001, `INSERT INTO kv (k, v) VALUES ('key1', 'val1')`)
	require.Len(t, entries, 1)
	assert.NotContains(t, entries[0].NewValues, "rowid")
}

func execAndReload(db *ReplicatedDatabase, stmt string) error {
	if _, err := db.GetWriteDB().Exec(stmt); err != nil {
		return err
	}
	return db.ReloadSchema()
}
