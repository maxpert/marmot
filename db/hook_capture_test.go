//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertTextEqual handles TEXT values that may be []byte or string from SQLite
func assertTextEqual(t *testing.T, expected string, actual interface{}) {
	t.Helper()
	switch v := actual.(type) {
	case string:
		assert.Equal(t, expected, v)
	case []byte:
		assert.Equal(t, expected, string(v))
	default:
		t.Errorf("expected string or []byte, got %T", actual)
	}
}

// TestHookCapture_Insert verifies INSERT captures NewValues, NewRowID
func TestHookCapture_Insert(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	// Create test table
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	require.NoError(t, err)

	// Reload schema
	require.NoError(t, replicatedDB.ReloadSchema())

	// Start hook session
	ctx := context.Background()
	txnID := uint64(1001)
	session, err := StartEphemeralSession(ctx, replicatedDB.hookDB, metaStore, replicatedDB.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	// Begin transaction
	require.NoError(t, session.BeginTx(ctx))

	// Execute INSERT
	err = session.ExecContext(ctx, "INSERT INTO test (id, name, value) VALUES (1, 'alice', 100)")
	require.NoError(t, err)

	// Verify capture WITHOUT calling ProcessCapturedRows
	var captured []CapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Assert
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, sqlite3.SQLITE_INSERT, captured[0].Op)
	assert.NotEqual(t, int64(0), captured[0].NewRowID)
	assert.Nil(t, captured[0].OldValues)
	require.NotNil(t, captured[0].NewValues)
	assert.Len(t, captured[0].NewValues, 3) // 3 columns

	// Verify values (TEXT may be []byte or string from SQLite)
	assert.Equal(t, int64(1), captured[0].NewValues[0])
	assertTextEqual(t, "alice", captured[0].NewValues[1])
	assert.Equal(t, int64(100), captured[0].NewValues[2])
}

// TestHookCapture_Update verifies UPDATE captures both OldValues and NewValues
func TestHookCapture_Update(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	// Create and populate table
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	require.NoError(t, err)
	_, err = replicatedDB.GetWriteDB().Exec(`INSERT INTO test (id, name, value) VALUES (1, 'alice', 100)`)
	require.NoError(t, err)

	require.NoError(t, replicatedDB.ReloadSchema())

	// Start hook session for UPDATE
	ctx := context.Background()
	txnID := uint64(1002)
	session, err := StartEphemeralSession(ctx, replicatedDB.hookDB, metaStore, replicatedDB.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))

	// Execute UPDATE
	err = session.ExecContext(ctx, "UPDATE test SET name = 'bob', value = 200 WHERE id = 1")
	require.NoError(t, err)

	// Verify capture
	var captured []CapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Assert
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, sqlite3.SQLITE_UPDATE, captured[0].Op)
	assert.NotEqual(t, int64(0), captured[0].OldRowID)
	assert.NotEqual(t, int64(0), captured[0].NewRowID)

	// Verify OldValues
	require.NotNil(t, captured[0].OldValues)
	assert.Len(t, captured[0].OldValues, 3)
	assert.Equal(t, int64(1), captured[0].OldValues[0])
	assertTextEqual(t, "alice", captured[0].OldValues[1])
	assert.Equal(t, int64(100), captured[0].OldValues[2])

	// Verify NewValues
	require.NotNil(t, captured[0].NewValues)
	assert.Len(t, captured[0].NewValues, 3)
	assert.Equal(t, int64(1), captured[0].NewValues[0])
	assertTextEqual(t, "bob", captured[0].NewValues[1])
	assert.Equal(t, int64(200), captured[0].NewValues[2])
}

// TestHookCapture_Delete verifies DELETE captures OldValues only
func TestHookCapture_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	// Create and populate table
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	require.NoError(t, err)
	_, err = replicatedDB.GetWriteDB().Exec(`INSERT INTO test (id, name, value) VALUES (1, 'alice', 100)`)
	require.NoError(t, err)

	require.NoError(t, replicatedDB.ReloadSchema())

	// Start hook session for DELETE
	ctx := context.Background()
	txnID := uint64(1003)
	session, err := StartEphemeralSession(ctx, replicatedDB.hookDB, metaStore, replicatedDB.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))

	// Execute DELETE
	err = session.ExecContext(ctx, "DELETE FROM test WHERE id = 1")
	require.NoError(t, err)

	// Verify capture
	var captured []CapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Assert
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, sqlite3.SQLITE_DELETE, captured[0].Op)
	assert.NotEqual(t, int64(0), captured[0].OldRowID)
	assert.Nil(t, captured[0].NewValues)

	// Verify OldValues
	require.NotNil(t, captured[0].OldValues)
	assert.Len(t, captured[0].OldValues, 3)
	assert.Equal(t, int64(1), captured[0].OldValues[0])
	assertTextEqual(t, "alice", captured[0].OldValues[1])
	assert.Equal(t, int64(100), captured[0].OldValues[2])
}

// TestHookCapture_SkipsInternalTables verifies __marmot* tables are NOT captured
func TestHookCapture_SkipsInternalTables(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	// Create internal and user tables
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE __marmot_internal (id INTEGER PRIMARY KEY, data TEXT)`)
	require.NoError(t, err)
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE user_table (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	require.NoError(t, replicatedDB.ReloadSchema())

	// Start hook session
	ctx := context.Background()
	txnID := uint64(1004)
	session, err := StartEphemeralSession(ctx, replicatedDB.hookDB, metaStore, replicatedDB.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))

	// Insert into internal table (should be skipped)
	err = session.ExecContext(ctx, "INSERT INTO __marmot_internal (id, data) VALUES (1, 'internal')")
	require.NoError(t, err)

	// Insert into user table (should be captured)
	err = session.ExecContext(ctx, "INSERT INTO user_table (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	// Verify only user table was captured
	var captured []CapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Only user_table should be captured
	require.Len(t, captured, 1)
	assert.Equal(t, "user_table", captured[0].Table)
}

// TestHookCapture_AllColumnTypes tests INTEGER, TEXT, REAL, BLOB, NULL
func TestHookCapture_AllColumnTypes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	// Create table with various types
	_, err = replicatedDB.GetWriteDB().Exec(`
		CREATE TABLE types_test (
			id INTEGER PRIMARY KEY,
			text_col TEXT,
			int_col INTEGER,
			real_col REAL,
			blob_col BLOB,
			null_col TEXT
		)
	`)
	require.NoError(t, err)

	require.NoError(t, replicatedDB.ReloadSchema())

	// Start hook session
	ctx := context.Background()
	txnID := uint64(1005)
	session, err := StartEphemeralSession(ctx, replicatedDB.hookDB, metaStore, replicatedDB.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))

	// Insert with various types
	_, err = session.tx.Exec(`
		INSERT INTO types_test (id, text_col, int_col, real_col, blob_col, null_col)
		VALUES (1, 'hello', 42, 3.14, X'DEADBEEF', NULL)
	`)
	require.NoError(t, err)

	// Verify capture
	var captured []CapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	require.Len(t, captured, 1)
	require.NotNil(t, captured[0].NewValues)
	assert.Len(t, captured[0].NewValues, 6)

	// Verify types
	assert.Equal(t, int64(1), captured[0].NewValues[0])                       // INTEGER
	assertTextEqual(t, "hello", captured[0].NewValues[1])                     // TEXT
	assert.Equal(t, int64(42), captured[0].NewValues[2])                      // INTEGER
	assert.InDelta(t, 3.14, captured[0].NewValues[3], 0.01)                   // REAL
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, captured[0].NewValues[4]) // BLOB
	assert.Nil(t, captured[0].NewValues[5])                                   // NULL
}

// TestHookCapture_MultipleOperations verifies multiple ops have correct sequence
func TestHookCapture_MultipleOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	// Create table
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	_, err = replicatedDB.GetWriteDB().Exec(`INSERT INTO test (id, name) VALUES (1, 'alice'), (2, 'bob')`)
	require.NoError(t, err)

	require.NoError(t, replicatedDB.ReloadSchema())

	// Start hook session
	ctx := context.Background()
	txnID := uint64(1006)
	session, err := StartEphemeralSession(ctx, replicatedDB.hookDB, metaStore, replicatedDB.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))

	// Multiple operations
	require.NoError(t, session.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (3, 'charlie')"))
	require.NoError(t, session.ExecContext(ctx, "UPDATE test SET name = 'ALICE' WHERE id = 1"))
	require.NoError(t, session.ExecContext(ctx, "DELETE FROM test WHERE id = 2"))

	// Verify captures
	var captured []CapturedRow
	var sequences []uint64
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		seq, data := cursor.Row()
		sequences = append(sequences, seq)
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Assert 3 operations captured
	require.Len(t, captured, 3)

	// Verify sequence numbers are monotonic
	assert.Equal(t, uint64(1), sequences[0])
	assert.Equal(t, uint64(2), sequences[1])
	assert.Equal(t, uint64(3), sequences[2])

	// Verify operation types
	assert.Equal(t, sqlite3.SQLITE_INSERT, captured[0].Op)
	assert.Equal(t, sqlite3.SQLITE_UPDATE, captured[1].Op)
	assert.Equal(t, sqlite3.SQLITE_DELETE, captured[2].Op)
}

// TestHookCapture_ValuesRetrievable verifies data can be read back correctly
func TestHookCapture_ValuesRetrievable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	// Create table
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, score REAL)`)
	require.NoError(t, err)

	require.NoError(t, replicatedDB.ReloadSchema())

	// Start hook session
	ctx := context.Background()
	txnID := uint64(1007)
	session, err := StartEphemeralSession(ctx, replicatedDB.hookDB, metaStore, replicatedDB.schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))

	// Insert multiple rows
	testData := []struct {
		id    int64
		name  string
		score float64
	}{
		{1, "alice", 95.5},
		{2, "bob", 87.3},
		{3, "charlie", 92.1},
	}

	for _, td := range testData {
		err = session.ExecContext(ctx, "INSERT INTO test (id, name, score) VALUES (?, ?, ?)", td.id, td.name, td.score)
		require.NoError(t, err)
	}

	// Read back and verify all data
	var captured []CapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	require.Len(t, captured, 3)

	// Verify each row matches input
	for i, td := range testData {
		require.NotNil(t, captured[i].NewValues)
		assert.Len(t, captured[i].NewValues, 3)
		assert.Equal(t, td.id, captured[i].NewValues[0])
		assertTextEqual(t, td.name, captured[i].NewValues[1])
		assert.InDelta(t, td.score, captured[i].NewValues[2], 0.01)
	}
}

// TestHookCapture_NoSchemaCache verifies behavior when table not in schema cache
func TestHookCapture_NoSchemaCache(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	// Create direct DB connection (bypassing ReplicatedDatabase)
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	// Create empty schema cache (table not loaded)
	schemaCache := NewSchemaCache()

	// Start hook session WITHOUT loading schema
	ctx := context.Background()
	txnID := uint64(1008)
	session, err := StartEphemeralSession(ctx, db, metaStore, schemaCache, txnID)
	require.NoError(t, err)
	defer session.Rollback()

	require.NoError(t, session.BeginTx(ctx))

	// Insert (hook should still capture even without schema)
	err = session.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	// Verify data was captured
	var captured []CapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		var row CapturedRow
		require.NoError(t, encoding.Unmarshal(data, &row))
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Hook captures raw data regardless of schema cache
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, sqlite3.SQLITE_INSERT, captured[0].Op)
}
