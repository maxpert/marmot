//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	var captured []*EncodedCapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Assert
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, uint8(OpTypeInsert), captured[0].Op)
	assert.Nil(t, captured[0].OldValues)
	require.NotNil(t, captured[0].NewValues)
	assert.Len(t, captured[0].NewValues, 3) // 3 columns
	assert.NotEmpty(t, captured[0].IntentKey)

	// Verify values are encoded
	var idVal int64
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["id"], &idVal))
	assert.Equal(t, int64(1), idVal)

	var nameVal string
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["name"], &nameVal))
	assert.Equal(t, "alice", nameVal)

	var valueVal int64
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["value"], &valueVal))
	assert.Equal(t, int64(100), valueVal)
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
	var captured []*EncodedCapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Assert
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, uint8(OpTypeUpdate), captured[0].Op)
	assert.NotEmpty(t, captured[0].IntentKey)

	// Verify OldValues
	require.NotNil(t, captured[0].OldValues)
	assert.Len(t, captured[0].OldValues, 3)

	var oldID, oldValue int64
	var oldName string
	require.NoError(t, encoding.Unmarshal(captured[0].OldValues["id"], &oldID))
	require.NoError(t, encoding.Unmarshal(captured[0].OldValues["name"], &oldName))
	require.NoError(t, encoding.Unmarshal(captured[0].OldValues["value"], &oldValue))
	assert.Equal(t, int64(1), oldID)
	assert.Equal(t, "alice", oldName)
	assert.Equal(t, int64(100), oldValue)

	// Verify NewValues
	require.NotNil(t, captured[0].NewValues)
	assert.Len(t, captured[0].NewValues, 3)

	var newID, newValue int64
	var newName string
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["id"], &newID))
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["name"], &newName))
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["value"], &newValue))
	assert.Equal(t, int64(1), newID)
	assert.Equal(t, "bob", newName)
	assert.Equal(t, int64(200), newValue)
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
	var captured []*EncodedCapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Assert
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, uint8(OpTypeDelete), captured[0].Op)
	assert.NotEmpty(t, captured[0].IntentKey)
	assert.Nil(t, captured[0].NewValues)

	// Verify OldValues
	require.NotNil(t, captured[0].OldValues)
	assert.Len(t, captured[0].OldValues, 3)

	var oldID, oldValue int64
	var oldName string
	require.NoError(t, encoding.Unmarshal(captured[0].OldValues["id"], &oldID))
	require.NoError(t, encoding.Unmarshal(captured[0].OldValues["name"], &oldName))
	require.NoError(t, encoding.Unmarshal(captured[0].OldValues["value"], &oldValue))
	assert.Equal(t, int64(1), oldID)
	assert.Equal(t, "alice", oldName)
	assert.Equal(t, int64(100), oldValue)
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
	var captured []*EncodedCapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
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
	var captured []*EncodedCapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	require.Len(t, captured, 1)
	require.NotNil(t, captured[0].NewValues)
	// NULL values are not included in the encoded map, so we have 5 not 6
	assert.Len(t, captured[0].NewValues, 5)

	// Verify types - values are now msgpack encoded
	var id int64
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["id"], &id))
	assert.Equal(t, int64(1), id)

	var text string
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["text_col"], &text))
	assert.Equal(t, "hello", text)

	var intCol int64
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["int_col"], &intCol))
	assert.Equal(t, int64(42), intCol)

	var realCol float64
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["real_col"], &realCol))
	assert.InDelta(t, 3.14, realCol, 0.01)

	var blobCol []byte
	require.NoError(t, encoding.Unmarshal(captured[0].NewValues["blob_col"], &blobCol))
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, blobCol)

	// NULL columns are not included in the map
	_, hasNullCol := captured[0].NewValues["null_col"]
	assert.False(t, hasNullCol)
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
	var captured []*EncodedCapturedRow
	var sequences []uint64
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		seq, data := cursor.Row()
		sequences = append(sequences, seq)
		row, err := DecodeRow(data)
		require.NoError(t, err)
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
	assert.Equal(t, uint8(OpTypeInsert), captured[0].Op)
	assert.Equal(t, uint8(OpTypeUpdate), captured[1].Op)
	assert.Equal(t, uint8(OpTypeDelete), captured[2].Op)
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
	var captured []*EncodedCapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	require.Len(t, captured, 3)

	// Verify each row matches input
	for i, td := range testData {
		require.NotNil(t, captured[i].NewValues)
		assert.Len(t, captured[i].NewValues, 3)

		var id int64
		var name string
		var score float64
		require.NoError(t, encoding.Unmarshal(captured[i].NewValues["id"], &id))
		require.NoError(t, encoding.Unmarshal(captured[i].NewValues["name"], &name))
		require.NoError(t, encoding.Unmarshal(captured[i].NewValues["score"], &score))

		assert.Equal(t, td.id, id)
		assert.Equal(t, td.name, name)
		assert.InDelta(t, td.score, score, 0.01)
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

	// Insert (schema was reloaded during StartEphemeralSession, so it should capture)
	err = session.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	// Verify data was captured (schema reloaded during session start)
	var captured []*EncodedCapturedRow
	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	defer cursor.Close()

	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
		captured = append(captured, row)
	}
	require.NoError(t, cursor.Err())

	// Hook captures data because StartEphemeralSession reloads schema from connection
	require.Len(t, captured, 1)
	assert.Equal(t, "test", captured[0].Table)
	assert.Equal(t, uint8(OpTypeInsert), captured[0].Op)
}
