//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessRows_InsertGeneratesIntentKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1001)
	capturedRow := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "users", entries[0].Table)
	assert.Contains(t, entries[0].IntentKey, "users")
	assert.Contains(t, entries[0].IntentKey, "1")
}

func TestProcessRows_UpdateGeneratesIntentKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1002)
	capturedRow := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_UPDATE,
		OldRowID:  1,
		NewRowID:  1,
		OldValues: []interface{}{int64(1), "alice"},
		NewValues: []interface{}{int64(1), "alice_updated"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint8(OpTypeUpdate), entries[0].Operation)
	assert.Contains(t, entries[0].IntentKey, "users")
	assert.Contains(t, entries[0].IntentKey, "1")
}

func TestProcessRows_DeleteGeneratesIntentKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1003)
	capturedRow := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_DELETE,
		OldRowID:  1,
		OldValues: []interface{}{int64(1), "alice"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint8(OpTypeDelete), entries[0].Operation)
	assert.Contains(t, entries[0].IntentKey, "users")
	assert.Contains(t, entries[0].IntentKey, "1")
}

func TestProcessRows_LockAcquired(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1004)
	capturedRow := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	intentKey := entries[0].IntentKey
	lockTxnID, err := metaStore.GetCDCRowLock("users", intentKey)
	require.NoError(t, err)
	assert.Equal(t, txnID, lockTxnID, "Row lock should be held by the transaction")
}

func TestProcessRows_ConflictDetected(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txn1 := uint64(1005)
	capturedRow1 := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice"},
	}
	rowData1, err := encoding.Marshal(&capturedRow1)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txn1, 1, rowData1)
	require.NoError(t, err)

	session1 := &EphemeralHookSession{
		txnID:       txn1,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session1.ProcessCapturedRows()
	require.NoError(t, err)

	txn2 := uint64(1006)
	capturedRow2 := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_UPDATE,
		OldRowID:  1,
		NewRowID:  1,
		OldValues: []interface{}{int64(1), "alice"},
		NewValues: []interface{}{int64(1), "bob"},
	}
	rowData2, err := encoding.Marshal(&capturedRow2)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txn2, 1, rowData2)
	require.NoError(t, err)

	session2 := &EphemeralHookSession{
		txnID:       txn2,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session2.ProcessCapturedRows()
	require.Error(t, err)

	var cdcErr ErrCDCRowLocked
	require.ErrorAs(t, err, &cdcErr)
	assert.Equal(t, "users", cdcErr.Table)
	assert.Equal(t, txn1, cdcErr.HeldByTxn)
}

func TestProcessRows_NoConflictDifferentRows(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txn1 := uint64(1007)
	capturedRow1 := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice"},
	}
	rowData1, err := encoding.Marshal(&capturedRow1)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txn1, 1, rowData1)
	require.NoError(t, err)

	session1 := &EphemeralHookSession{
		txnID:       txn1,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session1.ProcessCapturedRows()
	require.NoError(t, err)

	txn2 := uint64(1008)
	capturedRow2 := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  2,
		NewValues: []interface{}{int64(2), "bob"},
	}
	rowData2, err := encoding.Marshal(&capturedRow2)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txn2, 1, rowData2)
	require.NoError(t, err)

	session2 := &EphemeralHookSession{
		txnID:       txn2,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session2.ProcessCapturedRows()
	require.NoError(t, err, "Different rows should not conflict")

	entries1, err := metaStore.GetIntentEntries(txn1)
	require.NoError(t, err)
	require.Len(t, entries1, 1)

	entries2, err := metaStore.GetIntentEntries(txn2)
	require.NoError(t, err)
	require.Len(t, entries2, 1)

	assert.NotEqual(t, entries1[0].IntentKey, entries2[0].IntentKey, "Different rows should have different intent keys")
}

func TestProcessRows_NoConflictDifferentTables(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE table_a (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE table_b (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txn1 := uint64(1009)
	capturedRow1 := CapturedRow{
		Table:     "table_a",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice"},
	}
	rowData1, err := encoding.Marshal(&capturedRow1)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txn1, 1, rowData1)
	require.NoError(t, err)

	session1 := &EphemeralHookSession{
		txnID:       txn1,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session1.ProcessCapturedRows()
	require.NoError(t, err)

	txn2 := uint64(1010)
	capturedRow2 := CapturedRow{
		Table:     "table_b",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "bob"},
	}
	rowData2, err := encoding.Marshal(&capturedRow2)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txn2, 1, rowData2)
	require.NoError(t, err)

	session2 := &EphemeralHookSession{
		txnID:       txn2,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session2.ProcessCapturedRows()
	require.NoError(t, err, "Different tables should not conflict even with same PK value")
}

func TestProcessRows_CDCEntriesGenerated(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1011)
	capturedRow := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice", "alice@example.com"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	entry := entries[0]
	assert.Equal(t, txnID, entry.TxnID)
	assert.Equal(t, uint8(OpTypeInsert), entry.Operation)
	assert.Equal(t, "users", entry.Table)
	assert.NotEmpty(t, entry.IntentKey)

	require.NotNil(t, entry.NewValues)
	require.Contains(t, entry.NewValues, "id")
	require.Contains(t, entry.NewValues, "name")
	require.Contains(t, entry.NewValues, "email")

	var name interface{}
	err = encoding.Unmarshal(entry.NewValues["name"], &name)
	require.NoError(t, err)
	assert.Equal(t, "alice", name)

	assert.Nil(t, entry.OldValues, "INSERT should have nil OldValues")
}

func TestProcessRows_MultipleRowsProcessed(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1012)

	rows := []CapturedRow{
		{
			Table:     "users",
			Op:        sqlite3.SQLITE_INSERT,
			NewRowID:  1,
			NewValues: []interface{}{int64(1), "alice"},
		},
		{
			Table:     "users",
			Op:        sqlite3.SQLITE_INSERT,
			NewRowID:  2,
			NewValues: []interface{}{int64(2), "bob"},
		},
		{
			Table:     "users",
			Op:        sqlite3.SQLITE_INSERT,
			NewRowID:  3,
			NewValues: []interface{}{int64(3), "charlie"},
		},
	}

	for i, row := range rows {
		rowData, err := encoding.Marshal(&row)
		require.NoError(t, err)
		err = metaStore.WriteCapturedRow(txnID, uint64(i+1), rowData)
		require.NoError(t, err)
	}

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 3, "All three rows should be processed")

	intentKeys := make(map[string]bool)
	for _, entry := range entries {
		assert.Equal(t, "users", entry.Table)
		assert.Equal(t, uint8(OpTypeInsert), entry.Operation)
		assert.NotEmpty(t, entry.IntentKey)
		intentKeys[entry.IntentKey] = true
	}

	assert.Len(t, intentKeys, 3, "Each row should have a unique intent key")
}

func TestProcessRows_CapturedRowsCleanedUp(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1013)
	capturedRow := CapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	require.True(t, cursor.Next(), "Captured row should exist before processing")
	cursor.Close()

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	cursor, err = metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	require.False(t, cursor.Next(), "Captured rows should be cleaned up after processing")
	cursor.Close()
}

func TestProcessRows_SchemaNotFoundSkipsRow(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	txnID := uint64(1014)
	capturedRow := CapturedRow{
		Table:     "nonexistent_table",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(1), "alice"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.Error(t, err, "Schema not found should cause error")
	require.Contains(t, err.Error(), "schema not found")
}

func TestProcessRows_CompositeKeyIntentGeneration(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE orders (user_id INTEGER, order_id INTEGER, amount REAL, PRIMARY KEY (user_id, order_id))`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1015)
	capturedRow := CapturedRow{
		Table:     "orders",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  1,
		NewValues: []interface{}{int64(42), int64(100), 99.99},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	assert.Contains(t, entries[0].IntentKey, "orders", "Intent key should contain table name")
	assert.NotEmpty(t, entries[0].IntentKey, "Intent key should be non-empty")
}

func TestProcessRows_RowIDBasedPK(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE notes (content TEXT)`)
	require.NoError(t, err)
	replicatedDB.ReloadSchema()

	txnID := uint64(1016)
	capturedRow := CapturedRow{
		Table:     "notes",
		Op:        sqlite3.SQLITE_INSERT,
		NewRowID:  42,
		NewValues: []interface{}{"My important note"},
	}
	rowData, err := encoding.Marshal(&capturedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	entries, err := metaStore.GetIntentEntries(txnID)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	assert.Contains(t, entries[0].IntentKey, "42", "Intent key should contain rowid value")
	assert.Contains(t, entries[0].IntentKey, "notes")
}
