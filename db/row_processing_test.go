//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodeValues encodes a map of column values to msgpack-encoded bytes
func encodeValues(values map[string]interface{}) (map[string][]byte, error) {
	encoded := make(map[string][]byte, len(values))
	for key, val := range values {
		data, err := encoding.Marshal(val)
		if err != nil {
			return nil, err
		}
		encoded[key] = data
	}
	return encoded, nil
}

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
	newVals, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeInsert),
		IntentKey: "users:1",
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	entries, err := session.GetIntentEntries()
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
	oldVals, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	newVals, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice_updated",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeUpdate),
		IntentKey: "users:1",
		OldValues: oldVals,
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	entries, err := session.GetIntentEntries()
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
	oldVals, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeDelete),
		IntentKey: "users:1",
		OldValues: oldVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	entries, err := session.GetIntentEntries()
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
	newVals, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeInsert),
		IntentKey: "users:1",
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	// ProcessCapturedRows acquires locks
	err = session.ProcessCapturedRows()
	require.NoError(t, err)

	// Now check that the lock was acquired
	lockTxnID, err := metaStore.GetCDCRowLock("users", "users:1")
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
	newVals1, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow1 := EncodedCapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_INSERT,
		IntentKey: "users:1",
		NewValues: newVals1,
	}
	rowData1, err := encoding.Marshal(&encodedRow1)
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
	oldVals2, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	newVals2, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "bob",
	})
	require.NoError(t, err)
	encodedRow2 := EncodedCapturedRow{
		Table:     "users",
		Op:        sqlite3.SQLITE_UPDATE,
		IntentKey: "users:1",
		OldValues: oldVals2,
		NewValues: newVals2,
	}
	rowData2, err := encoding.Marshal(&encodedRow2)
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
	newVals1, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow1 := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeInsert),
		IntentKey: "users:1",
		NewValues: newVals1,
	}
	rowData1, err := encoding.Marshal(&encodedRow1)
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
	newVals2, err := encodeValues(map[string]interface{}{
		"id":   int64(2),
		"name": "bob",
	})
	require.NoError(t, err)
	encodedRow2 := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeInsert),
		IntentKey: "users:2",
		NewValues: newVals2,
	}
	rowData2, err := encoding.Marshal(&encodedRow2)
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
	newVals1, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow1 := EncodedCapturedRow{
		Table:     "table_a",
		Op:        uint8(OpTypeInsert),
		IntentKey: "table_a:1",
		NewValues: newVals1,
	}
	rowData1, err := encoding.Marshal(&encodedRow1)
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
	newVals2, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "bob",
	})
	require.NoError(t, err)
	encodedRow2 := EncodedCapturedRow{
		Table:     "table_b",
		Op:        uint8(OpTypeInsert),
		IntentKey: "table_b:1",
		NewValues: newVals2,
	}
	rowData2, err := encoding.Marshal(&encodedRow2)
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
	newVals, err := encodeValues(map[string]interface{}{
		"id":    int64(1),
		"name":  "alice",
		"email": "alice@example.com",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeInsert),
		IntentKey: "users:1",
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	entries, err := session.GetIntentEntries()
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

	// Create test rows using EncodedCapturedRow format
	testCases := []struct {
		id   int64
		name string
	}{
		{1, "alice"},
		{2, "bob"},
		{3, "charlie"},
	}

	for i, tc := range testCases {
		newVals, err := encodeValues(map[string]interface{}{
			"id":   tc.id,
			"name": tc.name,
		})
		require.NoError(t, err)
		encodedRow := EncodedCapturedRow{
			Table:     "users",
			Op:        uint8(OpTypeInsert),
			IntentKey: fmt.Sprintf("users:%d", tc.id),
			NewValues: newVals,
		}
		rowData, err := encoding.Marshal(&encodedRow)
		require.NoError(t, err)
		err = metaStore.WriteCapturedRow(txnID, uint64(i+1), rowData)
		require.NoError(t, err)
	}

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	entries, err := session.GetIntentEntries()
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

func TestProcessRows_CapturedRowsRetained(t *testing.T) {
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
	newVals, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeInsert),
		IntentKey: "users:1",
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
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
	require.True(t, cursor.Next(), "Captured rows should be retained for replication until GC")
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
	newVals, err := encodeValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "nonexistent_table",
		Op:        uint8(OpTypeInsert),
		IntentKey: "nonexistent_table:1",
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	err = session.ProcessCapturedRows()
	require.NoError(t, err, "ProcessCapturedRows should succeed - schema check happens at capture time")

	cursor, err := metaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	require.True(t, cursor.Next(), "Captured row should still be accessible after ProcessCapturedRows")
	cursor.Close()
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
	newVals, err := encodeValues(map[string]interface{}{
		"user_id":  int64(42),
		"order_id": int64(100),
		"amount":   99.99,
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "orders",
		Op:        uint8(OpTypeInsert),
		IntentKey: "orders:42:100",
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	entries, err := session.GetIntentEntries()
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
	newVals, err := encodeValues(map[string]interface{}{
		"content": "My important note",
	})
	require.NoError(t, err)
	encodedRow := EncodedCapturedRow{
		Table:     "notes",
		Op:        uint8(OpTypeInsert),
		IntentKey: "notes:42",
		NewValues: newVals,
	}
	rowData, err := encoding.Marshal(&encodedRow)
	require.NoError(t, err)
	err = metaStore.WriteCapturedRow(txnID, 1, rowData)
	require.NoError(t, err)

	session := &EphemeralHookSession{
		txnID:       txnID,
		metaStore:   metaStore,
		schemaCache: replicatedDB.schemaCache,
	}

	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	assert.Contains(t, entries[0].IntentKey, "42", "Intent key should contain rowid value")
	assert.Contains(t, entries[0].IntentKey, "notes")
}
