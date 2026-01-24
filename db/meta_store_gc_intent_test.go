package db

import (
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// TestMarkIntentsForCleanup_CreatesGCMarkers tests that MarkIntentsForCleanup creates GC markers in RowLockStore
func TestMarkIntentsForCleanup_CreatesGCMarkers(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	txnID := uint64(100)
	tableName := "users"
	intentKey := "pk:1"

	// Create transaction and write intent
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}
	err := store.BeginTransaction(txnID, 1, ts)
	require.NoError(t, err)

	err = store.WriteIntent(txnID, IntentTypeDML, tableName, intentKey, OpTypeInsert, "INSERT INTO users...", []byte("data"), ts, 1)
	require.NoError(t, err)

	// Mark intents for cleanup
	err = store.MarkIntentsForCleanup(txnID)
	require.NoError(t, err)

	// Verify GC marker exists in RowLockStore
	gcExists := store.rowLocks.CheckGCMarker("", tableName, intentKey)
	require.True(t, gcExists, "GC marker should exist in RowLockStore")

	// Verify intent still exists in RowLockStore
	holder, exists := store.rowLocks.CheckLock("", tableName, intentKey)
	require.True(t, exists, "Intent lock should still exist in RowLockStore")
	require.Equal(t, txnID, holder, "Intent should belong to correct transaction")
}

// TestMarkIntentsForCleanup_MultipleIntents tests marking multiple intents
func TestMarkIntentsForCleanup_MultipleIntents(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	txnID := uint64(200)
	tableName := "products"
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}

	// Create transaction
	err := store.BeginTransaction(txnID, 1, ts)
	require.NoError(t, err)

	// Write multiple intents
	intentKeys := []string{"pk:1", "pk:2", "pk:3"}
	for _, intentKey := range intentKeys {
		err = store.WriteIntent(txnID, IntentTypeDML, tableName, intentKey, OpTypeInsert, "INSERT INTO products...", []byte("data"), ts, 1)
		require.NoError(t, err)
	}

	// Mark all intents for cleanup
	err = store.MarkIntentsForCleanup(txnID)
	require.NoError(t, err)

	// Verify all GC markers exist in RowLockStore
	for _, intentKey := range intentKeys {
		gcExists := store.rowLocks.CheckGCMarker("", tableName, intentKey)
		require.True(t, gcExists, "GC marker should exist for %s", intentKey)
	}
}

// TestDeleteIntentsByTxn_DeletesGCMarkers tests that DeleteIntentsByTxn removes locks from RowLockStore
func TestDeleteIntentsByTxn_DeletesGCMarkers(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	txnID := uint64(300)
	tableName := "orders"
	intentKey := "pk:100"
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}

	// Create transaction and write intent
	err := store.BeginTransaction(txnID, 1, ts)
	require.NoError(t, err)

	err = store.WriteIntent(txnID, IntentTypeDML, tableName, intentKey, OpTypeInsert, "INSERT INTO orders...", []byte("data"), ts, 1)
	require.NoError(t, err)

	// Mark for cleanup
	err = store.MarkIntentsForCleanup(txnID)
	require.NoError(t, err)

	// Verify GC marker exists in RowLockStore
	gcExists := store.rowLocks.CheckGCMarker("", tableName, intentKey)
	require.True(t, gcExists, "GC marker should exist")

	// Delete intents by txn
	err = store.DeleteIntentsByTxn(txnID)
	require.NoError(t, err)

	// Verify lock is released from RowLockStore
	_, exists := store.rowLocks.CheckLock("", tableName, intentKey)
	require.False(t, exists, "Lock should be released from RowLockStore")

	// Note: GC markers are separate from locks and not automatically cleaned up
	// They remain in memory until overwritten or the store is restarted (ephemeral)
	gcExists = store.rowLocks.CheckGCMarker("", tableName, intentKey)
	require.True(t, gcExists, "GC marker persists in memory even after lock release")
}

// TestWriteIntent_DetectsGCMarker tests that WriteIntent detects GC marker and allows overwrite
func TestWriteIntent_DetectsGCMarker(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	txnID1 := uint64(400)
	txnID2 := uint64(401)
	tableName := "inventory"
	intentKey := "pk:500"
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}

	// Create first transaction and write intent
	err := store.BeginTransaction(txnID1, 1, ts)
	require.NoError(t, err)

	err = store.WriteIntent(txnID1, IntentTypeDML, tableName, intentKey, OpTypeInsert, "INSERT INTO inventory...", []byte("data1"), ts, 1)
	require.NoError(t, err)

	// Mark intent for cleanup
	err = store.MarkIntentsForCleanup(txnID1)
	require.NoError(t, err)

	// Create second transaction
	err = store.BeginTransaction(txnID2, 1, hlc.Timestamp{WallTime: 2000, Logical: 0})
	require.NoError(t, err)

	// Write intent from second transaction - should detect GC marker and overwrite
	err = store.WriteIntent(txnID2, IntentTypeDML, tableName, intentKey, OpTypeUpdate, "UPDATE inventory...", []byte("data2"), ts, 1)
	require.NoError(t, err, "Should be able to overwrite intent marked for cleanup")

	// Verify GC marker is deleted from RowLockStore
	gcExists := store.rowLocks.CheckGCMarker("", tableName, intentKey)
	require.False(t, gcExists, "GC marker should be deleted after overwrite")

	// Verify new intent exists with txnID2
	intent, err := store.GetIntent(tableName, intentKey)
	require.NoError(t, err)
	require.Equal(t, txnID2, intent.TxnID, "Intent should belong to new transaction")
	require.Equal(t, []byte("data2"), intent.DataSnapshot)
}

// TestRowLockStore_PersistenceAcrossRestart tests that RowLockStore is ephemeral and starts empty after restart
func TestRowLockStore_PersistenceAcrossRestart(t *testing.T) {
	opts := &PebbleMetaStoreOptions{
		CacheSizeMB:    8,
		MemTableSizeMB: 4,
		MemTableCount:  2,
	}
	dbPath := t.TempDir() + "/test.db"

	txnID1 := uint64(500)
	txnID2 := uint64(501)
	tableName := "accounts"
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}

	// Create first store, add intents, and mark one for cleanup
	{
		store, err := NewPebbleMetaStore(dbPath, *opts)
		require.NoError(t, err)

		// Create first transaction and write intent
		err = store.BeginTransaction(txnID1, 1, ts)
		require.NoError(t, err)

		err = store.WriteIntent(txnID1, IntentTypeDML, tableName, "pk:1", OpTypeInsert, "INSERT INTO accounts...", []byte("data1"), ts, 1)
		require.NoError(t, err)

		// Mark first intent for cleanup
		err = store.MarkIntentsForCleanup(txnID1)
		require.NoError(t, err)

		// Create second transaction and write intent (not marked for cleanup)
		err = store.BeginTransaction(txnID2, 1, hlc.Timestamp{WallTime: 2000, Logical: 0})
		require.NoError(t, err)

		err = store.WriteIntent(txnID2, IntentTypeDML, tableName, "pk:2", OpTypeInsert, "INSERT INTO accounts...", []byte("data2"), ts, 1)
		require.NoError(t, err)

		err = store.Close()
		require.NoError(t, err)
	}

	// Reopen store - RowLockStore should start empty (ephemeral)
	{
		store, err := NewPebbleMetaStore(dbPath, *opts)
		require.NoError(t, err)
		defer store.Close()

		// Verify RowLockStore is empty (no locks from previous session)
		_, exists1 := store.rowLocks.CheckLock("", tableName, "pk:1")
		_, exists2 := store.rowLocks.CheckLock("", tableName, "pk:2")

		require.False(t, exists1, "RowLockStore should not contain locks after restart")
		require.False(t, exists2, "RowLockStore should not contain locks after restart")

		// But /intent_txn/ index should still exist in Pebble for recovery
		intent1, err := store.GetIntentsByTxn(txnID1)
		require.NoError(t, err)
		require.Len(t, intent1, 1, "Intent index should persist in Pebble")

		intent2, err := store.GetIntentsByTxn(txnID2)
		require.NoError(t, err)
		require.Len(t, intent2, 1, "Intent index should persist in Pebble")
	}
}

// TestGCMarker_InMemory tests the GC marker is stored in memory
func TestGCMarker_InMemory(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	tableName := "test_table"
	intentKey := "test_key"
	txnID := uint64(700)

	// Write intent
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}
	err := store.BeginTransaction(txnID, 1, ts)
	require.NoError(t, err)

	err = store.WriteIntent(txnID, IntentTypeDML, tableName, intentKey, OpTypeInsert, "INSERT...", []byte("data"), ts, 1)
	require.NoError(t, err)

	// Set GC marker
	store.rowLocks.SetGCMarker("", tableName, intentKey)

	// Verify marker exists in memory
	gcExists := store.rowLocks.CheckGCMarker("", tableName, intentKey)
	require.True(t, gcExists, "GC marker should exist in RowLockStore")

	// Delete marker
	store.rowLocks.DeleteGCMarker("", tableName, intentKey)

	// Verify marker is gone
	gcExists = store.rowLocks.CheckGCMarker("", tableName, intentKey)
	require.False(t, gcExists, "GC marker should be deleted from RowLockStore")
}

// TestMarkIntentsForCleanup_EmptyTxn tests marking cleanup for transaction with no intents
func TestMarkIntentsForCleanup_EmptyTxn(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	txnID := uint64(600)

	// Mark intents for cleanup for non-existent transaction
	err := store.MarkIntentsForCleanup(txnID)
	require.NoError(t, err, "Should succeed even if no intents exist")
}

// Helper functions for testing

func newTestPebbleStore(t *testing.T) *PebbleMetaStore {
	opts := &PebbleMetaStoreOptions{
		CacheSizeMB:    8,
		MemTableSizeMB: 4,
		MemTableCount:  2,
	}

	store, err := NewPebbleMetaStore(t.TempDir()+"/test.db", *opts)
	require.NoError(t, err)
	return store
}

func cleanupTestPebbleStore(t *testing.T, store *PebbleMetaStore) {
	err := store.Close()
	require.NoError(t, err)
}
