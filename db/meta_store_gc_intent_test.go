package db

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// TestMarkIntentsForCleanup_CreatesGCMarkers tests that MarkIntentsForCleanup creates GC markers
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

	// Verify GC marker exists
	gcKey := pebbleGCIntentKey(tableName, intentKey)
	_, closer, err := store.db.Get(gcKey)
	require.NoError(t, err, "GC marker should exist")
	closer.Close()

	// Verify intent still exists (not deleted, just marked)
	intentKeyBytes := pebbleIntentKey(tableName, intentKey)
	_, closer, err = store.db.Get(intentKeyBytes)
	require.NoError(t, err, "Intent should still exist")
	closer.Close()
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

	// Verify all GC markers exist
	for _, intentKey := range intentKeys {
		gcKey := pebbleGCIntentKey(tableName, intentKey)
		_, closer, err := store.db.Get(gcKey)
		require.NoError(t, err, "GC marker should exist for %s", intentKey)
		closer.Close()
	}
}

// TestDeleteIntentsByTxn_DeletesGCMarkers tests that DeleteIntentsByTxn deletes GC markers
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

	// Verify GC marker exists
	gcKey := pebbleGCIntentKey(tableName, intentKey)
	_, closer, err := store.db.Get(gcKey)
	require.NoError(t, err)
	closer.Close()

	// Delete intents by txn
	err = store.DeleteIntentsByTxn(txnID)
	require.NoError(t, err)

	// Verify GC marker is deleted
	_, _, err = store.db.Get(gcKey)
	require.ErrorIs(t, err, pebble.ErrNotFound, "GC marker should be deleted")

	// Verify intent is also deleted
	intentKeyBytes := pebbleIntentKey(tableName, intentKey)
	_, _, err = store.db.Get(intentKeyBytes)
	require.ErrorIs(t, err, pebble.ErrNotFound, "Intent should be deleted")
}

// TestWriteIntentSlowPath_DetectsGCMarker tests that writeIntentSlowPath detects GC marker before unmarshaling
func TestWriteIntentSlowPath_DetectsGCMarker(t *testing.T) {
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

	// Verify GC marker is deleted
	gcKey := pebbleGCIntentKey(tableName, intentKey)
	_, _, err = store.db.Get(gcKey)
	require.ErrorIs(t, err, pebble.ErrNotFound, "GC marker should be deleted after overwrite")

	// Verify new intent exists with txnID2
	intent, err := store.GetIntent(tableName, intentKey)
	require.NoError(t, err)
	require.Equal(t, txnID2, intent.TxnID, "Intent should belong to new transaction")
	require.Equal(t, []byte("data2"), intent.DataSnapshot)
}

// TestRebuildIntentFilter_SkipsGCMarkedIntents tests that rebuildIntentFilter skips intents with GC markers
func TestRebuildIntentFilter_SkipsGCMarkedIntents(t *testing.T) {
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

	// Reopen store to trigger rebuild
	{
		store, err := NewPebbleMetaStore(dbPath, *opts)
		require.NoError(t, err)
		defer store.Close()

		// Verify filter contains only txnID2 (not txnID1 which is marked for cleanup)
		tbHash1 := ComputeIntentHash(tableName, "pk:1")
		tbHash2 := ComputeIntentHash(tableName, "pk:2")

		exists1 := store.intentFilter.Check(tbHash1)
		exists2 := store.intentFilter.Check(tbHash2)

		require.False(t, exists1, "Filter should not contain GC-marked intent")
		require.True(t, exists2, "Filter should contain non-marked intent")
	}
}

// TestGCMarkerKey_Format tests the GC marker key format matches intent key format
func TestGCMarkerKey_Format(t *testing.T) {
	tableName := "test_table"
	intentKey := "test_key"

	gcKey := pebbleGCIntentKey(tableName, intentKey)
	require.NotNil(t, gcKey)

	// Verify key starts with GC prefix
	require.True(t, len(gcKey) > len(pebblePrefixGCIntent))
	require.Equal(t, pebblePrefixGCIntent, string(gcKey[:len(pebblePrefixGCIntent)]))

	// Key should contain table name length prefix (2 bytes) + table name + intent key
	expectedLen := len(pebblePrefixGCIntent) + 2 + len(tableName) + len(intentKey)
	require.Equal(t, expectedLen, len(gcKey))
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
