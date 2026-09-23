package db

import (
	"fmt"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// TestDeleteIntentsByTxn_ReleasesLocks tests that DeleteIntentsByTxn removes locks from RowLockStore
func TestDeleteIntentsByTxn_ReleasesLocks(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	txnID := uint64(300)
	tableName := "orders"
	intentKey := "pk:100"
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}

	require.NoError(t, store.BeginTransaction(txnID, 1, ts))
	require.NoError(t, store.WriteIntent(txnID, IntentTypeDML, tableName, intentKey, OpTypeInsert, "INSERT INTO orders...", []byte("data"), ts, 1))

	require.NoError(t, store.DeleteIntentsByTxn(txnID))

	_, exists := store.rowLocks.CheckLock("", tableName, intentKey)
	require.False(t, exists, "Lock should be released from RowLockStore")
}

// TestWriteIntent_OverwritesACommittedHolder tests that an intent whose
// transaction has committed, but whose lock is still held, is overwritten by
// the next writer.
func TestWriteIntent_OverwritesACommittedHolder(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	txnID1 := uint64(400)
	txnID2 := uint64(401)
	tableName := "inventory"
	intentKey := "pk:500"
	ts := hlc.Timestamp{WallTime: 1000, Logical: 0}

	require.NoError(t, store.BeginTransaction(txnID1, 1, ts))
	require.NoError(t, store.WriteIntent(txnID1, IntentTypeDML, tableName, intentKey, OpTypeInsert, "INSERT INTO inventory...", []byte("data1"), ts, 1))
	require.NoError(t, store.CommitTransaction(txnID1, hlc.Timestamp{WallTime: 1500}, nil, "testdb", tableName, 0, 1))

	require.NoError(t, store.BeginTransaction(txnID2, 1, hlc.Timestamp{WallTime: 2000, Logical: 0}))
	require.NoError(t, store.WriteIntent(txnID2, IntentTypeDML, tableName, intentKey, OpTypeUpdate, "UPDATE inventory...", []byte("data2"), ts, 1),
		"a committed holder's intent must be overwritable")

	intent, err := store.GetIntent(tableName, intentKey)
	require.NoError(t, err)
	require.Equal(t, txnID2, intent.TxnID, "Intent should belong to new transaction")
	require.Nil(t, intent.DataSnapshot, "DML row payloads live in CDC segment storage, not write intents")
}

// TestCommittedTransactionsLeaveNoPerRowState: committing leaves nothing behind
// per row written. The store used to keep a GC marker for every row a
// finished transaction had locked, one entry per distinct row ever written.
//
// Mutation: in RowLockStore.ReleaseByTxn, read the reverse index with Load
// instead of LoadAndDelete. "committed transactions left reverse-index
// entries" fires.
func TestCommittedTransactionsLeaveNoPerRowState(t *testing.T) {
	store := newTestPebbleStore(t)
	defer cleanupTestPebbleStore(t, store)

	const commits = 2000
	ts := hlc.Timestamp{WallTime: 1000}
	for i := uint64(1); i <= commits; i++ {
		require.NoError(t, store.BeginTransaction(i, 1, ts))
		require.NoError(t, store.WriteIntent(i, IntentTypeDML, "rows", fmt.Sprintf("pk:%d", i), OpTypeInsert, "", nil, ts, 1))
		require.NoError(t, store.CleanupAfterCommit(i))
	}

	locks, txns, tables := store.rowLocks.Stats()
	require.Zero(t, locks)
	require.Zero(t, txns)
	require.Zero(t, tables)
	require.Zero(t, store.rowLocks.byTxn.Size(), "committed transactions left reverse-index entries")
	rowMap, ok := store.rowLocks.tables.Load(makeTableKey("", "rows"))
	require.True(t, ok)
	require.Zero(t, rowMap.Size(), "committed transactions left row entries")
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

	// Create first store and add intents
	{
		store, err := NewPebbleMetaStore(dbPath, *opts)
		require.NoError(t, err)

		// Create first transaction and write intent
		err = store.BeginTransaction(txnID1, 1, ts)
		require.NoError(t, err)

		err = store.WriteIntent(txnID1, IntentTypeDML, tableName, "pk:1", OpTypeInsert, "INSERT INTO accounts...", []byte("data1"), ts, 1)
		require.NoError(t, err)

		// Create second transaction and write intent
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

		// DML redo is recovered from CDC segment manifests, not /intent_txn/.
		// Row locks and DML intent metadata are intentionally transient.
		intent1, err := store.GetIntentsByTxn(txnID1)
		require.NoError(t, err)
		require.Empty(t, intent1, "DML intent index should not persist in Pebble")

		intent2, err := store.GetIntentsByTxn(txnID2)
		require.NoError(t, err)
		require.Empty(t, intent2, "DML intent index should not persist in Pebble")
	}
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

// BenchmarkWriteIntentThenCleanupAfterCommit measures one DML row lock taken
// and released through the commit path: the row-lock work every replicated
// row write pays at PREPARE and COMMIT.
func BenchmarkWriteIntentThenCleanupAfterCommit(b *testing.B) {
	store, err := NewPebbleMetaStore(b.TempDir()+"/bench.db", PebbleMetaStoreOptions{CacheSizeMB: 8, MemTableSizeMB: 4, MemTableCount: 2})
	require.NoError(b, err)
	defer store.Close()
	ts := hlc.Timestamp{WallTime: 1000}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("pk:%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txnID := uint64(i + 1)
		if err := store.WriteIntent(txnID, IntentTypeDML, "rows", keys[i%len(keys)], OpTypeInsert, "", nil, ts, 1); err != nil {
			b.Fatalf("WriteIntent: %v", err)
		}
		if err := store.CleanupAfterCommit(txnID); err != nil {
			b.Fatalf("CleanupAfterCommit: %v", err)
		}
	}
}
