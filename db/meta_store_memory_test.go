package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

func setupMemoryMetaStore(t *testing.T) (*MemoryMetaStore, func()) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "test_meta.pebble")

	pebble, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:           8,
		MemTableSizeMB:        4,
		MemTableCount:         2,
		L0CompactionThreshold: 2,
		L0StopWrites:          4,
		MaxConcurrentCompact:  1,
		DisableWAL:            true,
	})
	require.NoError(t, err)

	store := NewMemoryMetaStore(pebble)

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return store, cleanup
}

func TestMemoryMetaStore_BeginTransaction(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	nodeID := uint64(1)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Verify immutable record is in Pebble
	immutable, err := store.pebble.readImmutableTxnRecord(txnID)
	require.NoError(t, err)
	require.NotNil(t, immutable)
	require.Equal(t, txnID, immutable.TxnID)
	require.Equal(t, nodeID, immutable.NodeID)
	require.Equal(t, startTS.WallTime, immutable.StartTSWall)

	// Verify status/heartbeat are in memory
	state, found := store.txnStore.Get(txnID)
	require.True(t, found)
	require.Equal(t, TxnStatusPending, state.Status)
	require.Greater(t, state.LastHeartbeat, int64(0))
	require.Equal(t, nodeID, state.NodeID)

	// Verify status NOT in Pebble (heartbeat is in-memory only, no Pebble storage)
	_, err = store.pebble.readTxnStatus(txnID)
	require.Error(t, err)
}

func TestMemoryMetaStore_GetTransaction(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(200)
	nodeID := uint64(2)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 5}

	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Get transaction
	rec, err := store.GetTransaction(txnID)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, txnID, rec.TxnID)
	require.Equal(t, nodeID, rec.NodeID)
	require.Equal(t, TxnStatusPending, rec.Status)
	require.Equal(t, startTS.WallTime, rec.StartTSWall)
	require.Equal(t, startTS.Logical, rec.StartTSLogical)
	require.Greater(t, rec.LastHeartbeat, int64(0))

	// Non-existent transaction
	rec, err = store.GetTransaction(999)
	require.NoError(t, err)
	require.Nil(t, rec)
}

func TestMemoryMetaStore_GetPendingTransactions(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	// Create multiple transactions
	txnIDs := []uint64{300, 301, 302}
	nodeID := uint64(3)

	for _, txnID := range txnIDs {
		startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}
		err := store.BeginTransaction(txnID, nodeID, startTS)
		require.NoError(t, err)
	}

	// Get pending transactions
	pending, err := store.GetPendingTransactions()
	require.NoError(t, err)
	require.Len(t, pending, 3)

	// Verify all are pending
	for _, rec := range pending {
		require.Equal(t, TxnStatusPending, rec.Status)
		require.Contains(t, txnIDs, rec.TxnID)
	}

	// Commit one transaction
	commitTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}
	err = store.CommitTransaction(txnIDs[0], commitTS, []byte("stmt"), "testdb", "table1", 1, 1)
	require.NoError(t, err)

	// Should now have 2 pending
	pending, err = store.GetPendingTransactions()
	require.NoError(t, err)
	require.Len(t, pending, 2)
}

func TestMemoryMetaStore_Heartbeat(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(400)
	nodeID := uint64(4)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Get initial heartbeat
	state1, _ := store.txnStore.Get(txnID)
	initialHeartbeat := state1.LastHeartbeat

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// Update heartbeat
	err = store.Heartbeat(txnID)
	require.NoError(t, err)

	// Verify heartbeat updated in memory
	state2, found := store.txnStore.Get(txnID)
	require.True(t, found)
	require.Greater(t, state2.LastHeartbeat, initialHeartbeat)

	// Verify GetTransaction returns updated heartbeat
	rec, err := store.GetTransaction(txnID)
	require.NoError(t, err)
	require.Equal(t, state2.LastHeartbeat, rec.LastHeartbeat)
}

func TestMemoryMetaStore_CommitTransaction(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(500)
	nodeID := uint64(5)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Commit transaction
	commitTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 10}
	statements := []byte("INSERT INTO test VALUES (1)")
	err = store.CommitTransaction(txnID, commitTS, statements, "testdb", "table1,table2", 42, 1)
	require.NoError(t, err)

	// Verify transaction is removed from memory (committed txns don't need to stay in memory)
	_, found := store.txnStore.Get(txnID)
	require.False(t, found, "Committed transaction should be removed from memory")

	// Verify commit record in Pebble
	commit, err := store.pebble.readCommitRecord(txnID)
	require.NoError(t, err)
	require.NotNil(t, commit)
	require.Equal(t, commitTS.WallTime, commit.CommitTSWall)
	require.Equal(t, commitTS.Logical, commit.CommitTSLogical)
	require.Equal(t, "testdb", commit.DatabaseName)
	require.Equal(t, "table1,table2", commit.TablesInvolved)
	require.Equal(t, uint64(42), commit.RequiredSchemaVersion)

	// Verify GetTransaction returns full record
	rec, err := store.GetTransaction(txnID)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, TxnStatusCommitted, rec.Status)
	require.Equal(t, commitTS.WallTime, rec.CommitTSWall)
	require.Equal(t, "testdb", rec.DatabaseName)
}

func TestMemoryMetaStore_AbortTransaction(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(600)
	nodeID := uint64(6)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Abort transaction
	err = store.AbortTransaction(txnID)
	require.NoError(t, err)

	// Verify removed from memory
	_, found := store.txnStore.Get(txnID)
	require.False(t, found)

	// Verify removed from Pebble
	immutable, err := store.pebble.readImmutableTxnRecord(txnID)
	require.NoError(t, err)
	require.Nil(t, immutable)

	// GetTransaction should return nil
	rec, err := store.GetTransaction(txnID)
	require.NoError(t, err)
	require.Nil(t, rec)
}

func TestMemoryMetaStore_CDCRowLocks(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(700)
	tableName := "users"
	intentKey := "pk:123"

	// Acquire lock
	err := store.AcquireCDCRowLock(txnID, tableName, intentKey)
	require.NoError(t, err)

	// Verify lock is held
	holder, err := store.GetCDCRowLock(tableName, intentKey)
	require.NoError(t, err)
	require.Equal(t, txnID, holder)

	// Try to acquire with different txn - should fail
	err = store.AcquireCDCRowLock(999, tableName, intentKey)
	require.Error(t, err)
	var lockErr ErrCDCRowLocked
	require.ErrorAs(t, err, &lockErr)

	// Release lock
	err = store.ReleaseCDCRowLock(tableName, intentKey, txnID)
	require.NoError(t, err)

	// Verify lock is released
	holder, err = store.GetCDCRowLock(tableName, intentKey)
	require.NoError(t, err)
	require.Equal(t, uint64(0), holder)

	// Verify NOT in Pebble
	holderPebble, err := store.pebble.GetCDCRowLock(tableName, intentKey)
	require.NoError(t, err)
	require.Equal(t, uint64(0), holderPebble)
}

func TestMemoryMetaStore_ReleaseCDCRowLocksByTxn(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(800)

	// Acquire multiple locks
	locks := []struct {
		table string
		key   string
	}{
		{"users", "pk:1"},
		{"users", "pk:2"},
		{"orders", "pk:100"},
	}

	for _, lock := range locks {
		err := store.AcquireCDCRowLock(txnID, lock.table, lock.key)
		require.NoError(t, err)
	}

	// Verify all locks are held
	for _, lock := range locks {
		holder, err := store.GetCDCRowLock(lock.table, lock.key)
		require.NoError(t, err)
		require.Equal(t, txnID, holder)
	}

	// Release all locks by txn
	err := store.ReleaseCDCRowLocksByTxn(txnID)
	require.NoError(t, err)

	// Verify all locks are released
	for _, lock := range locks {
		holder, err := store.GetCDCRowLock(lock.table, lock.key)
		require.NoError(t, err)
		require.Equal(t, uint64(0), holder)
	}
}

func TestMemoryMetaStore_CleanupStaleTransactions(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	// Create transactions with different heartbeat times
	txn1 := uint64(900)
	txn2 := uint64(901)
	txn3 := uint64(902)
	nodeID := uint64(9)

	// Create all transactions
	for _, txnID := range []uint64{txn1, txn2, txn3} {
		startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}
		err := store.BeginTransaction(txnID, nodeID, startTS)
		require.NoError(t, err)
	}

	// Manually set heartbeat for txn1 to be stale
	state1, _ := store.txnStore.Get(txn1)
	state1.LastHeartbeat = time.Now().Add(-2 * time.Hour).UnixNano()

	// txn2 is fresh
	err := store.Heartbeat(txn2)
	require.NoError(t, err)

	// txn3 is also stale
	state3, _ := store.txnStore.Get(txn3)
	state3.LastHeartbeat = time.Now().Add(-3 * time.Hour).UnixNano()

	// Cleanup stale transactions (older than 1 hour)
	cleaned, err := store.CleanupStaleTransactions(1 * time.Hour)
	require.NoError(t, err)
	require.Equal(t, 2, cleaned) // txn1 and txn3

	// Verify txn1 and txn3 are removed
	_, found1 := store.txnStore.Get(txn1)
	require.False(t, found1)
	_, found3 := store.txnStore.Get(txn3)
	require.False(t, found3)

	// Verify txn2 is still there
	state2, found2 := store.txnStore.Get(txn2)
	require.True(t, found2)
	require.Equal(t, TxnStatusPending, state2.Status)
}

func TestMemoryMetaStore_DelegationMethods(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	// Test that delegation methods work correctly
	t.Run("GetSchemaVersion", func(t *testing.T) {
		version, err := store.GetSchemaVersion("testdb")
		require.NoError(t, err)
		require.Equal(t, int64(0), version)
	})

	t.Run("UpdateSchemaVersion", func(t *testing.T) {
		err := store.UpdateSchemaVersion("testdb", 1, "CREATE TABLE test", 1)
		require.NoError(t, err)

		version, err := store.GetSchemaVersion("testdb")
		require.NoError(t, err)
		require.Equal(t, int64(1), version)
	})

	t.Run("GetNextSeqNum", func(t *testing.T) {
		seq1, err := store.GetNextSeqNum(1)
		require.NoError(t, err)
		require.Greater(t, seq1, uint64(0))

		seq2, err := store.GetNextSeqNum(1)
		require.NoError(t, err)
		require.Equal(t, seq1+1, seq2)
	})
}

func TestMemoryMetaStore_MemoryVsPebbleIsolation(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(1000)
	nodeID := uint64(10)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	// Begin transaction
	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Verify memory has status/heartbeat
	state, found := store.txnStore.Get(txnID)
	require.True(t, found)
	require.Equal(t, TxnStatusPending, state.Status)

	// Verify Pebble does NOT have status key (heartbeat is in-memory only)
	_, err = store.pebble.readTxnStatus(txnID)
	require.Error(t, err, "Status should not be in Pebble")

	// Verify Pebble HAS immutable record
	immutable, err := store.pebble.readImmutableTxnRecord(txnID)
	require.NoError(t, err)
	require.NotNil(t, immutable)

	// Update heartbeat (in-memory only)
	err = store.Heartbeat(txnID)
	require.NoError(t, err)

	// Commit transaction
	commitTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}
	err = store.CommitTransaction(txnID, commitTS, []byte("stmt"), "db", "table", 1, 1)
	require.NoError(t, err)

	// Verify commit record IS in Pebble
	commit, err := store.pebble.readCommitRecord(txnID)
	require.NoError(t, err)
	require.NotNil(t, commit)

	// Verify transaction is removed from memory after commit
	_, found = store.txnStore.Get(txnID)
	require.False(t, found, "Committed transaction should be removed from memory")
}

func TestReconstructFromPebble_CleansOrphanedCDCRaw(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(1100)
	nodeID := uint64(11)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	// Begin transaction (writes immutable record to Pebble)
	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Write CDC raw data directly to Pebble (simulating CDC capture during transaction)
	err = store.WriteCapturedRow(txnID, 1, []byte("row1"))
	require.NoError(t, err)
	err = store.WriteCapturedRow(txnID, 2, []byte("row2"))
	require.NoError(t, err)

	// Verify CDC raw data exists
	cursor, err := store.IterateCapturedRows(txnID)
	require.NoError(t, err)
	count := 0
	for cursor.Next() {
		count++
	}
	require.NoError(t, cursor.Err())
	cursor.Close()
	require.Equal(t, 2, count)

	// Simulate crash: Remove from memory but keep Pebble data (no commit record)
	store.txnStore.Remove(txnID)

	// Create a new store (simulating restart)
	newStore := NewMemoryMetaStore(store.pebble)

	// Run reconstruction
	err = newStore.ReconstructFromPebble()
	require.NoError(t, err)

	// Verify orphaned CDC raw data was deleted
	cursor, err = newStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	count = 0
	for cursor.Next() {
		count++
	}
	require.NoError(t, cursor.Err())
	cursor.Close()
	require.Equal(t, 0, count, "Orphaned CDC raw data should be deleted")

	// Verify immutable record still exists
	immutable, err := newStore.pebble.readImmutableTxnRecord(txnID)
	require.NoError(t, err)
	require.NotNil(t, immutable, "Immutable record should still exist")
}

func TestReconstructFromPebble_KeepsCommittedCDCRaw(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(1200)
	nodeID := uint64(12)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}
	commitTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 1}

	// Begin transaction
	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Write CDC raw data
	err = store.WriteCapturedRow(txnID, 1, []byte("committed_row1"))
	require.NoError(t, err)
	err = store.WriteCapturedRow(txnID, 2, []byte("committed_row2"))
	require.NoError(t, err)

	// Commit transaction (writes commit record to Pebble)
	err = store.CommitTransaction(txnID, commitTS, []byte("stmt"), "testdb", "table1", 1, 2)
	require.NoError(t, err)

	// Verify CDC raw data exists
	cursor, err := store.IterateCapturedRows(txnID)
	require.NoError(t, err)
	count := 0
	for cursor.Next() {
		count++
	}
	require.NoError(t, cursor.Err())
	cursor.Close()
	require.Equal(t, 2, count)

	// Simulate restart: Create new store
	newStore := NewMemoryMetaStore(store.pebble)

	// Run reconstruction
	err = newStore.ReconstructFromPebble()
	require.NoError(t, err)

	// Verify committed CDC raw data is KEPT (needed for replication)
	cursor, err = newStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	count = 0
	for cursor.Next() {
		count++
	}
	require.NoError(t, cursor.Err())
	cursor.Close()
	require.Equal(t, 2, count, "Committed CDC raw data should be kept for replication")

	// Verify commit record exists
	commit, err := newStore.pebble.readCommitRecord(txnID)
	require.NoError(t, err)
	require.NotNil(t, commit)
	require.Equal(t, uint32(2), commit.RowCount)
}

func TestReconstructFromPebble_MultipleTransactions(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	// Create 3 transactions: one orphaned, one committed, one orphaned
	txn1 := uint64(1300)
	txn2 := uint64(1301)
	txn3 := uint64(1302)
	nodeID := uint64(13)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	// Transaction 1: Orphaned (no commit)
	err := store.BeginTransaction(txn1, nodeID, startTS)
	require.NoError(t, err)
	err = store.WriteCapturedRow(txn1, 1, []byte("orphan1"))
	require.NoError(t, err)
	store.txnStore.Remove(txn1) // Simulate crash

	// Transaction 2: Committed
	err = store.BeginTransaction(txn2, nodeID, startTS)
	require.NoError(t, err)
	err = store.WriteCapturedRow(txn2, 1, []byte("committed1"))
	require.NoError(t, err)
	commitTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 1}
	err = store.CommitTransaction(txn2, commitTS, []byte("stmt"), "testdb", "table1", 1, 1)
	require.NoError(t, err)

	// Transaction 3: Orphaned (no commit)
	err = store.BeginTransaction(txn3, nodeID, startTS)
	require.NoError(t, err)
	err = store.WriteCapturedRow(txn3, 1, []byte("orphan2"))
	require.NoError(t, err)
	store.txnStore.Remove(txn3) // Simulate crash

	// Create new store (simulating restart)
	newStore := NewMemoryMetaStore(store.pebble)

	// Run reconstruction
	err = newStore.ReconstructFromPebble()
	require.NoError(t, err)

	// Verify txn1 orphaned data deleted
	cursor, err := newStore.IterateCapturedRows(txn1)
	require.NoError(t, err)
	count := 0
	for cursor.Next() {
		count++
	}
	cursor.Close()
	require.Equal(t, 0, count, "Orphaned txn1 CDC data should be deleted")

	// Verify txn2 committed data kept
	cursor, err = newStore.IterateCapturedRows(txn2)
	require.NoError(t, err)
	count = 0
	for cursor.Next() {
		count++
	}
	cursor.Close()
	require.Equal(t, 1, count, "Committed txn2 CDC data should be kept")

	// Verify txn3 orphaned data deleted
	cursor, err = newStore.IterateCapturedRows(txn3)
	require.NoError(t, err)
	count = 0
	for cursor.Next() {
		count++
	}
	cursor.Close()
	require.Equal(t, 0, count, "Orphaned txn3 CDC data should be deleted")
}

func TestReconstructFromPebble_EmptyDatabase(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	// Run reconstruction on empty database
	err := store.ReconstructFromPebble()
	require.NoError(t, err)
}

func TestReconstructFromPebble_NoCDCData(t *testing.T) {
	store, cleanup := setupMemoryMetaStore(t)
	defer cleanup()

	txnID := uint64(1400)
	nodeID := uint64(14)
	startTS := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 0}

	// Begin transaction but don't write any CDC data
	err := store.BeginTransaction(txnID, nodeID, startTS)
	require.NoError(t, err)

	// Simulate crash
	store.txnStore.Remove(txnID)

	// Create new store
	newStore := NewMemoryMetaStore(store.pebble)

	// Run reconstruction (should not fail)
	err = newStore.ReconstructFromPebble()
	require.NoError(t, err)
}
