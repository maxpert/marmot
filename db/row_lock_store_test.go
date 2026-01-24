package db

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRowLockStore(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	require.NotNil(t, store)
	require.NotNil(t, store.tables)
	require.NotNil(t, store.gc)
	require.NotNil(t, store.byTxn)
}

func TestAcquireLock_Success(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	existingTxn, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)
	require.Equal(t, uint64(100), existingTxn)

	holder := store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, uint64(100), holder)
}

func TestAcquireLock_Conflict_SameTxn(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	existingTxn, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)
	require.Equal(t, uint64(100), existingTxn)

	// Same txn re-acquiring should succeed
	existingTxn2, acquired2 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired2)
	require.Equal(t, uint64(100), existingTxn2)
}

func TestAcquireLock_Conflict_DifferentTxn(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	existingTxn, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)
	require.Equal(t, uint64(100), existingTxn)

	// Different txn trying to acquire should fail
	existingTxn2, acquired2 := store.AcquireLock("db1", "table1", "row1", 200)
	require.False(t, acquired2)
	require.Equal(t, uint64(100), existingTxn2)

	// Original lock should still be held by txn 100
	holder := store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, uint64(100), holder)
}

func TestReleaseLock_Exists(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)

	store.ReleaseLock("db1", "table1", "row1")

	holder := store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, uint64(0), holder)

	// Should be able to acquire again with different txn
	_, acquired2 := store.AcquireLock("db1", "table1", "row1", 200)
	require.True(t, acquired2)
}

func TestReleaseLock_NotExists(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	// Should not panic
	require.NotPanics(t, func() {
		store.ReleaseLock("db1", "table1", "row1")
	})
}

func TestCheckLock_Exists(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)

	txnID, exists := store.CheckLock("db1", "table1", "row1")
	require.True(t, exists)
	require.Equal(t, uint64(100), txnID)
}

func TestCheckLock_NotExists(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	txnID, exists := store.CheckLock("db1", "table1", "row1")
	require.False(t, exists)
	require.Equal(t, uint64(0), txnID)
}

func TestGetLockHolder(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	// No lock
	holder := store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, uint64(0), holder)

	// With lock
	_, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)

	holder = store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, uint64(100), holder)
}

func TestSetGCMarker(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	store.SetGCMarker("db1", "table1", "row1")

	hasMarker := store.CheckGCMarker("db1", "table1", "row1")
	require.True(t, hasMarker)
}

func TestCheckGCMarker_Exists(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	store.SetGCMarker("db1", "table1", "row1")

	hasMarker := store.CheckGCMarker("db1", "table1", "row1")
	require.True(t, hasMarker)
}

func TestCheckGCMarker_NotExists(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	hasMarker := store.CheckGCMarker("db1", "table1", "row1")
	require.False(t, hasMarker)
}

func TestDeleteGCMarker(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	store.SetGCMarker("db1", "table1", "row1")
	require.True(t, store.CheckGCMarker("db1", "table1", "row1"))

	store.DeleteGCMarker("db1", "table1", "row1")
	require.False(t, store.CheckGCMarker("db1", "table1", "row1"))

	// Deleting non-existent marker should not panic
	require.NotPanics(t, func() {
		store.DeleteGCMarker("db1", "table1", "row2")
	})
}

func TestReleaseByTxn_SingleLock(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	existingTxn, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)
	require.Equal(t, uint64(100), existingTxn)

	store.ReleaseByTxn(100)

	holder := store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, uint64(0), holder)
}

func TestReleaseByTxn_MultipleLocks(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	_, acquired2 := store.AcquireLock("db1", "table1", "row2", 100)
	require.True(t, acquired2)

	_, acquired3 := store.AcquireLock("db1", "table2", "row3", 100)
	require.True(t, acquired3)

	_, acquired4 := store.AcquireLock("db2", "table1", "row4", 100)
	require.True(t, acquired4)

	// Different txn should not be affected
	_, acquired5 := store.AcquireLock("db1", "table1", "row5", 200)
	require.True(t, acquired5)

	store.ReleaseByTxn(100)

	// All locks for txn 100 should be released
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row2"))
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table2", "row3"))
	require.Equal(t, uint64(0), store.GetLockHolder("db2", "table1", "row4"))

	// Lock for txn 200 should still be held
	require.Equal(t, uint64(200), store.GetLockHolder("db1", "table1", "row5"))
}

func TestReleaseByTxn_NoLocks(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	// Should not panic
	require.NotPanics(t, func() {
		store.ReleaseByTxn(999)
	})
}

func TestReleaseByTable_SingleTable(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)

	store.ReleaseByTable("db1", "table1")

	holder := store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, uint64(0), holder)
}

func TestReleaseByTable_MultipleRows(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	_, acquired2 := store.AcquireLock("db1", "table1", "row2", 200)
	require.True(t, acquired2)

	_, acquired3 := store.AcquireLock("db1", "table1", "row3", 300)
	require.True(t, acquired3)

	store.ReleaseByTable("db1", "table1")

	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row2"))
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row3"))
}

func TestReleaseByTable_OtherTablesUnaffected(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	_, acquired2 := store.AcquireLock("db1", "table2", "row2", 200)
	require.True(t, acquired2)

	_, acquired3 := store.AcquireLock("db2", "table1", "row3", 300)
	require.True(t, acquired3)

	store.ReleaseByTable("db1", "table1")

	// table1 in db1 should be released
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))

	// Other tables should be unaffected
	require.Equal(t, uint64(200), store.GetLockHolder("db1", "table2", "row2"))
	require.Equal(t, uint64(300), store.GetLockHolder("db2", "table1", "row3"))
}

func TestReleaseByDatabase_SingleDB(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	_, acquired2 := store.AcquireLock("db1", "table2", "row2", 200)
	require.True(t, acquired2)

	store.ReleaseByDatabase("db1")

	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table2", "row2"))
}

func TestReleaseByDatabase_OtherDBsUnaffected(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	_, acquired2 := store.AcquireLock("db2", "table1", "row2", 200)
	require.True(t, acquired2)

	store.ReleaseByDatabase("db1")

	// db1 should be released
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))

	// db2 should be unaffected
	require.Equal(t, uint64(200), store.GetLockHolder("db2", "table1", "row2"))
}

func TestClear(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	_, acquired2 := store.AcquireLock("db2", "table2", "row2", 200)
	require.True(t, acquired2)

	store.SetGCMarker("db1", "table1", "row1")

	store.Clear()

	// All locks should be cleared
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))
	require.Equal(t, uint64(0), store.GetLockHolder("db2", "table2", "row2"))

	// All GC markers should be cleared
	require.False(t, store.CheckGCMarker("db1", "table1", "row1"))

	// Should be empty
	locks := store.GetLocksByTxn(100)
	require.Empty(t, locks)
}

func TestGetLocksByTxn(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	// No locks
	locks := store.GetLocksByTxn(100)
	require.Empty(t, locks)

	// Add multiple locks
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	_, acquired2 := store.AcquireLock("db1", "table1", "row2", 100)
	require.True(t, acquired2)

	_, acquired3 := store.AcquireLock("db2", "table2", "row3", 100)
	require.True(t, acquired3)

	locks = store.GetLocksByTxn(100)
	require.Len(t, locks, 3)
	require.Contains(t, locks, "db1:table1:row1")
	require.Contains(t, locks, "db1:table1:row2")
	require.Contains(t, locks, "db2:table2:row3")

	// Other txn should have no locks
	locks = store.GetLocksByTxn(200)
	require.Empty(t, locks)
}

func TestHasLocksForTable_True(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)

	hasLocks := store.HasLocksForTable("db1", "table1")
	require.True(t, hasLocks)
}

func TestHasLocksForTable_False(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	hasLocks := store.HasLocksForTable("db1", "table1")
	require.False(t, hasLocks)

	// Add lock to different table
	_, acquired := store.AcquireLock("db1", "table2", "row1", 100)
	require.True(t, acquired)

	hasLocks = store.HasLocksForTable("db1", "table1")
	require.False(t, hasLocks)
}

func TestConcurrentAcquire_DifferentKeys(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			rowKey := fmt.Sprintf("row%d", idx)
			txnID := uint64(idx + 1)
			_, acquired := store.AcquireLock("db1", "table1", rowKey, txnID)
			require.True(t, acquired)
		}(i)
	}

	wg.Wait()

	// Verify all locks were acquired
	for i := 0; i < numGoroutines; i++ {
		rowKey := fmt.Sprintf("row%d", i)
		txnID := uint64(i + 1)
		holder := store.GetLockHolder("db1", "table1", rowKey)
		require.Equal(t, txnID, holder)
	}
}

func TestConcurrentAcquire_SameKey(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	successCount := int32(0)
	var mu sync.Mutex
	var winners []uint64

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			txnID := uint64(idx + 1)
			_, acquired := store.AcquireLock("db1", "table1", "row1", txnID)
			if acquired {
				mu.Lock()
				successCount++
				winners = append(winners, txnID)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Exactly one should win
	require.Equal(t, int32(1), successCount)
	require.Len(t, winners, 1)

	// Verify the winner holds the lock
	holder := store.GetLockHolder("db1", "table1", "row1")
	require.Equal(t, winners[0], holder)
}

func TestConcurrentReleaseByTxn(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	numGoroutines := 10
	numLocksPerTxn := 10

	// Acquire locks for multiple txns
	for i := 0; i < numGoroutines; i++ {
		txnID := uint64(i + 1)
		for j := 0; j < numLocksPerTxn; j++ {
			rowKey := fmt.Sprintf("txn%d_row%d", i, j)
			_, acquired := store.AcquireLock("db1", "table1", rowKey, txnID)
			require.True(t, acquired)
		}
	}

	// Release all txns concurrently
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			txnID := uint64(idx + 1)
			store.ReleaseByTxn(txnID)
		}(i)
	}

	wg.Wait()

	// Verify all locks are released
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numLocksPerTxn; j++ {
			rowKey := fmt.Sprintf("txn%d_row%d", i, j)
			holder := store.GetLockHolder("db1", "table1", rowKey)
			require.Equal(t, uint64(0), holder)
		}
	}
}

func TestEmptyStrings(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	// Empty strings should be treated as valid keys
	_, acquired := store.AcquireLock("", "", "", 100)
	require.True(t, acquired)

	holder := store.GetLockHolder("", "", "")
	require.Equal(t, uint64(100), holder)

	store.ReleaseLock("", "", "")
	holder = store.GetLockHolder("", "", "")
	require.Equal(t, uint64(0), holder)
}

func TestSpecialCharactersInKeys(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		db     string
		table  string
		rowKey string
	}{
		{"Colons", "db:1", "table:1", "row:1"},
		{"Spaces", "db 1", "table 1", "row 1"},
		{"Unicode", "数据库", "表", "行"},
		{"Special chars", "db-1_2", "table@#$", "row!@#$%^&*()"},
		{"Mixed", "db:1", "table 2", "row_3"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := NewRowLockStore()
			_, acquired := store.AcquireLock(tc.db, tc.table, tc.rowKey, 100)
			require.True(t, acquired)

			holder := store.GetLockHolder(tc.db, tc.table, tc.rowKey)
			require.Equal(t, uint64(100), holder)

			store.ReleaseLock(tc.db, tc.table, tc.rowKey)
			holder = store.GetLockHolder(tc.db, tc.table, tc.rowKey)
			require.Equal(t, uint64(0), holder)
		})
	}
}

func TestLargeNumberOfLocks(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	numLocks := 1000

	// Acquire many locks
	for i := 0; i < numLocks; i++ {
		db := fmt.Sprintf("db%d", i%10)
		table := fmt.Sprintf("table%d", i%20)
		rowKey := fmt.Sprintf("row%d", i)
		txnID := uint64(i%50 + 1)

		_, acquired := store.AcquireLock(db, table, rowKey, txnID)
		require.True(t, acquired)
	}

	// Verify locks are held
	for i := 0; i < numLocks; i++ {
		db := fmt.Sprintf("db%d", i%10)
		table := fmt.Sprintf("table%d", i%20)
		rowKey := fmt.Sprintf("row%d", i)
		txnID := uint64(i%50 + 1)

		holder := store.GetLockHolder(db, table, rowKey)
		require.Equal(t, txnID, holder)
	}

	// Release by txn
	for txnID := uint64(1); txnID <= 50; txnID++ {
		store.ReleaseByTxn(txnID)
	}

	// Verify all locks are released
	for i := 0; i < numLocks; i++ {
		db := fmt.Sprintf("db%d", i%10)
		table := fmt.Sprintf("table%d", i%20)
		rowKey := fmt.Sprintf("row%d", i)

		holder := store.GetLockHolder(db, table, rowKey)
		require.Equal(t, uint64(0), holder)
	}
}

func TestGCMarkerWithLocks(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	// Set GC marker and lock on same row
	store.SetGCMarker("db1", "table1", "row1")
	_, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)

	// Both should exist independently
	require.True(t, store.CheckGCMarker("db1", "table1", "row1"))
	require.Equal(t, uint64(100), store.GetLockHolder("db1", "table1", "row1"))

	// Release lock shouldn't affect GC marker
	store.ReleaseLock("db1", "table1", "row1")
	require.True(t, store.CheckGCMarker("db1", "table1", "row1"))
	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))

	// Delete GC marker shouldn't affect lock (if re-acquired)
	_, acquired = store.AcquireLock("db1", "table1", "row1", 200)
	require.True(t, acquired)

	store.DeleteGCMarker("db1", "table1", "row1")
	require.False(t, store.CheckGCMarker("db1", "table1", "row1"))
	require.Equal(t, uint64(200), store.GetLockHolder("db1", "table1", "row1"))
}

func TestReleaseByTable_WithGCMarkers(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	// Add locks and GC markers
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	store.SetGCMarker("db1", "table1", "row2")

	// ReleaseByTable releases locks and also cleans up GC markers for that table
	store.ReleaseByTable("db1", "table1")

	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))
	require.False(t, store.CheckGCMarker("db1", "table1", "row2"))
}

func TestMultipleTxnsSameRow_Sequential(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	// Txn 100 acquires
	_, acquired := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired)

	// Txn 200 fails
	existingTxn, acquired := store.AcquireLock("db1", "table1", "row1", 200)
	require.False(t, acquired)
	require.Equal(t, uint64(100), existingTxn)

	// Txn 100 releases
	store.ReleaseLock("db1", "table1", "row1")

	// Txn 200 succeeds
	_, acquired = store.AcquireLock("db1", "table1", "row1", 200)
	require.True(t, acquired)

	require.Equal(t, uint64(200), store.GetLockHolder("db1", "table1", "row1"))
}

func TestReleaseByDatabase_WithGCMarkers(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()

	// Add locks and GC markers
	_, acquired1 := store.AcquireLock("db1", "table1", "row1", 100)
	require.True(t, acquired1)

	store.SetGCMarker("db1", "table1", "row2")

	_, acquired2 := store.AcquireLock("db2", "table1", "row3", 200)
	require.True(t, acquired2)

	store.SetGCMarker("db2", "table1", "row4")

	// ReleaseByDatabase releases locks and GC markers for all tables in that DB
	store.ReleaseByDatabase("db1")

	require.Equal(t, uint64(0), store.GetLockHolder("db1", "table1", "row1"))
	require.Equal(t, uint64(200), store.GetLockHolder("db2", "table1", "row3"))

	// GC markers for db1 should be cleaned up, but db2 should remain
	require.False(t, store.CheckGCMarker("db1", "table1", "row2"))
	require.True(t, store.CheckGCMarker("db2", "table1", "row4"))
}
