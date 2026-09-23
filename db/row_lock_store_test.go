package db

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRowLockStore(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	require.NotNil(t, store)
	require.NotNil(t, store.tables)
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

// TestReleaseKeepsTheTableMapAnAcquirerAlreadyLoaded replays, step by step,
// the interleaving that let two transactions hold one row. AcquireLock loads a
// table's row map and inserts into it as two separate steps. A release that
// empties the map used to delete it from the store in between, so the insert
// landed in a detached map and the next acquirer took the same row in a fresh
// one. Every release path is driven.
//
// Mutation: in releaseRowIfHeldBy (or ReleaseLock), delete the table's row map
// from s.tables once it is empty. "a third acquirer took a row B holds" fires.
func TestReleaseKeepsTheTableMapAnAcquirerAlreadyLoaded(t *testing.T) {
	t.Parallel()

	const table, row = "t", "autoinc:testdb:users"
	releases := map[string]func(s *RowLockStore){
		"ReleaseLockIfHeldBy": func(s *RowLockStore) { s.ReleaseLockIfHeldBy("", table, row, 1) },
		"ReleaseByTxn":        func(s *RowLockStore) { s.ReleaseByTxn(1) },
		"ReleaseLock":         func(s *RowLockStore) { s.ReleaseLock("", table, row) },
	}
	for name, release := range releases {
		t.Run(name, func(t *testing.T) {
			s := NewRowLockStore()
			_, ok := s.AcquireLock("", table, row, 1)
			require.True(t, ok)

			// B has done AcquireLock's first step: it holds the table's row map.
			rowMap, ok := s.tables.Load(makeTableKey("", table))
			require.True(t, ok)

			// A releases the only lock in the table.
			release(s)

			// B's second step inserts into the map it loaded.
			_, loaded := rowMap.LoadOrStore(row, 2)
			require.False(t, loaded, "A's lock survived its release")

			holder, acquired := s.AcquireLock("", table, row, 3)
			require.False(t, acquired, "a third acquirer took a row B holds: B's lock was dropped with a detached map")
			require.Equal(t, uint64(2), holder)
		})
	}
}

// TestConcurrentAcquireReleaseKeepsMutualExclusion is a soak for the same
// property under real scheduling; the deterministic proof is
// TestReleaseKeepsTheTableMapAnAcquirerAlreadyLoaded. The mutex stands in for
// PebbleMetaStore.intentLockFor, which serialises acquires on one key while the
// commit path releases without it.
func TestConcurrentAcquireReleaseKeepsMutualExclusion(t *testing.T) {
	t.Parallel()

	const (
		goroutines = 4
		iterations = 20000
		row        = "autoinc:testdb:users"
	)
	store := NewRowLockStore()
	var (
		acquireMu sync.Mutex
		holders   atomic.Int32
		overlaps  atomic.Int32
		wg        sync.WaitGroup
	)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := uint64(1); i <= iterations; i++ {
				txnID := uint64(g+1)<<40 | i
				acquireMu.Lock()
				_, acquired := store.AcquireLock("", "t", row, txnID)
				acquireMu.Unlock()
				if !acquired {
					continue
				}
				if holders.Add(1) > 1 {
					overlaps.Add(1)
				}
				runtime.Gosched()
				holders.Add(-1)
				store.ReleaseByTxn(txnID)
			}
		}(g)
	}
	wg.Wait()
	require.Zero(t, overlaps.Load(), "two transactions held the same row at once")
}

// TestReleaseByTxnLeavesARowAnotherTransactionHolds pins the holder check in
// ReleaseByTxn. A transaction's reverse index and the row map are updated in
// separate steps, so the index can still name a row whose lock was released
// and retaken: ReleaseLockIfHeldBy deletes the row entry before it removes the
// index entry, and another transaction can acquire the row in between.
//
// Mutation: release each indexed row unconditionally (ReleaseLock) instead of
// releaseRowIfHeldBy. "ReleaseByTxn released a row another transaction holds"
// fires.
func TestReleaseByTxnLeavesARowAnotherTransactionHolds(t *testing.T) {
	t.Parallel()

	store := NewRowLockStore()
	_, ok := store.AcquireLock("", "t", "r", 1)
	require.True(t, ok)

	// The row passes to txn 2 while txn 1's index still names it.
	rowMap, ok := store.tables.Load(makeTableKey("", "t"))
	require.True(t, ok)
	rowMap.Store("r", 2)

	keys := store.ReleaseByTxn(1)
	require.Equal(t, []string{makeFullKey("", "t", "r")}, keys,
		"every key the index named must be returned for intent-record cleanup")

	holder, held := store.CheckLock("", "t", "r")
	require.True(t, held, "ReleaseByTxn released a row another transaction holds")
	require.Equal(t, uint64(2), holder)
}
