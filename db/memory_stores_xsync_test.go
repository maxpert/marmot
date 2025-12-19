package db

import (
	"sync"
	"testing"
	"time"
)

// TestXsyncTransactionStore_BasicCRUD tests basic transaction store operations.
func TestXsyncTransactionStore_BasicCRUD(t *testing.T) {
	store := NewXsyncTransactionStore()

	// Begin a transaction
	txnID := uint64(1)
	state := &TxnState{
		NodeID:         1,
		Status:         TxnStatusPending,
		StartTSWall:    time.Now().UnixNano(),
		StartTSLogical: 0,
		LastHeartbeat:  time.Now().UnixNano(),
		RowCount:       0,
	}
	store.Begin(txnID, state)

	// Get the transaction
	retrieved, ok := store.Get(txnID)
	if !ok {
		t.Fatal("expected transaction to exist")
	}
	if retrieved.NodeID != 1 || retrieved.Status != TxnStatusPending {
		t.Errorf("unexpected state: %+v", retrieved)
	}

	// Update heartbeat
	newHeartbeat := time.Now().UnixNano()
	store.UpdateHeartbeat(txnID, newHeartbeat)
	retrieved, _ = store.Get(txnID)
	if retrieved.LastHeartbeat != newHeartbeat {
		t.Errorf("expected heartbeat %d, got %d", newHeartbeat, retrieved.LastHeartbeat)
	}

	// Remove transaction
	store.Remove(txnID)
	_, ok = store.Get(txnID)
	if ok {
		t.Error("expected transaction to be removed")
	}
}

// TestXsyncTransactionStore_StatusTransitions tests status updates.
func TestXsyncTransactionStore_StatusTransitions(t *testing.T) {
	store := NewXsyncTransactionStore()

	txnID := uint64(1)
	state := &TxnState{
		NodeID: 1,
		Status: TxnStatusPending,
	}
	store.Begin(txnID, state)

	// Verify pending count
	if count := store.CountPending(); count != 1 {
		t.Errorf("expected 1 pending, got %d", count)
	}

	// Update to committed
	store.UpdateStatus(txnID, TxnStatusCommitted)
	retrieved, _ := store.Get(txnID)
	if retrieved.Status != TxnStatusCommitted {
		t.Errorf("expected committed status, got %v", retrieved.Status)
	}

	// Should no longer be pending
	if count := store.CountPending(); count != 0 {
		t.Errorf("expected 0 pending after commit, got %d", count)
	}

	// Update to pending again
	store.UpdateStatus(txnID, TxnStatusPending)
	if count := store.CountPending(); count != 1 {
		t.Errorf("expected 1 pending after status change, got %d", count)
	}

	// Update to aborted
	store.UpdateStatus(txnID, TxnStatusAborted)
	if count := store.CountPending(); count != 0 {
		t.Errorf("expected 0 pending after abort, got %d", count)
	}
}

// TestXsyncTransactionStore_RangePending tests iteration over pending transactions.
func TestXsyncTransactionStore_RangePending(t *testing.T) {
	store := NewXsyncTransactionStore()

	// Add multiple transactions
	for i := uint64(1); i <= 5; i++ {
		status := TxnStatusPending
		if i%2 == 0 {
			status = TxnStatusCommitted
		}
		store.Begin(i, &TxnState{
			NodeID: i,
			Status: status,
		})
	}

	// Count pending
	pendingCount := 0
	store.RangePending(func(txnID uint64) bool {
		pendingCount++
		return true
	})

	if pendingCount != 3 {
		t.Errorf("expected 3 pending transactions, got %d", pendingCount)
	}

	// Test early termination
	count := 0
	store.RangePending(func(txnID uint64) bool {
		count++
		return count < 2
	})

	if count != 2 {
		t.Errorf("expected early termination at 2, got %d", count)
	}
}

// TestXsyncTransactionStore_Concurrent tests concurrent operations.
func TestXsyncTransactionStore_Concurrent(t *testing.T) {
	store := NewXsyncTransactionStore()
	numGoroutines := 100
	numOpsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writes
	for g := 0; g < numGoroutines; g++ {
		go func(routineID int) {
			defer wg.Done()
			for i := 0; i < numOpsPerGoroutine; i++ {
				txnID := uint64(routineID*numOpsPerGoroutine + i)
				store.Begin(txnID, &TxnState{
					NodeID: uint64(routineID),
					Status: TxnStatusPending,
				})
				store.UpdateHeartbeat(txnID, time.Now().UnixNano())
				store.UpdateStatus(txnID, TxnStatusCommitted)
			}
		}(g)
	}

	wg.Wait()

	// Verify all transactions exist
	totalCount := 0
	store.RangeAll(func(txnID uint64, state *TxnState) bool {
		totalCount++
		return true
	})

	expectedCount := numGoroutines * numOpsPerGoroutine
	if totalCount != expectedCount {
		t.Errorf("expected %d transactions, got %d", expectedCount, totalCount)
	}
}

// TestXsyncCDCLockStore_BasicCRUD tests basic CDC lock operations.
func TestXsyncCDCLockStore_BasicCRUD(t *testing.T) {
	store := NewXsyncCDCLockStore()

	txnID := uint64(1)
	table := "users"
	key := "user123"

	// Acquire lock
	err := store.Acquire(txnID, table, key)
	if err != nil {
		t.Fatalf("unexpected error acquiring lock: %v", err)
	}

	// Get holder
	holder, held := store.GetHolder(table, key)
	if !held {
		t.Fatal("expected lock to be held")
	}
	if holder != txnID {
		t.Errorf("expected holder %d, got %d", txnID, holder)
	}

	// Release lock
	store.Release(table, key, txnID)
	_, held = store.GetHolder(table, key)
	if held {
		t.Error("expected lock to be released")
	}
}

// TestXsyncCDCLockStore_ConflictDetection tests that Acquire returns error for conflicts.
func TestXsyncCDCLockStore_ConflictDetection(t *testing.T) {
	store := NewXsyncCDCLockStore()

	txnID1 := uint64(1)
	txnID2 := uint64(2)
	table := "users"
	key := "user123"

	// Acquire lock
	err := store.Acquire(txnID1, table, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to acquire same lock from different transaction
	err = store.Acquire(txnID2, table, key)
	if err != ErrLockHeld {
		t.Errorf("expected ErrLockHeld, got %v", err)
	}

	// Verify original holder unchanged
	holder, held := store.GetHolder(table, key)
	if !held || holder != txnID1 {
		t.Error("original holder should be unchanged")
	}

	// Same transaction can re-acquire
	err = store.Acquire(txnID1, table, key)
	if err != nil {
		t.Errorf("same transaction should be able to re-acquire: %v", err)
	}
}

// TestXsyncCDCLockStore_ReleaseByTxn tests releasing all locks for a transaction.
func TestXsyncCDCLockStore_ReleaseByTxn(t *testing.T) {
	store := NewXsyncCDCLockStore()

	txnID := uint64(1)
	table := "users"

	// Acquire multiple locks
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		err := store.Acquire(txnID, table, key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Verify all locks held
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		holder, held := store.GetHolder(table, key)
		if !held || holder != txnID {
			t.Errorf("lock %s should be held by txn %d", key, txnID)
		}
	}

	// Release all locks for transaction
	store.ReleaseByTxn(txnID)

	// Verify all locks released
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		_, held := store.GetHolder(table, key)
		if held {
			t.Errorf("lock %s should be released", key)
		}
	}
}

// TestXsyncCDCLockStore_Concurrent tests concurrent lock operations.
func TestXsyncCDCLockStore_Concurrent(t *testing.T) {
	store := NewXsyncCDCLockStore()
	numGoroutines := 50
	numLocksPerTxn := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	successCount := make([]int, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(txnID int) {
			defer wg.Done()
			table := "users"
			for i := 0; i < numLocksPerTxn; i++ {
				key := string(rune('a' + i))
				err := store.Acquire(uint64(txnID), table, key)
				if err == nil {
					successCount[txnID]++
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify that each lock is held by exactly one transaction
	table := "users"
	for i := 0; i < numLocksPerTxn; i++ {
		key := string(rune('a' + i))
		holder, held := store.GetHolder(table, key)
		if !held {
			t.Errorf("lock %s should be held", key)
		}
		if holder >= uint64(numGoroutines) {
			t.Errorf("invalid holder %d for lock %s", holder, key)
		}
	}

	// Total successes should equal number of locks
	total := 0
	for _, count := range successCount {
		total += count
	}
	if total != numLocksPerTxn {
		t.Errorf("expected %d total lock acquisitions, got %d", numLocksPerTxn, total)
	}
}

// Benchmarks

func BenchmarkXsyncTransactionStore_Begin(b *testing.B) {
	store := NewXsyncTransactionStore()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txnID := uint64(i)
		store.Begin(txnID, &TxnState{
			NodeID: 1,
			Status: TxnStatusPending,
		})
	}
}

func BenchmarkXsyncTransactionStore_Get(b *testing.B) {
	store := NewXsyncTransactionStore()
	for i := 0; i < 1000; i++ {
		store.Begin(uint64(i), &TxnState{
			NodeID: 1,
			Status: TxnStatusPending,
		})
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Get(uint64(i % 1000))
	}
}

func BenchmarkXsyncTransactionStore_UpdateStatus(b *testing.B) {
	store := NewXsyncTransactionStore()
	for i := 0; i < 1000; i++ {
		store.Begin(uint64(i), &TxnState{
			NodeID: 1,
			Status: TxnStatusPending,
		})
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.UpdateStatus(uint64(i%1000), TxnStatusCommitted)
	}
}

func BenchmarkXsyncCDCLockStore_Acquire(b *testing.B) {
	store := NewXsyncCDCLockStore()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txnID := uint64(i / 100)
		key := string(rune('a' + (i % 26)))
		store.Acquire(txnID, "users", key)
	}
}

func BenchmarkXsyncCDCLockStore_GetHolder(b *testing.B) {
	store := NewXsyncCDCLockStore()
	for i := 0; i < 1000; i++ {
		key := string(rune('a' + (i % 26)))
		store.Acquire(uint64(i/100), "users", key)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := string(rune('a' + (i % 26)))
		store.GetHolder("users", key)
	}
}
