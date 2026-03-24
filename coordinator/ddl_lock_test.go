package coordinator

import (
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

func testTimestamp() hlc.Timestamp {
	return hlc.Timestamp{}
}

// TestDDLReleaseLockIdempotent verifies that calling ReleaseLock twice does not panic.
func TestDDLReleaseLockIdempotent(t *testing.T) {
	t.Parallel()

	mgr := NewDDLLockManager(10 * time.Second)
	_, err := mgr.AcquireLock("db1", 1, 100, testTimestamp())
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}

	if err := mgr.ReleaseLock("db1", 100); err != nil {
		t.Fatalf("first ReleaseLock: %v", err)
	}

	// Second call: lock is already gone from activeLocks, should be a no-op.
	if err := mgr.ReleaseLock("db1", 100); err != nil {
		t.Fatalf("second ReleaseLock: %v", err)
	}
}

// TestDDLReleaseAfterCleanupNoPanic verifies that calling ReleaseLock after
// CleanupExpiredLocks has already evicted the same lock does not panic.
func TestDDLReleaseAfterCleanupNoPanic(t *testing.T) {
	t.Parallel()

	// Use a very short lease so the lock expires immediately.
	mgr := NewDDLLockManager(1 * time.Millisecond)
	_, err := mgr.AcquireLock("db2", 1, 200, testTimestamp())
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}

	// Wait for the lock to expire.
	time.Sleep(5 * time.Millisecond)

	cleaned := mgr.CleanupExpiredLocks()
	if cleaned != 1 {
		t.Fatalf("expected 1 cleaned lock, got %d", cleaned)
	}

	// Now attempt to release the already-cleaned lock — must not panic.
	// The lock is gone from activeLocks, so ReleaseLock returns nil (idempotent).
	if err := mgr.ReleaseLock("db2", 200); err != nil {
		t.Fatalf("ReleaseLock after cleanup: %v", err)
	}
}

// TestDDLReleaseAllClearsLocks verifies ReleaseAll empties all active locks.
func TestDDLReleaseAllClearsLocks(t *testing.T) {
	t.Parallel()

	mgr := NewDDLLockManager(10 * time.Second)
	databases := []string{"db-a", "db-b", "db-c"}
	for i, db := range databases {
		if _, err := mgr.AcquireLock(db, 1, uint64(i+1), testTimestamp()); err != nil {
			t.Fatalf("AcquireLock %s: %v", db, err)
		}
	}

	if got := len(mgr.GetActiveLocks()); got != 3 {
		t.Fatalf("expected 3 active locks before ReleaseAll, got %d", got)
	}

	mgr.ReleaseAll()

	if got := len(mgr.GetActiveLocks()); got != 0 {
		t.Fatalf("expected 0 active locks after ReleaseAll, got %d", got)
	}
}

// TestDDLReleaseAllUnblocksWaiters verifies that WaitForLock callers are
// unblocked when ReleaseAll is called.
func TestDDLReleaseAllUnblocksWaiters(t *testing.T) {
	t.Parallel()

	mgr := NewDDLLockManager(10 * time.Second)
	if _, err := mgr.AcquireLock("db-wait", 1, 999, testTimestamp()); err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		errCh <- mgr.WaitForLock("db-wait", 5*time.Second)
	}()

	// Give the goroutine time to block on WaitForLock.
	time.Sleep(10 * time.Millisecond)

	mgr.ReleaseAll()
	wg.Wait()

	if err := <-errCh; err != nil {
		t.Fatalf("WaitForLock returned unexpected error after ReleaseAll: %v", err)
	}
}
