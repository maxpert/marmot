package db

import (
	"os"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func setupTestBadger(t *testing.T) (*badger.DB, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "persistent_counter_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to open badger: %v", err)
	}

	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestPersistentCounter_BasicOps(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	pc := NewPersistentCounter(db, "/counters/", 100)

	// Test Load (default 0)
	val, err := pc.Load("test1")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if val != 0 {
		t.Errorf("Expected 0, got %d", val)
	}

	// Test Store
	err = pc.Store("test1", 42)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	val, err = pc.Load("test1")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}

	// Test Inc
	newVal, err := pc.Inc("test1", 10)
	if err != nil {
		t.Fatalf("Inc failed: %v", err)
	}
	if newVal != 52 {
		t.Errorf("Expected 52, got %d", newVal)
	}

	// Test Dec
	newVal, err = pc.Dec("test1", 5)
	if err != nil {
		t.Fatalf("Dec failed: %v", err)
	}
	if newVal != 47 {
		t.Errorf("Expected 47, got %d", newVal)
	}

	// Test Dec clamps to 0
	newVal, err = pc.Dec("test1", 100)
	if err != nil {
		t.Fatalf("Dec failed: %v", err)
	}
	if newVal != 0 {
		t.Errorf("Expected 0 (clamped), got %d", newVal)
	}
}

func TestPersistentCounter_UpdateMax(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	pc := NewPersistentCounter(db, "/counters/", 100)

	// Set initial value
	pc.Store("max_test", 50)

	// UpdateMax with lower value (no change)
	val, err := pc.UpdateMax("max_test", 30)
	if err != nil {
		t.Fatalf("UpdateMax failed: %v", err)
	}
	if val != 50 {
		t.Errorf("Expected 50, got %d", val)
	}

	// UpdateMax with higher value (updates)
	val, err = pc.UpdateMax("max_test", 100)
	if err != nil {
		t.Fatalf("UpdateMax failed: %v", err)
	}
	if val != 100 {
		t.Errorf("Expected 100, got %d", val)
	}

	// Verify persisted
	val, err = pc.Load("max_test")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if val != 100 {
		t.Errorf("Expected 100, got %d", val)
	}
}

func TestPersistentCounter_Persistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "persistent_counter_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// First session - write values
	{
		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		if err != nil {
			t.Fatalf("Failed to open badger: %v", err)
		}

		pc := NewPersistentCounter(db, "/counters/", 100)
		pc.Store("persist_test", 12345)
		pc.Inc("persist_test", 5)

		db.Close()
	}

	// Second session - verify values persisted
	{
		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		if err != nil {
			t.Fatalf("Failed to open badger: %v", err)
		}
		defer db.Close()

		pc := NewPersistentCounter(db, "/counters/", 100)
		val, err := pc.Load("persist_test")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if val != 12350 {
			t.Errorf("Expected 12350, got %d", val)
		}
	}
}

func TestPersistentCounter_LRUEviction(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	// Small cache size for testing
	pc := NewPersistentCounter(db, "/counters/", 3)

	// Add 3 counters (fills cache)
	pc.Store("c1", 1)
	pc.Store("c2", 2)
	pc.Store("c3", 3)

	// Add 4th counter - should evict c1
	pc.Store("c4", 4)

	// c1 should be evicted from cache but still in DB
	pc.mu.RLock()
	_, c1InCache := pc.counters["c1"]
	pc.mu.RUnlock()

	if c1InCache {
		t.Error("c1 should have been evicted from cache")
	}

	// But c1 should still be loadable from DB
	val, err := pc.Load("c1")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}
}

func TestPersistentCounter_Concurrent(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	pc := NewPersistentCounter(db, "/counters/", 100)

	// Initialize counter
	pc.Store("concurrent", 0)

	// Run concurrent increments
	var wg sync.WaitGroup
	numGoroutines := 10
	incsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incsPerGoroutine; j++ {
				pc.Inc("concurrent", 1)
			}
		}()
	}

	wg.Wait()

	// Verify total
	val, err := pc.Load("concurrent")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	expected := int64(numGoroutines * incsPerGoroutine)
	if val != expected {
		t.Errorf("Expected %d, got %d", expected, val)
	}
}

func TestPersistentCounter_MultipleCounters(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	pc := NewPersistentCounter(db, "/counters/", 100)

	// Different counters with different values
	pc.Store("users", 100)
	pc.Store("orders", 500)
	pc.Store("items", 1000)

	// Modify independently
	pc.Inc("users", 5)
	pc.Dec("orders", 50)
	pc.UpdateMax("items", 2000)

	// Verify independence
	users, _ := pc.Load("users")
	orders, _ := pc.Load("orders")
	items, _ := pc.Load("items")

	if users != 105 {
		t.Errorf("users: expected 105, got %d", users)
	}
	if orders != 450 {
		t.Errorf("orders: expected 450, got %d", orders)
	}
	if items != 2000 {
		t.Errorf("items: expected 2000, got %d", items)
	}
}

func TestPersistentCounter_InTxn(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	pc := NewPersistentCounter(db, "/counters/", 100)

	// Initialize
	pc.Store("txn_test", 100)

	// Use IncInTxn
	err := db.Update(func(txn *badger.Txn) error {
		_, err := pc.IncInTxn(txn, "txn_test", 50)
		return err
	})
	if err != nil {
		t.Fatalf("IncInTxn failed: %v", err)
	}

	val, _ := pc.Load("txn_test")
	if val != 150 {
		t.Errorf("Expected 150, got %d", val)
	}

	// Use DecInTxn
	err = db.Update(func(txn *badger.Txn) error {
		_, err := pc.DecInTxn(txn, "txn_test", 30)
		return err
	})
	if err != nil {
		t.Fatalf("DecInTxn failed: %v", err)
	}

	val, _ = pc.Load("txn_test")
	if val != 120 {
		t.Errorf("Expected 120, got %d", val)
	}

	// Use UpdateMaxInTxn
	err = db.Update(func(txn *badger.Txn) error {
		_, err := pc.UpdateMaxInTxn(txn, "txn_test", 200)
		return err
	})
	if err != nil {
		t.Fatalf("UpdateMaxInTxn failed: %v", err)
	}

	val, _ = pc.Load("txn_test")
	if val != 200 {
		t.Errorf("Expected 200, got %d", val)
	}
}

func TestPersistentCounter_Invalidate(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	pc := NewPersistentCounter(db, "/counters/", 100)

	pc.Store("inv_test", 42)

	// Verify in cache
	pc.mu.RLock()
	_, inCache := pc.counters["inv_test"]
	pc.mu.RUnlock()
	if !inCache {
		t.Error("Should be in cache")
	}

	// Invalidate
	pc.Invalidate("inv_test")

	// Verify not in cache
	pc.mu.RLock()
	_, inCache = pc.counters["inv_test"]
	pc.mu.RUnlock()
	if inCache {
		t.Error("Should not be in cache after invalidate")
	}

	// Should still load from DB
	val, err := pc.Load("inv_test")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
}

func TestPersistentCounter_LoadUint64(t *testing.T) {
	db, cleanup := setupTestBadger(t)
	defer cleanup()

	pc := NewPersistentCounter(db, "/counters/", 100)

	pc.Store("uint_test", 9876543210)

	val, err := pc.LoadUint64("uint_test")
	if err != nil {
		t.Fatalf("LoadUint64 failed: %v", err)
	}
	if val != 9876543210 {
		t.Errorf("Expected 9876543210, got %d", val)
	}
}

func BenchmarkPersistentCounter_Inc(b *testing.B) {
	dir, _ := os.MkdirTemp("", "persistent_counter_bench")
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	pc := NewPersistentCounter(db, "/counters/", 100)
	pc.Store("bench", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc.Inc("bench", 1)
	}
}

func BenchmarkPersistentCounter_Load(b *testing.B) {
	dir, _ := os.MkdirTemp("", "persistent_counter_bench")
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	pc := NewPersistentCounter(db, "/counters/", 100)
	pc.Store("bench", 12345)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc.Load("bench")
	}
}
