package db

import (
	"fmt"
	"sync"
	"testing"
)

func TestIntentFilter_BasicOperations(t *testing.T) {
	f := NewIntentFilter()

	h1 := ComputeIntentHash("users", "1")
	h2 := ComputeIntentHash("users", "2")

	// Initially empty - no hits
	if f.Check(h1) {
		t.Error("empty filter should not contain h1")
	}

	// Add h1 for txn 100
	f.Add(100, h1)

	// Now h1 should hit
	if !f.Check(h1) {
		t.Error("filter should contain h1 after add")
	}

	// h2 should still miss
	if f.Check(h2) {
		t.Error("filter should not contain h2")
	}

	// Remove txn 100
	f.Remove(100)

	// h1 should miss again
	if f.Check(h1) {
		t.Error("filter should not contain h1 after remove")
	}
}

func TestIntentFilter_MultipleAdds(t *testing.T) {
	f := NewIntentFilter()

	hashes := []uint64{
		ComputeIntentHash("users", "1"),
		ComputeIntentHash("users", "2"),
		ComputeIntentHash("users", "3"),
	}

	for _, h := range hashes {
		f.Add(100, h)
	}

	for _, h := range hashes {
		if !f.Check(h) {
			t.Errorf("filter should contain hash %d", h)
		}
	}

	if f.Size() != 3 {
		t.Errorf("expected size 3, got %d", f.Size())
	}

	f.Remove(100)

	if f.Size() != 0 {
		t.Errorf("expected size 0 after remove, got %d", f.Size())
	}
}

func TestIntentFilter_MultipleTransactions(t *testing.T) {
	f := NewIntentFilter()

	h1 := ComputeIntentHash("users", "1")
	h2 := ComputeIntentHash("orders", "1")

	f.Add(100, h1)
	f.Add(200, h2)

	if f.TxnCount() != 2 {
		t.Errorf("expected 2 txns, got %d", f.TxnCount())
	}

	// Remove txn 100 - h2 should still hit
	f.Remove(100)

	if f.Check(h1) {
		t.Error("h1 should be removed with txn 100")
	}
	if !f.Check(h2) {
		t.Error("h2 should still exist for txn 200")
	}
}

func TestIntentFilter_RemoveHash(t *testing.T) {
	f := NewIntentFilter()

	h1 := ComputeIntentHash("users", "1")
	h2 := ComputeIntentHash("users", "2")
	h3 := ComputeIntentHash("users", "3")

	// Add all hashes to same txn
	f.Add(100, h1)
	f.Add(100, h2)
	f.Add(100, h3)

	if f.Size() != 3 {
		t.Errorf("expected size 3, got %d", f.Size())
	}

	// Remove just h2
	f.RemoveHash(100, h2)

	if f.Check(h2) {
		t.Error("h2 should be removed")
	}
	if !f.Check(h1) {
		t.Error("h1 should still exist")
	}
	if !f.Check(h3) {
		t.Error("h3 should still exist")
	}

	// Remove remaining hashes individually
	f.RemoveHash(100, h1)
	f.RemoveHash(100, h3)

	// Txn should be cleaned up
	if f.TxnCount() != 0 {
		t.Errorf("expected 0 txns, got %d", f.TxnCount())
	}
}

func TestIntentFilter_RemoveNonExistent(t *testing.T) {
	f := NewIntentFilter()

	// Removing non-existent txn should be safe
	f.Remove(999)
	f.RemoveHash(999, 12345)

	// Should not panic or error
	if f.TxnCount() != 0 {
		t.Errorf("expected 0 txns, got %d", f.TxnCount())
	}
}

func TestIntentFilter_CrossTableCollision(t *testing.T) {
	f := NewIntentFilter()

	// Different tables, same row key should NOT collide
	h1 := ComputeIntentHash("users", "1")
	h2 := ComputeIntentHash("orders", "1")

	if h1 == h2 {
		t.Error("different tables should have different hashes")
	}

	f.Add(100, h1)
	f.Add(200, h2)

	// Remove users:1, orders:1 should still be present
	f.Remove(100)
	if !f.Check(h2) {
		t.Error("orders:1 should still be in filter")
	}
}

func TestIntentFilter_Concurrent(t *testing.T) {
	f := NewIntentFilter()

	var wg sync.WaitGroup
	numGoroutines := 100
	opsPerGoroutine := 100

	// Concurrent add/check/remove
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			txnID := uint64(gid)

			for i := 0; i < opsPerGoroutine; i++ {
				h := ComputeIntentHash("table", fmt.Sprintf("%d:%d", gid, i))
				f.Add(txnID, h)
			}

			// Check some random hashes
			for i := 0; i < 10; i++ {
				h := ComputeIntentHash("table", fmt.Sprintf("%d:%d", gid, i))
				_ = f.Check(h)
			}

			f.Remove(txnID)
		}(g)
	}

	wg.Wait()

	if f.TxnCount() != 0 {
		t.Errorf("expected 0 txns after all removed, got %d", f.TxnCount())
	}
}

func TestComputeIntentHash_Consistency(t *testing.T) {
	// Same input should always produce same hash
	h1 := ComputeIntentHash("users", "abc123")
	h2 := ComputeIntentHash("users", "abc123")

	if h1 != h2 {
		t.Error("same input should produce same hash")
	}

	// Different input should (very likely) produce different hash
	h3 := ComputeIntentHash("users", "abc124")
	if h1 == h3 {
		t.Error("different input should produce different hash")
	}
}

// Benchmarks

func BenchmarkIntentFilter_Check(b *testing.B) {
	f := NewIntentFilter()

	// Pre-populate with 100K hashes
	for i := uint64(0); i < 100000; i++ {
		f.Add(i%1000, i)
	}

	h := ComputeIntentHash("users", "test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Check(h)
	}
}

func BenchmarkIntentFilter_CheckHit(b *testing.B) {
	f := NewIntentFilter()
	h := ComputeIntentHash("users", "test")
	f.Add(1, h)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Check(h)
	}
}

func BenchmarkIntentFilter_Add(b *testing.B) {
	f := NewIntentFilter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Add(uint64(i%1000), uint64(i))
	}
}

func BenchmarkIntentFilter_AddRemove(b *testing.B) {
	f := NewIntentFilter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txnID := uint64(i)
		f.Add(txnID, uint64(i))
		f.Remove(txnID)
	}
}

func BenchmarkIntentFilter_ConcurrentCheck(b *testing.B) {
	f := NewIntentFilter()

	// Pre-populate
	for i := uint64(0); i < 10000; i++ {
		f.Add(i%100, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		h := ComputeIntentHash("users", "test")
		for pb.Next() {
			f.Check(h)
		}
	})
}

func BenchmarkComputeIntentHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ComputeIntentHash("users", "user_12345")
	}
}
