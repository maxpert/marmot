package replica

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTxnWaitQueue_SingleWaiter(t *testing.T) {
	q := NewTxnWaitQueue()

	done := make(chan error, 1)
	go func() {
		done <- q.Wait(context.Background(), 100)
	}()

	// Give waiter time to register
	time.Sleep(10 * time.Millisecond)

	if q.Len() != 1 {
		t.Fatalf("expected 1 waiter, got %d", q.Len())
	}

	q.NotifyUpTo(100)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waiter not notified")
	}

	if q.Len() != 0 {
		t.Fatalf("expected 0 waiters after notification, got %d", q.Len())
	}
}

func TestTxnWaitQueue_MultipleWaiters(t *testing.T) {
	q := NewTxnWaitQueue()

	var wg sync.WaitGroup
	results := make([]error, 5)

	txnIDs := []uint64{10, 20, 30, 40, 50}
	for i, txnID := range txnIDs {
		wg.Add(1)
		go func(idx int, id uint64) {
			defer wg.Done()
			results[idx] = q.Wait(context.Background(), id)
		}(i, txnID)
	}

	// Give waiters time to register
	time.Sleep(20 * time.Millisecond)

	if q.Len() != 5 {
		t.Fatalf("expected 5 waiters, got %d", q.Len())
	}

	// Notify up to 30 - should wake first 3 waiters
	q.NotifyUpTo(30)

	// Give some time for waiters to wake up
	time.Sleep(20 * time.Millisecond)

	if q.Len() != 2 {
		t.Fatalf("expected 2 waiters remaining, got %d", q.Len())
	}

	// Notify remaining
	q.NotifyUpTo(50)

	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Errorf("waiter %d got error: %v", i, err)
		}
	}

	if q.Len() != 0 {
		t.Fatalf("expected 0 waiters after all notifications, got %d", q.Len())
	}
}

func TestTxnWaitQueue_ContextCancellation(t *testing.T) {
	q := NewTxnWaitQueue()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- q.Wait(ctx, 100)
	}()

	// Give waiter time to register
	time.Sleep(10 * time.Millisecond)

	if q.Len() != 1 {
		t.Fatalf("expected 1 waiter, got %d", q.Len())
	}

	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waiter did not return on cancel")
	}

	// Give cleanup time to run
	time.Sleep(10 * time.Millisecond)

	if q.Len() != 0 {
		t.Fatalf("expected 0 waiters after cancel, got %d", q.Len())
	}
}

func TestTxnWaitQueue_NotifyBeforeWait(t *testing.T) {
	q := NewTxnWaitQueue()

	// Notify before anyone waits
	q.NotifyUpTo(50)

	// Now wait for a txnID that's already passed
	done := make(chan error, 1)
	go func() {
		done <- q.Wait(context.Background(), 100)
	}()

	// This should still wait since queue doesn't track history
	time.Sleep(20 * time.Millisecond)

	if q.Len() != 1 {
		t.Fatalf("expected 1 waiter, got %d", q.Len())
	}

	q.NotifyUpTo(100)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waiter not notified")
	}
}

func TestTxnWaitQueue_DuplicateTxnIDs(t *testing.T) {
	q := NewTxnWaitQueue()

	var wg sync.WaitGroup
	results := make([]error, 3)

	// Multiple waiters for same txnID
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = q.Wait(context.Background(), 100)
		}(i)
	}

	// Give waiters time to register
	time.Sleep(20 * time.Millisecond)

	if q.Len() != 3 {
		t.Fatalf("expected 3 waiters, got %d", q.Len())
	}

	// Single notify should wake all
	q.NotifyUpTo(100)

	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Errorf("waiter %d got error: %v", i, err)
		}
	}

	if q.Len() != 0 {
		t.Fatalf("expected 0 waiters after notification, got %d", q.Len())
	}
}

func TestTxnWaitQueue_PartialNotify(t *testing.T) {
	q := NewTxnWaitQueue()

	var wg sync.WaitGroup

	// Start waiters for txnIDs: 10, 20, 30
	txnIDs := []uint64{10, 20, 30}
	notified := make([]atomic.Bool, 3)

	for i, txnID := range txnIDs {
		wg.Add(1)
		go func(idx int, id uint64) {
			defer wg.Done()
			if err := q.Wait(context.Background(), id); err == nil {
				notified[idx].Store(true)
			}
		}(i, txnID)
	}

	// Give waiters time to register
	time.Sleep(20 * time.Millisecond)

	// Notify up to 15 - should only wake waiter for txnID 10
	q.NotifyUpTo(15)

	// Give some time for notification
	time.Sleep(20 * time.Millisecond)

	if !notified[0].Load() {
		t.Error("waiter for txnID 10 should be notified")
	}
	if notified[1].Load() || notified[2].Load() {
		t.Error("waiters for txnID 20, 30 should not be notified yet")
	}

	if q.Len() != 2 {
		t.Fatalf("expected 2 waiters remaining, got %d", q.Len())
	}

	// Notify rest
	q.NotifyUpTo(30)
	wg.Wait()

	if !notified[1].Load() || !notified[2].Load() {
		t.Error("all waiters should be notified")
	}
}

func TestTxnWaitQueue_Timeout(t *testing.T) {
	q := NewTxnWaitQueue()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := q.Wait(ctx, 100)
	duration := time.Since(start)

	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	if duration < 40*time.Millisecond || duration > 100*time.Millisecond {
		t.Fatalf("timeout took %v, expected ~50ms", duration)
	}

	// Give cleanup time to run
	time.Sleep(10 * time.Millisecond)

	if q.Len() != 0 {
		t.Fatalf("expected 0 waiters after timeout, got %d", q.Len())
	}
}

func TestTxnWaitQueue_ConcurrentWaitAndNotify(t *testing.T) {
	q := NewTxnWaitQueue()

	var wg sync.WaitGroup
	var successCount atomic.Int32

	// Start 100 waiters
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			if err := q.Wait(ctx, txnID); err == nil {
				successCount.Add(1)
			}
		}(uint64(i))
	}

	// Give waiters time to register
	time.Sleep(50 * time.Millisecond)

	// Concurrently notify in batches
	for i := 0; i < 10; i++ {
		q.NotifyUpTo(uint64((i + 1) * 10))
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	if successCount.Load() != 100 {
		t.Errorf("expected 100 successful waits, got %d", successCount.Load())
	}

	if q.Len() != 0 {
		t.Fatalf("expected 0 waiters remaining, got %d", q.Len())
	}
}

func BenchmarkTxnWaitQueue_SingleWaiter(b *testing.B) {
	q := NewTxnWaitQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func(txnID uint64) {
			q.Wait(context.Background(), txnID)
		}(uint64(i))

		q.NotifyUpTo(uint64(i))
	}
}

func BenchmarkTxnWaitQueue_1000Waiters(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q := NewTxnWaitQueue()
		var wg sync.WaitGroup

		// Start 1000 waiters
		for j := 0; j < 1000; j++ {
			wg.Add(1)
			go func(txnID uint64) {
				defer wg.Done()
				q.Wait(context.Background(), txnID)
			}(uint64(j))
		}

		// Give goroutines time to register
		for q.Len() < 1000 {
			time.Sleep(time.Microsecond)
		}

		b.StartTimer()
		// Notify all at once
		q.NotifyUpTo(1000)
		wg.Wait()
		b.StopTimer()
	}
}

func BenchmarkTxnWaitQueue_BatchNotify(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q := NewTxnWaitQueue()
		var wg sync.WaitGroup

		// Start 1000 waiters
		for j := 0; j < 1000; j++ {
			wg.Add(1)
			go func(txnID uint64) {
				defer wg.Done()
				q.Wait(context.Background(), txnID)
			}(uint64(j))
		}

		// Give goroutines time to register
		for q.Len() < 1000 {
			time.Sleep(time.Microsecond)
		}

		b.StartTimer()
		// Notify in batches of 100
		for j := 0; j < 10; j++ {
			q.NotifyUpTo(uint64((j + 1) * 100))
		}
		wg.Wait()
		b.StopTimer()
	}
}
