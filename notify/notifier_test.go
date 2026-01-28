package notify

import (
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/db"
)

func TestHub_BasicSubscribeSignal(t *testing.T) {
	hub := NewHub()

	// Subscribe to all databases
	signals, cancel := hub.Subscribe(db.CDCFilter{})
	defer cancel()

	// Send a signal
	hub.Signal("testdb", 1)

	// Should receive the signal
	select {
	case sig := <-signals:
		if sig.Database != "testdb" || sig.TxnID != 1 {
			t.Errorf("expected (testdb, 1), got (%s, %d)", sig.Database, sig.TxnID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for signal")
	}
}

func TestHub_FilterSpecificDatabase(t *testing.T) {
	hub := NewHub()

	// Subscribe only to "db1"
	signals, cancel := hub.Subscribe(db.CDCFilter{Databases: []string{"db1"}})
	defer cancel()

	// Send signal to db1 (should receive)
	hub.Signal("db1", 1)

	select {
	case sig := <-signals:
		if sig.Database != "db1" || sig.TxnID != 1 {
			t.Errorf("expected (db1, 1), got (%s, %d)", sig.Database, sig.TxnID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for signal")
	}

	// Send signal to db2 (should NOT receive)
	hub.Signal("db2", 2)

	select {
	case sig := <-signals:
		t.Errorf("should not receive signal for db2, got (%s, %d)", sig.Database, sig.TxnID)
	case <-time.After(50 * time.Millisecond):
		// Expected - no signal
	}
}

func TestHub_FilterAllDatabases(t *testing.T) {
	hub := NewHub()

	// Subscribe to all databases (nil filter)
	signals, cancel := hub.Subscribe(db.CDCFilter{})
	defer cancel()

	// Should receive signals from any database
	hub.Signal("db1", 1)
	hub.Signal("db2", 2)
	hub.Signal("db3", 3)

	received := 0
	for i := 0; i < 3; i++ {
		select {
		case <-signals:
			received++
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timeout waiting for signal %d", i+1)
		}
	}

	if received != 3 {
		t.Errorf("expected 3 signals, got %d", received)
	}
}

func TestHub_FilterMultipleDatabases(t *testing.T) {
	hub := NewHub()

	// Subscribe to db1 and db3
	signals, cancel := hub.Subscribe(db.CDCFilter{Databases: []string{"db1", "db3"}})
	defer cancel()

	hub.Signal("db1", 1)
	hub.Signal("db2", 2) // Should be filtered out
	hub.Signal("db3", 3)

	// Should receive 2 signals (db1 and db3)
	received := make(map[string]uint64)
	for i := 0; i < 2; i++ {
		select {
		case sig := <-signals:
			received[sig.Database] = sig.TxnID
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timeout waiting for signal %d", i+1)
		}
	}

	if len(received) != 2 {
		t.Errorf("expected 2 unique signals, got %d", len(received))
	}
	if received["db1"] != 1 || received["db3"] != 3 {
		t.Errorf("received unexpected signals: %v", received)
	}

	// Should NOT receive any more signals
	select {
	case sig := <-signals:
		t.Errorf("should not receive signal, got (%s, %d)", sig.Database, sig.TxnID)
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

func TestHub_CancelUnsubscribes(t *testing.T) {
	hub := NewHub()

	signals, cancel := hub.Subscribe(db.CDCFilter{})

	// Send signal before cancel
	hub.Signal("testdb", 1)

	select {
	case <-signals:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for signal")
	}

	// Cancel subscription
	cancel()

	// Channel should be closed
	select {
	case _, ok := <-signals:
		if ok {
			t.Error("channel should be closed after cancel")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for channel close")
	}

	// Subsequent signals should not panic
	hub.Signal("testdb", 2)
}

func TestHub_MultipleSubscribers(t *testing.T) {
	hub := NewHub()

	// Create 3 subscribers
	signals1, cancel1 := hub.Subscribe(db.CDCFilter{})
	defer cancel1()
	signals2, cancel2 := hub.Subscribe(db.CDCFilter{Databases: []string{"db1"}})
	defer cancel2()
	signals3, cancel3 := hub.Subscribe(db.CDCFilter{Databases: []string{"db2"}})
	defer cancel3()

	// Send signal to db1
	hub.Signal("db1", 1)

	// signals1 and signals2 should receive
	select {
	case sig := <-signals1:
		if sig.Database != "db1" || sig.TxnID != 1 {
			t.Errorf("signals1: expected (db1, 1), got (%s, %d)", sig.Database, sig.TxnID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout on signals1")
	}

	select {
	case sig := <-signals2:
		if sig.Database != "db1" || sig.TxnID != 1 {
			t.Errorf("signals2: expected (db1, 1), got (%s, %d)", sig.Database, sig.TxnID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout on signals2")
	}

	// signals3 should NOT receive
	select {
	case sig := <-signals3:
		t.Errorf("signals3 should not receive, got (%s, %d)", sig.Database, sig.TxnID)
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

func TestHub_ConcurrentSignalSubscribe(t *testing.T) {
	hub := NewHub()
	const numGoroutines = 10
	const numSignals = 100

	var wg sync.WaitGroup

	// Start goroutines that subscribe and receive signals
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			signals, cancel := hub.Subscribe(db.CDCFilter{})
			defer cancel()

			received := 0
			timeout := time.After(2 * time.Second)
			for received < numSignals {
				select {
				case <-signals:
					received++
				case <-timeout:
					return
				}
			}
		}(i)
	}

	// Start goroutine that sends signals
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numSignals; i++ {
			hub.Signal("testdb", uint64(i))
		}
	}()

	wg.Wait()
}

func TestHub_BufferOverflowNonBlocking(t *testing.T) {
	hub := NewHub()

	signals, cancel := hub.Subscribe(db.CDCFilter{})
	defer cancel()

	// Fill the buffer (16) and send more
	for i := 0; i < 20; i++ {
		hub.Signal("testdb", uint64(i))
	}

	// Should receive at least 16 signals without blocking
	received := 0
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case <-signals:
			received++
		case <-timeout:
			if received < 16 {
				t.Errorf("expected at least 16 signals, got %d", received)
			}
			return
		}
	}
}

func TestHub_SignalBeforeSubscribe(t *testing.T) {
	hub := NewHub()

	// Send signal before any subscription
	hub.Signal("testdb", 1)

	// Should not panic
	signals, cancel := hub.Subscribe(db.CDCFilter{})
	defer cancel()

	// Should not receive the old signal
	select {
	case sig := <-signals:
		t.Errorf("should not receive old signal, got (%s, %d)", sig.Database, sig.TxnID)
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

func TestHub_DoubleCancel(t *testing.T) {
	hub := NewHub()

	_, cancel := hub.Subscribe(db.CDCFilter{})

	// First cancel
	cancel()

	// Second cancel should not panic
	cancel()
}

func TestHub_UniqueSubscriptionIDs(t *testing.T) {
	hub := NewHub()

	// Create multiple subscriptions and verify unique IDs
	const numSubs = 100
	cancels := make([]func(), numSubs)

	for i := 0; i < numSubs; i++ {
		_, cancel := hub.Subscribe(db.CDCFilter{})
		cancels[i] = cancel
	}

	// All subscriptions should have unique IDs
	if len(hub.subscriptions) != numSubs {
		t.Errorf("expected %d subscriptions, got %d", numSubs, len(hub.subscriptions))
	}

	// Cleanup
	for _, cancel := range cancels {
		cancel()
	}

	// All subscriptions should be removed
	if len(hub.subscriptions) != 0 {
		t.Errorf("expected 0 subscriptions after cancel, got %d", len(hub.subscriptions))
	}
}
