package common

import (
	"sync"
	"testing"
)

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestBroadcast_NotifyClosesEarlierNextOnly pins the wake-up contract on the
// zero value: a channel taken before a Notify is closed by it, and one taken
// after it stays open until the next Notify.
//
// Mutation: Notify closing the fresh channel instead of the old one fails
// "before" here and TestBroadcast_NotifyWithoutWaiters.
func TestBroadcast_NotifyClosesEarlierNextOnly(t *testing.T) {
	var b Broadcast
	before := b.Next()
	if closed(before) {
		t.Fatal("a channel was closed before any Notify")
	}
	b.Notify()
	if !closed(before) {
		t.Fatal("before: Notify did not close a channel taken before it")
	}
	after := b.Next()
	if closed(after) {
		t.Fatal("after: a channel taken after the Notify is already closed")
	}
	b.Notify()
	if !closed(after) {
		t.Fatal("the second Notify did not close the channel taken after the first")
	}
}

// TestBroadcast_NotifyWithoutWaiters pins that Notify on the zero value, with
// no Next taken, neither panics nor pre-closes the next channel.
func TestBroadcast_NotifyWithoutWaiters(t *testing.T) {
	var b Broadcast
	b.Notify()
	b.Notify()
	if closed(b.Next()) {
		t.Fatal("a channel taken after every Notify is closed")
	}
}

// TestBroadcast_ConcurrentNextAndNotify runs Next and Notify from many
// goroutines, for -race, and checks that no waiter misses the final Notify.
func TestBroadcast_ConcurrentNextAndNotify(t *testing.T) {
	var b Broadcast
	const workers, rounds = 8, 1000
	var wg sync.WaitGroup
	for range workers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range rounds {
				_ = b.Next()
			}
		}()
		go func() {
			defer wg.Done()
			for range rounds {
				b.Notify()
			}
		}()
	}
	wg.Wait()

	last := b.Next()
	b.Notify()
	if !closed(last) {
		t.Fatal("a channel taken after the concurrent phase missed the final Notify")
	}
}
