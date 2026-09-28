package stallwatch

import (
	"sync/atomic"
	"testing"
	"time"
)

const (
	testTimeout = 300 * time.Millisecond
	testPoll    = 20 * time.Millisecond
)

// TestWatchdogFiresWhenNothingChanges: a cluster whose observed activity and
// ledger both stand still is reported stalled, once, soon after the timeout.
//
// Mutation: never call onStall. "a silent cluster was not reported" fires.
func TestWatchdogFiresWhenNothingChanges(t *testing.T) {
	fired := make(chan time.Duration, 2)
	w := Start(testTimeout, testPoll, func() int64 { return 7 }, func(idle time.Duration) { fired <- idle })
	defer w.Stop()
	select {
	case idle := <-fired:
		if idle < testTimeout {
			t.Fatalf("reported stalled after %s, before the %s timeout", idle, testTimeout)
		}
	case <-time.After(10 * testTimeout):
		t.Fatal("a silent cluster was not reported")
	}
	select {
	case <-fired:
		t.Fatal("a stall was reported twice")
	case <-time.After(3 * testTimeout):
	}
}

// TestWatchdogStaysQuietWhileTheClusterMakesProgress: activity the watchdog
// observes, or progress the test reports, keeps it from firing.
//
// Mutation: ignore observed changes. "observed activity was reported as a
// stall" fires.
func TestWatchdogStaysQuietWhileTheClusterMakesProgress(t *testing.T) {
	var counter atomic.Int64
	fired := make(chan time.Duration, 1)
	w := Start(testTimeout, testPoll, func() int64 { return counter.Add(1) }, func(idle time.Duration) { fired <- idle })
	time.Sleep(4 * testTimeout)
	w.Stop()
	select {
	case <-fired:
		t.Fatal("observed activity was reported as a stall")
	default:
	}

	w = Start(testTimeout, testPoll, func() int64 { return 1 }, func(idle time.Duration) { fired <- idle })
	deadline := time.Now().Add(4 * testTimeout)
	for time.Now().Before(deadline) {
		w.Progress()
		time.Sleep(testPoll)
	}
	w.Stop()
	select {
	case <-fired:
		t.Fatal("reported progress was reported as a stall")
	default:
	}
}
