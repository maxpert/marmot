// Package stallwatch detects a test cluster that has stopped making progress,
// so a hung cluster test fails within a bounded time instead of running into
// its package timeout.
package stallwatch

import (
	"sync"
	"sync/atomic"
	"time"
)

// Watchdog calls onStall once when, for longer than timeout, neither the
// observed value changed nor Progress was called. The observed value is any
// cheap digest of the cluster's activity (for a process cluster: the bytes
// its nodes have written).
type Watchdog struct {
	lastProgress atomic.Int64 // unix nanoseconds
	stop         chan struct{}
	stopOnce     sync.Once
	done         chan struct{}
}

// Start begins watching: observe is polled every poll interval.
func Start(timeout, poll time.Duration, observe func() int64, onStall func(idle time.Duration)) *Watchdog {
	w := &Watchdog{stop: make(chan struct{}), done: make(chan struct{})}
	w.Progress()
	go w.run(timeout, poll, observe, onStall)
	return w
}

// Progress records that the caller saw the cluster make progress.
func (w *Watchdog) Progress() {
	w.lastProgress.Store(time.Now().UnixNano())
}

// Stop ends watching and waits for the watcher to exit. It is safe to call
// more than once, including after onStall has run.
func (w *Watchdog) Stop() {
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
}

func (w *Watchdog) run(timeout, poll time.Duration, observe func() int64, onStall func(idle time.Duration)) {
	defer close(w.done)
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	last := observe()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
		}
		if v := observe(); v != last {
			last = v
			w.Progress()
			continue
		}
		idle := time.Since(time.Unix(0, w.lastProgress.Load()))
		if idle > timeout {
			onStall(idle)
			return
		}
	}
}
