package common

import "sync/atomic"

// Broadcast wakes every waiter at once, without locks: Next returns a channel
// the next Notify closes. A waiter takes the channel before it checks the
// condition it waits for, so a Notify that follows the check always wakes
// it. The zero value is ready to use.
type Broadcast struct {
	ch atomic.Pointer[chan struct{}]
}

// Next returns the channel the next Notify closes.
func (b *Broadcast) Next() <-chan struct{} {
	for {
		if ch := b.ch.Load(); ch != nil {
			return *ch
		}
		fresh := make(chan struct{})
		b.ch.CompareAndSwap(nil, &fresh)
	}
}

// Notify wakes every waiter on the current channel and arms a new one.
func (b *Broadcast) Notify() {
	fresh := make(chan struct{})
	if old := b.ch.Swap(&fresh); old != nil {
		close(*old)
	}
}
