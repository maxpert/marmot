package notify

import (
	"sync"
	"sync/atomic"

	"github.com/maxpert/marmot/db"
)

// defaultSignalBufferSize is the buffer size for CDC signal channels.
// Sized to handle typical burst rates while keeping memory low.
// Subscribers that can't keep up will have signals dropped (non-blocking send).
const defaultSignalBufferSize = 16

// subscription represents a single subscriber.
type subscription struct {
	id     uint64
	filter db.CDCFilter
	ch     chan db.CDCSignal
	closed atomic.Bool
}

// matches checks if the database matches this subscription's filter.
func (s *subscription) matches(database string) bool {
	// nil or empty = all databases
	if len(s.filter.Databases) == 0 {
		return true
	}

	for _, db := range s.filter.Databases {
		if db == database {
			return true
		}
	}
	return false
}

// close closes the subscription channel if not already closed.
func (s *subscription) close() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
}

// Hub implements db.CDCHub interface.
// Thread-safe notification hub for CDC signals.
type Hub struct {
	mu            sync.RWMutex
	subscriptions map[uint64]*subscription
	nextID        atomic.Uint64
}

// NewHub creates a new CDC notification hub.
func NewHub() *Hub {
	return &Hub{
		subscriptions: make(map[uint64]*subscription),
	}
}

// Signal sends a CDC signal to all matching subscribers (non-blocking).
func (h *Hub) Signal(database string, txnID uint64) {
	signal := db.CDCSignal{
		Database: database,
		TxnID:    txnID,
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, sub := range h.subscriptions {
		if !sub.matches(database) {
			continue
		}

		// Non-blocking send - drop if buffer full
		select {
		case sub.ch <- signal:
		default:
			// Buffer full, skip this subscriber
		}
	}
}

// Subscribe creates a new subscription and returns the signal channel and cancel function.
// The returned channel is buffered. If the subscriber cannot keep up with the signal rate,
// signals will be dropped silently by Signal(). The cancel function is idempotent.
func (h *Hub) Subscribe(filter db.CDCFilter) (<-chan db.CDCSignal, func()) {
	sub := &subscription{
		id:     h.nextID.Add(1),
		filter: filter,
		ch:     make(chan db.CDCSignal, defaultSignalBufferSize),
	}

	h.mu.Lock()
	h.subscriptions[sub.id] = sub
	h.mu.Unlock()

	cancel := func() {
		h.unsubscribe(sub.id)
	}

	return sub.ch, cancel
}

// unsubscribe removes a subscription and closes its channel.
func (h *Hub) unsubscribe(id uint64) {
	h.mu.Lock()
	sub, ok := h.subscriptions[id]
	if ok {
		delete(h.subscriptions, id)
	}
	h.mu.Unlock()

	if ok {
		sub.close()
	}
}
