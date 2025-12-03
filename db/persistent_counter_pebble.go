package db

import (
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/pebble"
)

// PebbleCounter provides thread-safe, write-through cached counters backed by Pebble.
// Counters are loaded on first access and cached in memory. All writes are persisted immediately.
type PebbleCounter struct {
	db     *pebble.DB
	prefix string

	mu       sync.RWMutex
	counters map[string]*pebbleCounterEntry
	lruOrder []string // Simple LRU tracking
	maxSize  int      // Max cached counters
}

type pebbleCounterEntry struct {
	mu    sync.Mutex
	value int64
}

// NewPebbleCounter creates a new persistent counter with LRU cache.
// prefix is prepended to all counter names for namespacing.
// maxCached limits memory usage (0 = unlimited).
func NewPebbleCounter(db *pebble.DB, prefix string, maxCached int) *PebbleCounter {
	if maxCached <= 0 {
		maxCached = 1000 // Default max
	}
	return &PebbleCounter{
		db:       db,
		prefix:   prefix,
		counters: make(map[string]*pebbleCounterEntry),
		lruOrder: make([]string, 0, maxCached),
		maxSize:  maxCached,
	}
}

// key returns the Pebble key for a counter name
func (pc *PebbleCounter) key(name string) []byte {
	return []byte(pc.prefix + name)
}

// getOrLoad gets counter from cache or loads from Pebble
func (pc *PebbleCounter) getOrLoad(name string) (*pebbleCounterEntry, error) {
	// Fast path: check cache with read lock
	pc.mu.RLock()
	entry, exists := pc.counters[name]
	pc.mu.RUnlock()

	if exists {
		return entry, nil
	}

	// Slow path: load from DB and cache
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Double-check after acquiring write lock
	if entry, exists = pc.counters[name]; exists {
		return entry, nil
	}

	// Load from Pebble
	var value int64
	val, closer, err := pc.db.Get(pc.key(name))
	if err == nil {
		if len(val) >= 8 {
			value = int64(binary.BigEndian.Uint64(val))
		}
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return nil, err
	}
	// ErrNotFound is fine - default to 0

	// Evict if at capacity (simple LRU)
	if len(pc.counters) >= pc.maxSize && pc.maxSize > 0 {
		pc.evictOldest()
	}

	entry = &pebbleCounterEntry{value: value}
	pc.counters[name] = entry
	pc.lruOrder = append(pc.lruOrder, name)

	return entry, nil
}

// evictOldest removes the oldest cached counter (must hold write lock)
func (pc *PebbleCounter) evictOldest() {
	if len(pc.lruOrder) == 0 {
		return
	}
	oldest := pc.lruOrder[0]
	pc.lruOrder = pc.lruOrder[1:]
	delete(pc.counters, oldest)
}

// persist writes the counter value to Pebble
func (pc *PebbleCounter) persist(name string, value int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return pc.db.Set(pc.key(name), buf, pebble.NoSync)
}

// Load returns the current value of a counter
func (pc *PebbleCounter) Load(name string) (int64, error) {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return 0, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.value, nil
}

// Store sets a counter to a specific value (write-through)
func (pc *PebbleCounter) Store(name string, value int64) error {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if err := pc.persist(name, value); err != nil {
		return err
	}
	entry.value = value
	return nil
}

// Inc atomically increments a counter by delta and returns the new value (write-through)
func (pc *PebbleCounter) Inc(name string, delta int64) (int64, error) {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return 0, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	newValue := entry.value + delta
	if err := pc.persist(name, newValue); err != nil {
		return entry.value, err
	}
	entry.value = newValue
	return newValue, nil
}

// Dec atomically decrements a counter by delta and returns the new value (write-through)
// Value is clamped to 0 (won't go negative)
func (pc *PebbleCounter) Dec(name string, delta int64) (int64, error) {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return 0, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	newValue := entry.value - delta
	if newValue < 0 {
		newValue = 0
	}
	if err := pc.persist(name, newValue); err != nil {
		return entry.value, err
	}
	entry.value = newValue
	return newValue, nil
}

// UpdateMax atomically updates the counter to max(current, value) and returns the new value
func (pc *PebbleCounter) UpdateMax(name string, value int64) (int64, error) {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return 0, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if value > entry.value {
		if err := pc.persist(name, value); err != nil {
			return entry.value, err
		}
		entry.value = value
	}
	return entry.value, nil
}

// IncInBatch increments counter within an existing Pebble batch.
// Updates cache immediately (caller must ensure batch succeeds).
// Returns the new value.
func (pc *PebbleCounter) IncInBatch(batch *pebble.Batch, name string, delta int64) error {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	newValue := entry.value + delta
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(newValue))
	if err := batch.Set(pc.key(name), buf, nil); err != nil {
		return err
	}
	entry.value = newValue
	return nil
}

// DecInBatch decrements counter within an existing Pebble batch.
// Value is clamped to 0. Returns the new value.
func (pc *PebbleCounter) DecInBatch(batch *pebble.Batch, name string, delta int64) error {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	newValue := entry.value - delta
	if newValue < 0 {
		newValue = 0
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(newValue))
	if err := batch.Set(pc.key(name), buf, nil); err != nil {
		return err
	}
	entry.value = newValue
	return nil
}

// UpdateMaxInBatch updates counter to max(current, value) within an existing batch.
// Returns the new value.
func (pc *PebbleCounter) UpdateMaxInBatch(batch *pebble.Batch, name string, value int64) error {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if value > entry.value {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(value))
		if err := batch.Set(pc.key(name), buf, nil); err != nil {
			return err
		}
		entry.value = value
	}
	return nil
}

// LoadUint64 returns the current value as uint64 (convenience method)
func (pc *PebbleCounter) LoadUint64(name string) (uint64, error) {
	v, err := pc.Load(name)
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

// Invalidate removes a counter from cache (forces reload on next access)
func (pc *PebbleCounter) Invalidate(name string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	delete(pc.counters, name)
	// Remove from LRU order
	for i, n := range pc.lruOrder {
		if n == name {
			pc.lruOrder = append(pc.lruOrder[:i], pc.lruOrder[i+1:]...)
			break
		}
	}
}

// Clear removes all counters from cache
func (pc *PebbleCounter) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.counters = make(map[string]*pebbleCounterEntry)
	pc.lruOrder = pc.lruOrder[:0]
}
