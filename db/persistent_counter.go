package db

import (
	"encoding/binary"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// PersistentCounter provides thread-safe, write-through cached counters backed by BadgerDB.
// Counters are loaded on first access and cached in memory. All writes are persisted immediately.
type PersistentCounter struct {
	db     *badger.DB
	prefix string

	mu       sync.RWMutex
	counters map[string]*counterEntry
	lruOrder []string // Simple LRU tracking
	maxSize  int      // Max cached counters
}

type counterEntry struct {
	mu    sync.Mutex
	value int64
}

// NewPersistentCounter creates a new persistent counter with LRU cache.
// prefix is prepended to all counter names for namespacing in BadgerDB.
// maxCached limits memory usage (0 = unlimited).
func NewPersistentCounter(db *badger.DB, prefix string, maxCached int) *PersistentCounter {
	if maxCached <= 0 {
		maxCached = 1000 // Default max
	}
	return &PersistentCounter{
		db:       db,
		prefix:   prefix,
		counters: make(map[string]*counterEntry),
		lruOrder: make([]string, 0, maxCached),
		maxSize:  maxCached,
	}
}

// key returns the BadgerDB key for a counter name
func (pc *PersistentCounter) key(name string) []byte {
	return []byte(pc.prefix + name)
}

// getOrLoad gets counter from cache or loads from BadgerDB
func (pc *PersistentCounter) getOrLoad(name string) (*counterEntry, error) {
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

	// Load from BadgerDB
	var value int64
	err := pc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(pc.key(name))
		if err == badger.ErrKeyNotFound {
			return nil // Default to 0
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) >= 8 {
				value = int64(binary.BigEndian.Uint64(val))
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Evict if at capacity (simple LRU)
	if len(pc.counters) >= pc.maxSize && pc.maxSize > 0 {
		pc.evictOldest()
	}

	entry = &counterEntry{value: value}
	pc.counters[name] = entry
	pc.lruOrder = append(pc.lruOrder, name)

	return entry, nil
}

// evictOldest removes the oldest cached counter (must hold write lock)
func (pc *PersistentCounter) evictOldest() {
	if len(pc.lruOrder) == 0 {
		return
	}
	oldest := pc.lruOrder[0]
	pc.lruOrder = pc.lruOrder[1:]
	delete(pc.counters, oldest)
}

// persist writes the counter value to BadgerDB
func (pc *PersistentCounter) persist(name string, value int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return pc.db.Update(func(txn *badger.Txn) error {
		return txn.Set(pc.key(name), buf)
	})
}

// persistInTxn writes the counter value within an existing transaction
func (pc *PersistentCounter) persistInTxn(txn *badger.Txn, name string, value int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return txn.Set(pc.key(name), buf)
}

// Load returns the current value of a counter
func (pc *PersistentCounter) Load(name string) (int64, error) {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return 0, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.value, nil
}

// Store sets a counter to a specific value (write-through)
func (pc *PersistentCounter) Store(name string, value int64) error {
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
func (pc *PersistentCounter) Inc(name string, delta int64) (int64, error) {
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
func (pc *PersistentCounter) Dec(name string, delta int64) (int64, error) {
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
func (pc *PersistentCounter) UpdateMax(name string, value int64) (int64, error) {
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

// IncInTxn increments counter within an existing BadgerDB transaction.
// Updates cache after txn commits (caller must ensure txn succeeds).
// Returns the new value.
func (pc *PersistentCounter) IncInTxn(txn *badger.Txn, name string, delta int64) (int64, error) {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return 0, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	newValue := entry.value + delta
	if err := pc.persistInTxn(txn, name, newValue); err != nil {
		return entry.value, err
	}
	entry.value = newValue
	return newValue, nil
}

// DecInTxn decrements counter within an existing BadgerDB transaction.
// Value is clamped to 0. Returns the new value.
func (pc *PersistentCounter) DecInTxn(txn *badger.Txn, name string, delta int64) (int64, error) {
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
	if err := pc.persistInTxn(txn, name, newValue); err != nil {
		return entry.value, err
	}
	entry.value = newValue
	return newValue, nil
}

// UpdateMaxInTxn updates counter to max(current, value) within an existing transaction.
// Returns the new value.
func (pc *PersistentCounter) UpdateMaxInTxn(txn *badger.Txn, name string, value int64) (int64, error) {
	entry, err := pc.getOrLoad(name)
	if err != nil {
		return 0, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if value > entry.value {
		if err := pc.persistInTxn(txn, name, value); err != nil {
			return entry.value, err
		}
		entry.value = value
	}
	return entry.value, nil
}

// LoadUint64 returns the current value as uint64 (convenience method)
func (pc *PersistentCounter) LoadUint64(name string) (uint64, error) {
	v, err := pc.Load(name)
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

// Invalidate removes a counter from cache (forces reload on next access)
func (pc *PersistentCounter) Invalidate(name string) {
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
func (pc *PersistentCounter) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.counters = make(map[string]*counterEntry)
	pc.lruOrder = pc.lruOrder[:0]
}
