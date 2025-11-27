package grpc

import (
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/rs/zerolog/log"
)

// ActiveGuard represents an active MutationGuard for a transaction.
// Guards can be either:
//   - Filter-only (large mutations): Only contains Bloom filter, no keys. Only one allowed per table.
//   - Key-based (small mutations): Contains both filter and keys. Can run concurrently.
type ActiveGuard struct {
	TxnID        uint64
	Table        string
	Filter       *filter.BloomFilter
	Keys         []uint64 // Empty for filter-only guards, populated for key-based guards
	IsFilterOnly bool     // True = large mutation with filter only, serialized per table
	Timestamp    hlc.Timestamp
	ExpiresAt    time.Time
	RowCount     int64
}

// ConflictResult describes the outcome of a conflict check
type ConflictResult struct {
	HasConflict    bool
	ConflictingTxn uint64
	ShouldWait     bool // If true, wait for conflicting txn; if false, wound (abort) conflicting txn
	Details        string
}

// GuardRegistry manages active MutationGuards for conflict detection.
//
// Key design: Only ONE filter-only guard can be active per table at a time.
// This serializes large mutations while allowing concurrent key-based mutations.
//
// Conflict Detection Matrix:
//
//	| Incoming     | Existing     | Detection Method                      |
//	|--------------|--------------|---------------------------------------|
//	| Filter-only  | Filter-only  | Block (one per table)                 |
//	| Filter-only  | Has keys     | Probe incoming filter with existing keys |
//	| Has keys     | Filter-only  | Probe existing filter with incoming keys |
//	| Has keys     | Has keys     | Probe existing filter with incoming keys |
type GuardRegistry struct {
	mu                    sync.RWMutex
	guards                map[string][]*ActiveGuard // table -> all guards
	byTxn                 map[uint64][]*ActiveGuard // txn_id -> guards
	activeFilterOnlyGuard map[string]*ActiveGuard   // table -> single active filter-only guard
	intentTTL             time.Duration
	cleanupInterval       time.Duration
	stopCh                chan struct{}
}

// NewGuardRegistry creates a new guard registry
func NewGuardRegistry(intentTTL time.Duration) *GuardRegistry {
	r := &GuardRegistry{
		guards:                make(map[string][]*ActiveGuard),
		byTxn:                 make(map[uint64][]*ActiveGuard),
		activeFilterOnlyGuard: make(map[string]*ActiveGuard),
		intentTTL:             intentTTL,
		cleanupInterval:       5 * time.Second,
		stopCh:                make(chan struct{}),
	}
	go r.cleanupLoop()
	return r
}

// Register adds a new guard to the registry.
// For filter-only guards, only one can be active per table at a time.
func (r *GuardRegistry) Register(guard *ActiveGuard) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guard.ExpiresAt = time.Now().Add(r.intentTTL)

	// Check if this is a filter-only guard
	if guard.IsFilterOnly {
		if existing := r.activeFilterOnlyGuard[guard.Table]; existing != nil {
			return fmt.Errorf("filter-only guard already active for table %s (txn %d)",
				guard.Table, existing.TxnID)
		}
		r.activeFilterOnlyGuard[guard.Table] = guard
	}

	r.guards[guard.Table] = append(r.guards[guard.Table], guard)
	r.byTxn[guard.TxnID] = append(r.byTxn[guard.TxnID], guard)

	log.Debug().
		Uint64("txn_id", guard.TxnID).
		Str("table", guard.Table).
		Bool("filter_only", guard.IsFilterOnly).
		Int("key_count", len(guard.Keys)).
		Int64("row_count", guard.RowCount).
		Msg("MutationGuard registered")

	return nil
}

// CheckConflict checks if a new guard conflicts with existing guards.
// Uses Wound-Wait algorithm: older transactions always proceed.
//
// For filter-only guards: Only one can be active per table (serialization).
// For key-based guards: Probe existing filters with incoming keys.
func (r *GuardRegistry) CheckConflict(newGuard *ActiveGuard) ConflictResult {
	r.mu.RLock()
	defer r.mu.RUnlock()

	existingGuards := r.guards[newGuard.Table]
	activeFilterOnly := r.activeFilterOnlyGuard[newGuard.Table]

	// Case 1: New guard is filter-only
	if newGuard.IsFilterOnly {
		// Check if another filter-only guard is already active
		if activeFilterOnly != nil && activeFilterOnly.TxnID != newGuard.TxnID {
			return applyWoundWait(newGuard, activeFilterOnly,
				"filter-only: table already has active large mutation")
		}

		// Check against key-based guards (probe new filter with their keys)
		for _, existing := range existingGuards {
			if existing.TxnID == newGuard.TxnID || existing.IsFilterOnly {
				continue
			}
			// Existing has keys - probe new filter with existing keys
			if bloomContainsAny(newGuard.Filter, existing.Keys) {
				return applyWoundWait(newGuard, existing,
					"filter-only conflicts with existing key-based guard")
			}
		}
	} else {
		// Case 2: New guard has keys - probe existing filters
		for _, existing := range existingGuards {
			if existing.TxnID == newGuard.TxnID {
				continue
			}
			// Probe existing filter with new guard's keys
			if bloomContainsAny(existing.Filter, newGuard.Keys) {
				return applyWoundWait(newGuard, existing,
					"key probe: conflict detected")
			}
		}
	}

	return ConflictResult{HasConflict: false}
}

// applyWoundWait applies the Wound-Wait deadlock prevention algorithm.
// Older transactions always proceed (wound younger or proceed past older).
func applyWoundWait(newGuard, existing *ActiveGuard, reason string) ConflictResult {
	if hlc.Compare(newGuard.Timestamp, existing.Timestamp) < 0 {
		// New transaction is older -> wound (abort) existing
		return ConflictResult{
			HasConflict:    true,
			ConflictingTxn: existing.TxnID,
			ShouldWait:     false, // Abort existing
			Details:        fmt.Sprintf("%s: older transaction wounds younger", reason),
		}
	}
	// New transaction is younger -> wait for existing
	return ConflictResult{
		HasConflict:    true,
		ConflictingTxn: existing.TxnID,
		ShouldWait:     true, // Wait for existing
		Details:        fmt.Sprintf("%s: younger transaction waits for older", reason),
	}
}

// bloomContainsAny checks if any key might be in the Bloom filter
func bloomContainsAny(f *filter.BloomFilter, keys []uint64) bool {
	if f == nil || len(keys) == 0 {
		return false
	}
	for _, key := range keys {
		if f.Contains(key) {
			return true
		}
	}
	return false
}

// MarkComplete marks a transaction's guards as complete.
// For filter-only guards, this releases the per-table slot.
func (r *GuardRegistry) MarkComplete(txnID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	guards := r.byTxn[txnID]
	for _, g := range guards {
		if g.IsFilterOnly {
			// Release filter-only slot
			if r.activeFilterOnlyGuard[g.Table] != nil &&
				r.activeFilterOnlyGuard[g.Table].TxnID == txnID {
				delete(r.activeFilterOnlyGuard, g.Table)
				log.Debug().
					Uint64("txn_id", txnID).
					Str("table", g.Table).
					Msg("Filter-only guard slot released")
			}
		}
	}
}

// RefreshTTL extends the TTL for all guards of a transaction
func (r *GuardRegistry) RefreshTTL(txnID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	guards := r.byTxn[txnID]
	newExpiry := time.Now().Add(r.intentTTL)
	for _, g := range guards {
		g.ExpiresAt = newExpiry
	}
}

// Remove removes all guards for a transaction
func (r *GuardRegistry) Remove(txnID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	guards := r.byTxn[txnID]
	delete(r.byTxn, txnID)

	for _, g := range guards {
		// Release filter-only slot if applicable
		if g.IsFilterOnly {
			if r.activeFilterOnlyGuard[g.Table] != nil &&
				r.activeFilterOnlyGuard[g.Table].TxnID == txnID {
				delete(r.activeFilterOnlyGuard, g.Table)
			}
		}
		r.removeGuardFromTable(g.Table, txnID)
	}
}

// removeGuardFromTable removes guards for a txn from a table's list
func (r *GuardRegistry) removeGuardFromTable(table string, txnID uint64) {
	guards := r.guards[table]
	filtered := make([]*ActiveGuard, 0, len(guards))
	for _, g := range guards {
		if g.TxnID != txnID {
			filtered = append(filtered, g)
		}
	}
	if len(filtered) == 0 {
		delete(r.guards, table)
	} else {
		r.guards[table] = filtered
	}
}

// HasActiveFilterOnlyGuard checks if a table has an active filter-only guard
func (r *GuardRegistry) HasActiveFilterOnlyGuard(table string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.activeFilterOnlyGuard[table] != nil
}

// GetActiveFilterOnlyGuard returns the active filter-only guard for a table, if any
func (r *GuardRegistry) GetActiveFilterOnlyGuard(table string) *ActiveGuard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.activeFilterOnlyGuard[table]
}

// GetGuard returns the guard for a specific transaction and table
func (r *GuardRegistry) GetGuard(txnID uint64, table string) *ActiveGuard {
	r.mu.RLock()
	defer r.mu.RUnlock()

	guards := r.byTxn[txnID]
	for _, g := range guards {
		if g.Table == table {
			return g
		}
	}
	return nil
}

// GetAllGuards returns all guards for a transaction
func (r *GuardRegistry) GetAllGuards(txnID uint64) []*ActiveGuard {
	r.mu.RLock()
	defer r.mu.RUnlock()

	guards := r.byTxn[txnID]
	result := make([]*ActiveGuard, len(guards))
	copy(result, guards)
	return result
}

// HasGuard checks if a transaction has any registered guards
func (r *GuardRegistry) HasGuard(txnID uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byTxn[txnID]) > 0
}

// Count returns the total number of active guards
func (r *GuardRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, guards := range r.guards {
		count += len(guards)
	}
	return count
}

// cleanupLoop periodically removes expired guards
func (r *GuardRegistry) cleanupLoop() {
	ticker := time.NewTicker(r.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.cleanupExpired()
		}
	}
}

// cleanupExpired removes all expired guards
func (r *GuardRegistry) cleanupExpired() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	expiredTxns := make([]uint64, 0)

	for txnID, guards := range r.byTxn {
		for _, g := range guards {
			if g.ExpiresAt.Before(now) {
				expiredTxns = append(expiredTxns, txnID)
				break
			}
		}
	}

	for _, txnID := range expiredTxns {
		guards := r.byTxn[txnID]
		delete(r.byTxn, txnID)

		for _, g := range guards {
			// Release filter-only slot if applicable
			if g.IsFilterOnly {
				if r.activeFilterOnlyGuard[g.Table] != nil &&
					r.activeFilterOnlyGuard[g.Table].TxnID == txnID {
					delete(r.activeFilterOnlyGuard, g.Table)
				}
			}
			r.removeGuardFromTable(g.Table, txnID)
		}

		log.Warn().
			Uint64("txn_id", txnID).
			Int("guard_count", len(guards)).
			Msg("MutationGuard expired, transaction aborted")
	}
}

// Stop stops the cleanup goroutine
func (r *GuardRegistry) Stop() {
	close(r.stopCh)
}

// GuardRegistryStats contains registry statistics
type GuardRegistryStats struct {
	TotalGuards           int
	TotalTxns             int
	FilterOnlyGuards      int
	TableCounts           map[string]int
	ActiveFilterOnlyCount int
}

// Stats returns registry statistics
func (r *GuardRegistry) Stats() GuardRegistryStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := GuardRegistryStats{
		TotalTxns:             len(r.byTxn),
		TableCounts:           make(map[string]int),
		ActiveFilterOnlyCount: len(r.activeFilterOnlyGuard),
	}

	for table, guards := range r.guards {
		stats.TableCounts[table] = len(guards)
		stats.TotalGuards += len(guards)
		for _, g := range guards {
			if g.IsFilterOnly {
				stats.FilterOnlyGuards++
			}
		}
	}

	return stats
}
