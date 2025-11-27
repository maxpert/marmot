package grpc

import (
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// ActiveGuard represents an active MutationGuard for a transaction.
// Uses XXH64 hash list for exact conflict detection (no false positives).
//
// Design:
//   - ≤64K rows: XXH64 hash list in KeySet for O(1) intersection checking
//   - >64K rows: Empty KeySet, conflicts detected by MVCC write intents
type ActiveGuard struct {
	TxnID     uint64
	Table     string
	KeySet    map[uint64]struct{} // XXH64 hashes of affected row keys
	Timestamp hlc.Timestamp
	ExpiresAt time.Time
	RowCount  int64
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
// Uses XXH64 hash list intersection for exact conflict detection:
// - O(min(|A|, |B|)) per comparison
// - Zero false positives (collision probability ~10⁻¹² at 64K scale)
// - Transactions >64K rows skip MutationGuard and rely on MVCC write intents
//
// Novel technique combining:
// - Early batch conflict detection (vs per-row during execution)
// - Compact write set representation (8 bytes/row)
// - Coordinator-side detection (vs storage layer)
// - Native leaderless architecture support
type GuardRegistry struct {
	mu              sync.RWMutex
	guards          map[string][]*ActiveGuard // table -> all guards
	byTxn           map[uint64][]*ActiveGuard // txn_id -> guards
	intentTTL       time.Duration
	cleanupInterval time.Duration
	stopCh          chan struct{}
}

// NewGuardRegistry creates a new guard registry
func NewGuardRegistry(intentTTL time.Duration) *GuardRegistry {
	r := &GuardRegistry{
		guards:          make(map[string][]*ActiveGuard),
		byTxn:           make(map[uint64][]*ActiveGuard),
		intentTTL:       intentTTL,
		cleanupInterval: 5 * time.Second,
		stopCh:          make(chan struct{}),
	}
	go r.cleanupLoop()
	return r
}

// Register adds a new guard to the registry.
func (r *GuardRegistry) Register(guard *ActiveGuard) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guard.ExpiresAt = time.Now().Add(r.intentTTL)

	r.guards[guard.Table] = append(r.guards[guard.Table], guard)
	r.byTxn[guard.TxnID] = append(r.byTxn[guard.TxnID], guard)

	log.Debug().
		Uint64("txn_id", guard.TxnID).
		Str("table", guard.Table).
		Int("key_count", len(guard.KeySet)).
		Int64("row_count", guard.RowCount).
		Msg("MutationGuard registered")

	return nil
}

// CheckConflict checks if a new guard conflicts with existing guards.
// Uses Wound-Wait algorithm: older transactions always proceed.
//
// Uses exact hash set intersection - no false positives.
// Guards with empty KeySet (>64K rows) are skipped, conflicts detected by MVCC.
func (r *GuardRegistry) CheckConflict(newGuard *ActiveGuard) ConflictResult {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Skip conflict check if new guard has no keys (>64K rows, rely on MVCC)
	if len(newGuard.KeySet) == 0 {
		return ConflictResult{HasConflict: false}
	}

	existingGuards := r.guards[newGuard.Table]

	for _, existing := range existingGuards {
		if existing.TxnID == newGuard.TxnID {
			continue
		}
		// Skip guards with no keys (>64K rows, MVCC handles conflicts)
		if len(existing.KeySet) == 0 {
			continue
		}
		// Exact intersection check - no false positives
		if hasIntersection(newGuard.KeySet, existing.KeySet) {
			return applyWoundWait(newGuard, existing, "hash intersection: conflict detected")
		}
	}

	return ConflictResult{HasConflict: false}
}

// hasIntersection checks if two key sets have any common elements.
// O(min(|a|, |b|)) - iterates over smaller set, probes larger set.
func hasIntersection(a, b map[uint64]struct{}) bool {
	// Iterate over smaller set for efficiency
	small, large := a, b
	if len(a) > len(b) {
		small, large = b, a
	}
	for k := range small {
		if _, ok := large[k]; ok {
			return true
		}
	}
	return false
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

// MarkComplete marks a transaction's guards as complete.
func (r *GuardRegistry) MarkComplete(txnID uint64) {
	// No-op for hash list guards - they're cleaned up by Remove()
	// Kept for API compatibility
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
	TotalGuards int
	TotalTxns   int
	TableCounts map[string]int
}

// Stats returns registry statistics
func (r *GuardRegistry) Stats() GuardRegistryStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := GuardRegistryStats{
		TotalTxns:   len(r.byTxn),
		TableCounts: make(map[string]int),
	}

	for table, guards := range r.guards {
		stats.TableCounts[table] = len(guards)
		stats.TotalGuards += len(guards)
	}

	return stats
}

// KeySetFromSlice converts a slice of hashes to a KeySet map for O(1) lookup.
func KeySetFromSlice(hashes []uint64) map[uint64]struct{} {
	keySet := make(map[uint64]struct{}, len(hashes))
	for _, h := range hashes {
		keySet[h] = struct{}{}
	}
	return keySet
}
