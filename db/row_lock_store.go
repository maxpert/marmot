package db

import (
	"strings"

	"github.com/puzpuzpuz/xsync/v3"
)

// RowLockStore implements in-memory row locking using lock-free concurrent maps.
type RowLockStore struct {
	// tables: "db:table" → ("rowkey" → txnID).
	//
	// A table's row map is created on first use and never removed, even once
	// it is empty. AcquireLock loads the row map and inserts into it as two
	// separate steps; a release that deleted an empty map in between would
	// detach the map holding the new lock, and the next acquirer would take
	// the same row in a fresh map. The maps are bounded by the number of
	// distinct tables this meta store has locked rows in.
	tables *xsync.MapOf[string, *xsync.MapOf[string, uint64]]

	// byTxn: reverse index txnID → set of "db:table:rowkey"
	byTxn *xsync.MapOf[uint64, *xsync.MapOf[string, struct{}]]
}

// NewRowLockStore creates a new lock-free row lock store.
func NewRowLockStore() *RowLockStore {
	return &RowLockStore{
		tables: xsync.NewMapOf[string, *xsync.MapOf[string, uint64]](),
		byTxn:  xsync.NewMapOf[uint64, *xsync.MapOf[string, struct{}]](),
	}
}

// AcquireLock attempts to acquire a lock on the specified row.
// Returns (existingTxnID, false) if the lock is held by another transaction.
// Returns (txnID, true) if the lock was successfully acquired.
func (s *RowLockStore) AcquireLock(db, table, rowKey string, txnID uint64) (existingTxnID uint64, acquired bool) {
	tableKey := makeTableKey(db, table)
	fullKey := makeFullKey(db, table, rowKey)

	// Get or create the row map for this table
	rowMap, _ := s.tables.LoadOrStore(tableKey, xsync.NewMapOf[string, uint64]())

	// Try to atomically acquire the lock
	holder, loaded := rowMap.LoadOrStore(rowKey, txnID)
	if loaded && holder != txnID {
		return holder, false
	}

	// Add to reverse index
	txnMap, _ := s.byTxn.LoadOrStore(txnID, xsync.NewMapOf[string, struct{}]())
	txnMap.Store(fullKey, struct{}{})

	return txnID, true
}

// ReleaseLock removes the lock on the specified row.
func (s *RowLockStore) ReleaseLock(db, table, rowKey string) {
	tableKey := makeTableKey(db, table)

	if rowMap, ok := s.tables.Load(tableKey); ok {
		if txnID, held := rowMap.Load(rowKey); held {
			rowMap.Delete(rowKey)

			// Remove from reverse index
			fullKey := makeFullKey(db, table, rowKey)
			if txnMap, ok := s.byTxn.Load(txnID); ok {
				txnMap.Delete(fullKey)
			}
		}
	}
}

// ReleaseLockIfHeldBy releases the row lock only while txnID still holds it,
// and reports whether it did. Unconditional release is a bug wherever the
// caller has a specific transaction in mind: between the decision to release
// and the release itself another transaction can take the lock, and dropping
// its lock hands the row to a third writer that never resolved the conflict.
func (s *RowLockStore) ReleaseLockIfHeldBy(db, table, rowKey string, txnID uint64) bool {
	if !s.releaseRowIfHeldBy(db, table, rowKey, txnID) {
		return false
	}

	fullKey := makeFullKey(db, table, rowKey)
	if txnMap, ok := s.byTxn.Load(txnID); ok {
		txnMap.Delete(fullKey)
		s.cleanupEmptyTxnMap(txnID, txnMap)
	}

	return true
}

// releaseRowIfHeldBy atomically deletes a row's lock entry if txnID holds it.
// It does not touch the reverse index.
func (s *RowLockStore) releaseRowIfHeldBy(db, table, rowKey string, txnID uint64) bool {
	rowMap, ok := s.tables.Load(makeTableKey(db, table))
	if !ok {
		return false
	}
	released := false
	rowMap.Compute(rowKey, func(holder uint64, loaded bool) (uint64, bool) {
		if loaded && holder == txnID {
			released = true
			return 0, true
		}
		// Leave a lock held by anyone else exactly as it is; an absent row
		// (loaded false) is deleted, which is a no-op.
		return holder, !loaded
	})
	return released
}

// CheckLock checks if a lock exists on the specified row.
// Returns (txnID, true) if locked, (0, false) if not locked.
func (s *RowLockStore) CheckLock(db, table, rowKey string) (txnID uint64, exists bool) {
	tableKey := makeTableKey(db, table)

	if rowMap, ok := s.tables.Load(tableKey); ok {
		txnID, exists = rowMap.Load(rowKey)
		return txnID, exists
	}

	return 0, false
}

// GetLockHolder returns the transaction ID holding the lock on the specified row.
// Returns 0 if the row is not locked.
func (s *RowLockStore) GetLockHolder(db, table, rowKey string) uint64 {
	txnID, _ := s.CheckLock(db, table, rowKey)
	return txnID
}

// ReleaseByTxn releases every row lock txnID still holds and returns the
// "db:table:rowkey" keys its reverse index named.
//
// The reverse index is removed with LoadAndDelete before it is walked, so a
// concurrent AcquireLock by the same transaction lands in a fresh map rather
// than in one being iterated. Each release is conditional on txnID still
// holding the row: the reverse index can name a row whose lock was already
// released and retaken by another transaction, and releasing that row is not
// this transaction's business. All keys are returned regardless, because the
// caller deletes this transaction's own intent records by them.
func (s *RowLockStore) ReleaseByTxn(txnID uint64) []string {
	txnMap, ok := s.byTxn.LoadAndDelete(txnID)
	if !ok {
		return nil
	}

	var keys []string
	txnMap.Range(func(fullKey string, _ struct{}) bool {
		keys = append(keys, fullKey)
		return true
	})

	for _, fullKey := range keys {
		db, table, rowKey := parseFullKey(fullKey)
		s.releaseRowIfHeldBy(db, table, rowKey, txnID)
	}

	return keys
}

// GetLocksByTxn returns all "db:table:rowkey" strings held by the specified transaction.
func (s *RowLockStore) GetLocksByTxn(txnID uint64) []string {
	if txnMap, ok := s.byTxn.Load(txnID); ok {
		var locks []string
		txnMap.Range(func(fullKey string, _ struct{}) bool {
			locks = append(locks, fullKey)
			return true
		})
		return locks
	}

	return nil
}

// HasLocksForTable checks if any locks exist for the specified table.
func (s *RowLockStore) HasLocksForTable(db, table string) bool {
	tableKey := makeTableKey(db, table)

	if rowMap, ok := s.tables.Load(tableKey); ok {
		hasLocks := false
		rowMap.Range(func(_ string, _ uint64) bool {
			hasLocks = true
			return false // Stop iteration after finding first lock
		})
		return hasLocks
	}

	return false
}

// makeTableKey creates a key for the table map in the format "db:table".
func makeTableKey(db, table string) string {
	return db + ":" + table
}

// makeFullKey creates a full key in the format "db:table:rowkey".
func makeFullKey(db, table, rowKey string) string {
	return db + ":" + table + ":" + rowKey
}

// parseFullKey parses a full key back into its components.
func parseFullKey(fullKey string) (db, table, rowKey string) {
	parts := strings.SplitN(fullKey, ":", 3)
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	return "", "", ""
}

// cleanupEmptyTxnMap removes the transaction map if it's empty to prevent memory leaks.
func (s *RowLockStore) cleanupEmptyTxnMap(txnID uint64, txnMap *xsync.MapOf[string, struct{}]) {
	isEmpty := true
	txnMap.Range(func(_ string, _ struct{}) bool {
		isEmpty = false
		return false // Stop after first element
	})

	if isEmpty {
		s.byTxn.Delete(txnID)
	}
}

// Stats returns current statistics about the lock store.
// Returns: (activeLocks, activeTransactions, tablesWithLocks)
func (s *RowLockStore) Stats() (activeLocks int, activeTransactions int, tablesWithLocks int) {
	// Count active transactions (transactions holding locks)
	s.byTxn.Range(func(_ uint64, txnMap *xsync.MapOf[string, struct{}]) bool {
		hasLocks := false
		txnMap.Range(func(_ string, _ struct{}) bool {
			hasLocks = true
			activeLocks++
			return true
		})
		if hasLocks {
			activeTransactions++
		}
		return true
	})

	// Count tables with locks
	s.tables.Range(func(_ string, rowMap *xsync.MapOf[string, uint64]) bool {
		hasEntries := false
		rowMap.Range(func(_ string, _ uint64) bool {
			hasEntries = true
			return false // Just check if any exist
		})
		if hasEntries {
			tablesWithLocks++
		}
		return true
	})

	return activeLocks, activeTransactions, tablesWithLocks
}
