package db

import (
	"strings"

	"github.com/puzpuzpuz/xsync/v3"
)

// RowLockStore implements in-memory row locking using lock-free concurrent maps.
// Supports fine-grained row locks, GC markers, and efficient bulk cleanup operations.
type RowLockStore struct {
	// tables: "db:table" → ("rowkey" → txnID)
	tables *xsync.MapOf[string, *xsync.MapOf[string, uint64]]

	// gc: "db:table" → ("rowkey" → struct{})
	gc *xsync.MapOf[string, *xsync.MapOf[string, struct{}]]

	// byTxn: reverse index txnID → set of "db:table:rowkey"
	byTxn *xsync.MapOf[uint64, *xsync.MapOf[string, struct{}]]
}

// NewRowLockStore creates a new lock-free row lock store.
func NewRowLockStore() *RowLockStore {
	return &RowLockStore{
		tables: xsync.NewMapOf[string, *xsync.MapOf[string, uint64]](),
		gc:     xsync.NewMapOf[string, *xsync.MapOf[string, struct{}]](),
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

			// Clean up empty maps
			s.cleanupEmptyRowMap(tableKey, rowMap)
		}
	}
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

// SetGCMarker marks a row for garbage collection.
func (s *RowLockStore) SetGCMarker(db, table, rowKey string) {
	tableKey := makeTableKey(db, table)

	gcMap, _ := s.gc.LoadOrStore(tableKey, xsync.NewMapOf[string, struct{}]())
	gcMap.Store(rowKey, struct{}{})
}

// CheckGCMarker checks if a row is marked for garbage collection.
func (s *RowLockStore) CheckGCMarker(db, table, rowKey string) bool {
	tableKey := makeTableKey(db, table)

	if gcMap, ok := s.gc.Load(tableKey); ok {
		_, exists := gcMap.Load(rowKey)
		return exists
	}

	return false
}

// DeleteGCMarker removes the GC marker for the specified row.
func (s *RowLockStore) DeleteGCMarker(db, table, rowKey string) {
	tableKey := makeTableKey(db, table)

	if gcMap, ok := s.gc.Load(tableKey); ok {
		gcMap.Delete(rowKey)
		s.cleanupEmptyGCMap(tableKey, gcMap)
	}
}

// ReleaseByTxn releases all locks held by the specified transaction.
func (s *RowLockStore) ReleaseByTxn(txnID uint64) {
	if txnMap, ok := s.byTxn.Load(txnID); ok {
		// Collect all keys to avoid modification during iteration
		var keys []string
		txnMap.Range(func(fullKey string, _ struct{}) bool {
			keys = append(keys, fullKey)
			return true
		})

		// Release all locks
		for _, fullKey := range keys {
			db, table, rowKey := parseFullKey(fullKey)
			tableKey := makeTableKey(db, table)

			if rowMap, ok := s.tables.Load(tableKey); ok {
				rowMap.Delete(rowKey)
				s.cleanupEmptyRowMap(tableKey, rowMap)
			}
		}

		// Remove the transaction's map
		s.byTxn.Delete(txnID)
	}
}

// ReleaseByTable releases all locks for the specified table.
func (s *RowLockStore) ReleaseByTable(db, table string) {
	tableKey := makeTableKey(db, table)

	if rowMap, ok := s.tables.Load(tableKey); ok {
		// Collect all row keys and their transaction IDs
		type lockInfo struct {
			rowKey string
			txnID  uint64
		}
		var locks []lockInfo

		rowMap.Range(func(rowKey string, txnID uint64) bool {
			locks = append(locks, lockInfo{rowKey: rowKey, txnID: txnID})
			return true
		})

		// Release all locks and update reverse index
		fullKeyPrefix := tableKey + ":"
		for _, lock := range locks {
			rowMap.Delete(lock.rowKey)

			fullKey := fullKeyPrefix + lock.rowKey
			if txnMap, ok := s.byTxn.Load(lock.txnID); ok {
				txnMap.Delete(fullKey)
				s.cleanupEmptyTxnMap(lock.txnID, txnMap)
			}
		}

		// Remove the table map
		s.tables.Delete(tableKey)
	}

	// Clean up GC markers for this table
	s.gc.Delete(tableKey)
}

// ReleaseByDatabase releases all locks for the specified database.
func (s *RowLockStore) ReleaseByDatabase(db string) {
	prefix := db + ":"

	// Collect all table keys for this database
	var tableKeys []string
	s.tables.Range(func(tableKey string, _ *xsync.MapOf[string, uint64]) bool {
		if strings.HasPrefix(tableKey, prefix) {
			tableKeys = append(tableKeys, tableKey)
			return true
		}
		return true
	})

	// Release locks for each table
	for _, tableKey := range tableKeys {
		parts := strings.SplitN(tableKey, ":", 2)
		if len(parts) == 2 {
			s.ReleaseByTable(parts[0], parts[1])
		}
	}
}

// Clear removes all locks from the store.
func (s *RowLockStore) Clear() {
	s.tables = xsync.NewMapOf[string, *xsync.MapOf[string, uint64]]()
	s.gc = xsync.NewMapOf[string, *xsync.MapOf[string, struct{}]]()
	s.byTxn = xsync.NewMapOf[uint64, *xsync.MapOf[string, struct{}]]()
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

// cleanupEmptyRowMap removes the row map if it's empty to prevent memory leaks.
func (s *RowLockStore) cleanupEmptyRowMap(tableKey string, rowMap *xsync.MapOf[string, uint64]) {
	isEmpty := true
	rowMap.Range(func(_ string, _ uint64) bool {
		isEmpty = false
		return false // Stop after first element
	})

	if isEmpty {
		s.tables.Delete(tableKey)
	}
}

// cleanupEmptyGCMap removes the GC map if it's empty to prevent memory leaks.
func (s *RowLockStore) cleanupEmptyGCMap(tableKey string, gcMap *xsync.MapOf[string, struct{}]) {
	isEmpty := true
	gcMap.Range(func(_ string, _ struct{}) bool {
		isEmpty = false
		return false // Stop after first element
	})

	if isEmpty {
		s.gc.Delete(tableKey)
	}
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
