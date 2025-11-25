package coordinator

import (
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// DDLLockManager provides cluster-wide DDL serialization
// Ensures only one DDL operation per database at a time across the cluster
type DDLLockManager struct {
	mu sync.RWMutex
	// activeLocks tracks active DDL locks per database
	// key = database name, value = lock info
	activeLocks map[string]*DDLLock
	// leaseDuration is how long a lock is valid
	leaseDuration time.Duration
}

// DDLLock represents a cluster-wide DDL lock
type DDLLock struct {
	Database    string
	NodeID      uint64
	TxnID       uint64
	AcquiredAt  hlc.Timestamp
	ExpiresAt   time.Time
	ReleaseChan chan struct{} // Closed when lock is released
}

// NewDDLLockManager creates a new DDL lock manager
func NewDDLLockManager(leaseDuration time.Duration) *DDLLockManager {
	return &DDLLockManager{
		activeLocks:   make(map[string]*DDLLock),
		leaseDuration: leaseDuration,
	}
}

// AcquireLock attempts to acquire a cluster-wide DDL lock for a database
// Returns the lock if successful, error if lock is held by another transaction
func (dlm *DDLLockManager) AcquireLock(database string, nodeID uint64, txnID uint64, ts hlc.Timestamp) (*DDLLock, error) {
	dlm.mu.Lock()
	defer dlm.mu.Unlock()

	// Check if lock exists and hasn't expired
	if existingLock, exists := dlm.activeLocks[database]; exists {
		if time.Now().Before(existingLock.ExpiresAt) {
			// Lock is still valid
			if existingLock.TxnID == txnID {
				// Same transaction trying to reacquire (idempotent)
				return existingLock, nil
			}
			return nil, fmt.Errorf("DDL lock for database '%s' is held by txn %d (node %d)",
				database, existingLock.TxnID, existingLock.NodeID)
		}
		// Lock expired, can be acquired
		log.Warn().
			Str("database", database).
			Uint64("expired_txn", existingLock.TxnID).
			Uint64("expired_node", existingLock.NodeID).
			Msg("DDL lock expired, allowing new acquisition")
	}

	// Acquire lock
	lock := &DDLLock{
		Database:    database,
		NodeID:      nodeID,
		TxnID:       txnID,
		AcquiredAt:  ts,
		ExpiresAt:   time.Now().Add(dlm.leaseDuration),
		ReleaseChan: make(chan struct{}),
	}

	dlm.activeLocks[database] = lock

	log.Info().
		Str("database", database).
		Uint64("node_id", nodeID).
		Uint64("txn_id", txnID).
		Msg("DDL lock acquired")

	return lock, nil
}

// ReleaseLock releases a DDL lock
func (dlm *DDLLockManager) ReleaseLock(database string, txnID uint64) error {
	dlm.mu.Lock()
	defer dlm.mu.Unlock()

	lock, exists := dlm.activeLocks[database]
	if !exists {
		// Already released (idempotent)
		return nil
	}

	if lock.TxnID != txnID {
		return fmt.Errorf("cannot release lock: held by txn %d, requested by txn %d", lock.TxnID, txnID)
	}

	// Close the release channel to notify any waiters
	close(lock.ReleaseChan)

	// Remove from active locks
	delete(dlm.activeLocks, database)

	log.Info().
		Str("database", database).
		Uint64("txn_id", txnID).
		Msg("DDL lock released")

	return nil
}

// CheckLock checks if a lock exists for a database without acquiring it
// Returns lock info if exists and not expired, nil otherwise
func (dlm *DDLLockManager) CheckLock(database string) *DDLLock {
	dlm.mu.RLock()
	defer dlm.mu.RUnlock()

	lock, exists := dlm.activeLocks[database]
	if !exists {
		return nil
	}

	if time.Now().After(lock.ExpiresAt) {
		// Lock expired
		return nil
	}

	return lock
}

// WaitForLock waits until a lock is released or timeout occurs
// Returns nil if lock was released, error if timeout
func (dlm *DDLLockManager) WaitForLock(database string, timeout time.Duration) error {
	dlm.mu.RLock()
	lock, exists := dlm.activeLocks[database]
	if !exists {
		dlm.mu.RUnlock()
		return nil // No lock, proceed
	}

	// Get the release channel before releasing read lock
	releaseChan := lock.ReleaseChan
	dlm.mu.RUnlock()

	// Wait for lock to be released or timeout
	select {
	case <-releaseChan:
		return nil // Lock released
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for DDL lock on database '%s'", database)
	}
}

// CleanupExpiredLocks removes expired locks
// Should be called periodically by a background goroutine
func (dlm *DDLLockManager) CleanupExpiredLocks() int {
	dlm.mu.Lock()
	defer dlm.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for database, lock := range dlm.activeLocks {
		if now.After(lock.ExpiresAt) {
			log.Warn().
				Str("database", database).
				Uint64("txn_id", lock.TxnID).
				Msg("Cleaning up expired DDL lock")
			close(lock.ReleaseChan)
			delete(dlm.activeLocks, database)
			cleaned++
		}
	}

	return cleaned
}

// GetActiveLocks returns a map of all active locks (for debugging/monitoring)
func (dlm *DDLLockManager) GetActiveLocks() map[string]*DDLLock {
	dlm.mu.RLock()
	defer dlm.mu.RUnlock()

	// Return a copy to avoid concurrent modification
	result := make(map[string]*DDLLock, len(dlm.activeLocks))
	for db, lock := range dlm.activeLocks {
		// Create a copy of the lock
		lockCopy := *lock
		result[db] = &lockCopy
	}

	return result
}
