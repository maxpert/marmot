package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// LockWaiter handles waiting for locks to be released with timeout
type LockWaiter struct {
	mu            sync.RWMutex
	activeTxns    map[uint64]*txnLockInfo    // txnID -> lock info
	waiters       map[uint64][]chan struct{} // txnID -> list of waiters
	checkInterval time.Duration
}

type txnLockInfo struct {
	txnID     uint64
	nodeID    uint64
	startTime time.Time
}

// NewLockWaiter creates a new lock waiter
func NewLockWaiter() *LockWaiter {
	return &LockWaiter{
		activeTxns:    make(map[uint64]*txnLockInfo),
		waiters:       make(map[uint64][]chan struct{}),
		checkInterval: 100 * time.Millisecond, // Check every 100ms
	}
}

// WaitForLock waits for a conflicting transaction to release its lock
// Returns nil if lock is released, or MySQLError if timeout
func (lw *LockWaiter) WaitForLock(ctx context.Context, conflictingTxnID uint64) error {
	timeout := time.Duration(cfg.Config.MVCC.LockWaitTimeoutSeconds) * time.Second
	deadline := time.Now().Add(timeout)
	start := time.Now()

	// Register as a waiter
	waitChan := make(chan struct{}, 1)
	lw.mu.Lock()
	lw.waiters[conflictingTxnID] = append(lw.waiters[conflictingTxnID], waitChan)
	lw.mu.Unlock()

	// Clean up on exit
	defer func() {
		lw.mu.Lock()
		if waiters, exists := lw.waiters[conflictingTxnID]; exists {
			// Remove this waiter from the list
			for i, ch := range waiters {
				if ch == waitChan {
					lw.waiters[conflictingTxnID] = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
			if len(lw.waiters[conflictingTxnID]) == 0 {
				delete(lw.waiters, conflictingTxnID)
			}
		}
		lw.mu.Unlock()
		close(waitChan)
	}()

	ticker := time.NewTicker(lw.checkInterval)
	defer ticker.Stop()

	// Track if we ever saw the transaction as active
	everSawActive := false

	for {
		select {
		case <-waitChan:
			// Lock was released, notified by ReleaseLock
			return nil

		case <-ticker.C:
			// Check timeout FIRST to avoid infinite waiting
			if time.Now().After(deadline) {
				log.Warn().
					Uint64("conflicting_txn", conflictingTxnID).
					Dur("waited", time.Since(start)).
					Msg("Lock wait timeout exceeded")
				return protocol.ErrLockWaitTimeout()
			}

			// Check if transaction is currently active
			lw.mu.RLock()
			_, stillActive := lw.activeTxns[conflictingTxnID]
			lw.mu.RUnlock()

			if stillActive {
				everSawActive = true
			} else if everSawActive {
				// Was active before, now released
				return nil
			}
			// If never saw it active, keep waiting until timeout

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// AcquireLock registers that a transaction has acquired a lock
func (lw *LockWaiter) AcquireLock(txnID, nodeID uint64) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	lw.activeTxns[txnID] = &txnLockInfo{
		txnID:     txnID,
		nodeID:    nodeID,
		startTime: time.Now(),
	}
}

// ReleaseLock releases a transaction's lock and notifies waiters
func (lw *LockWaiter) ReleaseLock(txnID uint64) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	delete(lw.activeTxns, txnID)

	// Notify all waiters for this transaction
	if waiters, exists := lw.waiters[txnID]; exists {
		for _, waitChan := range waiters {
			select {
			case waitChan <- struct{}{}:
			default:
				// Channel already notified or closed
			}
		}
		delete(lw.waiters, txnID)
	}
}

// IsLockHeld checks if a transaction currently holds a lock
func (lw *LockWaiter) IsLockHeld(txnID uint64) bool {
	lw.mu.RLock()
	defer lw.mu.RUnlock()
	_, held := lw.activeTxns[txnID]
	return held
}
