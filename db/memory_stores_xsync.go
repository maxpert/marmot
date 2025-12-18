package db

import (
	"errors"

	"github.com/puzpuzpuz/xsync/v3"
)

var (
	ErrLockHeld = errors.New("lock held by another transaction")
)

// XsyncTransactionStore implements TransactionStore using lock-free concurrent maps.
type XsyncTransactionStore struct {
	txns    *xsync.MapOf[uint64, *TxnState]
	pending *xsync.MapOf[uint64, struct{}]
}

// NewXsyncTransactionStore creates a new xsync-backed transaction store.
func NewXsyncTransactionStore() *XsyncTransactionStore {
	return &XsyncTransactionStore{
		txns:    xsync.NewMapOf[uint64, *TxnState](),
		pending: xsync.NewMapOf[uint64, struct{}](),
	}
}

func (s *XsyncTransactionStore) Begin(txnID uint64, state *TxnState) {
	s.txns.Store(txnID, state)
	if state.Status == TxnStatusPending {
		s.pending.Store(txnID, struct{}{})
	}
}

func (s *XsyncTransactionStore) Get(txnID uint64) (*TxnState, bool) {
	return s.txns.Load(txnID)
}

func (s *XsyncTransactionStore) UpdateStatus(txnID uint64, status TxnStatus) {
	if state, ok := s.txns.Load(txnID); ok {
		state.Status = status
		if status != TxnStatusPending {
			s.pending.Delete(txnID)
		} else {
			s.pending.Store(txnID, struct{}{})
		}
	}
}

func (s *XsyncTransactionStore) UpdateHeartbeat(txnID uint64, ts int64) {
	if state, ok := s.txns.Load(txnID); ok {
		state.LastHeartbeat = ts
	}
}

func (s *XsyncTransactionStore) Remove(txnID uint64) {
	s.txns.Delete(txnID)
	s.pending.Delete(txnID)
}

func (s *XsyncTransactionStore) RangePending(fn func(txnID uint64) bool) {
	s.pending.Range(func(txnID uint64, _ struct{}) bool {
		return fn(txnID)
	})
}

func (s *XsyncTransactionStore) RangeAll(fn func(txnID uint64, state *TxnState) bool) {
	s.txns.Range(func(txnID uint64, state *TxnState) bool {
		return fn(txnID, state)
	})
}

func (s *XsyncTransactionStore) CountPending() int {
	count := 0
	s.pending.Range(func(_ uint64, _ struct{}) bool {
		count++
		return true
	})
	return count
}

func makeIntentKey(table, key string) string {
	return table + ":" + key
}

// XsyncCDCLockStore implements CDCLockStore using lock-free concurrent maps.
type XsyncCDCLockStore struct {
	locks *xsync.MapOf[string, uint64]
	byTxn *xsync.MapOf[uint64, *xsync.MapOf[string, struct{}]]
}

// NewXsyncCDCLockStore creates a new xsync-backed CDC lock store.
func NewXsyncCDCLockStore() *XsyncCDCLockStore {
	return &XsyncCDCLockStore{
		locks: xsync.NewMapOf[string, uint64](),
		byTxn: xsync.NewMapOf[uint64, *xsync.MapOf[string, struct{}]](),
	}
}

func (s *XsyncCDCLockStore) Acquire(txnID uint64, table, intentKey string) error {
	key := makeIntentKey(table, intentKey)

	// Try to atomically acquire the lock
	holder, loaded := s.locks.LoadOrStore(key, txnID)
	if loaded && holder != txnID {
		// Lock is held by another transaction
		return ErrLockHeld
	}

	// Add to byTxn index
	txnMap, _ := s.byTxn.LoadOrStore(txnID, xsync.NewMapOf[string, struct{}]())
	txnMap.Store(key, struct{}{})

	return nil
}

func (s *XsyncCDCLockStore) GetHolder(table, intentKey string) (txnID uint64, held bool) {
	key := makeIntentKey(table, intentKey)
	txnID, held = s.locks.Load(key)
	return
}

func (s *XsyncCDCLockStore) Release(table, intentKey string, txnID uint64) {
	key := makeIntentKey(table, intentKey)

	// Only release if held by the specified transaction
	if holder, held := s.locks.Load(key); held && holder == txnID {
		s.locks.Delete(key)

		// Remove from byTxn index
		if txnMap, ok := s.byTxn.Load(txnID); ok {
			txnMap.Delete(key)
		}
	}
}

func (s *XsyncCDCLockStore) ReleaseByTxn(txnID uint64) {
	if txnMap, ok := s.byTxn.Load(txnID); ok {
		// Collect all keys first to avoid modification during iteration
		var keys []string
		txnMap.Range(func(key string, _ struct{}) bool {
			keys = append(keys, key)
			return true
		})

		// Release all locks
		for _, key := range keys {
			s.locks.Delete(key)
		}

		// Remove the transaction's map
		s.byTxn.Delete(txnID)
	}
}
