package db

// TxnState represents lightweight in-memory state for active transactions.
// This is used for fast lookups during transaction processing without hitting disk.
type TxnState struct {
	NodeID         uint64
	Status         TxnStatus
	StartTSWall    int64
	StartTSLogical int32
	LastHeartbeat  int64
	RowCount       uint32
}

// IntentMeta represents lightweight intent metadata for distributed lock tracking.
// Intents prevent conflicting concurrent operations on the same row.
type IntentMeta struct {
	TxnID     uint64
	Timestamp int64
}

// TransactionStore manages in-memory transaction state for active transactions.
// This interface provides fast lookups and updates without disk I/O overhead.
type TransactionStore interface {
	// Begin registers a new transaction with initial state.
	Begin(txnID uint64, state *TxnState)

	// Get retrieves transaction state. Returns false if transaction not found.
	Get(txnID uint64) (*TxnState, bool)

	// UpdateStatus updates transaction status (Pending, Committed, Aborted).
	UpdateStatus(txnID uint64, status TxnStatus)

	// UpdateHeartbeat updates the last heartbeat timestamp for the transaction.
	UpdateHeartbeat(txnID uint64, ts int64)

	// Remove deletes transaction state from memory.
	Remove(txnID uint64)

	// RangePending iterates over pending transactions.
	// Iterator returns true to continue, false to stop.
	RangePending(fn func(txnID uint64) bool)

	// RangeAll iterates over all transactions regardless of status.
	// Iterator returns true to continue, false to stop.
	RangeAll(fn func(txnID uint64, state *TxnState) bool)

	// CountPending returns the number of pending transactions.
	CountPending() int
}

// IntentStore manages distributed locks (intents) for transaction isolation.
// Each intent represents a pending write operation on a specific row.
type IntentStore interface {
	// Add registers an intent for a transaction on a specific table row.
	// Returns error if an intent already exists for this key.
	Add(txnID uint64, table, intentKey string, meta *IntentMeta) error

	// Get retrieves intent metadata for a table row. Returns false if no intent exists.
	Get(table, intentKey string) (*IntentMeta, bool)

	// Remove deletes an intent for a specific table row.
	Remove(table, intentKey string)

	// RangeByTxn iterates over all intents belonging to a transaction.
	// Iterator returns true to continue, false to stop.
	RangeByTxn(txnID uint64, fn func(table, intentKey string) bool)

	// RemoveByTxn removes all intents belonging to a transaction.
	RemoveByTxn(txnID uint64)

	// CountByTxn returns the number of intents held by a transaction.
	CountByTxn(txnID uint64) int
}

// CDCLockStore manages row-level locks for CDC replication.
// These locks ensure only one transaction can modify a row during CDC processing.
type CDCLockStore interface {
	// Acquire obtains a CDC lock on a table row for a transaction.
	// Returns error if the lock is already held by another transaction.
	Acquire(txnID uint64, table, intentKey string) error

	// GetHolder returns the transaction ID holding the lock, or false if unlocked.
	GetHolder(table, intentKey string) (txnID uint64, held bool)

	// Release removes a CDC lock if held by the specified transaction.
	Release(table, intentKey string, txnID uint64)

	// ReleaseByTxn removes all CDC locks held by a transaction.
	ReleaseByTxn(txnID uint64)
}
