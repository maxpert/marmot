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

	// AcquireDDL obtains a table-level DDL lock.
	// Returns ErrDDLLockHeld if held by another txn.
	// Returns ErrDMLInProgress if any row locks exist for table.
	AcquireDDL(txnID uint64, table string) error

	// ReleaseDDL releases DDL lock if held by txnID.
	ReleaseDDL(table string, txnID uint64)

	// GetDDLHolder returns txnID holding DDL lock, or (0, false) if none.
	GetDDLHolder(table string) (txnID uint64, held bool)

	// HasRowLocks returns true if any row locks exist for table.
	HasRowLocks(table string) bool
}
