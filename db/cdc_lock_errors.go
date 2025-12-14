package db

import "fmt"

// ErrCDCRowLocked is returned when trying to lock a row already locked by another transaction.
// This error indicates a write-write conflict in the CDC replication system.
// When IntentKey is "__ddl__", it indicates a DDL lock conflict.
type ErrCDCRowLocked struct {
	Table     string
	IntentKey string
	HeldByTxn uint64
}

func (e ErrCDCRowLocked) Error() string {
	return fmt.Sprintf("CDC row lock conflict: %s:%s held by txn %d", e.Table, e.IntentKey, e.HeldByTxn)
}

// ErrCDCTableDDLInProgress is returned when DML attempts to write to a table
// that has an active DDL operation in progress.
type ErrCDCTableDDLInProgress struct {
	Table     string
	HeldByTxn uint64
}

func (e ErrCDCTableDDLInProgress) Error() string {
	return fmt.Sprintf("CDC DDL lock conflict: table %s has DDL in progress (txn %d)", e.Table, e.HeldByTxn)
}

// ErrCDCDMLInProgress is returned when DDL attempts to modify a table
// that has active DML operations in progress.
type ErrCDCDMLInProgress struct {
	Table string
}

func (e ErrCDCDMLInProgress) Error() string {
	return fmt.Sprintf("CDC DML lock conflict: table %s has DML in progress", e.Table)
}

// ErrSchemaCacheMiss is returned when a table's schema is not found in the cache.
// This typically means the schema cache needs to be reloaded.
type ErrSchemaCacheMiss struct {
	Table string
}

func (e ErrSchemaCacheMiss) Error() string {
	return fmt.Sprintf("schema cache miss for table %s", e.Table)
}
