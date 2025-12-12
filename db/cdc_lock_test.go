package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCDCRowLock_AcquireRelease(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"
	rowKey := "user:1"

	// Acquire lock
	err := store.AcquireCDCRowLock(txnID, table, rowKey)
	require.NoError(t, err)

	// Verify lock is held
	lockTxnID, err := store.GetCDCRowLock(table, rowKey)
	require.NoError(t, err)
	assert.Equal(t, txnID, lockTxnID)

	// Release lock
	err = store.ReleaseCDCRowLock(table, rowKey, txnID)
	require.NoError(t, err)

	// Verify lock is released
	lockTxnID, err = store.GetCDCRowLock(table, rowKey)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lockTxnID)
}

func TestCDCRowLock_SameTxnReacquire(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"
	rowKey := "user:1"

	// Acquire lock first time
	err := store.AcquireCDCRowLock(txnID, table, rowKey)
	require.NoError(t, err)

	// Same txn re-acquires - should succeed (idempotent)
	err = store.AcquireCDCRowLock(txnID, table, rowKey)
	require.NoError(t, err)

	// Verify still held by same txn
	lockTxnID, err := store.GetCDCRowLock(table, rowKey)
	require.NoError(t, err)
	assert.Equal(t, txnID, lockTxnID)
}

func TestCDCRowLock_ConflictDifferentTxn(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txn1 := uint64(100)
	txn2 := uint64(200)
	table := "users"
	rowKey := "user:1"

	// Txn1 acquires lock
	err := store.AcquireCDCRowLock(txn1, table, rowKey)
	require.NoError(t, err)

	// Txn2 tries to acquire same lock - should fail
	err = store.AcquireCDCRowLock(txn2, table, rowKey)
	require.Error(t, err)

	var cdcErr ErrCDCRowLocked
	require.ErrorAs(t, err, &cdcErr)
	assert.Equal(t, table, cdcErr.Table)
	assert.Equal(t, rowKey, cdcErr.RowKey)
	assert.Equal(t, txn1, cdcErr.HeldByTxn)

	// Release txn1 lock
	err = store.ReleaseCDCRowLock(table, rowKey, txn1)
	require.NoError(t, err)

	// Now txn2 can acquire
	err = store.AcquireCDCRowLock(txn2, table, rowKey)
	require.NoError(t, err)

	lockTxnID, err := store.GetCDCRowLock(table, rowKey)
	require.NoError(t, err)
	assert.Equal(t, txn2, lockTxnID)
}

func TestCDCRowLock_ReleaseBulkByTxn(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"

	// Acquire locks on multiple rows
	rows := []string{"user:1", "user:2", "user:3"}
	for _, rowKey := range rows {
		err := store.AcquireCDCRowLock(txnID, table, rowKey)
		require.NoError(t, err)
	}

	// Verify all locks held
	for _, rowKey := range rows {
		lockTxnID, err := store.GetCDCRowLock(table, rowKey)
		require.NoError(t, err)
		assert.Equal(t, txnID, lockTxnID)
	}

	// Release all locks for this txn
	err := store.ReleaseCDCRowLocksByTxn(txnID)
	require.NoError(t, err)

	// Verify all locks released
	for _, rowKey := range rows {
		lockTxnID, err := store.GetCDCRowLock(table, rowKey)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), lockTxnID)
	}
}

func TestCDCRowLock_DifferentTables(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txn1 := uint64(100)
	txn2 := uint64(200)
	table1 := "users"
	table2 := "orders"
	rowKey := "row:1"

	// Txn1 locks row in table1
	err := store.AcquireCDCRowLock(txn1, table1, rowKey)
	require.NoError(t, err)

	// Txn2 locks same rowKey in table2 - should succeed (different tables)
	err = store.AcquireCDCRowLock(txn2, table2, rowKey)
	require.NoError(t, err)

	// Verify both locks held
	lockTxnID1, err := store.GetCDCRowLock(table1, rowKey)
	require.NoError(t, err)
	assert.Equal(t, txn1, lockTxnID1)

	lockTxnID2, err := store.GetCDCRowLock(table2, rowKey)
	require.NoError(t, err)
	assert.Equal(t, txn2, lockTxnID2)
}

func TestCDCRowLock_DifferentRows(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txn1 := uint64(100)
	txn2 := uint64(200)
	table := "users"
	row1 := "user:1"
	row2 := "user:2"

	// Txn1 locks row1
	err := store.AcquireCDCRowLock(txn1, table, row1)
	require.NoError(t, err)

	// Txn2 locks row2 in same table - should succeed (different rows)
	err = store.AcquireCDCRowLock(txn2, table, row2)
	require.NoError(t, err)

	// Verify both locks held
	lockTxnID1, err := store.GetCDCRowLock(table, row1)
	require.NoError(t, err)
	assert.Equal(t, txn1, lockTxnID1)

	lockTxnID2, err := store.GetCDCRowLock(table, row2)
	require.NoError(t, err)
	assert.Equal(t, txn2, lockTxnID2)
}

func TestCDCDDLLock_AcquireRelease(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"

	// Acquire DDL lock
	err := store.AcquireCDCTableDDLLock(txnID, table)
	require.NoError(t, err)

	// Verify lock is held
	lockTxnID, err := store.GetCDCTableDDLLock(table)
	require.NoError(t, err)
	assert.Equal(t, txnID, lockTxnID)

	// Release DDL lock
	err = store.ReleaseCDCTableDDLLock(table, txnID)
	require.NoError(t, err)

	// Verify lock is released
	lockTxnID, err = store.GetCDCTableDDLLock(table)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lockTxnID)
}

func TestCDCDDLLock_BlockedByDML(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	dmlTxn := uint64(100)
	ddlTxn := uint64(200)
	table := "users"
	rowKey := "user:1"

	// DML txn acquires row lock
	err := store.AcquireCDCRowLock(dmlTxn, table, rowKey)
	require.NoError(t, err)

	// DDL txn tries to acquire DDL lock - should fail (DML in progress)
	err = store.AcquireCDCTableDDLLock(ddlTxn, table)
	require.Error(t, err)

	var dmlErr ErrCDCDMLInProgress
	require.ErrorAs(t, err, &dmlErr)
	assert.Equal(t, table, dmlErr.Table)

	// Release DML lock
	err = store.ReleaseCDCRowLock(table, rowKey, dmlTxn)
	require.NoError(t, err)

	// Now DDL can acquire
	err = store.AcquireCDCTableDDLLock(ddlTxn, table)
	require.NoError(t, err)

	lockTxnID, err := store.GetCDCTableDDLLock(table)
	require.NoError(t, err)
	assert.Equal(t, ddlTxn, lockTxnID)
}

func TestCDCDDLLock_CheckBeforeRowLock(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	ddlTxn := uint64(100)
	dmlTxn := uint64(200)
	table := "users"
	rowKey := "user:1"

	// DDL txn acquires DDL lock
	err := store.AcquireCDCTableDDLLock(ddlTxn, table)
	require.NoError(t, err)

	// DML should check DDL lock before proceeding
	lockTxnID, err := store.GetCDCTableDDLLock(table)
	require.NoError(t, err)
	assert.Equal(t, ddlTxn, lockTxnID)

	// DML should NOT acquire row lock if DDL in progress
	// (This is enforced by application logic, not the lock API itself)

	// Release DDL lock
	err = store.ReleaseCDCTableDDLLock(table, ddlTxn)
	require.NoError(t, err)

	// Now DML can proceed
	lockTxnID, err = store.GetCDCTableDDLLock(table)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lockTxnID)

	err = store.AcquireCDCRowLock(dmlTxn, table, rowKey)
	require.NoError(t, err)
}

func TestCDCRowLock_MultipleRowsSameTxn(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"

	// Acquire locks on multiple rows with same txn
	rows := []string{"user:1", "user:2", "user:3", "user:4", "user:5"}
	for _, rowKey := range rows {
		err := store.AcquireCDCRowLock(txnID, table, rowKey)
		require.NoError(t, err)
	}

	// Verify all locks held by same txn
	for _, rowKey := range rows {
		lockTxnID, err := store.GetCDCRowLock(table, rowKey)
		require.NoError(t, err)
		assert.Equal(t, txnID, lockTxnID)
	}

	// Release all at once
	err := store.ReleaseCDCRowLocksByTxn(txnID)
	require.NoError(t, err)

	// Verify all released
	for _, rowKey := range rows {
		lockTxnID, err := store.GetCDCRowLock(table, rowKey)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), lockTxnID)
	}
}

func TestCDCRowLock_IdempotentRelease(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"
	rowKey := "user:1"

	// Acquire lock
	err := store.AcquireCDCRowLock(txnID, table, rowKey)
	require.NoError(t, err)

	// Release lock
	err = store.ReleaseCDCRowLock(table, rowKey, txnID)
	require.NoError(t, err)

	// Release again - should be idempotent (no error)
	err = store.ReleaseCDCRowLock(table, rowKey, txnID)
	require.NoError(t, err)

	// Release by txn - should also be idempotent
	err = store.ReleaseCDCRowLocksByTxn(txnID)
	require.NoError(t, err)
}

func TestCDCDDLLock_IdempotentRelease(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"

	// Acquire DDL lock
	err := store.AcquireCDCTableDDLLock(txnID, table)
	require.NoError(t, err)

	// Release lock
	err = store.ReleaseCDCTableDDLLock(table, txnID)
	require.NoError(t, err)

	// Release again - should be idempotent (no error)
	err = store.ReleaseCDCTableDDLLock(table, txnID)
	require.NoError(t, err)
}

func TestCDCRowLock_WrongTxnCannotRelease(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txn1 := uint64(100)
	txn2 := uint64(200)
	table := "users"
	rowKey := "user:1"

	// Txn1 acquires lock
	err := store.AcquireCDCRowLock(txn1, table, rowKey)
	require.NoError(t, err)

	// Txn2 tries to release - should be no-op (lock still held by txn1)
	err = store.ReleaseCDCRowLock(table, rowKey, txn2)
	require.NoError(t, err)

	// Verify lock still held by txn1
	lockTxnID, err := store.GetCDCRowLock(table, rowKey)
	require.NoError(t, err)
	assert.Equal(t, txn1, lockTxnID)
}

func TestCDCDDLLock_WrongTxnCannotRelease(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txn1 := uint64(100)
	txn2 := uint64(200)
	table := "users"

	// Txn1 acquires DDL lock
	err := store.AcquireCDCTableDDLLock(txn1, table)
	require.NoError(t, err)

	// Txn2 tries to release - should be no-op (lock still held by txn1)
	err = store.ReleaseCDCTableDDLLock(table, txn2)
	require.NoError(t, err)

	// Verify lock still held by txn1
	lockTxnID, err := store.GetCDCTableDDLLock(table)
	require.NoError(t, err)
	assert.Equal(t, txn1, lockTxnID)
}

func TestCDCRowLock_HasCDCRowLocksForTable(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"
	rowKey := "user:1"

	// Initially no locks
	hasLocks, err := store.HasCDCRowLocksForTable(table)
	require.NoError(t, err)
	assert.False(t, hasLocks)

	// Acquire row lock
	err = store.AcquireCDCRowLock(txnID, table, rowKey)
	require.NoError(t, err)

	// Now has locks
	hasLocks, err = store.HasCDCRowLocksForTable(table)
	require.NoError(t, err)
	assert.True(t, hasLocks)

	// Release lock
	err = store.ReleaseCDCRowLock(table, rowKey, txnID)
	require.NoError(t, err)

	// No locks again
	hasLocks, err = store.HasCDCRowLocksForTable(table)
	require.NoError(t, err)
	assert.False(t, hasLocks)
}

func TestCDCDDLLock_DoesNotCountAsDMLLock(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(100)
	table := "users"

	// Acquire DDL lock
	err := store.AcquireCDCTableDDLLock(txnID, table)
	require.NoError(t, err)

	// HasCDCRowLocksForTable should return false (DDL lock doesn't count as DML)
	hasLocks, err := store.HasCDCRowLocksForTable(table)
	require.NoError(t, err)
	assert.False(t, hasLocks)
}

func TestCDCRowLock_MultipleTxnsMultipleTables(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	// Txn1: locks users:1 and orders:1
	// Txn2: locks users:2 and products:1
	// Txn3: locks orders:2
	txn1 := uint64(100)
	txn2 := uint64(200)
	txn3 := uint64(300)

	locks := []struct {
		txnID  uint64
		table  string
		rowKey string
	}{
		{txn1, "users", "row:1"},
		{txn1, "orders", "row:1"},
		{txn2, "users", "row:2"},
		{txn2, "products", "row:1"},
		{txn3, "orders", "row:2"},
	}

	// Acquire all locks
	for _, lock := range locks {
		err := store.AcquireCDCRowLock(lock.txnID, lock.table, lock.rowKey)
		require.NoError(t, err)
	}

	// Verify all locks
	for _, lock := range locks {
		lockTxnID, err := store.GetCDCRowLock(lock.table, lock.rowKey)
		require.NoError(t, err)
		assert.Equal(t, lock.txnID, lockTxnID)
	}

	// Release txn1 locks
	err := store.ReleaseCDCRowLocksByTxn(txn1)
	require.NoError(t, err)

	// Verify txn1 locks released
	lockTxnID, err := store.GetCDCRowLock("users", "row:1")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lockTxnID)

	lockTxnID, err = store.GetCDCRowLock("orders", "row:1")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lockTxnID)

	// Verify other locks still held
	lockTxnID, err = store.GetCDCRowLock("users", "row:2")
	require.NoError(t, err)
	assert.Equal(t, txn2, lockTxnID)

	lockTxnID, err = store.GetCDCRowLock("orders", "row:2")
	require.NoError(t, err)
	assert.Equal(t, txn3, lockTxnID)
}
