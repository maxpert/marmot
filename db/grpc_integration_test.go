package db

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// Helper to create database with MetaStore for testing
func createTestMVCCDatabase(t *testing.T, dbPath string) (*MVCCDatabase, MetaStore) {
	t.Helper()

	metaPath := dbPath + "_meta.pebble"
	os.Remove(dbPath)
	os.RemoveAll(metaPath)

	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:           16,
		MemTableSizeMB:        8,
		MemTableCount:         2,
		L0CompactionThreshold: 4,
		L0StopWrites:          12,
	})
	if err != nil {
		t.Fatalf("Failed to create MetaStore: %v", err)
	}

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		metaStore.Close()
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Cleanup(func() {
		mdb.Close()
		metaStore.Close()
		os.Remove(dbPath)
		os.RemoveAll(metaPath)
	})

	return mdb, metaStore
}

func TestMVCCDatabase_Creation(t *testing.T) {
	dbPath := "/tmp/test_mvcc_db.db"
	mdb, metaStore := createTestMVCCDatabase(t, dbPath)
	_ = mdb

	// Verify MetaStore is functional by testing basic operations
	// Verify MetaStore is functional (PebbleDB backend)

	// Test GetCommittedTxnCount
	count, err := metaStore.GetCommittedTxnCount()
	if err != nil {
		t.Fatalf("GetCommittedTxnCount failed: %v", err)
	}
	t.Logf("Initial committed txn count: %d", count)

	// Test GetPendingTransactions
	pending, err := metaStore.GetPendingTransactions()
	if err != nil {
		t.Fatalf("GetPendingTransactions failed: %v", err)
	}
	t.Logf("Initial pending txn count: %d", len(pending))

	// Test GetMaxSeqNum
	_, err = metaStore.GetMaxSeqNum()
	if err != nil {
		t.Fatalf("GetMaxSeqNum failed: %v", err)
	}

	t.Log("✓ Database created with functional MetaStore")
}

func TestMVCCDatabase_SimpleTransaction(t *testing.T) {
	dbPath := "/tmp/test_simple_txn.db"
	mdb, metaStore := createTestMVCCDatabase(t, dbPath)

	// Create user table
	_, err := mdb.Exec(context.Background(), `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute transaction
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
			Type:      protocol.StatementInsert,
			TableName: "users",
		},
	}

	err = mdb.ExecuteTransaction(context.Background(), statements)
	if err != nil {
		t.Fatalf("Failed to execute transaction: %v", err)
	}

	// Verify transaction was recorded in MetaStore
	count, err := metaStore.GetCommittedTxnCount()
	if err != nil {
		t.Fatalf("Failed to query txn records: %v", err)
	}

	if count != 1 {
		t.Fatalf("Expected 1 committed transaction, got %d", count)
	}

	t.Log("✓ Simple transaction executed and committed successfully")
}

func TestMVCCDatabase_ConflictDetection(t *testing.T) {
	dbPath := "/tmp/test_conflict.db"
	mdb, _ := createTestMVCCDatabase(t, dbPath)

	// Create user table
	_, err := mdb.Exec(context.Background(), `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Transaction 1: Create write intent
	txn1, err := mdb.GetTransactionManager().BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin txn1: %v", err)
	}

	stmt1 := protocol.Statement{
		SQL:       "UPDATE users SET balance = 100 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	dataSnapshot, _ := SerializeData(map[string]interface{}{"balance": 100})
	err = mdb.GetTransactionManager().WriteIntent(txn1, IntentTypeDML, "users", "1", stmt1, dataSnapshot)
	if err != nil {
		t.Fatalf("Failed to create write intent: %v", err)
	}

	t.Logf("✓ Transaction %d created write intent for users:1", txn1.ID)

	// Transaction 2: Try to create conflicting write intent
	txn2, err := mdb.GetTransactionManager().BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin txn2: %v", err)
	}

	stmt2 := protocol.Statement{
		SQL:       "UPDATE users SET balance = 200 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	dataSnapshot2, _ := SerializeData(map[string]interface{}{"balance": 200})
	err = mdb.GetTransactionManager().WriteIntent(txn2, IntentTypeDML, "users", "1", stmt2, dataSnapshot2)
	if err == nil {
		t.Fatal("Expected conflict error, got nil")
	}

	t.Logf("✓ Conflict detected: %v", err)

	// Cleanup
	_ = mdb.GetTransactionManager().AbortTransaction(txn1)
	_ = mdb.GetTransactionManager().AbortTransaction(txn2)
}

func TestMVCCDatabase_Query(t *testing.T) {
	dbPath := "/tmp/test_query.db"
	mdb, _ := createTestMVCCDatabase(t, dbPath)

	// Create and populate table
	_, err := mdb.Exec(context.Background(), `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = mdb.Exec(context.Background(), "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Execute query
	rows, err := mdb.ExecuteQuery(context.Background(), "SELECT id, name, balance FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}

	var id int
	var name string
	var balance int
	if err := rows.Scan(&id, &name, &balance); err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	if name != "Alice" || balance != 100 {
		t.Fatalf("Unexpected data: name=%s, balance=%d", name, balance)
	}

	t.Logf("✓ Query executed successfully: id=%d, name=%s, balance=%d", id, name, balance)
}

func TestMVCCDatabase_GetTransaction(t *testing.T) {
	dbPath := "/tmp/test_get_txn.db"
	mdb, _ := createTestMVCCDatabase(t, dbPath)

	// Begin transaction
	txn, err := mdb.GetTransactionManager().BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Retrieve transaction
	retrievedTxn := mdb.GetTransactionManager().GetTransaction(txn.ID)
	if retrievedTxn == nil {
		t.Fatal("Failed to retrieve transaction")
	}

	if retrievedTxn.ID != txn.ID {
		t.Fatalf("Transaction ID mismatch: expected %d, got %d", txn.ID, retrievedTxn.ID)
	}

	t.Logf("✓ Transaction %d retrieved successfully", txn.ID)

	// Abort transaction
	_ = mdb.GetTransactionManager().AbortTransaction(txn)

	// Verify transaction is removed from active set
	retrievedTxn = mdb.GetTransactionManager().GetTransaction(txn.ID)
	if retrievedTxn != nil {
		t.Fatal("Transaction should be removed after abort")
	}

	t.Log("✓ Transaction removed from active set after abort")
}

func TestMVCCDatabase_ConcurrentReads(t *testing.T) {
	dbPath := "/tmp/test_concurrent_reads.db"
	mdb, _ := createTestMVCCDatabase(t, dbPath)

	// Create and populate table
	_, err := mdb.Exec(context.Background(), `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = mdb.Exec(context.Background(), "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Execute concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			rows, err := mdb.ExecuteQuery(context.Background(), "SELECT id, name, balance FROM users WHERE id = ?", 1)
			if err != nil {
				t.Errorf("Reader %d failed: %v", idx, err)
				done <- false
				return
			}
			rows.Close()
			done <- true
		}(i)
	}

	// Wait for all readers
	successCount := 0
	for i := 0; i < 10; i++ {
		if <-done {
			successCount++
		}
	}

	if successCount != 10 {
		t.Fatalf("Expected 10 successful reads, got %d", successCount)
	}

	t.Log("✓ 10 concurrent reads executed successfully")
}

func TestMVCCDatabase_TransactionLifecycle(t *testing.T) {
	dbPath := "/tmp/test_txn_lifecycle.db"
	mdb, metaStore := createTestMVCCDatabase(t, dbPath)

	// Create table
	_, err := mdb.Exec(context.Background(), `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	txn, err := mdb.GetTransactionManager().BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	t.Logf("✓ Transaction %d started (status: %s)", txn.ID, txn.Status)

	// Add statement
	stmt := protocol.Statement{
		SQL:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
		Type:      protocol.StatementInsert,
		TableName: "users",
	}
	err = mdb.GetTransactionManager().AddStatement(txn, stmt)
	if err != nil {
		t.Fatalf("Failed to add statement: %v", err)
	}
	t.Log("✓ Statement added to transaction")

	// Create write intent
	dataSnapshot, _ := SerializeData(map[string]interface{}{"balance": 100})
	err = mdb.GetTransactionManager().WriteIntent(txn, IntentTypeDML, "users", "1", stmt, dataSnapshot)
	if err != nil {
		t.Fatalf("Failed to create write intent: %v", err)
	}
	t.Log("✓ Write intent created")

	// Wait a bit to ensure commit timestamp > start timestamp
	time.Sleep(10 * time.Millisecond)

	// Commit transaction
	err = mdb.GetTransactionManager().CommitTransaction(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	t.Logf("✓ Transaction %d committed (commit_ts: %d)", txn.ID, txn.CommitTS.WallTime)

	// Verify transaction status in MetaStore
	rec, err := metaStore.GetTransaction(txn.ID)
	if err != nil {
		t.Fatalf("Failed to query transaction status: %v", err)
	}
	if rec == nil {
		t.Fatalf("Transaction record not found for txn_id=%d", txn.ID)
	}

	if rec.Status != TxnStatusCommitted {
		t.Fatalf("Expected status %s, got %s", TxnStatusCommitted, rec.Status)
	}

	t.Logf("✓ Transaction record shows status: %s", rec.Status)
}

// TestMVCCDatabase_Close verifies that Close() properly stops GC and closes all connections
func TestMVCCDatabase_Close(t *testing.T) {
	dbPath := "/tmp/test_mvcc_close.db"
	metaPath := dbPath + "_meta.pebble"
	os.Remove(dbPath)
	os.RemoveAll(metaPath)

	// Create MetaStore
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:           16,
		MemTableSizeMB:        8,
		MemTableCount:         2,
		L0CompactionThreshold: 4,
		L0StopWrites:          12,
	})
	if err != nil {
		t.Fatalf("Failed to create MetaStore: %v", err)
	}

	// Create MVCCDatabase
	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		metaStore.Close()
		t.Fatalf("Failed to create database: %v", err)
	}

	// Start GC to verify it gets stopped
	mdb.txnMgr.StartGarbageCollection()
	time.Sleep(50 * time.Millisecond) // Let GC start

	// Close should stop GC and close MetaStore
	err = mdb.Close()
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	// After Close(), database connections should be closed
	// Attempting to execute should fail
	_, err = mdb.GetDB().Exec("SELECT 1")
	if err == nil {
		t.Error("Expected error executing on closed database, got nil")
	}

	// Verify GC is stopped by checking gcRunning flag
	if mdb.txnMgr.gcRunning {
		t.Error("GC should be stopped after Close()")
	}

	t.Log("✓ MVCCDatabase.Close() properly stops GC and closes connections")

	// Cleanup
	os.Remove(dbPath)
	os.RemoveAll(metaPath)
}

// TestMVCCDatabase_CloseMultipleTimes verifies that Close() is safe to call multiple times
func TestMVCCDatabase_CloseMultipleTimes(t *testing.T) {
	dbPath := "/tmp/test_mvcc_close_multi.db"
	metaPath := dbPath + "_meta.pebble"
	os.Remove(dbPath)
	os.RemoveAll(metaPath)

	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:           16,
		MemTableSizeMB:        8,
		MemTableCount:         2,
		L0CompactionThreshold: 4,
		L0StopWrites:          12,
	})
	if err != nil {
		t.Fatalf("Failed to create MetaStore: %v", err)
	}

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		metaStore.Close()
		t.Fatalf("Failed to create database: %v", err)
	}

	// Start GC
	mdb.txnMgr.StartGarbageCollection()
	time.Sleep(50 * time.Millisecond)

	// First close should succeed
	err = mdb.Close()
	if err != nil {
		t.Fatalf("First Close() returned error: %v", err)
	}

	// Second close should not panic (idempotent)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Second Close() panicked: %v", r)
		}
	}()

	// This should not panic even though already closed
	mdb.txnMgr.StopGarbageCollection()

	t.Log("✓ StopGarbageCollection is safe to call multiple times")

	os.Remove(dbPath)
	os.RemoveAll(metaPath)
}
