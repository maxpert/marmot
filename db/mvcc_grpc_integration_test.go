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

func TestMVCCDatabase_Creation(t *testing.T) {
	dbPath := "/tmp/test_mvcc_db.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Verify MVCC tables exist
	tables := []string{
		"__marmot__txn_records",
		"__marmot__write_intents",
		"__marmot__mvcc_versions",
		"__marmot__metadata",
	}

	for _, table := range tables {
		var name string
		err := mdb.GetDB().QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			t.Fatalf("MVCC table %s not found: %v", table, err)
		}
	}

	t.Log("✓ MVCC database created with all system tables")
}

func TestMVCCDatabase_SimpleTransaction(t *testing.T) {
	dbPath := "/tmp/test_simple_txn.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create user table
	_, err = mdb.Exec(context.Background(), `
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

	// Verify transaction was recorded
	var count int
	err = mdb.GetDB().QueryRow("SELECT COUNT(*) FROM __marmot__txn_records WHERE status = ?", TxnStatusCommitted).Scan(&count)
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
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create user table
	_, err = mdb.Exec(context.Background(), `
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
	err = mdb.GetTransactionManager().WriteIntent(txn1, "users", "1", stmt1, dataSnapshot)
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
	err = mdb.GetTransactionManager().WriteIntent(txn2, "users", "1", stmt2, dataSnapshot2)
	if err == nil {
		t.Fatal("Expected conflict error, got nil")
	}

	t.Logf("✓ Conflict detected: %v", err)

	// Cleanup
	mdb.GetTransactionManager().AbortTransaction(txn1)
	mdb.GetTransactionManager().AbortTransaction(txn2)
}

func TestMVCCDatabase_Query(t *testing.T) {
	dbPath := "/tmp/test_query.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create and populate table
	_, err = mdb.Exec(context.Background(), `
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
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

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
	mdb.GetTransactionManager().AbortTransaction(txn)

	// Verify transaction is removed from active set
	retrievedTxn = mdb.GetTransactionManager().GetTransaction(txn.ID)
	if retrievedTxn != nil {
		t.Fatal("Transaction should be removed after abort")
	}

	t.Log("✓ Transaction removed from active set after abort")
}

func TestMVCCDatabase_ConcurrentReads(t *testing.T) {
	dbPath := "/tmp/test_concurrent_reads.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create and populate table
	_, err = mdb.Exec(context.Background(), `
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
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table
	_, err = mdb.Exec(context.Background(), `
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
	err = mdb.GetTransactionManager().WriteIntent(txn, "users", "1", stmt, dataSnapshot)
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

	// Verify transaction status in database
	var status string
	err = mdb.GetDB().QueryRow("SELECT status FROM __marmot__txn_records WHERE txn_id = ?", txn.ID).Scan(&status)
	if err != nil {
		t.Fatalf("Failed to query transaction status: %v", err)
	}

	if status != TxnStatusCommitted {
		t.Fatalf("Expected status %s, got %s", TxnStatusCommitted, status)
	}

	t.Logf("✓ Transaction record shows status: %s", status)
}
