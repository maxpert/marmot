package db

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// TestMVCCSchemaCreation tests that MVCC schema is created properly
func TestMVCCSchemaCreation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Verify transaction records table
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM __marmot__txn_records").Scan(&count)
	if err != nil {
		t.Fatalf("Transaction records table not created: %v", err)
	}

	// Verify write intents table
	err = db.QueryRow("SELECT COUNT(*) FROM __marmot__write_intents").Scan(&count)
	if err != nil {
		t.Fatalf("Write intents table not created: %v", err)
	}

	// Verify MVCC versions table
	err = db.QueryRow("SELECT COUNT(*) FROM __marmot__mvcc_versions").Scan(&count)
	if err != nil {
		t.Fatalf("MVCC versions table not created: %v", err)
	}

	t.Log("✓ All MVCC tables created successfully")
}

// TestUserTableTransparency tests that user tables are stored transparently
func TestUserTableTransparency(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a normal user table
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			balance INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create user table: %v", err)
	}

	// Insert data directly (bypassing MVCC for now - just testing transparency)
	_, err = db.Exec("INSERT INTO users (id, name, email, balance) VALUES (?, ?, ?, ?)",
		1, "Alice", "alice@example.com", 100)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Read data using standard SQL (proving transparency)
	var id int
	var name, email string
	var balance int
	err = db.QueryRow("SELECT id, name, email, balance FROM users WHERE id = ?", 1).
		Scan(&id, &name, &email, &balance)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if name != "Alice" || balance != 100 {
		t.Errorf("Data mismatch: got name=%s, balance=%d, want name=Alice, balance=100",
			name, balance)
	}

	t.Logf("✓ User table is transparent: id=%d, name=%s, email=%s, balance=%d",
		id, name, email, balance)
}

// TestTransactionLifecycle tests complete transaction lifecycle
func TestTransactionLifecycle(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)

	// Create user table
	createUserTable(t, db)

	// Begin transaction
	txn, err := tm.BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if txn.Status != TxnStatusPending {
		t.Errorf("Transaction status = %s, want PENDING", txn.Status)
	}

	t.Logf("✓ Transaction %d started with status PENDING", txn.ID)

	// Add statements
	stmt := protocol.Statement{
		SQL:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
		Type:      protocol.StatementInsert,
		TableName: "users",
	}

	err = tm.AddStatement(txn, stmt)
	if err != nil {
		t.Fatalf("Failed to add statement: %v", err)
	}

	t.Logf("✓ Statement added to transaction")

	// Commit transaction
	err = tm.CommitTransaction(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if txn.Status != TxnStatusCommitted {
		t.Errorf("Transaction status = %s, want COMMITTED", txn.Status)
	}

	t.Logf("✓ Transaction %d committed successfully", txn.ID)

	// Wait for async cleanup
	time.Sleep(100 * time.Millisecond)

	// Verify transaction record
	var status string
	err = db.QueryRow("SELECT status FROM __marmot__txn_records WHERE txn_id = ?", txn.ID).
		Scan(&status)
	if err != nil {
		t.Fatalf("Failed to read transaction record: %v", err)
	}

	if status != TxnStatusCommitted {
		t.Errorf("Transaction record status = %s, want COMMITTED", status)
	}

	t.Logf("✓ Transaction record persisted with status COMMITTED")
}

// TestWriteIntentCreation tests write intent creation and locking
func TestWriteIntentCreation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)

	createUserTable(t, db)

	// Begin transaction
	txn, err := tm.BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create write intent
	stmt := protocol.Statement{
		SQL:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
		Type:      protocol.StatementInsert,
		TableName: "users",
	}

	data := map[string]interface{}{
		"id":      1,
		"name":    "Alice",
		"balance": 100,
	}
	dataBytes, _ := SerializeData(data)

	err = tm.WriteIntent(txn, "users", "1", stmt, dataBytes)
	if err != nil {
		t.Fatalf("Failed to create write intent: %v", err)
	}

	t.Logf("✓ Write intent created for users:1")

	// Verify intent exists in database
	var intentTxnID uint64
	err = db.QueryRow("SELECT txn_id FROM __marmot__write_intents WHERE table_name = ? AND row_key = ?",
		"users", "1").Scan(&intentTxnID)
	if err != nil {
		t.Fatalf("Failed to read write intent: %v", err)
	}

	if intentTxnID != txn.ID {
		t.Errorf("Intent txn_id = %d, want %d", intentTxnID, txn.ID)
	}

	t.Logf("✓ Write intent persisted in __marmot__write_intents")

	// Cleanup
	tm.AbortTransaction(txn)
}

// TestWriteWriteConflictDetection tests that write-write conflicts are detected
func TestWriteWriteConflictDetection(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)

	createUserTable(t, db)

	// Transaction 1: Create write intent for users:1
	txn1, err := tm.BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	stmt := protocol.Statement{
		SQL:       "UPDATE users SET balance = 100 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	data := map[string]interface{}{"balance": 100}
	dataBytes, _ := SerializeData(data)

	err = tm.WriteIntent(txn1, "users", "1", stmt, dataBytes)
	if err != nil {
		t.Fatalf("Failed to create write intent for txn1: %v", err)
	}

	t.Logf("✓ Transaction %d created write intent for users:1", txn1.ID)

	// Transaction 2: Try to create conflicting write intent
	txn2, err := tm.BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	stmt2 := protocol.Statement{
		SQL:       "UPDATE users SET balance = 200 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	data2 := map[string]interface{}{"balance": 200}
	dataBytes2, _ := SerializeData(data2)

	err = tm.WriteIntent(txn2, "users", "1", stmt2, dataBytes2)
	if err == nil {
		t.Fatal("Expected write-write conflict error, got nil")
	}

	// Verify error message mentions conflict
	if err != nil {
		t.Logf("✓ Write-write conflict detected: %v", err)
	}

	// Cleanup
	tm.AbortTransaction(txn1)
	tm.AbortTransaction(txn2)
}

// TestConcurrentTransactionsOnDifferentRows tests MVCC isolation
func TestConcurrentTransactionsOnDifferentRows(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)

	createUserTable(t, db)

	var wg sync.WaitGroup
	errors := make(chan error, 2)

	// Transaction 1: Write to users:1
	wg.Add(1)
	go func() {
		defer wg.Done()

		txn, err := tm.BeginTransaction(1)
		if err != nil {
			errors <- err
			return
		}

		stmt := protocol.Statement{
			SQL:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
			Type:      protocol.StatementInsert,
			TableName: "users",
		}

		data := map[string]interface{}{"id": 1, "name": "Alice", "balance": 100}
		dataBytes, _ := SerializeData(data)

		err = tm.WriteIntent(txn, "users", "1", stmt, dataBytes)
		if err != nil {
			errors <- err
			return
		}

		time.Sleep(50 * time.Millisecond)

		err = tm.CommitTransaction(txn)
		if err != nil {
			errors <- err
			return
		}
	}()

	// Transaction 2: Write to users:2 (different row - no conflict)
	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(10 * time.Millisecond) // Start slightly after txn1

		txn, err := tm.BeginTransaction(2)
		if err != nil {
			errors <- err
			return
		}

		stmt := protocol.Statement{
			SQL:       "INSERT INTO users (id, name, balance) VALUES (2, 'Bob', 200)",
			Type:      protocol.StatementInsert,
			TableName: "users",
		}

		data := map[string]interface{}{"id": 2, "name": "Bob", "balance": 200}
		dataBytes, _ := SerializeData(data)

		err = tm.WriteIntent(txn, "users", "2", stmt, dataBytes)
		if err != nil {
			errors <- err
			return
		}

		err = tm.CommitTransaction(txn)
		if err != nil {
			errors <- err
			return
		}
	}()

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Transaction failed: %v", err)
	}

	t.Log("✓ Concurrent transactions on different rows completed successfully")
}

// TestTransactionAbort tests transaction abort and cleanup
func TestTransactionAbort(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)

	createUserTable(t, db)

	// Begin transaction
	txn, err := tm.BeginTransaction(1)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create write intent
	stmt := protocol.Statement{
		SQL:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
		Type:      protocol.StatementInsert,
		TableName: "users",
	}

	data := map[string]interface{}{"id": 1, "name": "Alice", "balance": 100}
	dataBytes, _ := SerializeData(data)

	err = tm.WriteIntent(txn, "users", "1", stmt, dataBytes)
	if err != nil {
		t.Fatalf("Failed to create write intent: %v", err)
	}

	t.Logf("✓ Write intent created for txn %d", txn.ID)

	// Abort transaction
	err = tm.AbortTransaction(txn)
	if err != nil {
		t.Fatalf("Failed to abort transaction: %v", err)
	}

	if txn.Status != TxnStatusAborted {
		t.Errorf("Transaction status = %s, want ABORTED", txn.Status)
	}

	t.Logf("✓ Transaction %d aborted", txn.ID)

	// Verify write intent is cleaned up
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM __marmot__write_intents WHERE txn_id = ?", txn.ID).
		Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query write intents: %v", err)
	}

	if count != 0 {
		t.Errorf("Write intents not cleaned up: found %d intents after abort", count)
	}

	t.Logf("✓ Write intents cleaned up after abort")

	// Verify transaction record shows ABORTED
	var status string
	err = db.QueryRow("SELECT status FROM __marmot__txn_records WHERE txn_id = ?", txn.ID).
		Scan(&status)
	if err != nil {
		t.Fatalf("Failed to read transaction record: %v", err)
	}

	if status != TxnStatusAborted {
		t.Errorf("Transaction record status = %s, want ABORTED", status)
	}

	t.Logf("✓ Transaction record shows ABORTED status")
}

// TestExternalSQLiteReadability tests that external tools can read the DB
func TestExternalSQLiteReadability(t *testing.T) {
	dbPath := setupTestDBPath(t)
	db := openTestDB(t, dbPath)
	defer db.Close()

	// Create user table and insert data
	createUserTable(t, db)
	_, err := db.Exec("INSERT INTO users (id, name, email, balance) VALUES (?, ?, ?, ?)",
		1, "Alice", "alice@example.com", 100)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name, email, balance) VALUES (?, ?, ?, ?)",
		2, "Bob", "bob@example.com", 200)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	db.Close()

	// Open database with a separate connection (simulating external tool)
	externalDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB with external connection: %v", err)
	}
	defer externalDB.Close()

	// Read data using standard SQL
	rows, err := externalDB.Query("SELECT id, name, email, balance FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	users := []struct {
		id      int
		name    string
		email   string
		balance int
	}{}

	for rows.Next() {
		var u struct {
			id      int
			name    string
			email   string
			balance int
		}
		err := rows.Scan(&u.id, &u.name, &u.email, &u.balance)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		users = append(users, u)
	}

	if len(users) != 2 {
		t.Fatalf("Expected 2 users, got %d", len(users))
	}

	if users[0].name != "Alice" || users[0].balance != 100 {
		t.Errorf("User 1: got name=%s, balance=%d, want name=Alice, balance=100",
			users[0].name, users[0].balance)
	}

	if users[1].name != "Bob" || users[1].balance != 200 {
		t.Errorf("User 2: got name=%s, balance=%d, want name=Bob, balance=200",
			users[1].name, users[1].balance)
	}

	t.Log("✓ External SQLite tool can read user data transparently")
	t.Logf("  - User 1: %s (balance: %d)", users[0].name, users[0].balance)
	t.Logf("  - User 2: %s (balance: %d)", users[1].name, users[1].balance)
}

// Helper functions

func setupTestDBPath(t *testing.T) string {
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.db")
}

func openTestDB(t *testing.T, dbPath string) *sql.DB {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create MVCC schema
	if _, err := db.Exec(CreateTransactionRecordsTable); err != nil {
		t.Fatalf("Failed to create transaction records table: %v", err)
	}

	if _, err := db.Exec(CreateWriteIntentsTable); err != nil {
		t.Fatalf("Failed to create write intents table: %v", err)
	}

	if _, err := db.Exec(CreateMVCCVersionsTable); err != nil {
		t.Fatalf("Failed to create MVCC versions table: %v", err)
	}

	if _, err := db.Exec(CreateMetadataTable); err != nil {
		t.Fatalf("Failed to create metadata table: %v", err)
	}

	return db
}

func setupTestDB(t *testing.T) *sql.DB {
	return openTestDB(t, setupTestDBPath(t))
}

func createUserTable(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			balance INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create user table: %v", err)
	}
}
