//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/filter"
)

// createTestSystemDB creates an in-memory SQLite database for intent entry storage during tests
func createTestSystemDB(t *testing.T) *sql.DB {
	t.Helper()
	systemDB, err := sql.Open("sqlite3", ":memory:?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("Failed to create test system DB: %v", err)
	}
	// Create intent_entries table
	_, err = systemDB.Exec(CreateIntentEntriesTable)
	if err != nil {
		t.Fatalf("Failed to create intent entries table: %v", err)
	}
	return systemDB
}

func TestMutationGuard_MultiRowInsert(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
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

	// Execute multi-row INSERT with hooks
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
			Type:      protocol.StatementInsert,
			TableName: "users",
		},
		{
			SQL:       "INSERT INTO users (id, name, balance) VALUES (2, 'Bob', 200)",
			Type:      protocol.StatementInsert,
			TableName: "users",
		},
		{
			SQL:       "INSERT INTO users (id, name, balance) VALUES (3, 'Charlie', 300)",
			Type:      protocol.StatementInsert,
			TableName: "users",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12345, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// Check row counts
	rowCounts := pendingExec.GetRowCounts()
	if rowCounts == nil {
		t.Fatal("Expected row counts, got nil")
	}

	totalRows := pendingExec.GetTotalRowCount()
	if totalRows != 3 {
		t.Errorf("Expected 3 total rows, got %d", totalRows)
	}

	// Check key hashes
	keyHashes := pendingExec.GetKeyHashes(0) // 0 = no limit
	if keyHashes == nil {
		t.Fatal("Expected key hashes, got nil")
	}

	hashes, ok := keyHashes["users"]
	if !ok {
		t.Fatal("Expected hashes for 'users' table")
	}

	if len(hashes) != 3 {
		t.Errorf("Expected 3 hashes, got %d", len(hashes))
	}

	// Commit the transaction
	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify data was committed
	var count int
	err = mdb.GetDB().QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows after commit, got %d", count)
	}

	t.Log("✓ MutationGuard multi-row INSERT test passed")
}

func TestMutationGuard_Rollback(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_rollback.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT,
			price INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute with hooks
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)",
			Type:      protocol.StatementInsert,
			TableName: "products",
		},
		{
			SQL:       "INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 200)",
			Type:      protocol.StatementInsert,
			TableName: "products",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12346, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// Verify we captured rows
	totalRows := pendingExec.GetTotalRowCount()
	if totalRows != 2 {
		t.Errorf("Expected 2 total rows, got %d", totalRows)
	}

	// Rollback instead of commit
	err = pendingExec.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify no data was committed
	var count int
	err = mdb.GetDB().QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", count)
	}

	t.Log("✓ MutationGuard rollback test passed")
}

func TestMutationGuard_BatchUpdate(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_update.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table and insert initial data
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY,
			name TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data directly
	_, err = mdb.GetDB().Exec(`
		INSERT INTO accounts (id, name, balance) VALUES
		(1, 'Alice', 1000),
		(2, 'Bob', 2000),
		(3, 'Charlie', 3000),
		(4, 'Diana', 4000),
		(5, 'Eve', 5000)
	`)
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Execute batch UPDATE with hooks
	statements := []protocol.Statement{
		{
			SQL:       "UPDATE accounts SET balance = balance + 100 WHERE balance < 3500",
			Type:      protocol.StatementUpdate,
			TableName: "accounts",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12347, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// Check row counts - should have captured 3 rows (Alice, Bob, Charlie)
	totalRows := pendingExec.GetTotalRowCount()
	if totalRows != 3 {
		t.Errorf("Expected 3 rows updated, got %d", totalRows)
	}

	// Check key hashes
	keyHashes := pendingExec.GetKeyHashes(0)
	if keyHashes == nil {
		t.Fatal("Expected key hashes, got nil")
	}

	hashes, ok := keyHashes["accounts"]
	if !ok {
		t.Fatal("Expected hashes for 'accounts' table")
	}

	if len(hashes) != 3 {
		t.Errorf("Expected 3 hashes, got %d", len(hashes))
	}

	// Commit
	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify updates were applied
	var sum int
	err = mdb.GetDB().QueryRow("SELECT SUM(balance) FROM accounts").Scan(&sum)
	if err != nil {
		t.Fatalf("Failed to sum balances: %v", err)
	}
	// Original: 1000+2000+3000+4000+5000 = 15000
	// After: +100 for each of 3 rows = 15300
	if sum != 15300 {
		t.Errorf("Expected sum 15300 after update, got %d", sum)
	}

	t.Log("✓ MutationGuard batch UPDATE test passed")
}

func TestMutationGuard_SingleRow(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_single.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute single row INSERT
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO items (id, name) VALUES (1, 'Single Item')",
			Type:      protocol.StatementInsert,
			TableName: "items",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12348, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// Check row counts - should be 1
	totalRows := pendingExec.GetTotalRowCount()
	if totalRows != 1 {
		t.Errorf("Expected 1 row, got %d", totalRows)
	}

	// Commit
	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Log("✓ MutationGuard single row test passed")
}

// TestMutationGuard_CompositeKeyWithSeparator tests that composite keys containing
// the separator character ":" don't collide (Fix #1: separator collision)
func TestMutationGuard_CompositeKeyWithSeparator(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_separator.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table with composite TEXT primary key
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE composite_test (
			part1 TEXT NOT NULL,
			part2 TEXT NOT NULL,
			value INTEGER,
			PRIMARY KEY (part1, part2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows that would collide without proper encoding:
	// ("a:b", "c") and ("a", "b:c") would both produce "composite_test:a:b:c"
	// With base64 encoding, they produce different keys
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO composite_test (part1, part2, value) VALUES ('a:b', 'c', 1)",
			Type:      protocol.StatementInsert,
			TableName: "composite_test",
		},
		{
			SQL:       "INSERT INTO composite_test (part1, part2, value) VALUES ('a', 'b:c', 2)",
			Type:      protocol.StatementInsert,
			TableName: "composite_test",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12349, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// Both rows should be captured as distinct keys
	totalRows := pendingExec.GetTotalRowCount()
	if totalRows != 2 {
		t.Errorf("Expected 2 distinct rows (no collision), got %d", totalRows)
	}

	// Verify we have 2 distinct hashes
	keyHashes := pendingExec.GetKeyHashes(0)
	if keyHashes == nil {
		t.Fatal("Expected key hashes, got nil")
	}

	hashes := keyHashes["composite_test"]
	if hashes == nil {
		t.Fatal("Expected hashes for composite_test table")
	}

	if len(hashes) != 2 {
		t.Errorf("Expected 2 distinct hashes (collision prevented), got %d", len(hashes))
	}

	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Log("✓ MutationGuard composite key separator collision test passed")
}

// TestMutationGuard_UpdatePKChange tests that UPDATE operations that change the
// primary key track both old and new keys (Fix #4: UPDATE PK change)
func TestMutationGuard_UpdatePKChange(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_pk_change.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE pk_change_test (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial row
	_, err = mdb.GetDB().Exec("INSERT INTO pk_change_test (id, name) VALUES (1, 'Original')")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// UPDATE that changes the primary key (id 1 -> id 100)
	statements := []protocol.Statement{
		{
			SQL:       "UPDATE pk_change_test SET id = 100 WHERE id = 1",
			Type:      protocol.StatementUpdate,
			TableName: "pk_change_test",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12350, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// For UPDATE that changes PK, we track 2 keys (old and new) for conflict detection.
	// GetTotalRowCount returns unique key count, not row count.
	// This ensures concurrent transactions accessing either old or new PK see conflicts.
	totalKeys := pendingExec.GetTotalRowCount()
	if totalKeys != 2 {
		t.Errorf("Expected 2 keys tracked (old=1, new=100), got %d", totalKeys)
	}

	keyHashes := pendingExec.GetKeyHashes(0)
	if keyHashes == nil {
		t.Fatal("Expected key hashes, got nil")
	}

	hashes := keyHashes["pk_change_test"]
	if hashes == nil {
		t.Fatal("Expected hashes for pk_change_test table")
	}

	// Should have 2 hashes: old key (id=1) and new key (id=100)
	if len(hashes) != 2 {
		t.Errorf("Expected 2 hashes (old and new PK), got %d", len(hashes))
	}

	// Verify both keys are in the hash set
	oldKeyHash := filter.HashRowKeyXXH64("pk_change_test:1")
	newKeyHash := filter.HashRowKeyXXH64("pk_change_test:100")

	hashSet := make(map[uint64]struct{})
	for _, h := range hashes {
		hashSet[h] = struct{}{}
	}

	if _, ok := hashSet[oldKeyHash]; !ok {
		t.Error("Hash set should contain old PK (id=1)")
	}
	if _, ok := hashSet[newKeyHash]; !ok {
		t.Error("Hash set should contain new PK (id=100)")
	}

	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify the update was applied
	var newID int
	err = mdb.GetDB().QueryRow("SELECT id FROM pk_change_test").Scan(&newID)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if newID != 100 {
		t.Errorf("Expected id=100 after update, got %d", newID)
	}

	t.Log("✓ MutationGuard UPDATE PK change test passed")
}

// TestMutationGuard_StringPKWithSpecialChars tests that string PKs with special
// characters are properly encoded (Fix #1: base64 encoding for non-numeric)
func TestMutationGuard_StringPKWithSpecialChars(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_string_pk.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table with TEXT primary key
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE string_pk_test (
			id TEXT PRIMARY KEY,
			value INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows with special characters in PK
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO string_pk_test (id, value) VALUES ('key:with:colons', 1)",
			Type:      protocol.StatementInsert,
			TableName: "string_pk_test",
		},
		{
			SQL:       "INSERT INTO string_pk_test (id, value) VALUES ('key with spaces', 2)",
			Type:      protocol.StatementInsert,
			TableName: "string_pk_test",
		},
		{
			SQL:       "INSERT INTO string_pk_test (id, value) VALUES ('key\nwith\nnewlines', 3)",
			Type:      protocol.StatementInsert,
			TableName: "string_pk_test",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12351, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	totalRows := pendingExec.GetTotalRowCount()
	if totalRows != 3 {
		t.Errorf("Expected 3 rows, got %d", totalRows)
	}

	keyHashes := pendingExec.GetKeyHashes(0)
	hashes := keyHashes["string_pk_test"]

	// All 3 should be distinct
	if len(hashes) != 3 {
		t.Errorf("Expected 3 distinct hashes, got %d", len(hashes))
	}

	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Log("✓ MutationGuard string PK with special chars test passed")
}

// TestMutationGuard_ASTHookCompatibility verifies that the hook-based row key generation
// produces keys compatible with the AST-based GenerateRowKey in protocol package.
// This is critical for conflict detection to work across both paths.
func TestMutationGuard_ASTHookCompatibility(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_compat.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE compat_test (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute INSERT to capture row key via hooks
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO compat_test (id, name) VALUES (42, 'test')",
			Type:      protocol.StatementInsert,
			TableName: "compat_test",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12352, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// Get the key hashes
	keyHashes := pendingExec.GetKeyHashes(0)
	hashes := keyHashes["compat_test"]
	if len(hashes) != 1 {
		t.Fatalf("Expected 1 hash, got %d", len(hashes))
	}

	// Generate the expected row key using AST path
	schema := &protocol.TableSchema{
		TableName:   "compat_test",
		PrimaryKeys: []string{"id"},
	}
	values := map[string][]byte{"id": []byte("42")}
	astRowKey, err := protocol.GenerateRowKey(schema, values)
	if err != nil {
		t.Fatalf("GenerateRowKey failed: %v", err)
	}

	// The AST-generated key should be "compat_test:42"
	expectedKey := "compat_test:42"
	if astRowKey != expectedKey {
		t.Errorf("AST row key mismatch: got %q, expected %q", astRowKey, expectedKey)
	}

	// Hash the AST key and check if it matches the hook-generated hash
	astKeyHash := filter.HashRowKeyXXH64(astRowKey)
	if hashes[0] != astKeyHash {
		t.Errorf("Hook hash does not match AST-generated key hash!\n  AST key: %q\n  AST hash: %d\n  Hook hash: %d",
			astRowKey, astKeyHash, hashes[0])
	}

	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Log("✓ MutationGuard AST-Hook compatibility test passed")
}

// TestMutationGuard_CompositeKeyCompatibility verifies compatibility for composite keys
func TestMutationGuard_CompositeKeyCompatibility(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_composite_compat.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table with composite primary key
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE composite_compat (
			user_id INTEGER NOT NULL,
			order_id INTEGER NOT NULL,
			value TEXT,
			PRIMARY KEY (user_id, order_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute INSERT to capture row key via hooks
	statements := []protocol.Statement{
		{
			SQL:       "INSERT INTO composite_compat (user_id, order_id, value) VALUES (1, 100, 'test')",
			Type:      protocol.StatementInsert,
			TableName: "composite_compat",
		},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12353, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// Get the key hashes
	keyHashes := pendingExec.GetKeyHashes(0)
	hashes := keyHashes["composite_compat"]
	if len(hashes) != 1 {
		t.Fatalf("Expected 1 hash, got %d", len(hashes))
	}

	// Generate the expected row key using AST path
	schema := &protocol.TableSchema{
		TableName:   "composite_compat",
		PrimaryKeys: []string{"user_id", "order_id"},
	}
	values := map[string][]byte{
		"user_id":  []byte("1"),
		"order_id": []byte("100"),
	}
	astRowKey, err := protocol.GenerateRowKey(schema, values)
	if err != nil {
		t.Fatalf("GenerateRowKey failed: %v", err)
	}

	// Hash the AST key and check if it matches the hook-generated hash
	astKeyHash := filter.HashRowKeyXXH64(astRowKey)
	if hashes[0] != astKeyHash {
		t.Errorf("Hook hash does not match AST-generated composite key hash!\n  AST key: %q\n  AST hash: %d\n  Hook hash: %d",
			astRowKey, astKeyHash, hashes[0])
	}

	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Logf("Composite key format: %q", astRowKey)
	t.Log("✓ MutationGuard composite key compatibility test passed")
}

// TestMutationGuard_MaxRowsLimit tests that GetKeyHashes returns nil when exceeding maxRows
func TestMutationGuard_MaxRowsLimit(t *testing.T) {
	dbPath := "/tmp/test_mutation_guard_maxrows.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	systemDB := createTestSystemDB(t)
	defer systemDB.Close()

	clock := hlc.NewClock(1)
	mdb, err := NewMVCCDatabase(dbPath, 1, clock, systemDB)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mdb.Close()

	// Create table
	_, err = mdb.Exec(context.Background(), `
		CREATE TABLE maxrows_test (
			id INTEGER PRIMARY KEY,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute 5 inserts
	statements := []protocol.Statement{
		{SQL: "INSERT INTO maxrows_test (id, value) VALUES (1, 'a')", Type: protocol.StatementInsert, TableName: "maxrows_test"},
		{SQL: "INSERT INTO maxrows_test (id, value) VALUES (2, 'b')", Type: protocol.StatementInsert, TableName: "maxrows_test"},
		{SQL: "INSERT INTO maxrows_test (id, value) VALUES (3, 'c')", Type: protocol.StatementInsert, TableName: "maxrows_test"},
		{SQL: "INSERT INTO maxrows_test (id, value) VALUES (4, 'd')", Type: protocol.StatementInsert, TableName: "maxrows_test"},
		{SQL: "INSERT INTO maxrows_test (id, value) VALUES (5, 'e')", Type: protocol.StatementInsert, TableName: "maxrows_test"},
	}

	ctx := context.Background()
	pendingExec, err := mdb.ExecuteLocalWithHooks(ctx, 12354, statements)
	if err != nil {
		t.Fatalf("Failed to execute with hooks: %v", err)
	}

	// With maxRows=0 (no limit), should get all 5 hashes
	keyHashes := pendingExec.GetKeyHashes(0)
	if len(keyHashes["maxrows_test"]) != 5 {
		t.Errorf("Expected 5 hashes with no limit, got %d", len(keyHashes["maxrows_test"]))
	}

	// With maxRows=3, should get nil (exceeds limit, MVCC fallback)
	keyHashes = pendingExec.GetKeyHashes(3)
	if keyHashes["maxrows_test"] != nil {
		t.Errorf("Expected nil hashes when exceeding maxRows, got %d hashes", len(keyHashes["maxrows_test"]))
	}

	// With maxRows=10, should get all 5 hashes (within limit)
	keyHashes = pendingExec.GetKeyHashes(10)
	if len(keyHashes["maxrows_test"]) != 5 {
		t.Errorf("Expected 5 hashes within limit, got %d", len(keyHashes["maxrows_test"]))
	}

	err = pendingExec.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Log("✓ MutationGuard maxRows limit test passed")
}
