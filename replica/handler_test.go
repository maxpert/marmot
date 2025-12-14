package replica

import (
	"os"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

func init() {
	// Initialize query pipeline for tests (nil ID generator - read-only)
	if err := protocol.InitializePipeline(10000, 8, nil); err != nil {
		panic("Failed to initialize pipeline: " + err.Error())
	}
}

// testHandler creates a handler with test database for testing
func testHandler(t *testing.T) (*ReadOnlyHandler, string, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "marmot-handler-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create database manager: %v", err)
	}

	// Create a test database
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create test database: %v", err)
	}
	testDB, err := dbMgr.GetDatabase("testdb")
	if err != nil {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to get test database: %v", err)
	}

	// Create test table
	_, err = testDB.GetDB().Exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT
	)`)
	if err != nil {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = testDB.GetDB().Exec(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to insert test data: %v", err)
	}

	handler := NewReadOnlyHandler(dbMgr, clock, nil)

	cleanup := func() {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
	}

	return handler, tmpDir, cleanup
}

// TestHandler_RejectInsert tests that INSERT queries are rejected
func TestHandler_RejectInsert(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	_, err := handler.HandleQuery(session, "INSERT INTO users (name, email) VALUES ('Bob', 'bob@test.com')", nil)
	if err == nil {
		t.Fatal("Expected error for INSERT on read-only replica, got nil")
	}

	mysqlErr, ok := err.(*protocol.MySQLError)
	if !ok {
		t.Fatalf("Expected MySQLError, got %T", err)
	}

	if mysqlErr.Code != 1290 {
		t.Errorf("Expected MySQL error code 1290, got %d", mysqlErr.Code)
	}
}

// TestHandler_RejectUpdate tests that UPDATE queries are rejected
func TestHandler_RejectUpdate(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	_, err := handler.HandleQuery(session, "UPDATE users SET name = 'Updated' WHERE id = 1", nil)
	if err == nil {
		t.Fatal("Expected error for UPDATE on read-only replica, got nil")
	}

	mysqlErr, ok := err.(*protocol.MySQLError)
	if !ok {
		t.Fatalf("Expected MySQLError, got %T", err)
	}

	if mysqlErr.Code != 1290 {
		t.Errorf("Expected MySQL error code 1290, got %d", mysqlErr.Code)
	}
}

// TestHandler_RejectDelete tests that DELETE queries are rejected
func TestHandler_RejectDelete(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	_, err := handler.HandleQuery(session, "DELETE FROM users WHERE id = 1", nil)
	if err == nil {
		t.Fatal("Expected error for DELETE on read-only replica, got nil")
	}

	mysqlErr, ok := err.(*protocol.MySQLError)
	if !ok {
		t.Fatalf("Expected MySQLError, got %T", err)
	}

	if mysqlErr.Code != 1290 {
		t.Errorf("Expected MySQL error code 1290, got %d", mysqlErr.Code)
	}
}

// TestHandler_RejectDDL tests that DDL statements are rejected
func TestHandler_RejectDDL(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	ddlStatements := []string{
		"CREATE TABLE test (id INT)",
		"ALTER TABLE users ADD COLUMN age INT",
		"DROP TABLE users",
		"TRUNCATE TABLE users",
	}

	for _, stmt := range ddlStatements {
		_, err := handler.HandleQuery(session, stmt, nil)
		if err == nil {
			t.Errorf("Expected error for '%s' on read-only replica, got nil", stmt)
			continue
		}

		mysqlErr, ok := err.(*protocol.MySQLError)
		if !ok {
			t.Errorf("Expected MySQLError for '%s', got %T", stmt, err)
			continue
		}

		if mysqlErr.Code != 1290 {
			t.Errorf("Expected MySQL error code 1290 for '%s', got %d", stmt, mysqlErr.Code)
		}
	}
}

// TestHandler_AllowSelect tests that SELECT queries are allowed
func TestHandler_AllowSelect(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	result, err := handler.HandleQuery(session, "SELECT * FROM users WHERE id = 1", nil)
	if err != nil {
		t.Fatalf("Expected SELECT to succeed, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result set, got nil")
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestHandler_AllowSelectCount tests that SELECT COUNT queries are allowed
func TestHandler_AllowSelectCount(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	result, err := handler.HandleQuery(session, "SELECT COUNT(*) FROM users", nil)
	if err != nil {
		t.Fatalf("Expected SELECT COUNT to succeed, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result set, got nil")
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestHandler_AllowBegin tests that BEGIN is allowed (for read-only transactions)
func TestHandler_AllowBegin(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	_, err := handler.HandleQuery(session, "BEGIN", nil)
	if err != nil {
		t.Fatalf("Expected BEGIN to succeed on read-only replica, got error: %v", err)
	}
}

// TestHandler_AllowCommit tests that COMMIT is allowed (no-op)
func TestHandler_AllowCommit(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	_, err := handler.HandleQuery(session, "COMMIT", nil)
	if err != nil {
		t.Fatalf("Expected COMMIT to succeed on read-only replica, got error: %v", err)
	}
}

// TestHandler_AllowRollback tests that ROLLBACK is allowed (no-op)
func TestHandler_AllowRollback(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	_, err := handler.HandleQuery(session, "ROLLBACK", nil)
	if err != nil {
		t.Fatalf("Expected ROLLBACK to succeed on read-only replica, got error: %v", err)
	}
}

// TestHandler_ShowDatabases tests SHOW DATABASES
func TestHandler_ShowDatabases(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID: 1,
	}

	result, err := handler.HandleQuery(session, "SHOW DATABASES", nil)
	if err != nil {
		t.Fatalf("Expected SHOW DATABASES to succeed, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result set, got nil")
	}

	// Should have at least testdb and system db
	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 database, got %d", len(result.Rows))
	}
}

// TestHandler_ShowTables tests SHOW TABLES
func TestHandler_ShowTables(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	result, err := handler.HandleQuery(session, "SHOW TABLES", nil)
	if err != nil {
		t.Fatalf("Expected SHOW TABLES to succeed, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result set, got nil")
	}

	// Should have at least users table
	found := false
	for _, row := range result.Rows {
		if len(row) > 0 && row[0] == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find 'users' table in SHOW TABLES result")
	}
}

// TestHandler_UseDatabase tests USE database
func TestHandler_UseDatabase(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "",
	}

	_, err := handler.HandleQuery(session, "USE testdb", nil)
	if err != nil {
		t.Fatalf("Expected USE testdb to succeed, got error: %v", err)
	}

	if session.CurrentDatabase != "testdb" {
		t.Errorf("Expected current database to be 'testdb', got '%s'", session.CurrentDatabase)
	}
}

// TestHandler_UseDatabase_NonExistent tests USE with non-existent database
func TestHandler_UseDatabase_NonExistent(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "",
	}

	_, err := handler.HandleQuery(session, "USE nonexistent", nil)
	if err == nil {
		t.Fatal("Expected error for USE with non-existent database, got nil")
	}
}

// TestHandler_SystemVariables_ReadOnly tests read-only system variables
func TestHandler_SystemVariables_ReadOnly(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	tests := []struct {
		query    string
		expected interface{}
	}{
		{"SELECT @@READ_ONLY", 1},
		{"SELECT @@GLOBAL.READ_ONLY", 1},
		{"SELECT @@TX_READ_ONLY", 1},
		{"SELECT @@INNODB_READ_ONLY", 1},
	}

	for _, tc := range tests {
		result, err := handler.HandleQuery(session, tc.query, nil)
		if err != nil {
			t.Errorf("Expected '%s' to succeed, got error: %v", tc.query, err)
			continue
		}

		if result == nil || len(result.Rows) == 0 {
			t.Errorf("Expected result for '%s', got nil or empty", tc.query)
			continue
		}

		if result.Rows[0][0] != tc.expected {
			t.Errorf("Expected %v for '%s', got %v", tc.expected, tc.query, result.Rows[0][0])
		}
	}
}

// TestHandler_SystemVariables_Version tests version system variables
func TestHandler_SystemVariables_Version(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	result, err := handler.HandleQuery(session, "SELECT @@VERSION", nil)
	if err != nil {
		t.Fatalf("Expected SELECT @@VERSION to succeed, got error: %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Fatal("Expected result set with version")
	}

	version, ok := result.Rows[0][0].(string)
	if !ok || version == "" {
		t.Error("Expected non-empty version string")
	}
}

// TestHandler_ShowColumns tests SHOW COLUMNS
func TestHandler_ShowColumns(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	result, err := handler.HandleQuery(session, "SHOW COLUMNS FROM users", nil)
	if err != nil {
		t.Fatalf("Expected SHOW COLUMNS to succeed, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result set, got nil")
	}

	// Should have 3 columns: id, name, email
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Rows))
	}
}

// TestHandler_Set_NoOp tests that SET is a no-op
func TestHandler_Set_NoOp(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	_, err := handler.HandleQuery(session, "SET autocommit = 1", nil)
	if err != nil {
		t.Fatalf("Expected SET to be a no-op, got error: %v", err)
	}
}

// TestHandler_NoDatabaseSelected tests error when no database is selected
func TestHandler_NoDatabaseSelected(t *testing.T) {
	handler, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "", // No database selected
	}

	_, err := handler.HandleQuery(session, "SELECT * FROM users", nil)
	if err == nil {
		t.Fatal("Expected error when no database selected, got nil")
	}
}

// TestNewReadOnlyHandler tests handler creation
func TestNewReadOnlyHandler(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot-handler-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create database manager: %v", err)
	}
	defer dbMgr.Close()

	handler := NewReadOnlyHandler(dbMgr, clock, nil)
	if handler == nil {
		t.Fatal("Expected handler to be created, got nil")
	}

	if handler.dbManager != dbMgr {
		t.Error("Handler dbManager not set correctly")
	}

	if handler.clock != clock {
		t.Error("Handler clock not set correctly")
	}
}

// Benchmark for SELECT query handling
func BenchmarkHandler_Select(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "marmot-handler-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		b.Fatalf("Failed to create database manager: %v", err)
	}
	defer dbMgr.Close()

	dbMgr.CreateDatabase("testdb")
	testDB, _ := dbMgr.GetDatabase("testdb")
	testDB.GetDB().Exec(`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)`)
	testDB.GetDB().Exec(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)

	handler := NewReadOnlyHandler(dbMgr, clock, nil)
	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.HandleQuery(session, "SELECT * FROM users WHERE id = 1", nil)
	}
}

// Benchmark for mutation rejection
func BenchmarkHandler_RejectMutation(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "marmot-handler-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		b.Fatalf("Failed to create database manager: %v", err)
	}
	defer dbMgr.Close()

	dbMgr.CreateDatabase("testdb")
	handler := NewReadOnlyHandler(dbMgr, clock, nil)
	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "testdb",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.HandleQuery(session, "INSERT INTO users (name) VALUES ('test')", nil)
	}
}
