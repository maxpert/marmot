package replica

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"google.golang.org/grpc"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

func init() {
	// Initialize query pipeline for tests (nil ID generator - read-only)
	if err := protocol.InitializePipeline(10000, nil); err != nil {
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "", // No database selected
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
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
		ConnID:               1,
		CurrentDatabase:      "testdb",
		TranspilationEnabled: true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.HandleQuery(session, "INSERT INTO users (name) VALUES ('test')", nil)
	}
}

// TestForwardedInsertIdNeverResetBySentinelZero pins the rule that a forwarded
// insert id of 0 must not overwrite the session's LAST_INSERT_ID().
//
// A leader reports 0 whenever a statement generated no AUTO_INCREMENT value: an
// INSERT that inserted nothing, an upsert that updated an existing row, a table
// with no auto-increment column. MySQL leaves LAST_INSERT_ID() unchanged there,
// so a replica that stores the 0 wipes a value the client is entitled to keep.
//
// Mutation: make ConnectionSession.RecordInsertId store unconditionally, or
// restore the direct session.LastInsertId.Store(resp.LastInsertId) at either
// call site in applyForwardedSessionState. Every assertion below fires.
func TestForwardedInsertIdNeverResetBySentinelZero(t *testing.T) {
	h, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{}

	// (b) a forwarded non-zero id becomes the session's value.
	h.applyForwardedSessionState(session, &marmotgrpc.ForwardQueryResponse{LastInsertId: 41})
	if got := session.LastInsertId.Load(); got != 41 {
		t.Fatalf("after a forwarded id of 41, session LAST_INSERT_ID() = %d, want 41", got)
	}

	// (b) a later non-zero id replaces it.
	h.applyForwardedSessionState(session, &marmotgrpc.ForwardQueryResponse{LastInsertId: 42})
	if got := session.LastInsertId.Load(); got != 42 {
		t.Fatalf("after a forwarded id of 42, session LAST_INSERT_ID() = %d, want 42", got)
	}

	// (a) a forwarded 0 leaves it alone, checked repeatedly rather than once:
	// this is a safety property, so it must hold past its first success.
	const noOpForwards = 3
	observed := 0
	for i := 0; i < noOpForwards; i++ {
		h.applyForwardedSessionState(session, &marmotgrpc.ForwardQueryResponse{LastInsertId: 0})
		if got := session.LastInsertId.Load(); got != 42 {
			t.Fatalf("forward %d of 0 changed session LAST_INSERT_ID() to %d, want it to stay 42", i, got)
		}
		observed++
	}
	// Guards the loop against silently degrading to a single sample.
	// Mutation: drop an iteration.
	if observed != noOpForwards {
		t.Fatalf("the zero-forward path ran %d times, want %d", observed, noOpForwards)
	}

	// The unrelated state the same helper carries must still be applied, or the
	// guard would have been bought by skipping the whole response.
	// Mutation: delete the ForwardedTxnActive assignment.
	h.applyForwardedSessionState(session, &marmotgrpc.ForwardQueryResponse{LastInsertId: 0, InTransaction: true})
	if !session.ForwardedTxnActive {
		t.Fatal("a forwarded response with InTransaction=true did not set ForwardedTxnActive")
	}
}

// TestSystemQueryReturnsRetainedInsertId is (c): the value a client actually
// reads back. handleSystemQuery is the only path serving LAST_INSERT_ID() on a
// replica, so a guard that held in the session but not here would be invisible.
//
// Mutation: make RecordInsertId store unconditionally; the retained 7 becomes 0.
func TestSystemQueryReturnsRetainedInsertId(t *testing.T) {
	h, _, cleanup := testHandler(t)
	defer cleanup()

	session := &protocol.ConnectionSession{}
	h.applyForwardedSessionState(session, &marmotgrpc.ForwardQueryResponse{LastInsertId: 7})
	h.applyForwardedSessionState(session, &marmotgrpc.ForwardQueryResponse{LastInsertId: 0})

	stmt := protocol.ParseStatement("SELECT LAST_INSERT_ID()")
	rs, err := h.handleSystemQuery(session, stmt)
	if err != nil {
		t.Fatalf("handleSystemQuery: %v", err)
	}
	if len(rs.Rows) != 1 || len(rs.Rows[0]) != 1 {
		t.Fatalf("SELECT LAST_INSERT_ID() returned %d rows, want one row of one column: %#v", len(rs.Rows), rs.Rows)
	}
	if got := fmt.Sprintf("%v", rs.Rows[0][0]); got != "7" {
		t.Fatalf("SELECT LAST_INSERT_ID() returned %s after a forwarded 0, want the retained 7", got)
	}
}

// TestForwardedErrorCarriesTheLeadersCode pins the whole replica half of the
// forwarded-error path: the leader's MySQL error code reaches the client, a
// leader that sends no code still yields ER_UNKNOWN_ERROR, and the message is
// never a formatted "ERROR <code> (<state>): ..." string.
//
// The formatted form this replaced had two defects. Its code was re-derived
// downstream by protocol.ConvertToMySQLError's message matching, so a leader
// message that merely contained the words "syntax error" arrived as 1064; and
// the formatted prefix was duplicated into the ERR packet's message.
func TestForwardedErrorCarriesTheLeadersCode(t *testing.T) {
	cases := []struct {
		name         string
		code         uint32
		sqlState     string
		message      string
		wantCode     uint16
		wantSQLState string
	}{
		{
			// The deliverable: D1's rule rejection keeps its own code across
			// the wire instead of being flattened.
			// Mutation: ignore resp.ErrorCode and always report 1105.
			name: "a rule rejection keeps 1235", code: 1235, sqlState: protocol.SQLStateSyntax,
			message:  "This version of MySQL doesn't yet support 'INSERT ... SELECT'",
			wantCode: 1235, wantSQLState: protocol.SQLStateSyntax,
		},
		{
			// The SQLSTATE is not on the wire; it must be derived, and derived
			// the same way the coordinator's own ERR path derives it.
			// Mutation: return SQLStateGeneral for every code.
			name: "a duplicate key keeps 1062 and its integrity SQLSTATE", code: 1062, sqlState: protocol.SQLStateIntegrity,
			message:  "Duplicate entry '7' for key 'PRIMARY'",
			wantCode: 1062, wantSQLState: protocol.SQLStateIntegrity,
		},
		{
			// Rolling upgrade: an older leader sends no code at all, and
			// proto3 delivers that as 0. Behaviour must not change for it.
			// Mutation: drop the `code == 0` branch; uint16(0) is reported.
			name: "an old leader sending no code still yields 1105", code: 0, sqlState: "",
			message:  "table is read only",
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
		},
		{
			// The leader's wording is data, not a code: it must not be
			// re-classified by the words inside it.
			// Mutation: restore fmt.Errorf("ERROR 1105 (HY000): %s", message).
			name: "a message containing the words syntax error is not re-classified", code: 0, sqlState: "",
			message:  `near "x": syntax error`,
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
		},
		{
			// Defensive: a current leader always fills both fields, so this
			// shape can only come from a hand-built response. It must not put
			// an empty SQLSTATE in the ERR packet.
			// Mutation: drop the `sqlState == ""` fallback; the client gets "".
			name: "a non-zero code with no SQLSTATE falls back to HY000", code: 1205, sqlState: "",
			message:  "Lock wait timeout exceeded",
			wantCode: protocol.ErrCodeLockTimeout, wantSQLState: protocol.SQLStateGeneral,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mysqlErr := protocol.ConvertToMySQLError(forwardedError(tc.code, tc.sqlState, tc.message))
			if mysqlErr.Code != tc.wantCode {
				t.Errorf("code = %d, want %d", mysqlErr.Code, tc.wantCode)
			}
			if mysqlErr.SQLState != tc.wantSQLState {
				t.Errorf("SQLSTATE = %q, want %q", mysqlErr.SQLState, tc.wantSQLState)
			}
			// The client must see the leader's message, not one with a code
			// spelled into it.
			if mysqlErr.Message != tc.message {
				t.Errorf("message = %q, want the leader's message %q verbatim", mysqlErr.Message, tc.message)
			}
		})
	}
}

// leaderErrors are the shapes a leader's ConvertToMySQLError produces, with the
// (code, SQLSTATE) pair a client connected directly to that leader would see.
// The replica must reproduce the pair exactly; anything else means a client
// gets a different answer from a replica than from a coordinator for the same
// failure.
//
// The generic-constraint row is the one that matters: the leader pairs code
// 1105 with TWO different SQLSTATEs (HY000 for an unclassified error, 23000 for
// an unclassified *constraint* failure), so a replica that rebuilds the SQLSTATE
// from the code alone has to guess, and guesses HY000.
func leaderErrors() []struct {
	name  string
	err   error
	state string
} {
	return []struct {
		name  string
		err   error
		state string
	}{
		{"generic constraint failure", sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintTrigger}, protocol.SQLStateIntegrity},
		{"unique violation", sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintUnique}, protocol.SQLStateIntegrity},
		{"deadlock", protocol.ErrDeadlock(), protocol.SQLStateDeadlock},
		{"server shutdown", protocol.ErrServerShutdown(), "08S01"},
		{"read only", protocol.ErrReadOnly(), protocol.SQLStateGeneral},
		{"lock timeout", protocol.ErrLockWaitTimeout(), protocol.SQLStateGeneral},
	}
}

// TestForwardedErrorRoundTripsTheLeadersSQLState pins that the (code, SQLSTATE)
// pair a replica client sees is the pair the leader itself produced.
//
// The two halves of the round trip are pinned separately because they live in
// different packages: TestForwardFailureCarriesTheMySQLCode (package grpc) pins
// that forwardFailure copies protocol.ConvertToMySQLError's output onto the
// wire, and this test pins that forwardedError reproduces that same output. The
// mapper is the shared seam, so neither test re-implements the other's half.
func TestForwardedErrorRoundTripsTheLeadersSQLState(t *testing.T) {
	for _, tc := range leaderErrors() {
		t.Run(tc.name, func(t *testing.T) {
			// What the leader puts on the wire.
			leader := protocol.ConvertToMySQLError(tc.err)
			if leader.SQLState != tc.state {
				t.Fatalf("fixture is wrong: the leader maps this to SQLSTATE %q, not %q", leader.SQLState, tc.state)
			}

			// What the replica rebuilds from it.
			replicaErr := protocol.ConvertToMySQLError(forwardedError(uint32(leader.Code), leader.SQLState, leader.Message))

			if replicaErr.Code != leader.Code {
				t.Errorf("code = %d, want the leader's %d", replicaErr.Code, leader.Code)
			}
			// Mutation: derive the SQLSTATE from the code instead of carrying
			// it. The generic-constraint row then reports HY000 for 23000.
			if replicaErr.SQLState != leader.SQLState {
				t.Errorf("SQLSTATE = %q, want the leader's %q", replicaErr.SQLState, leader.SQLState)
			}
		})
	}
}

// fakeLeaderClient stands in for the gRPC client a replica uses to reach its
// leader. It embeds the interface so only the two methods the forwarding sites
// actually call need bodies; everything else panics if it is ever reached,
// which is what we want from a stand-in.
type fakeLeaderClient struct {
	marmotgrpc.MarmotServiceClient
	err error
}

func (f fakeLeaderClient) ForwardQuery(context.Context, *marmotgrpc.ForwardQueryRequest, ...grpc.CallOption) (*marmotgrpc.ForwardQueryResponse, error) {
	return nil, f.err
}

func (f fakeLeaderClient) ForwardLoadData(context.Context, *marmotgrpc.ForwardLoadDataRequest, ...grpc.CallOption) (*marmotgrpc.ForwardQueryResponse, error) {
	return nil, f.err
}

// capturingLeaderClient accepts every forwarded query and records it.
type capturingLeaderClient struct {
	marmotgrpc.MarmotServiceClient
	got *[]*marmotgrpc.ForwardQueryRequest
}

func (c capturingLeaderClient) ForwardQuery(_ context.Context, req *marmotgrpc.ForwardQueryRequest, _ ...grpc.CallOption) (*marmotgrpc.ForwardQueryResponse, error) {
	*c.got = append(*c.got, req)
	return &marmotgrpc.ForwardQueryResponse{Success: true, RowsAffected: 1}, nil
}

// TestForwardedMutationCarriesTheClientSQL pins R3c-13: a replica forwards the
// client's own SQL, so a qualified write keeps its database through a
// replica, and the leader transpiles it once. Forwarding the transpiled text
// stripped the qualifier, so the leader applied the write in the session's
// database: 1146 there, or a same-named table silently written.
//
// Mutation: forward stmt.SQL again. "the forwarded SQL lost the client's
// qualifier" fires.
func TestForwardedMutationCarriesTheClientSQL(t *testing.T) {
	var got []*marmotgrpc.ForwardQueryRequest
	h := handlerWithLeaderClient(t, capturingLeaderClient{got: &got})
	session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "testdb", TranspilationEnabled: true}

	for _, tc := range []struct {
		sql    string
		params []interface{}
	}{
		{"INSERT INTO other.users (name) VALUES ('x')", nil},
		{"INSERT INTO `other`.`users` (name) VALUES (?)", []interface{}{"y"}},
		{"UPDATE other.users SET name = ? WHERE id = ?", []interface{}{"z", int64(1)}},
	} {
		got = nil
		_, err := h.HandleQuery(session, tc.sql, tc.params)
		if err != nil {
			t.Fatalf("%q: %v", tc.sql, err)
		}
		if len(got) != 1 {
			t.Fatalf("%q: forwarded %d requests, want 1", tc.sql, len(got))
		}
		if got[0].Sql != tc.sql {
			t.Errorf("the forwarded SQL lost the client's qualifier: forwarded %q, client sent %q", got[0].Sql, tc.sql)
		}
		if got[0].Database != "testdb" {
			t.Errorf("%q: forwarded with database %q, want the session's", tc.sql, got[0].Database)
		}
	}
}

// handlerWithLeaderClient builds a forwarding-enabled handler whose leader link
// is the given client. A nil client reproduces "not connected"; a client that
// returns an error reproduces "lost connection". Neither needs a live leader.
func handlerWithLeaderClient(t *testing.T, client marmotgrpc.MarmotServiceClient) *ReadOnlyHandler {
	t.Helper()
	h, _, cleanup := testHandler(t)
	t.Cleanup(cleanup)
	h.forwardWrites = true
	h.replica = &Replica{streamClient: &StreamClient{client: client}}
	return h
}

// TestReplicaEmittedErrorCodesAreServerSide drives every site where this replica
// raises an error of its own and asserts the code and SQLSTATE a client receives.
//
// The rule is that an ERR packet carries server-side codes only. 2003
// (CR_CONN_HOST_ERROR) and 2013 (CR_SERVER_LOST) are client-library codes that no
// MySQL server emits; a driver seeing one may tear the session down or reconnect
// rather than surface a statement error, and they fire exactly when the leader
// link is down, which is when a replica most wants the client to keep its
// session. Master reported 1105/HY000 for both, with the number as decoration
// inside the message, so 1105 is also the behaviour-preserving choice.
//
// 1046/3D000 stays: ER_NO_DB_ERROR is MySQL's own server code for that condition.
func TestReplicaEmittedErrorCodesAreServerSide(t *testing.T) {
	const noDatabase = "" // an unset Database is what reaches executeLocalRead

	session := func() *protocol.ConnectionSession {
		return &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "testdb"}
	}
	insert := protocol.Statement{SQL: "INSERT INTO users (name) VALUES ('x')", Database: "testdb"}
	begin := protocol.Statement{SQL: "BEGIN", Type: protocol.StatementBegin, Database: "testdb"}

	cases := []struct {
		name         string
		client       marmotgrpc.MarmotServiceClient
		call         func(h *ReadOnlyHandler, s *protocol.ConnectionSession) error
		wantCode     uint16
		wantSQLState string
		wantMessage  string
	}{
		{
			name: "HandleLoadData, not connected", client: nil,
			call: func(h *ReadOnlyHandler, s *protocol.ConnectionSession) error {
				_, err := h.HandleLoadData(s, "LOAD DATA LOCAL INFILE 'x' INTO TABLE users", []byte("1\n"))
				return err
			},
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
			wantMessage: "Not connected to leader",
		},
		{
			name: "forwardMutation, not connected", client: nil,
			call: func(h *ReadOnlyHandler, s *protocol.ConnectionSession) error {
				_, err := h.forwardMutation(s, insert.SQL, insert, nil)
				return err
			},
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
			wantMessage: "Not connected to leader",
		},
		{
			name: "forwardTxnControl, not connected", client: nil,
			call: func(h *ReadOnlyHandler, s *protocol.ConnectionSession) error {
				_, err := h.forwardTxnControl(s, begin)
				return err
			},
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
			wantMessage: "Not connected to leader",
		},
		{
			name: "HandleLoadData, lost connection", client: fakeLeaderClient{err: errors.New("boom")},
			call: func(h *ReadOnlyHandler, s *protocol.ConnectionSession) error {
				_, err := h.HandleLoadData(s, "LOAD DATA LOCAL INFILE 'x' INTO TABLE users", []byte("1\n"))
				return err
			},
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
			wantMessage: "Lost connection to leader",
		},
		{
			name: "forwardMutation, lost connection", client: fakeLeaderClient{err: errors.New("boom")},
			call: func(h *ReadOnlyHandler, s *protocol.ConnectionSession) error {
				_, err := h.forwardMutation(s, insert.SQL, insert, nil)
				return err
			},
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
			wantMessage: "Lost connection to leader",
		},
		{
			name: "forwardTxnControl, lost connection", client: fakeLeaderClient{err: errors.New("boom")},
			call: func(h *ReadOnlyHandler, s *protocol.ConnectionSession) error {
				_, err := h.forwardTxnControl(s, begin)
				return err
			},
			wantCode: protocol.ErrCodeUnknown, wantSQLState: protocol.SQLStateGeneral,
			wantMessage: "Lost connection to leader",
		},
		{
			// Not a leader-link failure, and deliberately not 1105: MySQL has
			// its own server code for this one.
			// Mutation: replace ErrCodeNoDB with ErrCodeUnknown.
			name: "executeLocalRead, no database selected", client: nil,
			call: func(h *ReadOnlyHandler, _ *protocol.ConnectionSession) error {
				_, err := h.executeLocalRead(protocol.Statement{Database: noDatabase}, nil)
				return err
			},
			wantCode: protocol.ErrCodeNoDB, wantSQLState: protocol.SQLStateNoDB,
			wantMessage: "No database selected",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := handlerWithLeaderClient(t, tc.client)

			err := tc.call(h, session())
			if err == nil {
				t.Fatalf("%s returned no error; the fixture did not reach the failure site", tc.name)
			}

			mysqlErr := protocol.ConvertToMySQLError(err)
			// Mutation: put a client-side code back at any of these sites -
			// 2003 at the not-connected sites, 2013 at the lost-connection
			// ones - and the matching row fires here.
			if mysqlErr.Code != tc.wantCode {
				t.Errorf("code = %d, want %d", mysqlErr.Code, tc.wantCode)
			}
			if mysqlErr.SQLState != tc.wantSQLState {
				t.Errorf("SQLSTATE = %q, want %q", mysqlErr.SQLState, tc.wantSQLState)
			}
			// Pins that the row reached the site it names rather than some
			// other failure that happens to carry the same code.
			// Mutation: swap two rows' call functions.
			if !strings.Contains(mysqlErr.Message, tc.wantMessage) {
				t.Errorf("message = %q, want it to contain %q", mysqlErr.Message, tc.wantMessage)
			}
		})
	}
}
