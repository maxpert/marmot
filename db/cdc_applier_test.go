//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
)

// mockSchemaProvider implements CDCSchemaProvider for testing
type mockSchemaProvider struct {
	schemas map[string][]string
}

func (m *mockSchemaProvider) GetPrimaryKeys(tableName string) ([]string, error) {
	pks, ok := m.schemas[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return pks, nil
}

// setupCDCTestDB creates an in-memory SQLite database for CDC testing
func setupCDCTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

// marshalValue is a helper to encode values for CDC
func marshalValue(t *testing.T, v interface{}) []byte {
	data, err := encoding.Marshal(v)
	if err != nil {
		t.Fatalf("Failed to marshal value %v: %v", v, err)
	}
	return data
}

// TestApplyCDCInsert_AllTypes verifies INSERT works with all SQLite types
func TestApplyCDCInsert_AllTypes(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	// Create test table with all types
	_, err := db.Exec(`
		CREATE TABLE test_types (
			id INTEGER PRIMARY KEY,
			name TEXT,
			score REAL,
			data BLOB,
			nullable TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Prepare CDC values with all types
	newValues := map[string][]byte{
		"id":       marshalValue(t, int64(1)),
		"name":     marshalValue(t, "test_user"),
		"score":    marshalValue(t, 95.5),
		"data":     marshalValue(t, []byte{0x01, 0x02, 0x03}),
		"nullable": marshalValue(t, nil),
	}

	// Apply INSERT
	err = ApplyCDCInsert(db, "test_types", newValues)
	if err != nil {
		t.Fatalf("ApplyCDCInsert failed: %v", err)
	}

	// Verify insertion
	var id int64
	var name string
	var score float64
	var data []byte
	var nullable sql.NullString

	err = db.QueryRow("SELECT id, name, score, data, nullable FROM test_types WHERE id = 1").
		Scan(&id, &name, &score, &data, &nullable)
	if err != nil {
		t.Fatalf("Failed to query inserted row: %v", err)
	}

	if id != 1 {
		t.Errorf("Expected id=1, got %d", id)
	}
	if name != "test_user" {
		t.Errorf("Expected name='test_user', got '%s'", name)
	}
	if score != 95.5 {
		t.Errorf("Expected score=95.5, got %f", score)
	}
	if len(data) != 3 || data[0] != 0x01 {
		t.Errorf("Expected data=[1,2,3], got %v", data)
	}
	if nullable.Valid {
		t.Errorf("Expected nullable=NULL, got %v", nullable)
	}
}

// TestApplyCDCInsert_TextTypeAffinity verifies TEXT is stored as TEXT, not BLOB
func TestApplyCDCInsert_TextTypeAffinity(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	// Create table with TEXT primary key
	_, err := db.Exec(`
		CREATE TABLE test_text (
			username TEXT PRIMARY KEY,
			email TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with TEXT values
	newValues := map[string][]byte{
		"username": marshalValue(t, "alice"),
		"email":    marshalValue(t, "alice@example.com"),
	}

	err = ApplyCDCInsert(db, "test_text", newValues)
	if err != nil {
		t.Fatalf("ApplyCDCInsert failed: %v", err)
	}

	// Verify TEXT storage (not BLOB)
	var username, email string
	err = db.QueryRow("SELECT username, email FROM test_text WHERE username = 'alice'").
		Scan(&username, &email)
	if err != nil {
		t.Fatalf("Failed to query with TEXT comparison: %v", err)
	}

	if username != "alice" {
		t.Errorf("Expected username='alice', got '%s'", username)
	}

	// Critical test: INSERT OR REPLACE should work with TEXT PK
	newValues["email"] = marshalValue(t, "alice.updated@example.com")
	err = ApplyCDCInsert(db, "test_text", newValues)
	if err != nil {
		t.Fatalf("Second ApplyCDCInsert failed: %v", err)
	}

	err = db.QueryRow("SELECT email FROM test_text WHERE username = 'alice'").Scan(&email)
	if err != nil {
		t.Fatalf("Failed to query after replace: %v", err)
	}

	if email != "alice.updated@example.com" {
		t.Errorf("Expected email='alice.updated@example.com', got '%s'", email)
	}

	// Verify only one row exists (REPLACE worked)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_text").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row, got %d (REPLACE failed)", count)
	}
}

// TestApplyCDCInsert_DeterministicColumnOrder verifies columns are sorted
func TestApplyCDCInsert_DeterministicColumnOrder(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_order (
			id INTEGER PRIMARY KEY,
			zulu TEXT,
			alpha TEXT,
			bravo TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Provide columns in random order
	newValues := map[string][]byte{
		"zulu":  marshalValue(t, "z"),
		"id":    marshalValue(t, int64(1)),
		"bravo": marshalValue(t, "b"),
		"alpha": marshalValue(t, "a"),
	}

	err = ApplyCDCInsert(db, "test_order", newValues)
	if err != nil {
		t.Fatalf("ApplyCDCInsert failed: %v", err)
	}

	// Verify all columns inserted correctly
	var id int64
	var zulu, alpha, bravo string
	err = db.QueryRow("SELECT id, zulu, alpha, bravo FROM test_order WHERE id = 1").
		Scan(&id, &zulu, &alpha, &bravo)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	if id != 1 || zulu != "z" || alpha != "a" || bravo != "b" {
		t.Errorf("Values not inserted correctly: id=%d, zulu=%s, alpha=%s, bravo=%s",
			id, zulu, alpha, bravo)
	}
}

// TestApplyCDCUpdate_WithPKChange verifies UPDATE when old PK differs from new PK
func TestApplyCDCUpdate_WithPKChange(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_pk_change (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial row
	_, err = db.Exec("INSERT INTO test_pk_change (id, name) VALUES (1, 'old_name')")
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	// Update with PK change: id 1 -> 2
	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_pk_change": {"id"},
		},
	}

	oldValues := map[string][]byte{
		"id":   marshalValue(t, int64(1)),
		"name": marshalValue(t, "old_name"),
	}

	newValues := map[string][]byte{
		"id":   marshalValue(t, int64(2)),
		"name": marshalValue(t, "new_name"),
	}

	err = ApplyCDCUpdate(db, schema, "test_pk_change", oldValues, newValues)
	if err != nil {
		t.Fatalf("ApplyCDCUpdate failed: %v", err)
	}

	// Verify old row was updated (WHERE id=1, SET id=2, name='new_name')
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_pk_change WHERE id = 1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query old PK: %v", err)
	}
	if count != 0 {
		t.Errorf("Old PK row should not exist, found %d rows", count)
	}

	// Verify new row exists
	var name string
	err = db.QueryRow("SELECT name FROM test_pk_change WHERE id = 2").Scan(&name)
	if err != nil {
		t.Fatalf("Failed to query new PK: %v", err)
	}
	if name != "new_name" {
		t.Errorf("Expected name='new_name', got '%s'", name)
	}
}

// TestApplyCDCUpdate_CompositePK verifies UPDATE with multi-column primary key
func TestApplyCDCUpdate_CompositePK(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_composite (
			user_id INTEGER,
			post_id INTEGER,
			content TEXT,
			PRIMARY KEY (user_id, post_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial row
	_, err = db.Exec("INSERT INTO test_composite (user_id, post_id, content) VALUES (1, 100, 'old content')")
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_composite": {"user_id", "post_id"},
		},
	}

	oldValues := map[string][]byte{
		"user_id": marshalValue(t, int64(1)),
		"post_id": marshalValue(t, int64(100)),
		"content": marshalValue(t, "old content"),
	}

	newValues := map[string][]byte{
		"user_id": marshalValue(t, int64(1)),
		"post_id": marshalValue(t, int64(100)),
		"content": marshalValue(t, "new content"),
	}

	err = ApplyCDCUpdate(db, schema, "test_composite", oldValues, newValues)
	if err != nil {
		t.Fatalf("ApplyCDCUpdate failed: %v", err)
	}

	// Verify update
	var content string
	err = db.QueryRow("SELECT content FROM test_composite WHERE user_id = 1 AND post_id = 100").
		Scan(&content)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if content != "new content" {
		t.Errorf("Expected content='new content', got '%s'", content)
	}
}

func TestApplyCDCUpdate_CompositePKWithNullComponent(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_null_composite (
			tenant TEXT,
			local_id INTEGER,
			content TEXT,
			PRIMARY KEY (tenant, local_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO test_null_composite (tenant, local_id, content) VALUES (NULL, 7, 'old')")
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_null_composite": {"tenant", "local_id"},
		},
	}
	oldValues := map[string][]byte{
		"tenant":   marshalValue(t, nil),
		"local_id": marshalValue(t, int64(7)),
		"content":  marshalValue(t, "old"),
	}
	newValues := map[string][]byte{
		"tenant":   marshalValue(t, nil),
		"local_id": marshalValue(t, int64(7)),
		"content":  marshalValue(t, "new"),
	}

	if err := ApplyCDCUpdate(db, schema, "test_null_composite", oldValues, newValues); err != nil {
		t.Fatalf("ApplyCDCUpdate failed: %v", err)
	}

	var content string
	err = db.QueryRow("SELECT content FROM test_null_composite WHERE tenant IS NULL AND local_id = 7").
		Scan(&content)
	if err != nil {
		t.Fatalf("Failed to query updated row: %v", err)
	}
	if content != "new" {
		t.Errorf("Expected content='new', got '%s'", content)
	}
}

// TestApplyCDCDelete_SinglePK verifies DELETE with single column primary key
func TestApplyCDCDelete_SinglePK(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_delete (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert row to delete
	_, err = db.Exec("INSERT INTO test_delete (id, name) VALUES (1, 'to_delete')")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_delete": {"id"},
		},
	}

	oldValues := map[string][]byte{
		"id":   marshalValue(t, int64(1)),
		"name": marshalValue(t, "to_delete"),
	}

	err = ApplyCDCDelete(db, schema, "test_delete", oldValues)
	if err != nil {
		t.Fatalf("ApplyCDCDelete failed: %v", err)
	}

	// Verify deletion
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_delete WHERE id = 1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("Row should be deleted, found %d rows", count)
	}
}

// TestApplyCDCDelete_CompositePK verifies DELETE with composite primary key
func TestApplyCDCDelete_CompositePK(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_delete_composite (
			user_id INTEGER,
			post_id INTEGER,
			content TEXT,
			PRIMARY KEY (user_id, post_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows
	_, err = db.Exec("INSERT INTO test_delete_composite (user_id, post_id, content) VALUES (1, 100, 'content1')")
	if err != nil {
		t.Fatalf("Failed to insert row 1: %v", err)
	}
	_, err = db.Exec("INSERT INTO test_delete_composite (user_id, post_id, content) VALUES (1, 101, 'content2')")
	if err != nil {
		t.Fatalf("Failed to insert row 2: %v", err)
	}

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_delete_composite": {"user_id", "post_id"},
		},
	}

	// Delete only (1, 100)
	oldValues := map[string][]byte{
		"user_id": marshalValue(t, int64(1)),
		"post_id": marshalValue(t, int64(100)),
		"content": marshalValue(t, "content1"),
	}

	err = ApplyCDCDelete(db, schema, "test_delete_composite", oldValues)
	if err != nil {
		t.Fatalf("ApplyCDCDelete failed: %v", err)
	}

	// Verify only (1, 100) was deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_delete_composite").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row remaining, got %d", count)
	}

	// Verify (1, 101) still exists
	var content string
	err = db.QueryRow("SELECT content FROM test_delete_composite WHERE user_id = 1 AND post_id = 101").
		Scan(&content)
	if err != nil {
		t.Fatalf("Failed to query remaining row: %v", err)
	}
	if content != "content2" {
		t.Errorf("Expected content='content2', got '%s'", content)
	}
}

func TestApplyCDCDelete_CompositePKWithNullComponent(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_delete_null_composite (
			tenant TEXT,
			local_id INTEGER,
			content TEXT,
			PRIMARY KEY (tenant, local_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO test_delete_null_composite (tenant, local_id, content) VALUES (NULL, 7, 'delete me')")
	if err != nil {
		t.Fatalf("Failed to insert null-PK row: %v", err)
	}
	_, err = db.Exec("INSERT INTO test_delete_null_composite (tenant, local_id, content) VALUES ('tenant-a', 7, 'keep me')")
	if err != nil {
		t.Fatalf("Failed to insert non-null-PK row: %v", err)
	}

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_delete_null_composite": {"tenant", "local_id"},
		},
	}
	oldValues := map[string][]byte{
		"tenant":   marshalValue(t, nil),
		"local_id": marshalValue(t, int64(7)),
		"content":  marshalValue(t, "delete me"),
	}

	if err := ApplyCDCDelete(db, schema, "test_delete_null_composite", oldValues); err != nil {
		t.Fatalf("ApplyCDCDelete failed: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_delete_null_composite WHERE tenant IS NULL AND local_id = 7").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count deleted row: %v", err)
	}
	if count != 0 {
		t.Errorf("Null-PK row should be deleted, found %d rows", count)
	}
	err = db.QueryRow("SELECT COUNT(*) FROM test_delete_null_composite WHERE tenant = 'tenant-a' AND local_id = 7").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count retained row: %v", err)
	}
	if count != 1 {
		t.Errorf("Non-null row should remain, found %d rows", count)
	}
}

// TestUnmarshalCDCValue_BytesToString verifies []byte converts to string
// TestUnmarshalCDCValue_PreservesEncodedType verifies unmarshalCDCValue uses
// STRICT msgpack decoding: it returns exactly the Go type that was encoded,
// with no byte-slice-to-string coercion. A []byte value decodes back as
// []byte (msgpack Bin), preserving BLOB storage class on apply.
//
// This replaces the old "bytes always convert to string" contract: that
// conversion was previously done unconditionally here (and via
// encoding.Unmarshal's loose interface decoding before that), which silently
// corrupted BLOB columns - they were captured as raw []byte too, with no way
// to tell them apart from a TEXT column's []byte at this point. The decision
// of whether a []byte should round-trip as string now happens earlier, at
// capture time, using the column's declared TEXT affinity
// (encodeValuesWithSchema in preupdate_hook.go: TEXT-affinity columns are
// converted to string BEFORE encoding, so they arrive here already encoded
// as msgpack Str and decode as string; see TestEncodeValuesWithSchema_BlobVsText
// and the TestBlobFidelity_* tests in cdc_blob_fidelity_test.go for the
// full capture->apply round trip this enables).
func TestUnmarshalCDCValue_PreservesEncodedType(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name:     "String value",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "Integer value",
			input:    int64(42),
			expected: int64(42),
		},
		{
			name:     "Float value",
			input:    3.14,
			expected: 3.14,
		},
		{
			name:     "Nil value",
			input:    nil,
			expected: nil,
		},
		{
			name:     "Byte slice (must stay []byte, not be coerced to string)",
			input:    []byte("blob_data"),
			expected: []byte("blob_data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := marshalValue(t, tt.input)
			result, err := unmarshalCDCValue(data)
			if err != nil {
				t.Fatalf("unmarshalCDCValue failed: %v", err)
			}

			if expectedBytes, ok := tt.expected.([]byte); ok {
				resultBytes, ok := result.([]byte)
				if !ok {
					t.Fatalf("Expected []byte, got %T", result)
				}
				if string(resultBytes) != string(expectedBytes) {
					t.Errorf("Expected %v, got %v", expectedBytes, resultBytes)
				}
			} else if result != tt.expected {
				t.Errorf("Expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

// TestApplyCDC_EmptyValues verifies error handling for empty maps
func TestApplyCDC_EmptyValues(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
		CREATE TABLE test_empty (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_empty": {"id"},
		},
	}

	// Test INSERT with empty values
	err = ApplyCDCInsert(db, "test_empty", map[string][]byte{})
	if err == nil {
		t.Error("Expected error for empty INSERT values, got nil")
	}

	// Test UPDATE with empty values
	err = ApplyCDCUpdate(db, schema, "test_empty", map[string][]byte{}, map[string][]byte{})
	if err == nil {
		t.Error("Expected error for empty UPDATE values, got nil")
	}

	// Test DELETE with empty values
	err = ApplyCDCDelete(db, schema, "test_empty", map[string][]byte{})
	if err == nil {
		t.Error("Expected error for empty DELETE values, got nil")
	}
}

// TestApplyCDCUpdate_NoRowsMatched verifies an UPDATE whose WHERE clause
// matches no row is NOT treated as an error - it must return nil so a
// legitimate no-op (e.g. the row was already removed by an FK ON DELETE
// CASCADE that ran ahead of this CDC entry) doesn't abort replication. The
// row-affected count is only surfaced via a Debug log for diagnosability, not
// as a failure.
func TestApplyCDCUpdate_NoRowsMatched(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`CREATE TABLE test_noop (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	// Table is intentionally left empty - no row with id=999 exists.

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_noop": {"id"},
		},
	}

	oldValues := map[string][]byte{"id": marshalValue(t, int64(999))}
	newValues := map[string][]byte{"id": marshalValue(t, int64(999)), "name": marshalValue(t, "ghost")}

	err = ApplyCDCUpdate(db, schema, "test_noop", oldValues, newValues)
	if err != nil {
		t.Fatalf("ApplyCDCUpdate on a non-matching row must return nil, got: %v", err)
	}
}

// TestApplyCDCDelete_NoRowsMatched verifies a DELETE whose WHERE clause
// matches no row is likewise not an error (see TestApplyCDCUpdate_NoRowsMatched).
func TestApplyCDCDelete_NoRowsMatched(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`CREATE TABLE test_noop_del (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	schema := &mockSchemaProvider{
		schemas: map[string][]string{
			"test_noop_del": {"id"},
		},
	}

	oldValues := map[string][]byte{"id": marshalValue(t, int64(999))}

	err = ApplyCDCDelete(db, schema, "test_noop_del", oldValues)
	if err != nil {
		t.Fatalf("ApplyCDCDelete on a non-matching row must return nil, got: %v", err)
	}
}

// TestLogZeroRowsAffected_OnlyLogsOnZero verifies the helper distinguishes a
// real zero-rows case from a normal affected-rows result and from a driver
// that can't report RowsAffected, without ever panicking or altering control
// flow (it has no return value to affect - this locks in that contract).
func TestLogZeroRowsAffected_OnlyLogsOnZero(t *testing.T) {
	db, cleanup := setupCDCTestDB(t)
	defer cleanup()

	_, err := db.Exec(`CREATE TABLE test_log (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	res, err := db.Exec(`INSERT INTO test_log (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	// sql.Result from a normal driver Exec: must not panic regardless of count.
	logZeroRowsAffected(res, "TestOp", "test_log")

	zeroRes, err := db.Exec(`DELETE FROM test_log WHERE id = 999`)
	if err != nil {
		t.Fatalf("Failed to exec no-op delete: %v", err)
	}
	logZeroRowsAffected(zeroRes, "TestOp", "test_log")
}
