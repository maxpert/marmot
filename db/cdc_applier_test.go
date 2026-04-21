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

// TestUnmarshalCDCValue_BytesToString verifies []byte converts to string
func TestUnmarshalCDCValue_BytesToString(t *testing.T) {
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
			name:     "Byte slice (should convert to string)",
			input:    []byte("text_data"),
			expected: "text_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := marshalValue(t, tt.input)
			result, err := unmarshalCDCValue(data)
			if err != nil {
				t.Fatalf("unmarshalCDCValue failed: %v", err)
			}

			// Special handling for []byte -> string conversion
			if tt.name == "Byte slice (should convert to string)" {
				if str, ok := result.(string); !ok {
					t.Errorf("Expected string, got %T", result)
				} else if str != tt.expected {
					t.Errorf("Expected '%v', got '%v'", tt.expected, result)
				}
			} else {
				if result != tt.expected {
					t.Errorf("Expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
				}
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
