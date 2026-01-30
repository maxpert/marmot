package transform

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestExtractLiterals_Insert(t *testing.T) {
	sql := "INSERT INTO users (name, age, rating) VALUES ('alice', 25, 4.5)"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params == nil {
		t.Fatal("Expected params, got nil")
	}

	if len(params) != 3 {
		t.Fatalf("Expected 3 params, got %d", len(params))
	}

	// Verify param types
	if name, ok := params[0].(string); !ok || name != "alice" {
		t.Errorf("Expected params[0] to be string('alice'), got %T: %v", params[0], params[0])
	}

	if age, ok := params[1].(int64); !ok || age != 25 {
		t.Errorf("Expected params[1] to be int64(25), got %T: %v", params[1], params[1])
	}

	if rating, ok := params[2].(float64); !ok || rating != 4.5 {
		t.Errorf("Expected params[2] to be float64(4.5), got %T: %v", params[2], params[2])
	}

	// Verify AST was modified - serialize and check for placeholders
	serializer := &SQLiteSerializer{}
	output := serializer.Serialize(stmt)

	// Check that literals were replaced with placeholders
	if output == sql {
		t.Error("AST was not modified - literals still present")
	}

	// SQLite serializer converts :v1 to ?
	expectedContains := "VALUES (?, ?, ?)"
	if !contains(output, expectedContains) {
		t.Errorf("Expected output to contain '%s', got: %s", expectedContains, output)
	}
}

func TestExtractLiterals_BinaryData(t *testing.T) {
	// Test string with embedded NULL byte
	sql := "INSERT INTO data (content) VALUES ('binary\x00data')"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params == nil {
		t.Fatal("Expected params, got nil")
	}

	if len(params) != 1 {
		t.Fatalf("Expected 1 param, got %d", len(params))
	}

	// Verify the binary data is preserved
	data, ok := params[0].(string)
	if !ok {
		t.Fatalf("Expected string, got %T", params[0])
	}

	expected := "binary\x00data"
	if data != expected {
		t.Errorf("Expected %q, got %q", expected, data)
	}
}

func TestExtractLiterals_Update(t *testing.T) {
	sql := "UPDATE users SET name = 'bob', age = 30 WHERE id = 5"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params == nil {
		t.Fatal("Expected params, got nil")
	}

	if len(params) != 3 {
		t.Fatalf("Expected 3 params, got %d", len(params))
	}

	// Verify params
	if name, ok := params[0].(string); !ok || name != "bob" {
		t.Errorf("Expected params[0] to be string('bob'), got %T: %v", params[0], params[0])
	}

	if age, ok := params[1].(int64); !ok || age != 30 {
		t.Errorf("Expected params[1] to be int64(30), got %T: %v", params[1], params[1])
	}

	if id, ok := params[2].(int64); !ok || id != 5 {
		t.Errorf("Expected params[2] to be int64(5), got %T: %v", params[2], params[2])
	}

	// Verify AST was modified
	serializer := &SQLiteSerializer{}
	output := serializer.Serialize(stmt)

	if !contains(output, "?") {
		t.Errorf("Expected placeholders in output, got: %s", output)
	}
}

func TestExtractLiterals_Select(t *testing.T) {
	sql := "SELECT * FROM users WHERE age > 18 AND name = 'test'"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params == nil {
		t.Fatal("Expected params, got nil")
	}

	if len(params) != 2 {
		t.Fatalf("Expected 2 params, got %d", len(params))
	}

	// Verify params
	if age, ok := params[0].(int64); !ok || age != 18 {
		t.Errorf("Expected params[0] to be int64(18), got %T: %v", params[0], params[0])
	}

	if name, ok := params[1].(string); !ok || name != "test" {
		t.Errorf("Expected params[1] to be string('test'), got %T: %v", params[1], params[1])
	}
}

func TestExtractLiterals_HexLiteral(t *testing.T) {
	sql := "INSERT INTO data (hash) VALUES (X'deadbeef')"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params == nil {
		t.Fatal("Expected params, got nil")
	}

	if len(params) != 1 {
		t.Fatalf("Expected 1 param, got %d", len(params))
	}

	// Verify hex was decoded to bytes
	data, ok := params[0].([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got %T", params[0])
	}

	expected := []byte{0xde, 0xad, 0xbe, 0xef}
	if len(data) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(data))
	}

	for i := range expected {
		if data[i] != expected[i] {
			t.Errorf("Byte mismatch at index %d: expected %02x, got %02x", i, expected[i], data[i])
		}
	}
}

func TestExtractLiterals_NoLiterals(t *testing.T) {
	sql := "SELECT * FROM users"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params != nil {
		t.Errorf("Expected nil for statement with no literals, got %d params", len(params))
	}
}

func TestExtractLiterals_MixedTypes(t *testing.T) {
	// Test all types in one statement
	sql := "INSERT INTO mixed (str, num, flt, hex) VALUES ('text', 42, 3.14, X'aabb')"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params == nil {
		t.Fatal("Expected params, got nil")
	}

	if len(params) != 4 {
		t.Fatalf("Expected 4 params, got %d", len(params))
	}

	// Verify each type
	if str, ok := params[0].(string); !ok || str != "text" {
		t.Errorf("Expected params[0] to be string('text'), got %T: %v", params[0], params[0])
	}

	if num, ok := params[1].(int64); !ok || num != 42 {
		t.Errorf("Expected params[1] to be int64(42), got %T: %v", params[1], params[1])
	}

	if flt, ok := params[2].(float64); !ok || flt != 3.14 {
		t.Errorf("Expected params[2] to be float64(3.14), got %T: %v", params[2], params[2])
	}

	if hex, ok := params[3].([]byte); !ok || len(hex) != 2 || hex[0] != 0xaa || hex[1] != 0xbb {
		t.Errorf("Expected params[3] to be []byte{0xaa, 0xbb}, got %T: %v", params[3], params[3])
	}
}

func TestExtractLiterals_NestedSubquery(t *testing.T) {
	sql := "SELECT * FROM users WHERE id IN (SELECT user_id FROM posts WHERE title = 'test' AND views > 100)"
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractLiterals(stmt)

	if params == nil {
		t.Fatal("Expected params, got nil")
	}

	// Should extract literals from both outer query and subquery
	if len(params) != 2 {
		t.Fatalf("Expected 2 params, got %d", len(params))
	}

	// Verify params
	if title, ok := params[0].(string); !ok || title != "test" {
		t.Errorf("Expected params[0] to be string('test'), got %T: %v", params[0], params[0])
	}

	if views, ok := params[1].(int64); !ok || views != 100 {
		t.Errorf("Expected params[1] to be int64(100), got %T: %v", params[1], params[1])
	}
}

func TestExtractLiterals_NilStatement(t *testing.T) {
	params := ExtractLiterals(nil)
	if params != nil {
		t.Errorf("Expected nil for nil statement, got %v", params)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
