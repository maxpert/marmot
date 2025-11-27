package protocol

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/maxpert/marmot/protocol/filter"
)

// TestRowKeyCompatibility verifies that GenerateRowKey produces the same format
// as db/preupdate_hook.go serializePK. This is critical for conflict detection
// to work correctly across both paths.
func TestRowKeyCompatibility(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		primaryKeys []string
		values      map[string][]byte
		expectedKey string
	}{
		{
			name:        "single numeric PK",
			tableName:   "users",
			primaryKeys: []string{"id"},
			values:      map[string][]byte{"id": []byte("123")},
			expectedKey: "users:123",
		},
		{
			name:        "single negative numeric PK",
			tableName:   "accounts",
			primaryKeys: []string{"id"},
			values:      map[string][]byte{"id": []byte("-42")},
			expectedKey: "accounts:-42",
		},
		{
			name:        "single string PK with special chars",
			tableName:   "items",
			primaryKeys: []string{"code"},
			values:      map[string][]byte{"code": []byte("a:b:c")},
			// "a:b:c" base64 encoded
			expectedKey: "items:b64:" + base64.RawStdEncoding.EncodeToString([]byte("a:b:c")),
		},
		{
			name:        "composite key - two integers",
			tableName:   "orders",
			primaryKeys: []string{"user_id", "order_id"},
			values: map[string][]byte{
				"user_id":  []byte("1"),
				"order_id": []byte("100"),
			},
			// Sorted alphabetically: order_id, user_id -> "100", "1"
			expectedKey: "orders:c:" + base64.RawStdEncoding.EncodeToString([]byte("100")) + ":" + base64.RawStdEncoding.EncodeToString([]byte("1")),
		},
		{
			name:        "composite key with separator in value",
			tableName:   "composite_test",
			primaryKeys: []string{"part1", "part2"},
			values: map[string][]byte{
				"part1": []byte("a:b"),
				"part2": []byte("c"),
			},
			// Sorted: part1, part2 -> "a:b", "c"
			expectedKey: "composite_test:c:" + base64.RawStdEncoding.EncodeToString([]byte("a:b")) + ":" + base64.RawStdEncoding.EncodeToString([]byte("c")),
		},
		{
			name:        "composite key - different order produces different key",
			tableName:   "composite_test",
			primaryKeys: []string{"part1", "part2"},
			values: map[string][]byte{
				"part1": []byte("a"),
				"part2": []byte("b:c"),
			},
			// Sorted: part1, part2 -> "a", "b:c"
			expectedKey: "composite_test:c:" + base64.RawStdEncoding.EncodeToString([]byte("a")) + ":" + base64.RawStdEncoding.EncodeToString([]byte("b:c")),
		},
		{
			name:        "composite key with NULL value",
			tableName:   "nullable_pk",
			primaryKeys: []string{"a", "b"},
			values: map[string][]byte{
				"a": []byte("1"),
				// "b" is missing (NULL)
			},
			expectedKey: "nullable_pk:c:" + base64.RawStdEncoding.EncodeToString([]byte("1")) + ":" + filter.NullSentinel,
		},
		{
			name:        "single NULL PK",
			tableName:   "single_null",
			primaryKeys: []string{"id"},
			values:      map[string][]byte{},
			expectedKey: "single_null:" + filter.NullSentinel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &TableSchema{
				TableName:   tt.tableName,
				PrimaryKeys: tt.primaryKeys,
			}

			rowKey, err := GenerateRowKey(schema, tt.values)
			if err != nil {
				t.Fatalf("GenerateRowKey failed: %v", err)
			}

			if rowKey != tt.expectedKey {
				t.Errorf("Row key mismatch:\n  got:      %q\n  expected: %q", rowKey, tt.expectedKey)
			}
		})
	}
}

// TestRowKeyNoCollision verifies that potentially colliding keys produce different row keys
func TestRowKeyNoCollision(t *testing.T) {
	schema := &TableSchema{
		TableName:   "test",
		PrimaryKeys: []string{"a", "b"},
	}

	// These would collide without proper encoding
	values1 := map[string][]byte{"a": []byte("x:y"), "b": []byte("z")}
	values2 := map[string][]byte{"a": []byte("x"), "b": []byte("y:z")}

	key1, err := GenerateRowKey(schema, values1)
	if err != nil {
		t.Fatalf("GenerateRowKey failed: %v", err)
	}

	key2, err := GenerateRowKey(schema, values2)
	if err != nil {
		t.Fatalf("GenerateRowKey failed: %v", err)
	}

	if key1 == key2 {
		t.Errorf("Keys should NOT be equal (collision detected):\n  key1: %q\n  key2: %q", key1, key2)
	}

	t.Logf("No collision:\n  (%q, %q) -> %q\n  (%q, %q) -> %q",
		"x:y", "z", key1, "x", "y:z", key2)
}

// TestRowKeyDeterminism verifies that the same input always produces the same output
func TestRowKeyDeterminism(t *testing.T) {
	schema := &TableSchema{
		TableName:   "test",
		PrimaryKeys: []string{"id", "name"},
	}

	values := map[string][]byte{
		"id":   []byte("123"),
		"name": []byte("test:user"),
	}

	// Generate key multiple times
	var keys []string
	for i := 0; i < 100; i++ {
		key, err := GenerateRowKey(schema, values)
		if err != nil {
			t.Fatalf("GenerateRowKey failed: %v", err)
		}
		keys = append(keys, key)
	}

	// All keys should be identical
	for i, key := range keys {
		if key != keys[0] {
			t.Errorf("Non-deterministic key at iteration %d: %q != %q", i, key, keys[0])
		}
	}
}

// TestRowKeyJSONValues verifies handling of JSON-encoded values
func TestRowKeyJSONValues(t *testing.T) {
	schema := &TableSchema{
		TableName:   "json_test",
		PrimaryKeys: []string{"id"},
	}

	// JSON-encoded string "123" -> should extract "123"
	values := map[string][]byte{
		"id": []byte(`"123"`),
	}

	key, err := GenerateRowKey(schema, values)
	if err != nil {
		t.Fatalf("GenerateRowKey failed: %v", err)
	}

	// Should extract the numeric value from JSON string
	expected := "json_test:123"
	if key != expected {
		t.Errorf("Row key mismatch:\n  got:      %q\n  expected: %q", key, expected)
	}
}

// TestRowKeySpecialCharacters verifies handling of various special characters
func TestRowKeySpecialCharacters(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"colon", "a:b"},
		{"newline", "a\nb"},
		{"tab", "a\tb"},
		{"null byte", "a\x00b"},
		{"unicode", "日本語"},
		{"emoji", "🎉"},
		{"spaces", "hello world"},
		{"quotes", `"quoted"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &TableSchema{
				TableName:   "special",
				PrimaryKeys: []string{"id"},
			}

			values := map[string][]byte{"id": []byte(tt.value)}

			key, err := GenerateRowKey(schema, values)
			if err != nil {
				t.Fatalf("GenerateRowKey failed for %q: %v", tt.value, err)
			}

			// Key should contain table name and be non-empty
			if key == "" {
				t.Error("Empty key generated")
			}

			// Should start with table name
			expectedPrefix := "special:"
			if len(key) < len(expectedPrefix) || key[:len(expectedPrefix)] != expectedPrefix {
				t.Errorf("Key %q should start with %q", key, expectedPrefix)
			}

			t.Logf("Special char %q -> %q", tt.value, key)
		})
	}
}

// TestCompositeKeyColumnOrdering verifies that column ordering is deterministic
func TestCompositeKeyColumnOrdering(t *testing.T) {
	// Create schema with columns in different orders
	schema1 := &TableSchema{
		TableName:   "test",
		PrimaryKeys: []string{"a", "b", "c"},
	}
	schema2 := &TableSchema{
		TableName:   "test",
		PrimaryKeys: []string{"c", "b", "a"},
	}
	schema3 := &TableSchema{
		TableName:   "test",
		PrimaryKeys: []string{"b", "c", "a"},
	}

	values := map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	}

	key1, _ := GenerateRowKey(schema1, values)
	key2, _ := GenerateRowKey(schema2, values)
	key3, _ := GenerateRowKey(schema3, values)

	// All should produce the same key (columns sorted alphabetically)
	if key1 != key2 || key2 != key3 {
		t.Errorf("Keys should be identical regardless of schema column order:\n  key1: %q\n  key2: %q\n  key3: %q",
			key1, key2, key3)
	}

	// Verify the format is correct (sorted: a, b, c -> 1, 2, 3)
	expected := fmt.Sprintf("test:c:%s:%s:%s",
		base64.RawStdEncoding.EncodeToString([]byte("1")),
		base64.RawStdEncoding.EncodeToString([]byte("2")),
		base64.RawStdEncoding.EncodeToString([]byte("3")))

	if key1 != expected {
		t.Errorf("Expected %q, got %q", expected, key1)
	}
}
