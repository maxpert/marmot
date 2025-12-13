package protocol

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/maxpert/marmot/encoding"
)

// TestIntentKeyCompatibility verifies that GenerateIntentKey produces the same format
// as db/preupdate_hook.go serializePK. This is critical for conflict detection
// to work correctly across both paths.
func TestIntentKeyCompatibility(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		primaryKeys []string
		values      map[string][]byte
		expectedKey string
		expectError bool
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
			name:        "missing PK column returns error (auto-increment)",
			tableName:   "nullable_pk",
			primaryKeys: []string{"a", "b"},
			values: map[string][]byte{
				"a": []byte("1"),
				// "b" is missing - this indicates auto-increment INSERT
			},
			expectError: true,
		},
		{
			name:        "all PKs missing returns error (auto-increment)",
			tableName:   "single_null",
			primaryKeys: []string{"id"},
			values:      map[string][]byte{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &TableSchema{
				TableName:   tt.tableName,
				PrimaryKeys: tt.primaryKeys,
			}

			intentKey, err := GenerateIntentKey(schema, tt.values)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got intent key: %q", intentKey)
				}
				return
			}
			if err != nil {
				t.Fatalf("GenerateIntentKey failed: %v", err)
			}

			if intentKey != tt.expectedKey {
				t.Errorf("Intent key mismatch:\n  got:      %q\n  expected: %q", intentKey, tt.expectedKey)
			}
		})
	}
}

// TestIntentKeyNoCollision verifies that potentially colliding keys produce different intent keys
func TestIntentKeyNoCollision(t *testing.T) {
	schema := &TableSchema{
		TableName:   "test",
		PrimaryKeys: []string{"a", "b"},
	}

	// These would collide without proper encoding
	values1 := map[string][]byte{"a": []byte("x:y"), "b": []byte("z")}
	values2 := map[string][]byte{"a": []byte("x"), "b": []byte("y:z")}

	key1, err := GenerateIntentKey(schema, values1)
	if err != nil {
		t.Fatalf("GenerateIntentKey failed: %v", err)
	}

	key2, err := GenerateIntentKey(schema, values2)
	if err != nil {
		t.Fatalf("GenerateIntentKey failed: %v", err)
	}

	if key1 == key2 {
		t.Errorf("Keys should NOT be equal (collision detected):\n  key1: %q\n  key2: %q", key1, key2)
	}

	t.Logf("No collision:\n  (%q, %q) -> %q\n  (%q, %q) -> %q",
		"x:y", "z", key1, "x", "y:z", key2)
}

// TestIntentKeyDeterminism verifies that the same input always produces the same output
func TestIntentKeyDeterminism(t *testing.T) {
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
		key, err := GenerateIntentKey(schema, values)
		if err != nil {
			t.Fatalf("GenerateIntentKey failed: %v", err)
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

// TestIntentKeyMsgpackValues verifies handling of msgpack-encoded values
func TestIntentKeyMsgpackValues(t *testing.T) {
	schema := &TableSchema{
		TableName:   "msgpack_test",
		PrimaryKeys: []string{"id"},
	}

	// msgpack-encoded string "123"
	encoded, err := encoding.Marshal("123")
	if err != nil {
		t.Fatalf("encoding.Marshal failed: %v", err)
	}
	values := map[string][]byte{
		"id": encoded,
	}

	key, err := GenerateIntentKey(schema, values)
	if err != nil {
		t.Fatalf("GenerateIntentKey failed: %v", err)
	}

	// Should extract the string value from msgpack
	expected := "msgpack_test:123"
	if key != expected {
		t.Errorf("Intent key mismatch:\n  got:      %q\n  expected: %q", key, expected)
	}
}

// TestIntentKeyZeroValueAsAutoIncrement verifies that id=0 is treated as auto-increment
// MySQL semantics: INSERT with id=0 means "use next auto-increment value"
func TestIntentKeyZeroValueAsAutoIncrement(t *testing.T) {
	// msgpack-encoded "0"
	msgpackZero, _ := encoding.Marshal("0")

	tests := []struct {
		name  string
		value []byte
	}{
		{"raw zero", []byte("0")},
		{"msgpack-encoded zero", msgpackZero},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &TableSchema{
				TableName:   "test",
				PrimaryKeys: []string{"id"},
			}

			values := map[string][]byte{"id": tt.value}

			_, err := GenerateIntentKey(schema, values)
			if err != ErrMissingPrimaryKey {
				t.Errorf("Expected ErrMissingPrimaryKey for id=%q, got: %v", tt.value, err)
			}
		})
	}
}

// TestIntentKeySpecialCharacters verifies handling of various special characters
func TestIntentKeySpecialCharacters(t *testing.T) {
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

			key, err := GenerateIntentKey(schema, values)
			if err != nil {
				t.Fatalf("GenerateIntentKey failed for %q: %v", tt.value, err)
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

	key1, _ := GenerateIntentKey(schema1, values)
	key2, _ := GenerateIntentKey(schema2, values)
	key3, _ := GenerateIntentKey(schema3, values)

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
