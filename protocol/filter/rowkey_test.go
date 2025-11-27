package filter

import (
	"testing"
)

func TestHashRowKeyXXH64(t *testing.T) {
	// Same key should produce same hash
	h1 := HashRowKeyXXH64("test_key_123")
	h2 := HashRowKeyXXH64("test_key_123")
	if h1 != h2 {
		t.Error("Same key should produce same hash")
	}

	// Different keys should produce different hashes
	h3 := HashRowKeyXXH64("test_key_456")
	if h1 == h3 {
		t.Error("Different keys should produce different hashes")
	}
}

func TestHashPrimaryKeyXXH64(t *testing.T) {
	pk1 := map[string][]byte{"id": []byte("123")}
	pk2 := map[string][]byte{"id": []byte("123")}
	pk3 := map[string][]byte{"id": []byte("456")}

	h1 := HashPrimaryKeyXXH64("users", pk1)
	h2 := HashPrimaryKeyXXH64("users", pk2)
	h3 := HashPrimaryKeyXXH64("users", pk3)
	h4 := HashPrimaryKeyXXH64("orders", pk1)

	if h1 != h2 {
		t.Error("Same table and pk should produce same hash")
	}
	if h1 == h3 {
		t.Error("Different pk should produce different hash")
	}
	if h1 == h4 {
		t.Error("Different table should produce different hash")
	}
}

func TestKeyHashCollector(t *testing.T) {
	c := NewKeyHashCollector()

	// Add row keys
	c.AddRowKey("row_key_1")
	c.AddRowKey("row_key_2")
	c.AddRowKey("row_key_3")

	if c.Count() != 3 {
		t.Errorf("Expected count 3, got %d", c.Count())
	}

	// Adding duplicate should not increase count
	c.AddRowKey("row_key_1")
	if c.Count() != 3 {
		t.Errorf("Expected count 3 after duplicate, got %d", c.Count())
	}

	// Get keys
	keys := c.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	// Verify all hashes are present
	keySet := make(map[uint64]struct{})
	for _, k := range keys {
		keySet[k] = struct{}{}
	}

	h1 := HashRowKeyXXH64("row_key_1")
	h2 := HashRowKeyXXH64("row_key_2")
	h3 := HashRowKeyXXH64("row_key_3")

	if _, ok := keySet[h1]; !ok {
		t.Error("Missing hash for row_key_1")
	}
	if _, ok := keySet[h2]; !ok {
		t.Error("Missing hash for row_key_2")
	}
	if _, ok := keySet[h3]; !ok {
		t.Error("Missing hash for row_key_3")
	}
}

func TestSerializeRowKey(t *testing.T) {
	// Single numeric PK
	key1 := SerializeRowKey("users", []string{"id"}, map[string][]byte{"id": []byte("123")})
	if key1 != "users:123" {
		t.Errorf("Expected users:123, got %s", key1)
	}

	// Single non-numeric PK (should use base64)
	key2 := SerializeRowKey("users", []string{"id"}, map[string][]byte{"id": []byte("abc:def")})
	if key2 != "users:b64:YWJjOmRlZg" {
		t.Errorf("Expected users:b64:YWJjOmRlZg, got %s", key2)
	}

	// Composite PK
	key3 := SerializeRowKey("orders", []string{"user_id", "order_id"},
		map[string][]byte{"user_id": []byte("1"), "order_id": []byte("100")})
	// Columns sorted alphabetically: order_id, user_id
	expected := "orders:c:MTAw:MQ" // base64(100):base64(1)
	if key3 != expected {
		t.Errorf("Expected %s, got %s", expected, key3)
	}
}

func BenchmarkHashRowKeyXXH64(b *testing.B) {
	key := "user:12345:profile"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HashRowKeyXXH64(key)
	}
}

func BenchmarkKeyHashCollector_AddRowKey(b *testing.B) {
	c := NewKeyHashCollector()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AddRowKey("row_key_" + string(rune(i%1000)))
	}
}
