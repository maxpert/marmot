package filter

import (
	"testing"
)

func TestHashIntentKeyXXH64(t *testing.T) {
	// Same key should produce same hash
	h1 := HashIntentKeyXXH64("test_key_123")
	h2 := HashIntentKeyXXH64("test_key_123")
	if h1 != h2 {
		t.Error("Same key should produce same hash")
	}

	// Different keys should produce different hashes
	h3 := HashIntentKeyXXH64("test_key_456")
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

	// Add intent keys
	c.AddIntentKey("intent_key_1")
	c.AddIntentKey("intent_key_2")
	c.AddIntentKey("intent_key_3")

	if c.Count() != 3 {
		t.Errorf("Expected count 3, got %d", c.Count())
	}

	// Adding duplicate should not increase count
	c.AddIntentKey("intent_key_1")
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

	h1 := HashIntentKeyXXH64("intent_key_1")
	h2 := HashIntentKeyXXH64("intent_key_2")
	h3 := HashIntentKeyXXH64("intent_key_3")

	if _, ok := keySet[h1]; !ok {
		t.Error("Missing hash for intent_key_1")
	}
	if _, ok := keySet[h2]; !ok {
		t.Error("Missing hash for intent_key_2")
	}
	if _, ok := keySet[h3]; !ok {
		t.Error("Missing hash for intent_key_3")
	}
}

func TestSerializeIntentKey(t *testing.T) {
	// Single numeric PK
	key1 := SerializeIntentKey("users", []string{"id"}, map[string][]byte{"id": []byte("123")})
	if key1 != "users:123" {
		t.Errorf("Expected users:123, got %s", key1)
	}

	// Single non-numeric PK (should use base64)
	key2 := SerializeIntentKey("users", []string{"id"}, map[string][]byte{"id": []byte("abc:def")})
	if key2 != "users:b64:YWJjOmRlZg" {
		t.Errorf("Expected users:b64:YWJjOmRlZg, got %s", key2)
	}

	// Composite PK - preserves PK declaration order (user_id, order_id)
	key3 := SerializeIntentKey("orders", []string{"user_id", "order_id"},
		map[string][]byte{"user_id": []byte("1"), "order_id": []byte("100")})
	expected := "orders:c:MQ:MTAw" // base64(1):base64(100) - PK order preserved
	if key3 != expected {
		t.Errorf("Expected %s, got %s", expected, key3)
	}
}

func BenchmarkHashIntentKeyXXH64(b *testing.B) {
	key := "user:12345:profile"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HashIntentKeyXXH64(key)
	}
}

func BenchmarkKeyHashCollector_AddIntentKey(b *testing.B) {
	c := NewKeyHashCollector()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AddIntentKey("intent_key_" + string(rune(i%1000)))
	}
}

// Binary intent key tests

func TestEncodeDecodeIntentKey_SingleInt64PK(t *testing.T) {
	table := "users"
	pkValues := []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(12345)},
	}

	encoded := EncodeIntentKey(table, pkValues)

	decodedTable, decodedPKs, keyType, err := DecodeIntentKey(encoded)
	if err != nil {
		t.Fatalf("DecodeIntentKey failed: %v", err)
	}
	if keyType != IntentKeyVersionDML {
		t.Errorf("Expected keyType %d, got %d", IntentKeyVersionDML, keyType)
	}
	if decodedTable != table {
		t.Errorf("Expected table %s, got %s", table, decodedTable)
	}
	if len(decodedPKs) != 1 {
		t.Fatalf("Expected 1 PK value, got %d", len(decodedPKs))
	}
	if decodedPKs[0].Type != PKTypeInt64 {
		t.Errorf("Expected type %d, got %d", PKTypeInt64, decodedPKs[0].Type)
	}
	if DecodeInt64(decodedPKs[0].Value) != 12345 {
		t.Errorf("Expected value 12345, got %d", DecodeInt64(decodedPKs[0].Value))
	}
}

func TestEncodeDecodeIntentKey_CompositePK(t *testing.T) {
	table := "orders"
	pkValues := []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(100)},
		{Type: PKTypeString, Value: []byte("order-abc")},
	}

	encoded := EncodeIntentKey(table, pkValues)

	decodedTable, decodedPKs, keyType, err := DecodeIntentKey(encoded)
	if err != nil {
		t.Fatalf("DecodeIntentKey failed: %v", err)
	}
	if keyType != IntentKeyVersionDML {
		t.Errorf("Expected keyType %d, got %d", IntentKeyVersionDML, keyType)
	}
	if decodedTable != table {
		t.Errorf("Expected table %s, got %s", table, decodedTable)
	}
	if len(decodedPKs) != 2 {
		t.Fatalf("Expected 2 PK values, got %d", len(decodedPKs))
	}
	if DecodeInt64(decodedPKs[0].Value) != 100 {
		t.Errorf("Expected first PK value 100, got %d", DecodeInt64(decodedPKs[0].Value))
	}
	if string(decodedPKs[1].Value) != "order-abc" {
		t.Errorf("Expected second PK value 'order-abc', got %s", string(decodedPKs[1].Value))
	}
}

func TestEncodeDecodeIntentKey_NullPK(t *testing.T) {
	table := "data"
	pkValues := []TypedPKValue{
		{Type: PKTypeNull, Value: nil},
	}

	encoded := EncodeIntentKey(table, pkValues)

	decodedTable, decodedPKs, keyType, err := DecodeIntentKey(encoded)
	if err != nil {
		t.Fatalf("DecodeIntentKey failed: %v", err)
	}
	if keyType != IntentKeyVersionDML {
		t.Errorf("Expected keyType %d, got %d", IntentKeyVersionDML, keyType)
	}
	if decodedTable != table {
		t.Errorf("Expected table %s, got %s", table, decodedTable)
	}
	if len(decodedPKs) != 1 {
		t.Fatalf("Expected 1 PK value, got %d", len(decodedPKs))
	}
	if decodedPKs[0].Type != PKTypeNull {
		t.Errorf("Expected NULL type, got %d", decodedPKs[0].Type)
	}
}

func TestEncodeDecodeIntentKey_Float64PK(t *testing.T) {
	table := "measurements"
	pkValues := []TypedPKValue{
		{Type: PKTypeFloat64, Value: EncodeFloat64(3.14159)},
	}

	encoded := EncodeIntentKey(table, pkValues)

	decodedTable, decodedPKs, _, err := DecodeIntentKey(encoded)
	if err != nil {
		t.Fatalf("DecodeIntentKey failed: %v", err)
	}
	if decodedTable != table {
		t.Errorf("Expected table %s, got %s", table, decodedTable)
	}
	if len(decodedPKs) != 1 {
		t.Fatalf("Expected 1 PK value, got %d", len(decodedPKs))
	}
	decodedVal := DecodeFloat64(decodedPKs[0].Value)
	if decodedVal != 3.14159 {
		t.Errorf("Expected value 3.14159, got %f", decodedVal)
	}
}

func TestEncodeDecodeIntentKey_BytesPK(t *testing.T) {
	table := "blobs"
	blobData := []byte{0x00, 0x01, 0xFF, 0xFE, 0x00}
	pkValues := []TypedPKValue{
		{Type: PKTypeBytes, Value: blobData},
	}

	encoded := EncodeIntentKey(table, pkValues)

	_, decodedPKs, _, err := DecodeIntentKey(encoded)
	if err != nil {
		t.Fatalf("DecodeIntentKey failed: %v", err)
	}
	if len(decodedPKs) != 1 {
		t.Fatalf("Expected 1 PK value, got %d", len(decodedPKs))
	}
	if decodedPKs[0].Type != PKTypeBytes {
		t.Errorf("Expected type %d, got %d", PKTypeBytes, decodedPKs[0].Type)
	}
	if string(decodedPKs[0].Value) != string(blobData) {
		t.Errorf("Bytes mismatch")
	}
}

func TestEncodeDDLIntentKey(t *testing.T) {
	table := "users"
	encoded := EncodeDDLIntentKey(table)

	decodedTable, pkValues, keyType, err := DecodeIntentKey(encoded)
	if err != nil {
		t.Fatalf("DecodeIntentKey failed: %v", err)
	}
	if keyType != IntentKeyMarkerDDL {
		t.Errorf("Expected DDL marker %d, got %d", IntentKeyMarkerDDL, keyType)
	}
	if decodedTable != table {
		t.Errorf("Expected table %s, got %s", table, decodedTable)
	}
	if len(pkValues) != 0 {
		t.Errorf("DDL intent key should have no PK values, got %d", len(pkValues))
	}
}

func TestEncodeDBOpIntentKey(t *testing.T) {
	database := "mydb"
	encoded := EncodeDBOpIntentKey(database)

	decodedDB, pkValues, keyType, err := DecodeIntentKey(encoded)
	if err != nil {
		t.Fatalf("DecodeIntentKey failed: %v", err)
	}
	if keyType != IntentKeyMarkerDBOp {
		t.Errorf("Expected DBOp marker %d, got %d", IntentKeyMarkerDBOp, keyType)
	}
	if decodedDB != database {
		t.Errorf("Expected database %s, got %s", database, decodedDB)
	}
	if len(pkValues) != 0 {
		t.Errorf("DBOp intent key should have no PK values, got %d", len(pkValues))
	}
}

func TestBuildIntentKeyPrefix(t *testing.T) {
	table := "users"
	prefix := BuildIntentKeyPrefix(table)

	// Using the prefix should produce same result as full encode
	pkValues := []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(42)},
	}

	fromPrefix := EncodeIntentKeyWithPrefix(prefix, pkValues)
	fromFull := EncodeIntentKey(table, pkValues)

	if string(fromPrefix) != string(fromFull) {
		t.Errorf("Prefix encode doesn't match full encode")
	}
}

func TestGetIntentKeyTable(t *testing.T) {
	table := "orders"
	pkValues := []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(123)},
	}

	encoded := EncodeIntentKey(table, pkValues)
	extractedTable, err := GetIntentKeyTable(encoded)
	if err != nil {
		t.Fatalf("GetIntentKeyTable failed: %v", err)
	}
	if extractedTable != table {
		t.Errorf("Expected table %s, got %s", table, extractedTable)
	}
}

func TestEncodeFloat64_NormalizesNegativeZero(t *testing.T) {
	positiveZero := EncodeFloat64(0.0)
	negativeZero := EncodeFloat64(-0.0)

	// Both should produce the same encoding
	if string(positiveZero) != string(negativeZero) {
		t.Error("-0 should be normalized to +0")
	}
}

func TestIntentKeyBase64RoundTrip(t *testing.T) {
	encoded := EncodeIntentKey("users", []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(999)},
	})

	b64 := IntentKeyToBase64(encoded)
	decoded, err := IntentKeyFromBase64(b64)
	if err != nil {
		t.Fatalf("IntentKeyFromBase64 failed: %v", err)
	}

	if string(decoded) != string(encoded) {
		t.Error("Base64 round-trip failed")
	}
}

func TestDecodeIntentKey_InvalidInputs(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{0x01}},
		{"invalid version", []byte{0x99, 0x05, 'u', 's', 'e', 'r', 's', 0x00}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := DecodeIntentKey(tc.input)
			if err == nil {
				t.Error("Expected error for invalid input")
			}
		})
	}
}

func TestHashBinaryIntentKey_Deterministic(t *testing.T) {
	key1 := EncodeIntentKey("users", []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(123)},
	})
	key2 := EncodeIntentKey("users", []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(123)},
	})

	if HashBinaryIntentKey(key1) != HashBinaryIntentKey(key2) {
		t.Error("Same keys should produce same hash")
	}

	key3 := EncodeIntentKey("users", []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(456)},
	})
	if HashBinaryIntentKey(key1) == HashBinaryIntentKey(key3) {
		t.Error("Different keys should produce different hashes")
	}
}

func BenchmarkEncodeIntentKey(b *testing.B) {
	pkValues := []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(12345)},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeIntentKey("users", pkValues)
	}
}

func BenchmarkEncodeIntentKeyWithPrefix(b *testing.B) {
	prefix := BuildIntentKeyPrefix("users")
	pkValues := []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(12345)},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeIntentKeyWithPrefix(prefix, pkValues)
	}
}

func BenchmarkDecodeIntentKey(b *testing.B) {
	encoded := EncodeIntentKey("users", []TypedPKValue{
		{Type: PKTypeInt64, Value: EncodeInt64(12345)},
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeIntentKey(encoded)
	}
}
