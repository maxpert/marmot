package grpc

import (
	"reflect"
	"testing"
)

func TestSerializeParams_NilAndEmpty(t *testing.T) {
	t.Run("nil params returns nil", func(t *testing.T) {
		result, err := SerializeParams(nil)
		if err != nil {
			t.Fatalf("SerializeParams(nil) failed: %v", err)
		}
		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})

	t.Run("empty params returns empty slice", func(t *testing.T) {
		result, err := SerializeParams([]interface{}{})
		if err != nil {
			t.Fatalf("SerializeParams([]) failed: %v", err)
		}
		if result == nil {
			t.Error("Expected non-nil result for empty slice")
		}
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got length %d", len(result))
		}
	})
}

func TestSerializeParams_Strings(t *testing.T) {
	params := []interface{}{"hello", "world", "test"}
	result, err := SerializeParams(params)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(result))
	}
	for i, data := range result {
		if len(data) == 0 {
			t.Errorf("Result[%d] is empty", i)
		}
	}
}

func TestSerializeParams_Integers(t *testing.T) {
	params := []interface{}{
		int(42),
		int8(127),
		int16(32767),
		int32(2147483647),
		int64(9223372036854775807),
		uint(42),
		uint8(255),
		uint16(65535),
		uint32(4294967295),
		uint64(18446744073709551615),
	}
	result, err := SerializeParams(params)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}
	if len(result) != len(params) {
		t.Fatalf("Expected %d results, got %d", len(params), len(result))
	}
	for i, data := range result {
		if len(data) == 0 {
			t.Errorf("Result[%d] is empty", i)
		}
	}
}

func TestSerializeParams_Floats(t *testing.T) {
	params := []interface{}{
		float32(3.14),
		float64(2.718281828459045),
		float64(0.0),
		float64(-123.456),
	}
	result, err := SerializeParams(params)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}
	if len(result) != len(params) {
		t.Fatalf("Expected %d results, got %d", len(params), len(result))
	}
	for i, data := range result {
		if len(data) == 0 {
			t.Errorf("Result[%d] is empty", i)
		}
	}
}

func TestSerializeParams_Binary(t *testing.T) {
	params := []interface{}{
		[]byte{0x00, 0x01, 0x02, 0xFF},
		[]byte{0xDE, 0xAD, 0xBE, 0xEF},
		[]byte{},
	}
	result, err := SerializeParams(params)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}
	if len(result) != len(params) {
		t.Fatalf("Expected %d results, got %d", len(params), len(result))
	}
	for i, data := range result {
		if len(data) == 0 {
			t.Errorf("Result[%d] is empty", i)
		}
	}
}

func TestSerializeParams_NilValues(t *testing.T) {
	params := []interface{}{
		"value1",
		nil,
		"value2",
		nil,
		int64(123),
	}
	result, err := SerializeParams(params)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}
	if len(result) != len(params) {
		t.Fatalf("Expected %d results, got %d", len(params), len(result))
	}
}

func TestSerializeParams_MixedTypes(t *testing.T) {
	params := []interface{}{
		"string value",
		int64(42),
		float64(3.14),
		[]byte{0xDE, 0xAD},
		nil,
		true,
		false,
	}
	result, err := SerializeParams(params)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}
	if len(result) != len(params) {
		t.Fatalf("Expected %d results, got %d", len(params), len(result))
	}
}

func TestDeserializeParams_NilAndEmpty(t *testing.T) {
	t.Run("nil data returns nil", func(t *testing.T) {
		result, err := DeserializeParams(nil)
		if err != nil {
			t.Fatalf("DeserializeParams(nil) failed: %v", err)
		}
		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})

	t.Run("empty data returns empty slice", func(t *testing.T) {
		result, err := DeserializeParams([][]byte{})
		if err != nil {
			t.Fatalf("DeserializeParams([]) failed: %v", err)
		}
		if result == nil {
			t.Error("Expected non-nil result for empty slice")
		}
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got length %d", len(result))
		}
	})
}

func TestRoundTrip_Strings(t *testing.T) {
	original := []interface{}{"hello", "world", "test string"}

	serialized, err := SerializeParams(original)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}

	deserialized, err := DeserializeParams(serialized)
	if err != nil {
		t.Fatalf("DeserializeParams failed: %v", err)
	}

	if !reflect.DeepEqual(original, deserialized) {
		t.Errorf("Round-trip mismatch:\nOriginal:     %v\nDeserialized: %v", original, deserialized)
	}
}

func TestRoundTrip_Integers(t *testing.T) {
	// Note: msgpack may normalize integer types to smallest representation
	// Testing with int64 to ensure consistent type after round-trip
	original := []interface{}{
		int64(0),
		int64(42),
		int64(-42),
		int64(2147483647),
		int64(-2147483648),
	}

	serialized, err := SerializeParams(original)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}

	deserialized, err := DeserializeParams(serialized)
	if err != nil {
		t.Fatalf("DeserializeParams failed: %v", err)
	}

	if len(original) != len(deserialized) {
		t.Fatalf("Length mismatch: original %d, deserialized %d", len(original), len(deserialized))
	}

	for i := range original {
		origInt := original[i].(int64)
		deserInt, ok := deserialized[i].(int64)
		if !ok {
			t.Errorf("Index %d: type mismatch, got %T", i, deserialized[i])
			continue
		}
		if origInt != deserInt {
			t.Errorf("Index %d: value mismatch, original %d, deserialized %d", i, origInt, deserInt)
		}
	}
}

func TestRoundTrip_Floats(t *testing.T) {
	original := []interface{}{
		float64(3.14159),
		float64(0.0),
		float64(-123.456),
		float64(2.718281828459045),
	}

	serialized, err := SerializeParams(original)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}

	deserialized, err := DeserializeParams(serialized)
	if err != nil {
		t.Fatalf("DeserializeParams failed: %v", err)
	}

	if !reflect.DeepEqual(original, deserialized) {
		t.Errorf("Round-trip mismatch:\nOriginal:     %v\nDeserialized: %v", original, deserialized)
	}
}

func TestRoundTrip_Binary(t *testing.T) {
	// Note: With UseLooseInterfaceDecoding, []byte becomes string
	// This is consistent with Marmot's encoding package behavior
	original := []interface{}{
		[]byte{0x00, 0x01, 0x02, 0xFF},
		[]byte{0xDE, 0xAD, 0xBE, 0xEF},
	}

	serialized, err := SerializeParams(original)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}

	deserialized, err := DeserializeParams(serialized)
	if err != nil {
		t.Fatalf("DeserializeParams failed: %v", err)
	}

	if len(original) != len(deserialized) {
		t.Fatalf("Length mismatch: original %d, deserialized %d", len(original), len(deserialized))
	}

	for i := range original {
		origBytes := original[i].([]byte)
		// With loose decoding, []byte becomes string
		deserStr, ok := deserialized[i].(string)
		if !ok {
			t.Errorf("Index %d: expected string (loose decoding), got %T", i, deserialized[i])
			continue
		}
		if string(origBytes) != deserStr {
			t.Errorf("Index %d: value mismatch", i)
		}
	}
}

func TestRoundTrip_NilValues(t *testing.T) {
	original := []interface{}{
		"value1",
		nil,
		int64(42),
		nil,
		"value2",
	}

	serialized, err := SerializeParams(original)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}

	deserialized, err := DeserializeParams(serialized)
	if err != nil {
		t.Fatalf("DeserializeParams failed: %v", err)
	}

	if !reflect.DeepEqual(original, deserialized) {
		t.Errorf("Round-trip mismatch:\nOriginal:     %v\nDeserialized: %v", original, deserialized)
	}
}

func TestRoundTrip_MixedTypes(t *testing.T) {
	original := []interface{}{
		"string value",
		int64(42),
		float64(3.14),
		nil,
		true,
		false,
	}

	serialized, err := SerializeParams(original)
	if err != nil {
		t.Fatalf("SerializeParams failed: %v", err)
	}

	deserialized, err := DeserializeParams(serialized)
	if err != nil {
		t.Fatalf("DeserializeParams failed: %v", err)
	}

	if len(original) != len(deserialized) {
		t.Fatalf("Length mismatch: original %d, deserialized %d", len(original), len(deserialized))
	}

	for i := range original {
		// Handle []byte special case (becomes string with loose decoding)
		if origBytes, ok := original[i].([]byte); ok {
			deserStr, ok := deserialized[i].(string)
			if !ok {
				t.Errorf("Index %d: expected string (loose decoding), got %T", i, deserialized[i])
				continue
			}
			if string(origBytes) != deserStr {
				t.Errorf("Index %d: value mismatch", i)
			}
			continue
		}

		// For all other types, use DeepEqual
		if !reflect.DeepEqual(original[i], deserialized[i]) {
			t.Errorf("Index %d: mismatch\nOriginal:     %v (%T)\nDeserialized: %v (%T)",
				i, original[i], original[i], deserialized[i], deserialized[i])
		}
	}
}

func TestDeserializeParams_InvalidData(t *testing.T) {
	// Use truncated msgpack data that will fail to decode
	// 0xd9 is msgpack str8 prefix, but missing length byte and string data
	invalidData := [][]byte{
		{0xd9}, // Incomplete msgpack str8 - missing length and data
	}

	_, err := DeserializeParams(invalidData)
	if err == nil {
		t.Error("Expected error for invalid msgpack data, got nil")
	}
}
