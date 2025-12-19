package encoding

import (
	"sync"
	"testing"
)

func TestMarshal_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{"string", "hello world"},
		{"int", 12345},
		{"int64", int64(9876543210)},
		{"float64", 3.14159},
		{"bool", true},
		{"slice", []int{1, 2, 3, 4, 5}},
		{"map", map[string]interface{}{"name": "alice", "age": 30}},
		{"nested", map[string]interface{}{
			"user": map[string]interface{}{
				"id":   123,
				"name": "bob",
			},
			"items": []string{"a", "b", "c"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := Marshal(tc.input)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}
			if len(data) == 0 {
				t.Error("Expected non-empty result")
			}
		})
	}
}

func TestMarshal_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	numGoroutines := 100
	iterations := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				data := map[string]interface{}{
					"goroutine": id,
					"iteration": j,
					"data":      "some test data",
				}
				result, err := Marshal(data)
				if err != nil {
					t.Errorf("Marshal failed: %v", err)
					return
				}
				if len(result) == 0 {
					t.Error("Expected non-empty result")
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestUnmarshal_StringNotBytes(t *testing.T) {
	// This test verifies that Unmarshal decodes strings as Go strings,
	// not []byte. This is critical for SQLite which treats BLOB and TEXT as
	// different types for PRIMARY KEY comparison.
	original := "rec_000000013049"
	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var result interface{}
	if err := Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Result should be string, not []byte
	str, ok := result.(string)
	if !ok {
		t.Fatalf("Expected string type, got %T", result)
	}
	if str != original {
		t.Errorf("String mismatch: got %q, want %q", str, original)
	}
}

func TestUnmarshal_MapWithStrings(t *testing.T) {
	original := map[string]interface{}{
		"id":   "primary_key_value",
		"name": "some text",
	}
	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var result interface{}
	if err := Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{}, got %T", result)
	}

	for key, val := range m {
		if _, ok := val.(string); !ok {
			t.Errorf("Value for key %q is %T, expected string", key, val)
		}
	}
}

func TestUnmarshal_MixedTypes(t *testing.T) {
	// With UseLooseInterfaceDecoding(true):
	// - Go string → msgpack str → decoded as Go string
	// - Go []byte → msgpack bin → decoded as Go string (loose decoding converts bin to string)
	tests := []struct {
		name    string
		input   interface{}
		checkFn func(t *testing.T, result interface{})
	}{
		{
			name:  "string_stays_string",
			input: "hello world",
			checkFn: func(t *testing.T, result interface{}) {
				if s, ok := result.(string); !ok || s != "hello world" {
					t.Fatalf("Expected string 'hello world', got %T %v", result, result)
				}
			},
		},
		{
			name:  "bytes_become_string",
			input: []byte{0x00, 0x01, 0x02, 0xFF},
			checkFn: func(t *testing.T, result interface{}) {
				// With UseLooseInterfaceDecoding, []byte becomes string
				s, ok := result.(string)
				if !ok {
					t.Fatalf("Expected string (loose decoding), got %T", result)
				}
				expected := string([]byte{0x00, 0x01, 0x02, 0xFF})
				if s != expected {
					t.Errorf("Content mismatch")
				}
			},
		},
		{
			name: "map_with_loose_decoding",
			input: map[string]interface{}{
				"text_field": "this is text",
				"bin_field":  []byte{0xDE, 0xAD},
				"id":         "rec_000000013049",
				"int_field":  int64(12345),
			},
			checkFn: func(t *testing.T, result interface{}) {
				m, ok := result.(map[string]interface{})
				if !ok {
					t.Fatalf("Expected map, got %T", result)
				}

				if v, ok := m["text_field"].(string); !ok || v != "this is text" {
					t.Errorf("text_field: got %T %v", m["text_field"], m["text_field"])
				}
				// With loose decoding, bin_field becomes string
				if _, ok := m["bin_field"].(string); !ok {
					t.Errorf("bin_field: got %T, want string (loose decoding)", m["bin_field"])
				}
				if v, ok := m["id"].(string); !ok || v != "rec_000000013049" {
					t.Errorf("id: got %T %v", m["id"], m["id"])
				}
				if v, ok := m["int_field"].(int64); !ok || v != 12345 {
					t.Errorf("int_field: got %T %v", m["int_field"], m["int_field"])
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := Marshal(tc.input)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}
			var result interface{}
			if err := Unmarshal(data, &result); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}
			tc.checkFn(t, result)
		})
	}
}

func BenchmarkMarshal(b *testing.B) {
	data := map[string]interface{}{
		"id":        12345,
		"name":      "benchmark test",
		"values":    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"nested":    map[string]string{"key": "value"},
		"timestamp": int64(1234567890),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Marshal(data)
	}
}

func BenchmarkMarshal_Parallel(b *testing.B) {
	data := map[string]interface{}{
		"id":        12345,
		"name":      "benchmark test",
		"values":    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"nested":    map[string]string{"key": "value"},
		"timestamp": int64(1234567890),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Marshal(data)
		}
	})
}
