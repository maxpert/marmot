package protocol

import (
	"sync"
	"testing"
)

func TestMarshalMsgpack_Basic(t *testing.T) {
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
			data, err := MarshalMsgpack(tc.input)
			if err != nil {
				t.Fatalf("MarshalMsgpack failed: %v", err)
			}
			if len(data) == 0 {
				t.Error("Expected non-empty result")
			}
		})
	}
}

func TestMarshalMsgpack_Concurrent(t *testing.T) {
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
				result, err := MarshalMsgpack(data)
				if err != nil {
					t.Errorf("MarshalMsgpack failed: %v", err)
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

func TestMarshalMsgpack_NilValues(t *testing.T) {
	// nil should be valid input
	data, err := MarshalMsgpack(nil)
	if err != nil {
		t.Fatalf("MarshalMsgpack(nil) failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty result for nil")
	}
}

func TestMarshalMsgpack_EmptyMap(t *testing.T) {
	data, err := MarshalMsgpack(map[string]interface{}{})
	if err != nil {
		t.Fatalf("MarshalMsgpack failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty result")
	}
}

func BenchmarkMarshalMsgpack(b *testing.B) {
	data := map[string]interface{}{
		"id":        12345,
		"name":      "benchmark test",
		"values":    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"nested":    map[string]string{"key": "value"},
		"timestamp": int64(1234567890),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = MarshalMsgpack(data)
	}
}

func BenchmarkMarshalMsgpack_Parallel(b *testing.B) {
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
			_, _ = MarshalMsgpack(data)
		}
	})
}
