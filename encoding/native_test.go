package encoding

import (
	"runtime"
	"sync"
	"testing"
)

func TestMarshalNative_Basic(t *testing.T) {
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
			native, err := MarshalNative(tc.input)
			if err != nil {
				t.Fatalf("MarshalNative failed: %v", err)
			}
			defer native.Dispose()

			if native.Len() == 0 {
				t.Error("Expected non-empty result")
			}

			data := native.Bytes()
			if len(data) != native.Len() {
				t.Errorf("Length mismatch: Bytes() returned %d, Len() returned %d", len(data), native.Len())
			}

			var result interface{}
			if err := Unmarshal(data, &result); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}
		})
	}
}

func TestMarshalNative_Dispose(t *testing.T) {
	native, err := MarshalNative("test data")
	if err != nil {
		t.Fatalf("MarshalNative failed: %v", err)
	}

	data := native.Bytes()
	if len(data) == 0 {
		t.Error("Expected non-empty data")
	}

	native.Dispose()
}

func TestMarshalNative_DoubleDispose(t *testing.T) {
	native, err := MarshalNative("test data")
	if err != nil {
		t.Fatalf("MarshalNative failed: %v", err)
	}

	native.Dispose()
	native.Dispose()
}

func TestMarshalNative_UseAfterDispose(t *testing.T) {
	native, err := MarshalNative("test data")
	if err != nil {
		t.Fatalf("MarshalNative failed: %v", err)
	}

	native.Dispose()

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when calling Bytes() after Dispose()")
		}
	}()

	native.Bytes()
}

func TestNativeArena_Basic(t *testing.T) {
	arena := NewArena()
	defer arena.Destroy()

	data1 := arena.Alloc(100)
	if data1 == nil || len(data1) != 100 {
		t.Errorf("Expected 100-byte slice, got %v", data1)
	}

	data2 := arena.Alloc(200)
	if data2 == nil || len(data2) != 200 {
		t.Errorf("Expected 200-byte slice, got %v", data2)
	}

	for i := range data1 {
		data1[i] = byte(i % 256)
	}
	for i := range data2 {
		data2[i] = byte((i + 50) % 256)
	}

	for i := range data1 {
		if data1[i] != byte(i%256) {
			t.Errorf("Data1 corrupted at index %d", i)
			break
		}
	}
}

func TestNativeArena_MultipleAllocs(t *testing.T) {
	arena := NewArena()
	defer arena.Destroy()

	allocs := make([][]byte, 0, 100)
	for i := 0; i < 100; i++ {
		size := (i + 1) * 10
		data := arena.Alloc(size)
		if data == nil || len(data) != size {
			t.Fatalf("Allocation %d failed: expected %d bytes, got %v", i, size, data)
		}
		for j := range data {
			data[j] = byte(i % 256)
		}
		allocs = append(allocs, data)
	}

	for i, data := range allocs {
		for j := range data {
			if data[j] != byte(i%256) {
				t.Errorf("Data corrupted in allocation %d at index %d", i, j)
				break
			}
		}
	}
}

func TestMarshalToArena_Basic(t *testing.T) {
	arena := NewArena()
	defer arena.Destroy()

	tests := []struct {
		name  string
		input interface{}
	}{
		{"string", "hello world"},
		{"map", map[string]interface{}{"name": "alice", "age": 30}},
		{"slice", []int{1, 2, 3, 4, 5}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := MarshalToArena(arena, tc.input)
			if err != nil {
				t.Fatalf("MarshalToArena failed: %v", err)
			}

			if len(data) == 0 {
				t.Error("Expected non-empty result")
			}

			var result interface{}
			if err := Unmarshal(data, &result); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}
		})
	}
}

func TestNativeArena_UseAfterDestroy(t *testing.T) {
	arena := NewArena()
	arena.Destroy()

	data := arena.Alloc(100)
	if data != nil {
		t.Error("Expected nil when allocating from destroyed arena")
	}
}

func TestNativeArena_DoubleDestroy(t *testing.T) {
	arena := NewArena()
	arena.Destroy()
	arena.Destroy()
}

func TestMarshalNative_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	numGoroutines := 100
	iterations := 100

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
				native, err := MarshalNative(data)
				if err != nil {
					t.Errorf("MarshalNative failed: %v", err)
					return
				}
				if native.Len() == 0 {
					t.Error("Expected non-empty result")
					native.Dispose()
					return
				}
				bytes := native.Bytes()
				if len(bytes) == 0 {
					t.Error("Expected non-empty bytes")
				}
				native.Dispose()
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkMarshalNative(b *testing.B) {
	data := map[string]interface{}{
		"id":        12345,
		"name":      "benchmark test",
		"values":    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"nested":    map[string]string{"key": "value"},
		"timestamp": int64(1234567890),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		native, _ := MarshalNative(data)
		native.Dispose()
	}
}

func BenchmarkMarshalStandard(b *testing.B) {
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

func BenchmarkMarshalNative_Parallel(b *testing.B) {
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
			native, _ := MarshalNative(data)
			native.Dispose()
		}
	})
}

func BenchmarkMarshalToArena(b *testing.B) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	data := map[string]interface{}{
		"id":        12345,
		"name":      "benchmark test",
		"values":    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"nested":    map[string]string{"key": "value"},
		"timestamp": int64(1234567890),
	}

	arena := NewArena()
	defer arena.Destroy()

	b.ResetTimer()
	b.SetParallelism(1)
	for i := 0; i < b.N; i++ {
		_, _ = MarshalToArena(arena, data)
	}
}
