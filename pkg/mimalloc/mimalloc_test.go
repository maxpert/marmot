//go:build cgo
// +build cgo

package mimalloc

import (
	"testing"
	"unsafe"
)

func TestMalloc(t *testing.T) {
	ptr := Malloc(1024)
	if ptr == nil {
		t.Fatal("Malloc returned nil")
	}
	defer Free(ptr)

	// Write some data to verify the memory is usable
	slice := unsafe.Slice((*byte)(ptr), 1024)
	for i := range slice {
		slice[i] = byte(i % 256)
	}

	// Verify data
	for i := range slice {
		if slice[i] != byte(i%256) {
			t.Fatalf("Memory corruption at index %d", i)
		}
	}
}

func TestCalloc(t *testing.T) {
	ptr := Calloc(100, 10)
	if ptr == nil {
		t.Fatal("Calloc returned nil")
	}
	defer Free(ptr)

	// Verify zero-initialized
	slice := unsafe.Slice((*byte)(ptr), 1000)
	for i := range slice {
		if slice[i] != 0 {
			t.Fatalf("Calloc memory not zero at index %d: got %d", i, slice[i])
		}
	}
}

func TestRealloc(t *testing.T) {
	ptr := Malloc(100)
	if ptr == nil {
		t.Fatal("Malloc returned nil")
	}

	// Write pattern
	slice := unsafe.Slice((*byte)(ptr), 100)
	for i := range slice {
		slice[i] = byte(i)
	}

	// Realloc to larger size
	ptr = Realloc(ptr, 200)
	if ptr == nil {
		t.Fatal("Realloc returned nil")
	}
	defer Free(ptr)

	// Verify original data preserved
	slice = unsafe.Slice((*byte)(ptr), 100)
	for i := range slice {
		if slice[i] != byte(i) {
			t.Fatalf("Data not preserved after Realloc at index %d", i)
		}
	}
}

func TestMSize(t *testing.T) {
	ptr := Malloc(100)
	if ptr == nil {
		t.Fatal("Malloc returned nil")
	}
	defer Free(ptr)

	size := MSize(ptr)
	if size < 100 {
		t.Fatalf("MSize returned %d, expected at least 100", size)
	}
}

func TestArena(t *testing.T) {
	arena := ArenaCreate()
	if arena == nil {
		t.Fatal("ArenaCreate returned nil")
	}

	// Allocate multiple blocks
	ptrs := make([]unsafe.Pointer, 10)
	for i := range ptrs {
		ptrs[i] = ArenaMalloc(arena, 1024)
		if ptrs[i] == nil {
			t.Fatalf("ArenaMalloc returned nil for allocation %d", i)
		}

		// Write pattern
		slice := unsafe.Slice((*byte)(ptrs[i]), 1024)
		for j := range slice {
			slice[j] = byte((i + j) % 256)
		}
	}

	// Verify all allocations
	for i, ptr := range ptrs {
		slice := unsafe.Slice((*byte)(ptr), 1024)
		for j := range slice {
			if slice[j] != byte((i+j)%256) {
				t.Fatalf("Memory corruption in arena allocation %d at index %d", i, j)
			}
		}
	}

	// Destroy arena (frees all allocations)
	ArenaDestroy(arena)
}

func TestArenaCalloc(t *testing.T) {
	arena := ArenaCreate()
	if arena == nil {
		t.Fatal("ArenaCreate returned nil")
	}
	defer ArenaDestroy(arena)

	ptr := ArenaCalloc(arena, 100, 10)
	if ptr == nil {
		t.Fatal("ArenaCalloc returned nil")
	}

	// Verify zero-initialized
	slice := unsafe.Slice((*byte)(ptr), 1000)
	for i := range slice {
		if slice[i] != 0 {
			t.Fatalf("ArenaCalloc memory not zero at index %d", i)
		}
	}
}

func BenchmarkMalloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ptr := Malloc(256)
		Free(ptr)
	}
}

func BenchmarkArenaMalloc(b *testing.B) {
	arena := ArenaCreate()
	defer ArenaDestroy(arena)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ArenaMalloc(arena, 256)
	}
}
