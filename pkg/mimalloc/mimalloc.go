//go:build cgo
// +build cgo

// Package mimalloc provides Go bindings for the mimalloc memory allocator.
// mimalloc is a compact general purpose allocator with excellent performance.
package mimalloc

/*
#cgo CFLAGS: -I${SRCDIR}/c/include -I${SRCDIR}/c/src -DMI_MALLOC_OVERRIDE=0
#cgo arm LDFLAGS: -latomic
#include "c/src/static.c"
*/
import "C"
import (
	"unsafe"
)

// Malloc allocates unmanaged memory of the given size in bytes.
func Malloc(size uintptr) unsafe.Pointer {
	return unsafe.Pointer(C.mi_malloc(C.size_t(size)))
}

// Calloc allocates a contiguous block of unmanaged memory.
// Total amount in bytes is count multiplied by size.
func Calloc(count uintptr, size uintptr) unsafe.Pointer {
	return unsafe.Pointer(C.mi_calloc(C.size_t(count), C.size_t(size)))
}

// Realloc changes the size of a previously allocated block of memory.
// Returns the new location on success.
func Realloc(ptr unsafe.Pointer, size uintptr) unsafe.Pointer {
	return unsafe.Pointer(C.mi_realloc(ptr, C.size_t(size)))
}

// Free deallocates a block of memory.
func Free(ptr unsafe.Pointer) {
	C.mi_free(ptr)
}

// MSize returns the size of an allocated block of unmanaged memory.
func MSize(ptr unsafe.Pointer) uintptr {
	return uintptr(C.mi_usable_size(ptr))
}

// ArenaCreate creates a new arena (heap). An arena groups a set of memory allocations.
func ArenaCreate() unsafe.Pointer {
	return unsafe.Pointer(C.mi_heap_new())
}

// ArenaDestroy destroys the given arena. All allocated memory of this arena will be also freed.
func ArenaDestroy(arena unsafe.Pointer) {
	C.mi_heap_destroy((*C.mi_heap_t)(arena))
}

// ArenaMalloc allocates memory like Malloc but in the given arena.
func ArenaMalloc(arena unsafe.Pointer, size uintptr) unsafe.Pointer {
	return unsafe.Pointer(C.mi_heap_malloc((*C.mi_heap_t)(arena), C.size_t(size)))
}

// ArenaCalloc allocates memory like Calloc but in the given arena.
func ArenaCalloc(arena unsafe.Pointer, count uintptr, size uintptr) unsafe.Pointer {
	return unsafe.Pointer(C.mi_heap_calloc((*C.mi_heap_t)(arena), C.size_t(count), C.size_t(size)))
}

// ArenaRealloc re-allocates memory like Realloc but in the given arena.
func ArenaRealloc(arena unsafe.Pointer, ptr unsafe.Pointer, size uintptr) unsafe.Pointer {
	return unsafe.Pointer(C.mi_heap_realloc((*C.mi_heap_t)(arena), ptr, C.size_t(size)))
}

// ArenaFree deallocates a block of memory in the given arena.
// Note: mimalloc allows freeing with just the pointer, arena is ignored.
func ArenaFree(_ unsafe.Pointer, ptr unsafe.Pointer) {
	C.mi_free(ptr)
}

// ArenaMSize returns the size of an allocated block of unmanaged memory.
func ArenaMSize(_ unsafe.Pointer, ptr unsafe.Pointer) uintptr {
	return uintptr(C.mi_usable_size(ptr))
}
