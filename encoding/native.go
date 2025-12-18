//go:build cgo
// +build cgo

package encoding

import (
	"sync/atomic"
	"unsafe"

	"github.com/maxpert/marmot/pkg/mimalloc"
)

// NativeBytes represents memory allocated outside the Go heap.
// It must be explicitly disposed when no longer needed.
type NativeBytes interface {
	Bytes() []byte
	Dispose()
	Len() int
}

// NativeArena represents a memory arena for batch allocations.
// All allocations are freed when the arena is destroyed.
type NativeArena interface {
	Alloc(size int) []byte
	Destroy()
}

// nativeBytes implements NativeBytes using mimalloc.
type nativeBytes struct {
	ptr      unsafe.Pointer
	len      int
	disposed atomic.Bool
}

// nativeArena implements NativeArena using mimalloc arenas.
type nativeArena struct {
	arena     unsafe.Pointer
	destroyed atomic.Bool
}

// MarshalNative marshals a value to msgpack format using native memory allocation.
// The returned NativeBytes must be explicitly disposed when no longer needed.
func MarshalNative(v interface{}) (NativeBytes, error) {
	entry := encoderPool.Get().(*encoderPoolEntry)
	entry.buf.Reset()

	if err := entry.enc.Encode(v); err != nil {
		encoderPool.Put(entry)
		return nil, err
	}

	size := entry.buf.Len()
	ptr := mimalloc.Malloc(uintptr(size))
	if ptr == nil {
		encoderPool.Put(entry)
		return nil, &MarshalError{msg: "mimalloc allocation failed"}
	}

	slice := unsafe.Slice((*byte)(ptr), size)
	copy(slice, entry.buf.Bytes())
	encoderPool.Put(entry)

	return &nativeBytes{
		ptr: ptr,
		len: size,
	}, nil
}

// MarshalToArena marshals a value to msgpack format using arena memory allocation.
// The returned slice is valid until the arena is destroyed.
func MarshalToArena(arena NativeArena, v interface{}) ([]byte, error) {
	entry := encoderPool.Get().(*encoderPoolEntry)
	entry.buf.Reset()

	if err := entry.enc.Encode(v); err != nil {
		encoderPool.Put(entry)
		return nil, err
	}

	size := entry.buf.Len()
	slice := arena.Alloc(size)
	if slice == nil {
		encoderPool.Put(entry)
		return nil, &MarshalError{msg: "arena allocation failed"}
	}

	copy(slice, entry.buf.Bytes())
	encoderPool.Put(entry)

	return slice, nil
}

// NewArena creates a new memory arena for batch allocations.
func NewArena() NativeArena {
	return &nativeArena{
		arena: mimalloc.ArenaCreate(),
	}
}

// Bytes returns the underlying byte slice.
// Panics if the memory has been disposed.
func (nb *nativeBytes) Bytes() []byte {
	if nb.disposed.Load() {
		panic("use after dispose: NativeBytes has been disposed")
	}
	return unsafe.Slice((*byte)(nb.ptr), nb.len)
}

// Dispose frees the native memory.
// Safe to call multiple times (no-op after first call).
func (nb *nativeBytes) Dispose() {
	if nb.disposed.CompareAndSwap(false, true) {
		mimalloc.Free(nb.ptr)
		nb.ptr = nil
	}
}

// Len returns the length of the byte slice.
func (nb *nativeBytes) Len() int {
	return nb.len
}

// Alloc allocates a byte slice from the arena.
func (na *nativeArena) Alloc(size int) []byte {
	if na.destroyed.Load() {
		return nil
	}
	ptr := mimalloc.ArenaMalloc(na.arena, uintptr(size))
	if ptr == nil {
		return nil
	}
	return unsafe.Slice((*byte)(ptr), size)
}

// Destroy frees all memory allocated from the arena.
// Safe to call multiple times (no-op after first call).
func (na *nativeArena) Destroy() {
	if na.destroyed.CompareAndSwap(false, true) {
		mimalloc.ArenaDestroy(na.arena)
		na.arena = nil
	}
}

// MarshalError represents a marshaling error.
type MarshalError struct {
	msg string
}

func (e *MarshalError) Error() string {
	return e.msg
}
