//go:build !cgo
// +build !cgo

package encoding

import "sync/atomic"

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

// goBytes implements NativeBytes using regular Go allocations.
type goBytes struct {
	data     []byte
	disposed atomic.Bool
}

// goArena implements NativeArena using regular Go allocations.
type goArena struct {
	allocations [][]byte
	destroyed   atomic.Bool
}

// MarshalNative falls back to regular Marshal when CGO is not available.
func MarshalNative(v interface{}) (NativeBytes, error) {
	data, err := Marshal(v)
	if err != nil {
		return nil, err
	}
	return &goBytes{data: data}, nil
}

// MarshalToArena falls back to regular Marshal when CGO is not available.
func MarshalToArena(arena NativeArena, v interface{}) ([]byte, error) {
	data, err := Marshal(v)
	if err != nil {
		return nil, err
	}
	return arena.Alloc(len(data)), nil
}

// NewArena creates a new memory arena using regular Go allocations.
func NewArena() NativeArena {
	return &goArena{
		allocations: make([][]byte, 0, 16),
	}
}

// Bytes returns the underlying byte slice.
// Panics if the memory has been disposed.
func (gb *goBytes) Bytes() []byte {
	if gb.disposed.Load() {
		panic("use after dispose: NativeBytes has been disposed")
	}
	return gb.data
}

// Dispose clears the reference to the byte slice.
// Safe to call multiple times (no-op after first call).
func (gb *goBytes) Dispose() {
	if gb.disposed.CompareAndSwap(false, true) {
		gb.data = nil
	}
}

// Len returns the length of the byte slice.
func (gb *goBytes) Len() int {
	return len(gb.data)
}

// Alloc allocates a byte slice from the arena.
func (ga *goArena) Alloc(size int) []byte {
	if ga.destroyed.Load() {
		return nil
	}
	data := make([]byte, size)
	ga.allocations = append(ga.allocations, data)
	return data
}

// Destroy clears all allocations from the arena.
// Safe to call multiple times (no-op after first call).
func (ga *goArena) Destroy() {
	if ga.destroyed.CompareAndSwap(false, true) {
		ga.allocations = nil
	}
}
