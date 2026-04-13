package encoding

import (
	"encoding/binary"
	"math"
)

// DecodeFloat32Slice decodes a raw byte slice as IEEE-754 little-endian float32 values.
func DecodeFloat32Slice(b []byte) []float32 {
	n := len(b) / 4
	out := make([]float32, n)
	for i := range n {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}

// EncodeFloat32Slice encodes a float32 slice as IEEE-754 little-endian bytes.
func EncodeFloat32Slice(v []float32) []byte {
	out := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(f))
	}
	return out
}
