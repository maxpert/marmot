// Package quant implements scalar int8 quantization for IVF posting lists.
// Layout per stored vector (SQ8):
//
//	[scale    float32 (4 bytes)]
//	[sqNorm2  float32 (4 bytes)]  // ||vec||^2 in original float32 space
//	[codes    int8[dim]         ]
//
// Total: 8 + dim bytes vs 4*dim for float32 — ~3.9× smaller at dim=1536.
package quant

import (
	"encoding/binary"
	"math"
)

// headerSize is the fixed prefix before the int8 code array.
const headerSize = 8

// Vector holds the quantized representation of a single float32 vector.
type Vector struct {
	Scale   float32
	SqNorm2 float32
	Codes   []int8
}

// Encode quantizes v using symmetric scalar int8 quantization.
// maxAbs(v) maps to ±127; scale = maxAbs/127.
// SqNorm2 is ||v||^2 in the original float32 space.
func Encode(v []float32) Vector {
	var out Vector
	EncodeInto(v, &out)
	return out
}

// EncodeInto quantizes v into dst, reusing dst.Codes if it has sufficient capacity.
func EncodeInto(v []float32, dst *Vector) {
	n := len(v)

	var maxAbs float32
	for _, x := range v {
		a := x
		if a < 0 {
			a = -a
		}
		if a > maxAbs {
			maxAbs = a
		}
	}

	var scale float32
	if maxAbs > 0 {
		scale = maxAbs / 127.0
	} else {
		scale = 1.0
	}

	if cap(dst.Codes) >= n {
		dst.Codes = dst.Codes[:n]
	} else {
		dst.Codes = make([]int8, n)
	}

	inv := float32(1.0) / scale
	var sqNorm2 float32
	for i, x := range v {
		q := x * inv
		if q > 127 {
			q = 127
		} else if q < -127 {
			q = -127
		}
		dst.Codes[i] = int8(q)
		sqNorm2 += x * x
	}

	dst.Scale = scale
	dst.SqNorm2 = sqNorm2
}

// Decode reconstructs approximate float32 values from a quantized vector.
// For debugging/tests only — not on the hot path.
func Decode(q Vector) []float32 {
	out := make([]float32, len(q.Codes))
	for i, c := range q.Codes {
		out[i] = float32(c) * q.Scale
	}
	return out
}

// EncodedSize returns the byte length of a marshaled SQ8 vector for the given dim.
func EncodedSize(dim int) int {
	return headerSize + dim
}

// MarshalBinary writes q into dst (which must have len >= 8+len(q.Codes)).
// Returns the sub-slice dst[:8+len(q.Codes)].
// Callers may pass a pre-allocated buffer to avoid allocation.
func MarshalBinary(q Vector, dst []byte) []byte {
	needed := headerSize + len(q.Codes)
	if cap(dst) < needed {
		dst = make([]byte, needed)
	}
	dst = dst[:needed]
	binary.LittleEndian.PutUint32(dst[0:4], math.Float32bits(q.Scale))
	binary.LittleEndian.PutUint32(dst[4:8], math.Float32bits(q.SqNorm2))
	for i, c := range q.Codes {
		dst[headerSize+i] = byte(c)
	}
	return dst
}

// UnmarshalHeader decodes scale and sqNorm2 from raw SQ8 bytes without copying
// the code array. codesOffset is always headerSize (8).
// raw must have len >= headerSize.
func UnmarshalHeader(raw []byte) (scale float32, sqNorm2 float32, codesOffset int) {
	scale = math.Float32frombits(binary.LittleEndian.Uint32(raw[0:4]))
	sqNorm2 = math.Float32frombits(binary.LittleEndian.Uint32(raw[4:8]))
	codesOffset = headerSize
	return
}

// DotInt8Bytes computes the inner product of q (int8 slice) with codes
// (raw byte slice of int8 values). Returns accumulation as int32.
// q and codes must have equal length.
func DotInt8Bytes(q []int8, codes []byte) int32 {
	n := len(q)
	var s0, s1, s2, s3 int32
	i := 0
	for ; i+3 < n; i += 4 {
		s0 += int32(q[i]) * int32(int8(codes[i]))
		s1 += int32(q[i+1]) * int32(int8(codes[i+1]))
		s2 += int32(q[i+2]) * int32(int8(codes[i+2]))
		s3 += int32(q[i+3]) * int32(int8(codes[i+3]))
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		sum += int32(q[i]) * int32(int8(codes[i]))
	}
	return sum
}

// L2SquaredFromSQ8 computes approximate squared L2 distance between a query
// (already encoded as SQ8) and a stored SQ8-encoded vector (raw bytes).
//
// Uses the identity:
//
//	||a - b||^2 = ||a||^2 + ||b||^2 - 2*<a,b>
//
// where <a,b> ≈ scale_q * scale_v * dot(q_int8, v_int8).
// vecBytes must have len >= 8+dim.
func L2SquaredFromSQ8(query Vector, vecBytes []byte) float32 {
	scaleV, sqNorm2V, off := UnmarshalHeader(vecBytes)
	codes := vecBytes[off:]
	dot := DotInt8Bytes(query.Codes, codes)
	crossTerm := query.Scale * scaleV * float32(dot) * 2
	return query.SqNorm2 + sqNorm2V - crossTerm
}

// CosineFromSQ8 computes cosine distance (1 - cos_similarity) between a query
// (already encoded as SQ8) and a stored SQ8-encoded vector (raw bytes).
// Both query and stored vector are assumed pre-normalized (||v||=1 in float32 space),
// as is the convention for MetricCosine in this index.
//
// cos_similarity ≈ scale_q * scale_v * dot(q_int8, v_int8)
// (SqNorm2 = 1 for both since vectors are pre-normalized.)
// vecBytes must have len >= 8+dim.
func CosineFromSQ8(query Vector, vecBytes []byte) float32 {
	scaleV, _, off := UnmarshalHeader(vecBytes)
	codes := vecBytes[off:]
	dot := DotInt8Bytes(query.Codes, codes)
	cos := query.Scale * scaleV * float32(dot)
	d := 1 - cos
	if d < 0 {
		return 0
	}
	return d
}

// DotFromSQ8 computes the negative inner product (for MetricDot) between a query
// (already encoded as SQ8) and a stored SQ8-encoded vector (raw bytes).
// Returns -dot as a distance (smaller = more similar).
// vecBytes must have len >= 8+dim.
func DotFromSQ8(query Vector, vecBytes []byte) float32 {
	scaleV, _, off := UnmarshalHeader(vecBytes)
	codes := vecBytes[off:]
	dot := DotInt8Bytes(query.Codes, codes)
	return -(query.Scale * scaleV * float32(dot))
}
