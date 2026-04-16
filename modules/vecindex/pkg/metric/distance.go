// Package metric provides SIMD-accelerated vector distance functions.
package metric

import (
	"math"
	"unsafe"

	"github.com/tphakala/simd/f32"
)

// bytesToFloat32Slice reinterprets a little-endian float32 byte slice as []float32
// without copying.  Valid only when:
//   - len(b) is a multiple of 4
//   - b is 4-byte aligned (Pebble value buffers satisfy this invariant)
//   - the host is little-endian (arm64 and amd64 are both LE)
//
// The caller must NOT retain the returned slice beyond the lifetime of b.
func bytesToFloat32Slice(b []byte) []float32 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4)
}

// L2Squared computes the squared Euclidean distance between two vectors.
// Implementation is the pure-Go 4-wide unrolled loop (L2SquaredGo) — the Go
// compiler auto-vectorises it effectively and measurement shows the SIMD
// sqrt-then-square alternative is NOT 2× faster at the dims used in
// k-means/IVF (32, 128, 1536); at dim=32 it is materially slower due to
// per-call dispatch overhead and the extra sqrt instruction.
//
// Measured arm64 / Apple M3 Pro, go1.26:
//
//	dim=32:   Go 8.45 ns/op  vs SIMD 16.57 ns/op  (SIMD 0.51×)
//	dim=128:  Go 31.35 ns/op vs SIMD 38.76 ns/op  (SIMD 0.81×)
//	dim=1536: Go 389.0 ns/op vs SIMD 380.0 ns/op  (SIMD 1.02×)
//
// L2SquaredSIMD remains exported for callers that want to experiment, and
// L2SquaredGo remains exported as an explicit pure-Go reference.
//
// Panics if len(a) != len(b).
func L2Squared(a, b []float32) float32 {
	return L2SquaredGo(a, b)
}

// L2SquaredSIMD computes the squared Euclidean distance via SIMD-accelerated
// f32.EuclideanDistance. Returns d*d where d = sqrt(sum(diff²)).
// Loses ~1 ULP of precision vs L2SquaredGo due to the sqrt-then-square
// roundtrip. Panics if len(a) != len(b).
func L2SquaredSIMD(a, b []float32) float32 {
	assertEqualLen(a, b)
	d := f32.EuclideanDistance(a, b)
	return d * d
}

// L2SquaredGo is the pure-Go 4-wide unrolled reference implementation of the
// squared Euclidean distance. Reliable auto-vectorised baseline used by
// L2Squared as the default, and kept exported so SIMD parity tests have a
// named reference.
// Panics if len(a) != len(b).
func L2SquaredGo(a, b []float32) float32 {
	assertEqualLen(a, b)
	n := len(a)
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+3 < n; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// L2 computes the Euclidean distance using SIMD (NEON on arm64, AVX on amd64).
// Panics if len(a) != len(b).
func L2(a, b []float32) float32 {
	assertEqualLen(a, b)
	return f32.EuclideanDistance(a, b)
}

// DotProduct computes the inner product using SIMD (NEON on arm64, AVX on amd64).
// Panics if len(a) != len(b).
func DotProduct(a, b []float32) float32 {
	assertEqualLen(a, b)
	return f32.DotProduct(a, b)
}

// CosineSimilarity computes the cosine similarity between two vectors.
// Returns a value in [-1, 1]. Returns 0 if either vector has near-zero norm
// or if the norm product underflows to zero.
// Uses SIMD-accelerated dot product and sum for norm computation.
// Panics if len(a) != len(b).
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
	na := norm(a)
	nb := norm(b)
	denom := na * nb
	if denom == 0 {
		return 0
	}
	return f32.DotProduct(a, b) / denom
}

// Norm computes the L2 norm of a vector using SIMD-accelerated dot product.
func Norm(v []float32) float32 {
	return norm(v)
}

// norm computes sqrt(sum(v[i]^2)) via SIMD self-dot-product.
func norm(v []float32) float32 {
	return float32(math.Sqrt(float64(f32.DotProduct(v, v))))
}

// Distance dispatches to the correct distance function based on the Metric
// enum. Returned values are always "smaller means closer":
//   - MetricL2     → squared Euclidean distance
//   - MetricDot    → negative inner product
//   - MetricCosine → 1 - cosine similarity
//
// Panics if len(a) != len(b) or if m is not a known Metric value.
func Distance(m Metric, a, b []float32) float32 {
	switch m {
	case MetricL2:
		return L2Squared(a, b)
	case MetricDot:
		return -DotProduct(a, b)
	case MetricCosine:
		return 1 - CosineSimilarity(a, b)
	default:
		panic("metric: unknown Metric value")
	}
}

// L2SquaredFromBytes computes squared Euclidean distance between query and a
// vector encoded as raw little-endian float32 bytes. Uses unsafe zero-copy
// reinterpretation of vecBytes as []float32 then delegates to L2Squared, which
// applies a 4-wide unrolled loop.  Valid only on little-endian hosts (arm64,
// amd64); Pebble value buffers are 4-byte aligned by construction.
// The caller must NOT retain vecBytes beyond the call — this matches the
// ScanClusterFunc invariant.
// Panics if len(vecBytes)/4 != len(query).
func L2SquaredFromBytes(query []float32, vecBytes []byte) float32 {
	assertFromBytesLen(query, vecBytes)
	return L2Squared(query, bytesToFloat32Slice(vecBytes))
}

// DotFromBytes computes the negative inner product between query and a vector
// encoded as raw little-endian float32 bytes. Negative because smaller means closer.
// Uses unsafe zero-copy reinterpretation then delegates to SIMD DotProduct.
// The caller must NOT retain vecBytes beyond the call.
// Panics if len(vecBytes)/4 != len(query).
func DotFromBytes(query []float32, vecBytes []byte) float32 {
	assertFromBytesLen(query, vecBytes)
	return -f32.DotProduct(query, bytesToFloat32Slice(vecBytes))
}

// CosineFromBytes computes cosine distance (1 - cosine_similarity) between query
// and a vector encoded as raw little-endian float32 bytes.
// Uses unsafe zero-copy reinterpretation then delegates to CosineSimilarity,
// which uses SIMD-accelerated dot product and norm computation.
// The caller must NOT retain vecBytes beyond the call.
// Panics if len(vecBytes)/4 != len(query).
func CosineFromBytes(query []float32, vecBytes []byte) float32 {
	assertFromBytesLen(query, vecBytes)
	return 1 - CosineSimilarity(query, bytesToFloat32Slice(vecBytes))
}

// DistanceFromBytes dispatches to the correct FromBytes distance function.
// Returned values are always "smaller means closer". The caller must NOT retain
// vecBytes beyond the call — this matches the ScanClusterFunc iterator invariant.
// Panics if m is unknown or if len(vecBytes)/4 != len(query).
func DistanceFromBytes(m Metric, query []float32, vecBytes []byte) float32 {
	switch m {
	case MetricL2:
		return L2SquaredFromBytes(query, vecBytes)
	case MetricDot:
		return DotFromBytes(query, vecBytes)
	case MetricCosine:
		return CosineFromBytes(query, vecBytes)
	default:
		panic("metric: unknown Metric value")
	}
}

func assertEqualLen(a, b []float32) {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
}

func assertFromBytesLen(query []float32, vecBytes []byte) {
	if len(vecBytes) != len(query)*4 {
		panic("metric: vecBytes length does not match query dimension")
	}
}
