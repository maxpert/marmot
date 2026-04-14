// Package metric provides SIMD-accelerated vector distance functions.
package metric

import (
	"math"

	"github.com/tphakala/simd/f32"
)

// L2Squared computes the squared Euclidean distance between two vectors.
// Uses a 4-wide unrolled loop to enable auto-vectorization and avoid the
// precision loss of sqrt(x)² ≠ x in float32.
// Panics if len(a) != len(b).
func L2Squared(a, b []float32) float32 {
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

func assertEqualLen(a, b []float32) {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
}
