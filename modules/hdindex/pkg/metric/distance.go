package metric

import (
	"math"

	"github.com/viterin/vek/vek32"
)

// L2Squared computes the squared Euclidean distance between two vectors.
// Uses 4-wide manual unrolling for auto-vectorization on both amd64 and arm64.
// Panics if len(a) != len(b).
func L2Squared(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
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

// L2 computes the Euclidean distance (square root of L2Squared).
// Panics if len(a) != len(b).
func L2(a, b []float32) float32 {
	return float32(math.Sqrt(float64(L2Squared(a, b))))
}

// DotProduct computes the inner product of two vectors using SIMD-optimized vek.
// Panics if len(a) != len(b).
func DotProduct(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
	return vek32.Dot(a, b)
}

// CosineSimilarity computes the cosine similarity between two vectors.
// Returns a value in [-1, 1]. Returns 0 if either vector has near-zero norm
// or if the norm product underflows to zero.
// Panics if len(a) != len(b).
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
	na := vek32.Norm(a)
	nb := vek32.Norm(b)
	denom := na * nb
	if denom == 0 {
		return 0
	}
	return vek32.Dot(a, b) / denom
}

// Norm computes the L2 norm of a vector using SIMD-optimized vek.
func Norm(v []float32) float32 {
	return vek32.Norm(v)
}
