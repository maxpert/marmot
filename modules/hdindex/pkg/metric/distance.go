package metric

import "math"

// L2Squared computes the squared Euclidean distance between two vectors.
// Panics if len(a) != len(b).
func L2Squared(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
	var sum float32
	for i := range a {
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

// DotProduct computes the inner product of two vectors.
// Panics if len(a) != len(b).
func DotProduct(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// CosineSimilarity computes the cosine similarity between two vectors.
// Returns a value in [-1, 1]. Returns 0 if either vector has near-zero norm.
// Panics if len(a) != len(b).
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("metric: mismatched vector lengths")
	}
	na := Norm(a)
	nb := Norm(b)
	denom := na * nb
	if denom == 0 {
		return 0
	}
	return DotProduct(a, b) / denom
}

// Norm computes the L2 norm of a vector.
func Norm(v []float32) float32 {
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	return float32(math.Sqrt(float64(sum)))
}
