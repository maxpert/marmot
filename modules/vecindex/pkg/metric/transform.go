// Package metric — transform.go
// MIPS-to-L2 reduction via dimension augmentation (Bachrach et al. 2014).
//
// Inner product ⟨q,v⟩ is not a metric, but we can reduce it to L2 by augmenting
// both data and query vectors by one dimension:
//
//	data:  v' = [v, sqrt(M² - ||v||²)]  ∈ R^(d+1)
//	query: q' = [q, 0]                  ∈ R^(d+1)
//
// Then ||q' - v'||² = ||q||² + M² - 2⟨q,v⟩, so minimising L2² on augmented
// vectors is equivalent to maximising inner product on original vectors.
// M is a fixed upper bound on vector norms (IVFSpec.MaxNorm).
package metric

import (
	"errors"
	"math"
)

// AugmentData augments a data vector for MIPS→L2 reduction.
// Returns [v..., sqrt(maxNorm² - ||v||²)] ∈ R^(d+1), written into dst.
// dst is grown if needed and returned. Returns an error if ||v|| > maxNorm.
func AugmentData(v []float32, maxNorm float32, dst []float32) ([]float32, error) {
	norm2 := Norm2(v)
	norm := float32(math.Sqrt(float64(norm2)))
	if norm > maxNorm {
		return nil, errors.New("metric: vector norm exceeds MaxNorm; re-create index with a larger MaxNorm")
	}
	needed := len(v) + 1
	if cap(dst) >= needed {
		dst = dst[:needed]
	} else {
		dst = make([]float32, needed)
	}
	copy(dst, v)
	dst[len(v)] = float32(math.Sqrt(float64(maxNorm*maxNorm - norm2)))
	return dst, nil
}

// AugmentQuery augments a query vector for MIPS→L2 reduction.
// Returns [q..., 0] ∈ R^(d+1), written into dst.
// dst is grown if needed and returned.
func AugmentQuery(q []float32, dst []float32) []float32 {
	needed := len(q) + 1
	if cap(dst) >= needed {
		dst = dst[:needed]
	} else {
		dst = make([]float32, needed)
	}
	copy(dst, q)
	dst[len(q)] = 0
	return dst
}

// RecoverDotFromL2Sq recovers ⟨q,v⟩ from augmented L2² distance and norms:
//
//	||q' - v'||² = ||q||² + M² - 2⟨q,v⟩
//	⟨q,v⟩       = (||q||² + M² - L2²_aug) / 2
func RecoverDotFromL2Sq(l2sqAug, queryNorm2, maxNorm float32) float32 {
	return (queryNorm2 + maxNorm*maxNorm - l2sqAug) / 2
}

// Norm2 returns ||v||² (sum of squares).
func Norm2(v []float32) float32 {
	var s float32
	for _, x := range v {
		s += x * x
	}
	return s
}
