package refobj

import (
	"errors"
	"math/rand/v2"

	"github.com/maxpert/marmot/modules/hdindex/pkg/metric"
)

// ReferenceSet holds the selected reference objects and their pairwise distances.
type ReferenceSet struct {
	Vectors   [][]float32 // m reference vectors
	PairDists []float32   // pairwise L2 distances, upper-triangle flat array, length m*(m-1)/2
	M         int         // number of reference objects
}

// PairIndex computes the flat upper-triangle array index for pair (i, j) where i < j.
// The mapping is row-major: row i starts at index i*(2m-i-1)/2.
func PairIndex(i, j, m int) int {
	// upper triangle: index = i*(2m - i - 1)/2 + (j - i - 1)
	return i*(2*m-i-1)/2 + (j - i - 1)
}

// EstimateDMax estimates the maximum pairwise distance in the dataset using the
// farthest-point heuristic with 5 iterations.
func EstimateDMax(vectors [][]float32, seed int64) float32 {
	if len(vectors) == 0 {
		return 0
	}

	rng := rand.New(rand.NewPCG(uint64(seed), 0))
	cur := rng.IntN(len(vectors))

	var dmax float32
	for range 5 {
		best := -1
		var bestDist float32
		for i, v := range vectors {
			if i == cur {
				continue
			}
			d := metric.L2(vectors[cur], v)
			if best == -1 || d > bestDist {
				bestDist = d
				best = i
			}
		}
		if best == -1 {
			break
		}
		if bestDist > dmax {
			dmax = bestDist
		}
		cur = best
	}
	return dmax
}

// SelectSSS selects m reference objects from the given vectors using the
// Sparse Spatial Selection algorithm with fraction f.
//
// vectors: the full dataset (or a representative sample).
// m: number of reference objects to select.
// f: fraction of d_max for minimum inter-reference distance (typically 0.3).
// seed: random seed for reproducibility.
//
// Returns the ReferenceSet or an error if len(vectors) < m.
func SelectSSS(vectors [][]float32, m int, f float64, seed int64) (*ReferenceSet, error) {
	if len(vectors) < m {
		return nil, errors.New("refobj: not enough vectors to select m reference objects")
	}

	dmax := EstimateDMax(vectors, seed)
	threshold := float32(f) * dmax

	rng := rand.New(rand.NewPCG(uint64(seed), 1))
	first := rng.IntN(len(vectors))

	selected := make([]int, 0, m)
	selected = append(selected, first)

	used := make([]bool, len(vectors))
	used[first] = true

	for len(selected) < m {
		idx := findNextRef(vectors, selected, threshold, used)
		used[idx] = true
		selected = append(selected, idx)
	}

	refs := make([][]float32, m)
	for i, idx := range selected {
		refs[i] = vectors[idx]
	}

	pairDists := computePairDists(refs, m)
	return &ReferenceSet{
		Vectors:   refs,
		PairDists: pairDists,
		M:         m,
	}, nil
}

// findNextRef returns the index of the next reference object candidate.
// Prefers any object whose minimum distance to all current refs exceeds threshold.
// Falls back to the object that maximizes minimum distance to current refs.
func findNextRef(vectors [][]float32, selected []int, threshold float32, used []bool) int {
	bestIdx := -1
	var bestMinDist float32

	for i, v := range vectors {
		if used[i] {
			continue
		}
		minDist := minDistToRefs(v, vectors, selected)
		if minDist > threshold {
			// Any object satisfying threshold is acceptable; pick first found
			return i
		}
		if bestIdx == -1 || minDist > bestMinDist {
			bestMinDist = minDist
			bestIdx = i
		}
	}
	return bestIdx
}

// minDistToRefs computes the minimum L2 distance from v to the already-selected ref vectors.
func minDistToRefs(v []float32, vectors [][]float32, selected []int) float32 {
	minD := float32(-1)
	for _, idx := range selected {
		d := metric.L2(v, vectors[idx])
		if minD < 0 || d < minD {
			minD = d
		}
	}
	return minD
}

// computePairDists computes the upper-triangle pairwise L2 distances for refs.
func computePairDists(refs [][]float32, m int) []float32 {
	size := m * (m - 1) / 2
	dists := make([]float32, size)
	for i := range m {
		for j := i + 1; j < m; j++ {
			dists[PairIndex(i, j, m)] = metric.L2(refs[i], refs[j])
		}
	}
	return dists
}

// ComputeRefDists computes L2 distances from a single vector to all reference objects.
// Returns a float32 slice of length ref.M.
func (ref *ReferenceSet) ComputeRefDists(v []float32) []float32 {
	dists := make([]float32, ref.M)
	for i, r := range ref.Vectors {
		dists[i] = metric.L2(v, r)
	}
	return dists
}
