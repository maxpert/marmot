package prune

import "slices"

// Candidate represents a candidate vector with its precomputed reference distances.
type Candidate struct {
	DocID    uint64
	RefDists []float32 // distances to each reference object, length m
}

// TriangleLowerBound computes the triangle inequality lower bound on d(q, o).
// queryRefDists: distances from query to each reference object (length m).
// candidateRefDists: distances from candidate to each reference object (length m).
// Returns the tightest (maximum) lower bound across all m reference objects.
func TriangleLowerBound(queryRefDists, candidateRefDists []float32) float32 {
	var best float32
	for i, qd := range queryRefDists {
		cd := candidateRefDists[i]
		diff := qd - cd
		if diff < 0 {
			diff = -diff
		}
		if diff > best {
			best = diff
		}
	}
	return best
}

// TrianglePrune filters candidates using the triangle inequality.
// Keeps the top-beta candidates with the smallest triangle lower bounds.
// Returns the pruned candidates sorted by triangle lower bound (ascending).
func TrianglePrune(queryRefDists []float32, candidates []Candidate, beta int) []Candidate {
	n := len(candidates)
	keep := min(beta, n)
	if keep == 0 {
		return nil
	}

	indices := TrianglePruneIndices(queryRefDists, candidates, beta)

	out := make([]Candidate, len(indices))
	for i, idx := range indices {
		out[i] = candidates[idx]
	}
	return out
}

// TrianglePruneIndices computes triangle lower bounds and returns the indices
// (into candidates) of the top-beta entries with the smallest bounds.
// Uses index-based sorting to avoid moving Candidate structs during sort,
// eliminating reflection overhead and GC write barriers.
func TrianglePruneIndices(queryRefDists []float32, candidates []Candidate, beta int) []int {
	n := len(candidates)
	keep := min(beta, n)
	if keep == 0 {
		return nil
	}

	bounds := make([]float32, n)
	indices := make([]int, n)
	for i := range n {
		bounds[i] = TriangleLowerBound(queryRefDists, candidates[i].RefDists)
		indices[i] = i
	}

	// Sort indices by bound value. Swapping 8-byte ints is ~4x faster than
	// swapping 32-byte Candidate structs with GC write barriers.
	slices.SortFunc(indices, func(a, b int) int {
		ba, bb := bounds[a], bounds[b]
		if ba < bb {
			return -1
		}
		if ba > bb {
			return 1
		}
		return 0
	})

	return indices[:keep]
}

// TrianglePruneRefDists filters ref distance slices directly (without requiring
// Candidate construction). Returns indices into the refDists slice of the
// top-beta entries with the smallest triangle lower bounds.
func TrianglePruneRefDists(queryRefDists []float32, refDists [][]float32, beta int) []int {
	n := len(refDists)
	keep := min(beta, n)
	if keep == 0 {
		return nil
	}

	bounds := make([]float32, n)
	indices := make([]int, n)
	for i := range n {
		bounds[i] = TriangleLowerBound(queryRefDists, refDists[i])
		indices[i] = i
	}

	slices.SortFunc(indices, func(a, b int) int {
		ba, bb := bounds[a], bounds[b]
		if ba < bb {
			return -1
		}
		if ba > bb {
			return 1
		}
		return 0
	})

	return indices[:keep]
}
