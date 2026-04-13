package prune

import "sort"

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
// queryRefDists: distances from query to each reference object.
// candidates: slice of candidates with their reference distances.
// beta: number of candidates to keep.
// Returns the pruned candidates sorted by triangle lower bound (ascending).
func TrianglePrune(queryRefDists []float32, candidates []Candidate, beta int) []Candidate {
	type scored struct {
		c     Candidate
		bound float32
	}

	list := make([]scored, len(candidates))
	for i, c := range candidates {
		list[i] = scored{c: c, bound: TriangleLowerBound(queryRefDists, c.RefDists)}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].bound < list[j].bound
	})

	keep := min(beta, len(list))
	out := make([]Candidate, keep)
	for i := range out {
		out[i] = list[i].c
	}
	return out
}
