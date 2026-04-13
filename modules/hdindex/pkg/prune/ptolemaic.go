package prune

import "sort"

// PtolemaicLowerBound computes the Ptolemaic inequality lower bound on d(q, o).
// queryRefDists: distances from query to each reference object (length m).
// candidateRefDists: distances from candidate to each reference object (length m).
// refPairDists: pairwise distances between reference objects.
//
//	Stored as a flat array in upper-triangle order: for m refs,
//	index of (i,j) where i<j is: i*m - i*(i+1)/2 + j - i - 1
//	Total length: m*(m-1)/2
//
// Returns the tightest (maximum) lower bound across all C(m,2) pairs.
func PtolemaicLowerBound(queryRefDists, candidateRefDists, refPairDists []float32) float32 {
	m := len(queryRefDists)
	var best float32
	for i := 0; i < m-1; i++ {
		for j := i + 1; j < m; j++ {
			rij := refPairDists[RefPairIndex(i, j, m)]
			if rij == 0 {
				continue
			}
			num := queryRefDists[i]*candidateRefDists[j] - queryRefDists[j]*candidateRefDists[i]
			if num < 0 {
				num = -num
			}
			lb := num / rij
			if lb > best {
				best = lb
			}
		}
	}
	return best
}

// PtolemaicPrune filters candidates using the Ptolemaic inequality.
// Keeps the top-gamma candidates with the smallest Ptolemaic lower bounds.
// queryRefDists: distances from query to each reference object.
// candidates: slice of candidates with their reference distances.
// refPairDists: pairwise distances between reference objects (m*(m-1)/2 floats).
// gamma: number of candidates to keep.
// Returns the pruned candidates sorted by Ptolemaic lower bound (ascending).
func PtolemaicPrune(queryRefDists []float32, candidates []Candidate, refPairDists []float32, gamma int) []Candidate {
	type scored struct {
		c     Candidate
		bound float32
	}

	scoredList := make([]scored, len(candidates))
	for i, c := range candidates {
		scoredList[i] = scored{c: c, bound: PtolemaicLowerBound(queryRefDists, c.RefDists, refPairDists)}
	}

	sort.Slice(scoredList, func(i, j int) bool {
		return scoredList[i].bound < scoredList[j].bound
	})

	keep := min(gamma, len(candidates))
	out := make([]Candidate, keep)
	for i := range out {
		out[i] = scoredList[i].c
	}
	return out
}

// RefPairIndex computes the flat array index for the pair (i, j) where i < j.
// m is the number of reference objects.
func RefPairIndex(i, j, m int) int {
	return i*m - i*(i+1)/2 + j - i - 1
}
