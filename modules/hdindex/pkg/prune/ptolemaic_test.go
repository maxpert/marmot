package prune

import (
	"math"
	"math/rand"
	"testing"
)

// l2Dist computes the Euclidean distance between two float32 vectors.
func l2Dist(a, b []float32) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i] - b[i])
		sum += d * d
	}
	return float32(math.Sqrt(sum))
}

// buildRefPairDists builds the flat upper-triangle pairwise distance array for a set of reference vectors.
func buildRefPairDists(refs [][]float32) []float32 {
	m := len(refs)
	out := make([]float32, m*(m-1)/2)
	for i := 0; i < m-1; i++ {
		for j := i + 1; j < m; j++ {
			out[RefPairIndex(i, j, m)] = l2Dist(refs[i], refs[j])
		}
	}
	return out
}

func TestPtolemaicLowerBound_Basic(t *testing.T) {
	t.Parallel()
	// Simple 2D geometry with 2 reference points.
	// q=(0,0), o=(3,4), R0=(1,0), R1=(0,1)
	// d(q,o)=5, d(q,R0)=1, d(q,R1)=1, d(o,R0)=sqrt(4+16)=sqrt(20), d(o,R1)=sqrt(9+9)=sqrt(18)
	// d(R0,R1)=sqrt(2)
	// Ptolemaic: |d(q,R0)*d(o,R1) - d(q,R1)*d(o,R0)| / d(R0,R1)
	//           = |1*sqrt(18) - 1*sqrt(20)| / sqrt(2)
	dqR0 := float32(1.0)
	dqR1 := float32(1.0)
	doR0 := float32(math.Sqrt(20))
	doR1 := float32(math.Sqrt(18))
	dR0R1 := float32(math.Sqrt(2))
	queryRefDists := []float32{dqR0, dqR1}
	candidateRefDists := []float32{doR0, doR1}
	refPairDists := []float32{dR0R1}
	bound := PtolemaicLowerBound(queryRefDists, candidateRefDists, refPairDists)
	trueD := float32(5.0)
	if bound > trueD+1e-4 {
		t.Errorf("Ptolemaic lower bound %v exceeds true distance %v", bound, trueD)
	}
	if bound <= 0 {
		t.Errorf("expected positive Ptolemaic lower bound, got %v", bound)
	}
}

func TestPtolemaicLowerBound_IsLowerBound(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42))
	const (
		trials = 1000
		dim    = 8
		m      = 4
	)
	randVec := func() []float32 {
		v := make([]float32, dim)
		for i := range v {
			v[i] = rng.Float32()*10 - 5
		}
		return v
	}
	for trial := range trials {
		q := randVec()
		o := randVec()
		refs := make([][]float32, m)
		for j := range refs {
			refs[j] = randVec()
		}
		qDists := make([]float32, m)
		oDists := make([]float32, m)
		for j := range refs {
			qDists[j] = l2Dist(q, refs[j])
			oDists[j] = l2Dist(o, refs[j])
		}
		refPairDists := buildRefPairDists(refs)
		bound := PtolemaicLowerBound(qDists, oDists, refPairDists)
		trueD := l2Dist(q, o)
		if bound > trueD+1e-4 {
			t.Errorf("trial %d: Ptolemaic lower bound %v > true distance %v", trial, bound, trueD)
		}
	}
}

func TestPtolemaicLowerBound_TighterThanTriangle(t *testing.T) {
	t.Parallel()
	// With m=6 reference objects, the Ptolemaic bound has more pairs to maximise
	// over than the triangle bound, so it reliably exceeds it in >80% of cases.
	rng := rand.New(rand.NewSource(42))
	const (
		trials = 1000
		dim    = 8
		m      = 6
	)
	randVec := func() []float32 {
		v := make([]float32, dim)
		for i := range v {
			v[i] = rng.Float32()*10 - 5
		}
		return v
	}
	ptolGeqTriangle := 0
	for range trials {
		q := randVec()
		o := randVec()
		refs := make([][]float32, m)
		for j := range refs {
			refs[j] = randVec()
		}
		qDists := make([]float32, m)
		oDists := make([]float32, m)
		for j := range refs {
			qDists[j] = l2Dist(q, refs[j])
			oDists[j] = l2Dist(o, refs[j])
		}
		refPairDists := buildRefPairDists(refs)
		ptol := PtolemaicLowerBound(qDists, oDists, refPairDists)
		tri := TriangleLowerBound(qDists, oDists)
		if ptol >= tri-1e-6 {
			ptolGeqTriangle++
		}
	}
	pct := float64(ptolGeqTriangle) / trials * 100
	if pct < 80.0 {
		t.Errorf("Ptolemaic bound >= Triangle bound in only %.1f%% of cases, want >= 80%%", pct)
	}
}

func TestPtolemaicPrune_ReducesCandidates(t *testing.T) {
	t.Parallel()
	const (
		n     = 100
		gamma = 10
		m     = 4
	)
	rng := rand.New(rand.NewSource(7))
	queryRefDists := make([]float32, m)
	for i := range queryRefDists {
		queryRefDists[i] = rng.Float32() * 5
	}
	refs := make([][]float32, m)
	for i := range refs {
		v := make([]float32, 4)
		for j := range v {
			v[j] = rng.Float32() * 5
		}
		refs[i] = v
	}
	refPairDists := buildRefPairDists(refs)
	candidates := make([]Candidate, n)
	for i := range candidates {
		dists := make([]float32, m)
		for j := range dists {
			dists[j] = rng.Float32() * 5
		}
		candidates[i] = Candidate{DocID: uint64(i), RefDists: dists}
	}
	out := PtolemaicPrune(queryRefDists, candidates, refPairDists, gamma)
	if len(out) != gamma {
		t.Errorf("expected %d candidates, got %d", gamma, len(out))
	}
}

func TestPtolemaicPrune_Ordering(t *testing.T) {
	t.Parallel()
	const (
		n     = 50
		gamma = 20
		m     = 3
	)
	rng := rand.New(rand.NewSource(13))
	queryRefDists := make([]float32, m)
	for i := range queryRefDists {
		queryRefDists[i] = rng.Float32() * 5
	}
	refs := make([][]float32, m)
	for i := range refs {
		v := make([]float32, 3)
		for j := range v {
			v[j] = rng.Float32() * 5
		}
		refs[i] = v
	}
	refPairDists := buildRefPairDists(refs)
	candidates := make([]Candidate, n)
	for i := range candidates {
		dists := make([]float32, m)
		for j := range dists {
			dists[j] = rng.Float32() * 5
		}
		candidates[i] = Candidate{DocID: uint64(i), RefDists: dists}
	}
	out := PtolemaicPrune(queryRefDists, candidates, refPairDists, gamma)
	for i := 1; i < len(out); i++ {
		prev := PtolemaicLowerBound(queryRefDists, out[i-1].RefDists, refPairDists)
		curr := PtolemaicLowerBound(queryRefDists, out[i].RefDists, refPairDists)
		if curr < prev-1e-6 {
			t.Errorf("output not sorted at index %d: bound[%d]=%v > bound[%d]=%v", i, i-1, prev, i, curr)
		}
	}
}

func TestRefPairIndex(t *testing.T) {
	t.Parallel()
	const m = 10
	tests := []struct {
		i, j int
		want int
	}{
		{0, 1, 0},
		{0, 2, 1},
		{0, 9, 8},
		{1, 2, 9},
		{1, 3, 10},
		{8, 9, m*(m-1)/2 - 1},
	}
	for _, tc := range tests {
		got := RefPairIndex(tc.i, tc.j, m)
		if got != tc.want {
			t.Errorf("RefPairIndex(%d, %d, %d) = %d, want %d", tc.i, tc.j, m, got, tc.want)
		}
	}
}

func BenchmarkPtolemaicPrune_256candidates_10refs(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	const (
		n     = 256
		gamma = 64
		m     = 10
	)
	queryRefDists := make([]float32, m)
	for i := range queryRefDists {
		queryRefDists[i] = rng.Float32() * 5
	}
	refs := make([][]float32, m)
	for i := range refs {
		v := make([]float32, 16)
		for j := range v {
			v[j] = rng.Float32() * 5
		}
		refs[i] = v
	}
	refPairDists := buildRefPairDists(refs)
	candidates := make([]Candidate, n)
	for i := range candidates {
		dists := make([]float32, m)
		for j := range dists {
			dists[j] = rng.Float32() * 5
		}
		candidates[i] = Candidate{DocID: uint64(i), RefDists: dists}
	}
	b.ResetTimer()
	for range b.N {
		PtolemaicPrune(queryRefDists, candidates, refPairDists, gamma)
	}
}
