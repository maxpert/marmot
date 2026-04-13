package prune

import (
	"math"
	"math/rand"
	"testing"
)

func TestTriangleLowerBound_Basic(t *testing.T) {
	t.Parallel()
	// q at (0,0), o at (3,4), R at (1,0)
	// d(q,R) = 1, d(o,R) = sqrt((3-1)^2 + 16) = sqrt(20) ≈ 4.472
	// triangle bound = |1 - 4.472| ≈ 3.472
	// true distance d(q,o) = 5 >= bound ✓
	dqR := float32(1.0)
	doR := float32(math.Sqrt(20))
	queryRefDists := []float32{dqR}
	candidateRefDists := []float32{doR}
	bound := TriangleLowerBound(queryRefDists, candidateRefDists)
	expected := float32(math.Abs(float64(dqR - doR)))
	if math.Abs(float64(bound-expected)) > 1e-5 {
		t.Errorf("TriangleLowerBound = %v, want ≈ %v", bound, expected)
	}
	trueD := float32(5.0)
	if bound > trueD+1e-5 {
		t.Errorf("lower bound %v exceeds true distance %v", bound, trueD)
	}
}

func TestTriangleLowerBound_MultipleRefs(t *testing.T) {
	t.Parallel()
	// Use 3 reference objects with known distances.
	// Verify that the returned bound is the maximum of the per-ref bounds.
	queryRefDists := []float32{1.0, 3.0, 2.0}
	candidateRefDists := []float32{4.0, 3.5, 6.0}
	// per-ref bounds: |1-4|=3, |3-3.5|=0.5, |2-6|=4
	// max = 4
	got := TriangleLowerBound(queryRefDists, candidateRefDists)
	want := float32(4.0)
	if math.Abs(float64(got-want)) > 1e-6 {
		t.Errorf("TriangleLowerBound = %v, want %v", got, want)
	}
}

func TestTriangleLowerBound_IsLowerBound(t *testing.T) {
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
	l2 := func(a, b []float32) float32 {
		var sum float64
		for i := range a {
			d := float64(a[i] - b[i])
			sum += d * d
		}
		return float32(math.Sqrt(sum))
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
			qDists[j] = l2(q, refs[j])
			oDists[j] = l2(o, refs[j])
		}
		bound := TriangleLowerBound(qDists, oDists)
		trueD := l2(q, o)
		if bound > trueD+1e-4 {
			t.Errorf("trial %d: lower bound %v > true distance %v", trial, bound, trueD)
		}
	}
}

func TestTrianglePrune_ReducesCandidates(t *testing.T) {
	t.Parallel()
	const (
		n    = 100
		beta = 10
		m    = 4
	)
	rng := rand.New(rand.NewSource(7))
	queryRefDists := make([]float32, m)
	for i := range queryRefDists {
		queryRefDists[i] = rng.Float32() * 5
	}
	candidates := make([]Candidate, n)
	for i := range candidates {
		dists := make([]float32, m)
		for j := range dists {
			dists[j] = rng.Float32() * 5
		}
		candidates[i] = Candidate{DocID: uint64(i), RefDists: dists}
	}
	out := TrianglePrune(queryRefDists, candidates, beta)
	if len(out) != beta {
		t.Errorf("expected %d candidates, got %d", beta, len(out))
	}
}

func TestTrianglePrune_Ordering(t *testing.T) {
	t.Parallel()
	const (
		n    = 50
		beta = 20
		m    = 3
	)
	rng := rand.New(rand.NewSource(13))
	queryRefDists := make([]float32, m)
	for i := range queryRefDists {
		queryRefDists[i] = rng.Float32() * 5
	}
	candidates := make([]Candidate, n)
	for i := range candidates {
		dists := make([]float32, m)
		for j := range dists {
			dists[j] = rng.Float32() * 5
		}
		candidates[i] = Candidate{DocID: uint64(i), RefDists: dists}
	}
	out := TrianglePrune(queryRefDists, candidates, beta)
	for i := 1; i < len(out); i++ {
		prev := TriangleLowerBound(queryRefDists, out[i-1].RefDists)
		curr := TriangleLowerBound(queryRefDists, out[i].RefDists)
		if curr < prev-1e-6 {
			t.Errorf("output not sorted at index %d: bound[%d]=%v > bound[%d]=%v", i, i-1, prev, i, curr)
		}
	}
}

func TestTrianglePrune_BetaGreaterThanInput(t *testing.T) {
	t.Parallel()
	const m = 3
	rng := rand.New(rand.NewSource(99))
	queryRefDists := make([]float32, m)
	for i := range queryRefDists {
		queryRefDists[i] = rng.Float32() * 5
	}
	candidates := make([]Candidate, 5)
	for i := range candidates {
		dists := make([]float32, m)
		for j := range dists {
			dists[j] = rng.Float32() * 5
		}
		candidates[i] = Candidate{DocID: uint64(i), RefDists: dists}
	}
	out := TrianglePrune(queryRefDists, candidates, 100)
	if len(out) != len(candidates) {
		t.Errorf("expected all %d candidates, got %d", len(candidates), len(out))
	}
}
