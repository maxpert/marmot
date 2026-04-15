package metric

import (
	"math/rand"
	"sort"
	"testing"
)

func TestAugmentData_AppendsSqrtDim(t *testing.T) {
	t.Parallel()
	v := []float32{3, 4} // ||v|| = 5
	maxNorm := float32(10)
	aug, err := AugmentData(v, maxNorm, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(aug) != 3 {
		t.Fatalf("expected length 3, got %d", len(aug))
	}
	// extra dim = sqrt(100 - 25) = sqrt(75) ≈ 8.66025
	want := float32(8.660254)
	if abs32(aug[2]-want) > 1e-4 {
		t.Errorf("extra dim: got %v, want %v", aug[2], want)
	}
}

func TestAugmentData_NormExceedsMaxNorm_ReturnsError(t *testing.T) {
	t.Parallel()
	v := []float32{3, 4} // ||v|| = 5
	_, err := AugmentData(v, 4, nil)
	if err == nil {
		t.Fatal("expected error for norm > maxNorm, got nil")
	}
}

func TestAugmentQuery_AppendsZero(t *testing.T) {
	t.Parallel()
	q := []float32{1, 2, 3}
	aug := AugmentQuery(q, nil)
	if len(aug) != 4 {
		t.Fatalf("expected length 4, got %d", len(aug))
	}
	if aug[3] != 0 {
		t.Errorf("expected zero appended, got %v", aug[3])
	}
}

func TestAugmentQuery_ReusesBuffer(t *testing.T) {
	t.Parallel()
	q := []float32{1, 2}
	buf := make([]float32, 10)
	aug := AugmentQuery(q, buf)
	if &aug[0] != &buf[0] {
		t.Error("expected buffer to be reused")
	}
}

func TestMIPS_RecoverDotAccuracy(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42))
	const maxNorm = float32(10)
	const dim = 64

	for i := 0; i < 100; i++ {
		q := randVecT(rng, dim, 0.5)
		v := randVecT(rng, dim, 0.5)
		scaleToNormT(q, 8.0)
		scaleToNormT(v, 7.0)

		qAug := AugmentQuery(q, nil)
		vAug, err := AugmentData(v, maxNorm, nil)
		if err != nil {
			t.Fatalf("AugmentData: %v", err)
		}

		l2sq := L2Squared(qAug, vAug)
		qNorm2 := Norm2(q)
		dot := RecoverDotFromL2Sq(l2sq, qNorm2, maxNorm)

		want := DotProduct(q, v)
		if abs32(dot-want) > 1e-3 {
			t.Errorf("iter %d: recovered dot %v, direct dot %v, diff %v", i, dot, want, abs32(dot-want))
		}
	}
}

func TestMIPS_DataRoundtripPreservesInnerProductOrder(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(99))
	const maxNorm = float32(20)
	const dim = 32
	const n = 100

	q := randVecT(rng, dim, 0.5)
	scaleToNormT(q, 10.0)

	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = randVecT(rng, dim, 0.5)
		scaleToNormT(vecs[i], float32(rng.Float64()*15+1))
	}

	type indexedDot struct {
		idx int
		dot float32
	}
	directDots := make([]indexedDot, n)
	for i, v := range vecs {
		directDots[i] = indexedDot{i, DotProduct(q, v)}
	}
	sort.Slice(directDots, func(a, b int) bool { return directDots[a].dot > directDots[b].dot })

	qAug := AugmentQuery(q, nil)
	type indexedL2 struct {
		idx int
		l2  float32
	}
	augL2 := make([]indexedL2, n)
	for i, v := range vecs {
		vAug, err := AugmentData(v, maxNorm, nil)
		if err != nil {
			t.Fatalf("AugmentData[%d]: %v", i, err)
		}
		augL2[i] = indexedL2{i, L2Squared(qAug, vAug)}
	}
	sort.Slice(augL2, func(a, b int) bool { return augL2[a].l2 < augL2[b].l2 })

	topK := 10
	for rank := 0; rank < topK; rank++ {
		if directDots[rank].idx != augL2[rank].idx {
			t.Errorf("rank %d: dot order idx=%d, L2 order idx=%d",
				rank, directDots[rank].idx, augL2[rank].idx)
		}
	}
}

func TestNorm2(t *testing.T) {
	t.Parallel()
	v := []float32{3, 4}
	if n2 := Norm2(v); abs32(n2-25) > 1e-6 {
		t.Errorf("Norm2([3,4]) = %v, want 25", n2)
	}
}

func randVecT(rng *rand.Rand, dim int, scale float64) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64() * scale)
	}
	return v
}

func scaleToNormT(v []float32, targetNorm float32) {
	n := Norm(v)
	if n == 0 {
		return
	}
	inv := targetNorm / n
	for i := range v {
		v[i] *= inv
	}
}

func abs32(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
