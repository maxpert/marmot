package refobj

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/maxpert/marmot/modules/hdindex/pkg/metric"
)

// makeVectors generates n random dim-dimensional float32 vectors.
func makeVectors(n, dim int, seed int64) [][]float32 {
	rng := rand.New(rand.NewPCG(uint64(seed), 42))
	vecs := make([][]float32, n)
	for i := range n {
		v := make([]float32, dim)
		for j := range dim {
			v[j] = rng.Float32()*2 - 1
		}
		vecs[i] = v
	}
	return vecs
}

func TestPairIndex(t *testing.T) {
	t.Parallel()
	cases := []struct {
		i, j, m int
		want    int
	}{
		{0, 1, 10, 0},
		{0, 9, 10, 8},
		{8, 9, 10, 44},
	}
	for _, tc := range cases {
		got := PairIndex(tc.i, tc.j, tc.m)
		if got != tc.want {
			t.Errorf("PairIndex(%d,%d,%d) = %d, want %d", tc.i, tc.j, tc.m, got, tc.want)
		}
	}
}

func TestSelectSSS_Basic(t *testing.T) {
	t.Parallel()
	vecs := makeVectors(1000, 16, 1)
	rs, err := SelectSSS(vecs, 10, 0.3, 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rs.M != 10 {
		t.Fatalf("M = %d, want 10", rs.M)
	}
	if len(rs.Vectors) != 10 {
		t.Fatalf("len(Vectors) = %d, want 10", len(rs.Vectors))
	}
	// All pairwise distances must be > 0
	for i := range rs.M {
		for j := i + 1; j < rs.M; j++ {
			d := metric.L2(rs.Vectors[i], rs.Vectors[j])
			if d <= 0 {
				t.Errorf("refs[%d] and refs[%d] are identical (d=%.6f)", i, j, d)
			}
		}
	}
}

func TestSelectSSS_Spacing(t *testing.T) {
	t.Parallel()
	vecs := makeVectors(1000, 16, 2)
	const (
		m = 10
		f = 0.3
	)
	rs, err := SelectSSS(vecs, m, f, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dmax := EstimateDMax(vecs, 7)
	threshold := float32(f) * dmax

	// Count how many pairs satisfy the threshold — greedy relaxation may not
	// guarantee all pairs, but the selection should produce well-spread objects
	// (i.e., average pairwise distance is well above threshold).
	var sum float32
	count := 0
	for i := range rs.M {
		for j := i + 1; j < rs.M; j++ {
			d := metric.L2(rs.Vectors[i], rs.Vectors[j])
			sum += d
			count++
		}
	}
	avg := sum / float32(count)
	if avg < threshold {
		t.Errorf("average pairwise distance %.4f < threshold %.4f; refs are not well-spread", avg, threshold)
	}
}

func TestSelectSSS_Deterministic(t *testing.T) {
	t.Parallel()
	vecs := makeVectors(500, 32, 3)
	rs1, err := SelectSSS(vecs, 8, 0.3, 99)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	rs2, err := SelectSSS(vecs, 8, 0.3, 99)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	for i := range rs1.M {
		for j, v := range rs1.Vectors[i] {
			if v != rs2.Vectors[i][j] {
				t.Fatalf("refs[%d][%d] differ: %.6f vs %.6f", i, j, v, rs2.Vectors[i][j])
			}
		}
	}
}

func TestSelectSSS_TooFewVectors(t *testing.T) {
	t.Parallel()
	vecs := makeVectors(5, 8, 4)
	_, err := SelectSSS(vecs, 10, 0.3, 1)
	if err == nil {
		t.Fatal("expected error for len(vectors) < m, got nil")
	}
}

func TestSelectSSS_PairDists(t *testing.T) {
	t.Parallel()
	vecs := makeVectors(200, 8, 5)
	const m = 6
	rs, err := SelectSSS(vecs, m, 0.3, 11)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantLen := m * (m - 1) / 2
	if len(rs.PairDists) != wantLen {
		t.Fatalf("len(PairDists) = %d, want %d", len(rs.PairDists), wantLen)
	}

	// Verify each entry matches a manual L2 computation.
	for i := range m {
		for j := i + 1; j < m; j++ {
			want := metric.L2(rs.Vectors[i], rs.Vectors[j])
			got := rs.PairDists[PairIndex(i, j, m)]
			if math.Abs(float64(got-want)) > 1e-5 {
				t.Errorf("PairDists[%d,%d] = %.6f, want %.6f", i, j, got, want)
			}
		}
	}
}

func TestComputeRefDists(t *testing.T) {
	t.Parallel()
	// Construct a known geometry in 2D.
	// refs at (0,0), (3,0), (0,4)
	// query at (1,1):
	//   dist to (0,0) = sqrt(1+1)   = sqrt(2)
	//   dist to (3,0) = sqrt(4+1)   = sqrt(5)
	//   dist to (0,4) = sqrt(1+9)   = sqrt(10)
	refs := [][]float32{
		{0, 0},
		{3, 0},
		{0, 4},
	}
	pairDists := computePairDists(refs, 3)
	rs := &ReferenceSet{Vectors: refs, PairDists: pairDists, M: 3}

	query := []float32{1, 1}
	got := rs.ComputeRefDists(query)
	if len(got) != 3 {
		t.Fatalf("expected 3 distances, got %d", len(got))
	}

	want := []float32{
		float32(math.Sqrt(2)),
		float32(math.Sqrt(5)),
		float32(math.Sqrt(10)),
	}
	for i, w := range want {
		if math.Abs(float64(got[i]-w)) > 1e-5 {
			t.Errorf("dist[%d] = %.6f, want %.6f", i, got[i], w)
		}
	}
}

func TestEstimateDMax(t *testing.T) {
	t.Parallel()
	// Build a 1D dataset in [0, 10]: true diameter is 10.
	// Populate densely so the heuristic is likely to find a near-optimal pair.
	vecs := make([][]float32, 100)
	for i := range 100 {
		vecs[i] = []float32{float32(i) / 10.0}
	}
	dmax := EstimateDMax(vecs, 1)
	// True max is distance(0, 9.9) = 9.9; heuristic should get within 10%.
	if dmax < 8.9 {
		t.Errorf("EstimateDMax = %.4f; expected >= 8.9 for a [0,9.9] 1D dataset", dmax)
	}
}

func BenchmarkSelectSSS_10000vecs_128dim(b *testing.B) {
	vecs := makeVectors(10000, 128, 0)
	b.ResetTimer()
	for range b.N {
		_, err := SelectSSS(vecs, 20, 0.3, 42)
		if err != nil {
			b.Fatal(err)
		}
	}
}
