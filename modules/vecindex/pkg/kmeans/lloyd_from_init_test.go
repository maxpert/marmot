package kmeans_test

import (
	"math"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// TestLloydFromInit_DeterministicWarmStart locks the contract from design
// §8.3 step 2 (fix G): k-means during REINDEX initialises from driftState
// centroids rather than running k-means++ afresh. Given the same inputs,
// repeated calls must produce byte-identical output.
func TestLloydFromInit_DeterministicWarmStart(t *testing.T) {
	t.Parallel()

	vecs := [][]float32{
		{1, 0}, {0.9, 0.1}, {1.1, -0.1},
		{0, 1}, {0.1, 0.9}, {-0.1, 1.1},
	}
	seed := [][]float32{
		{1, 0},
		{0, 1},
	}
	a, err := kmeans.LloydFromInit(vecs, seed, 7, 20)
	if err != nil {
		t.Fatal(err)
	}
	b, err := kmeans.LloydFromInit(vecs, seed, 7, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatalf("len mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			t.Fatalf("centroid %d len mismatch", i)
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				t.Fatalf("centroid %d[%d] mismatch: %v vs %v", i, j, a[i][j], b[i][j])
			}
		}
	}
	// Confirm we actually got k centroids back.
	if len(a) != len(seed) {
		t.Fatalf("output centroid count = %d, want %d", len(a), len(seed))
	}
}

// TestLloydFromInit_ConvergesToCentroids: two well-separated clusters seeded
// near the true means should converge to centroids close to the true means.
// This proves warm-start is doing real Lloyd iterations, not just passing
// the seed through.
func TestLloydFromInit_ConvergesToCentroids(t *testing.T) {
	t.Parallel()

	var vecs [][]float32
	// Cluster around (10,10)
	for i := 0; i < 20; i++ {
		x := 10 + float32(i%3)*0.1
		y := 10 + float32((i/3)%3)*0.1
		vecs = append(vecs, []float32{x, y})
	}
	// Cluster around (-10,-10)
	for i := 0; i < 20; i++ {
		x := -10 + float32(i%3)*0.1
		y := -10 + float32((i/3)%3)*0.1
		vecs = append(vecs, []float32{x, y})
	}

	// Seed slightly off from the true means — Lloyd should pull them in.
	seed := [][]float32{{9, 9}, {-11, -11}}

	cs, err := kmeans.LloydFromInit(vecs, seed, 42, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 2 {
		t.Fatalf("got %d centroids, want 2", len(cs))
	}

	// Each centroid should be within 1 unit of one of the two true means.
	closeTo := func(c []float32, tx, ty float32) bool {
		dx := float64(c[0] - tx)
		dy := float64(c[1] - ty)
		return math.Sqrt(dx*dx+dy*dy) < 1
	}
	matched := 0
	for _, c := range cs {
		if closeTo(c, 10, 10) || closeTo(c, -10, -10) {
			matched++
		}
	}
	if matched != 2 {
		t.Fatalf("centroids %v did not converge near the two cluster means", cs)
	}
}

// TestLloydFromInit_PreservesCentroidCount proves that warm-start is
// count-stable: driftState with k centroids must yield exactly k centroids
// after warm-start, or the §8.3 swap step will produce a mismatched
// member-cluster universe.
func TestLloydFromInit_PreservesCentroidCount(t *testing.T) {
	t.Parallel()

	vecs := [][]float32{
		{1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0},
		{10, 0}, {11, 0}, {12, 0}, {13, 0}, {14, 0},
		{20, 0}, {21, 0}, {22, 0},
	}
	seed := [][]float32{{1, 0}, {10, 0}, {20, 0}}

	cs, err := kmeans.LloydFromInit(vecs, seed, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 3 {
		t.Fatalf("len=%d, want 3", len(cs))
	}
}

// TestLloydFromInit_InputValidation covers the failure modes the REINDEX
// pipeline may stumble on — empty staging sample, empty drift seed,
// dimension mismatch between drift and sample.
func TestLloydFromInit_InputValidation(t *testing.T) {
	t.Parallel()

	t.Run("empty vectors", func(t *testing.T) {
		_, err := kmeans.LloydFromInit(nil, [][]float32{{1, 0}}, 0, 10)
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("empty init centroids", func(t *testing.T) {
		_, err := kmeans.LloydFromInit([][]float32{{1, 0}}, nil, 0, 10)
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("dim mismatch", func(t *testing.T) {
		_, err := kmeans.LloydFromInit(
			[][]float32{{1, 0, 0}},
			[][]float32{{1, 0}}, // dim=2 vs vector dim=3
			0, 10,
		)
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("maxIter zero", func(t *testing.T) {
		_, err := kmeans.LloydFromInit([][]float32{{1, 0}}, [][]float32{{0.5, 0}}, 0, 0)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

// sanity: ensure metric import still round-trips — guards against unused
// import churn when refactoring.
var _ = metric.MetricL2
