package kmeans

import (
	"math/rand"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// sseSorted returns the sum of squared errors: Σ min_j D²(x[i], centroids[j]).
func sseSorted(vectors [][]float32, centroids [][]float32) float64 {
	var total float64
	for _, v := range vectors {
		best := float64(metric.L2Squared(v, centroids[0]))
		for j := 1; j < len(centroids); j++ {
			if d := float64(metric.L2Squared(v, centroids[j])); d < best {
				best = d
			}
		}
		total += best
	}
	return total
}

// TestKMeansParallelInit_SmallKQualityParity verifies that the parallel
// init + Lloyd pipeline produces SSE within 1.1× of the reference
// k-means++ + Lloyd pipeline on a small-k/small-dim problem.
//
// Thresholds are derived from the Bahmani 2012 theoretical bound (O(log k)
// in expectation, ~1% average in practice) with a conservative 10% margin
// for test robustness against per-seed variance.
func TestKMeansParallelInit_SmallKQualityParity(t *testing.T) {
	t.Parallel()
	const (
		n    = 1000
		d    = 16
		k    = 32
		seed = int64(2024)
	)
	vecs := syntheticVectors(rand.New(rand.NewSource(seed)), n, d, "clustered")

	rngRef := rand.New(rand.NewSource(foldSeed(uint64(seed))))
	refInit := kMeansPlusPlusInitReference(vecs, k, d, rngRef)
	refCentroids := lloydIterations(vecs, refInit, k, d, 50, rngRef)

	rngPar := rand.New(rand.NewSource(foldSeed(uint64(seed))))
	parInit := kMeansParallelInit(vecs, k, d, rngPar)
	parCentroids := lloydIterations(vecs, parInit, k, d, 50, rngPar)

	sseRef := sseSorted(vecs, refCentroids)
	ssePar := sseSorted(vecs, parCentroids)
	ratio := ssePar / sseRef
	if ratio > 1.10 {
		t.Fatalf("SSE parity violated: parallel=%g reference=%g ratio=%.3f > 1.10", ssePar, sseRef, ratio)
	}
	t.Logf("small-k SSE parity ratio=%.3f (parallel=%g vs reference=%g)", ratio, ssePar, sseRef)
}

// TestKMeansParallelInit_LargeKQualityParity asserts the same contract at
// the larger (n, k) the parallel path is actually designed for. Wider SSE
// margin (1.15×) to absorb per-seed variance at scale.
func TestKMeansParallelInit_LargeKQualityParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skip large-k quality parity under -short")
	}
	t.Parallel()
	const (
		n    = 50_000
		d    = 32
		k    = 512
		seed = int64(8675309)
	)
	vecs := syntheticVectors(rand.New(rand.NewSource(seed)), n, d, "clustered")

	rngRef := rand.New(rand.NewSource(foldSeed(uint64(seed))))
	refInit := kMeansPlusPlusInitReference(vecs, k, d, rngRef)
	refCentroids := lloydIterations(vecs, refInit, k, d, 20, rngRef)

	rngPar := rand.New(rand.NewSource(foldSeed(uint64(seed))))
	parInit := kMeansParallelInit(vecs, k, d, rngPar)
	parCentroids := lloydIterations(vecs, parInit, k, d, 20, rngPar)

	sseRef := sseSorted(vecs, refCentroids)
	ssePar := sseSorted(vecs, parCentroids)
	ratio := ssePar / sseRef
	if ratio > 1.15 {
		t.Fatalf("SSE parity violated: parallel=%g reference=%g ratio=%.3f > 1.15", ssePar, sseRef, ratio)
	}
	t.Logf("large-k SSE parity ratio=%.3f (parallel=%g vs reference=%g)", ratio, ssePar, sseRef)
}

// TestKMeansParallelInit_EdgeCases covers the boundary conditions of the
// parallel path: all-duplicate vectors (ψ=0), all-zero vectors, and a
// cluster-heavy distribution.
func TestKMeansParallelInit_EdgeCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		init func() ([][]float32, int, int)
	}{
		{"all-zero", func() ([][]float32, int, int) {
			n, d := 500, 8
			vs := make([][]float32, n)
			for i := range vs {
				vs[i] = make([]float32, d)
			}
			return vs, 64, d
		}},
		{"all-duplicate", func() ([][]float32, int, int) {
			n, d := 500, 8
			base := []float32{1, 2, 3, 4, 5, 6, 7, 8}
			vs := make([][]float32, n)
			for i := range vs {
				cp := make([]float32, d)
				copy(cp, base)
				vs[i] = cp
			}
			return vs, 64, d
		}},
		{"small-clustered-large-k", func() ([][]float32, int, int) {
			vs := syntheticVectors(rand.New(rand.NewSource(1)), 2000, 16, "clustered")
			return vs, 128, 16
		}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vs, k, d := tc.init()
			rng := rand.New(rand.NewSource(foldSeed(42)))
			got := kMeansParallelInit(vs, k, d, rng)
			if len(got) != k {
				t.Fatalf("got %d centroids, want %d", len(got), k)
			}
			for i, c := range got {
				if len(c) != d {
					t.Fatalf("centroid %d has dim %d, want %d", i, len(c), d)
				}
			}
		})
	}
}
