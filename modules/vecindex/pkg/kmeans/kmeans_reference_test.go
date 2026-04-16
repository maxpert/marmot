package kmeans

import (
	"math/rand"
	"runtime"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// kMeansPlusPlusInitReference is the original O(n·k²) k-means++ initialisation
// preserved here as a reference implementation for parity testing against the
// incremental O(n·k) path. Lives in a test file so production binaries do
// not pay for two copies of the init code.
//
// This is the SAME code that used to live in kmeans.go as kMeansPlusPlusInit
// before the split. Behaviour must remain bit-identical to the pre-split
// version so the byte-determinism contract (same vectors, k, seed → same
// centroids) survives the refactor.
func kMeansPlusPlusInitReference(vectors [][]float32, k, _ int, rng *rand.Rand) [][]float32 {
	n := len(vectors)
	centroids := make([][]float32, 0, k)

	first := copyVec(vectors[rng.Intn(n)])
	centroids = append(centroids, first)

	dists := make([]float64, n)

	for len(centroids) < k {
		var total float64
		for i, v := range vectors {
			minD := float64(metric.L2Squared(v, centroids[0]))
			for _, c := range centroids[1:] {
				if d := float64(metric.L2Squared(v, c)); d < minD {
					minD = d
				}
			}
			dists[i] = minD
			total += minD
		}

		target := rng.Float64() * total
		var cumulative float64
		chosen := n - 1
		for i, d := range dists {
			cumulative += d
			if cumulative >= target {
				chosen = i
				break
			}
		}
		centroids = append(centroids, copyVec(vectors[chosen]))
	}

	return centroids
}

// TestKMeansPlusPlusIncremental_ByteIdenticalToReference pins the contract
// that the O(n·k) incremental k-means++ path produces byte-identical output
// to the O(n·k²) reference. This guards the cross-node determinism contract
// at §8.1 L — if two nodes race on CREATE VECTOR INDEX with the same seed,
// both must converge to the same centroids regardless of which init path
// they take.
func TestKMeansPlusPlusIncremental_ByteIdenticalToReference(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		n, d, k  int
		seed     int64
		distType string
	}{
		{"small-uniform", 200, 8, 4, 1, "uniform"},
		{"medium-gaussian", 1000, 16, 8, 42, "gaussian"},
		{"kEqualN", 10, 4, 10, 7, "uniform"},
		{"k=1", 50, 8, 1, 99, "uniform"},
		{"k=2", 50, 8, 2, 13, "uniform"},
		{"duplicates", 100, 4, 8, 3, "duplicate"},
		{"clustered", 400, 8, 16, 5, "clustered"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vecs := syntheticVectors(rand.New(rand.NewSource(tc.seed)), tc.n, tc.d, tc.distType)

			rngA := rand.New(rand.NewSource(foldSeed(uint64(tc.seed))))
			refCentroids := kMeansPlusPlusInitReference(vecs, tc.k, tc.d, rngA)

			rngB := rand.New(rand.NewSource(foldSeed(uint64(tc.seed))))
			incCentroids := kMeansPlusPlusInitIncremental(vecs, tc.k, tc.d, rngB)

			if len(refCentroids) != len(incCentroids) {
				t.Fatalf("len mismatch: ref=%d inc=%d", len(refCentroids), len(incCentroids))
			}
			for i := range refCentroids {
				for j := range refCentroids[i] {
					if refCentroids[i][j] != incCentroids[i][j] {
						t.Fatalf("centroid %d[%d] mismatch: ref=%v inc=%v (n=%d,k=%d,seed=%d)",
							i, j, refCentroids[i][j], incCentroids[i][j], tc.n, tc.k, tc.seed)
					}
				}
			}
		})
	}
}

// TestKMeansParallelInit_DeterministicAcrossGOMAXPROCS locks the key
// determinism property of the k-means|| implementation: the same (vectors,
// k, seed) must yield byte-identical centroids regardless of how many OS
// threads are used. Without this property two peers with different
// GOMAXPROCS would produce divergent centroid sets when racing on CREATE
// VECTOR INDEX.
func TestKMeansParallelInit_DeterministicAcrossGOMAXPROCS(t *testing.T) {
	// Not t.Parallel — we mutate GOMAXPROCS.
	const (
		n    = 5000
		d    = 32
		k    = 128
		seed = int64(987654321)
	)
	vecs := syntheticVectors(rand.New(rand.NewSource(seed)), n, d, "gaussian")

	procs := []int{1, 2, 4, 8, 12}
	var baseline [][]float32
	prior := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prior)

	for _, p := range procs {
		runtime.GOMAXPROCS(p)
		rng := rand.New(rand.NewSource(foldSeed(uint64(seed))))
		got := kMeansParallelInit(vecs, k, d, rng)
		if len(got) != k {
			t.Fatalf("GOMAXPROCS=%d: got %d centroids, want %d", p, len(got), k)
		}
		if baseline == nil {
			baseline = got
			continue
		}
		if len(got) != len(baseline) {
			t.Fatalf("GOMAXPROCS=%d: len mismatch vs baseline", p)
		}
		for i := range baseline {
			for j := range baseline[i] {
				if baseline[i][j] != got[i][j] {
					t.Fatalf("GOMAXPROCS=%d: centroid %d[%d] diverged: %v vs %v",
						p, i, j, baseline[i][j], got[i][j])
				}
			}
		}
	}
}

// TestKMeansParallelInit_DeterministicSameSeed asserts repeated calls under
// fixed GOMAXPROCS are byte-identical (baseline determinism).
func TestKMeansParallelInit_DeterministicSameSeed(t *testing.T) {
	t.Parallel()
	const (
		n    = 2000
		d    = 16
		k    = 64
		seed = int64(314159)
	)
	vecs := syntheticVectors(rand.New(rand.NewSource(seed)), n, d, "gaussian")
	rngA := rand.New(rand.NewSource(foldSeed(uint64(seed))))
	a := kMeansParallelInit(vecs, k, d, rngA)
	rngB := rand.New(rand.NewSource(foldSeed(uint64(seed))))
	b := kMeansParallelInit(vecs, k, d, rngB)
	for i := range a {
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				t.Fatalf("non-deterministic: centroid %d[%d] %v vs %v", i, j, a[i][j], b[i][j])
			}
		}
	}
}

// syntheticVectors produces n vectors of dim d following the requested
// distribution. Used across init tests.
func syntheticVectors(rng *rand.Rand, n, d int, dist string) [][]float32 {
	vs := make([][]float32, n)
	switch dist {
	case "uniform":
		for i := range vs {
			v := make([]float32, d)
			for j := range v {
				v[j] = float32(rng.Float64())
			}
			vs[i] = v
		}
	case "gaussian":
		for i := range vs {
			v := make([]float32, d)
			for j := range v {
				v[j] = float32(rng.NormFloat64())
			}
			vs[i] = v
		}
	case "duplicate":
		base := make([]float32, d)
		for j := range base {
			base[j] = float32(rng.Float64())
		}
		for i := range vs {
			v := make([]float32, d)
			copy(v, base)
			vs[i] = v
		}
	case "clustered":
		// 8 gaussian blobs.
		const blobs = 8
		means := make([][]float32, blobs)
		for b := range means {
			m := make([]float32, d)
			for j := range m {
				m[j] = float32(rng.NormFloat64() * 20)
			}
			means[b] = m
		}
		for i := range vs {
			b := i % blobs
			v := make([]float32, d)
			for j := range v {
				v[j] = means[b][j] + float32(rng.NormFloat64())
			}
			vs[i] = v
		}
	default:
		panic("unknown distribution: " + dist)
	}
	return vs
}
