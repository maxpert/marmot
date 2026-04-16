package kmeans

import (
	"math/rand"
	"testing"
	"time"
)

// The init benchmarks below ASSERT their latency targets via b.Fatalf so a
// regression shows up as a test failure, not as a silent slowdown. Run with
// `-benchtime=1x -timeout=30m` — one iteration is sufficient because each
// call is already O(seconds).

func benchInit(b *testing.B, n, k, d int, budget time.Duration) {
	rng := rand.New(rand.NewSource(42))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, d)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vecs[i] = v
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		start := time.Now()
		initRng := rand.New(rand.NewSource(foldSeed(uint64(iter + 1))))
		out := kMeansParallelInit(vecs, k, d, initRng)
		elapsed := time.Since(start)
		if len(out) != k {
			b.Fatalf("got %d centroids, want %d", len(out), k)
		}
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/init")
		if elapsed > budget {
			b.Fatalf("n=%d k=%d d=%d init=%v exceeded budget %v", n, k, d, elapsed, budget)
		}
	}
}

func BenchmarkKMeansInit_N10K_K128_D128(b *testing.B) { benchInit(b, 10_000, 128, 128, 1*time.Second) }
func BenchmarkKMeansInit_N100K_K512_D128(b *testing.B) {
	benchInit(b, 100_000, 512, 128, 5*time.Second)
}
func BenchmarkKMeansInit_N100K_K1264_D128(b *testing.B) {
	benchInit(b, 100_000, 1264, 128, 15*time.Second)
}
func BenchmarkKMeansInit_N1M_K2048_D128(b *testing.B) {
	if testing.Short() {
		b.Skip("1M benchmark skipped under -short")
	}
	benchInit(b, 1_000_000, 2048, 128, 120*time.Second)
}
