package hdindex

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/hdindex/pkg/metric"
)

// bruteForceTopK computes the exact top-k nearest neighbors for a query using brute force.
func bruteForceTopK(query []float32, dataset [][]float32, k int, metricType Metric) []int {
	type distIdx struct {
		idx  int
		dist float32
	}
	all := make([]distIdx, len(dataset))
	for i, v := range dataset {
		var d float32
		switch metricType {
		case MetricEuclidean:
			d = metric.L2(query, v)
		case MetricCosine:
			d = 1.0 - metric.CosineSimilarity(query, v)
		case MetricDot:
			d = -metric.DotProduct(query, v)
		}
		all[i] = distIdx{idx: i, dist: d}
	}
	sort.Slice(all, func(a, b int) bool { return all[a].dist < all[b].dist })
	result := make([]int, min(k, len(all)))
	for i := range result {
		result[i] = all[i].idx
	}
	return result
}

// computeRecall computes recall@k between approximate result IDs and exact indices.
func computeRecall(approxIDs []string, exactIndices []int, idPrefix string) float64 {
	exactSet := make(map[string]struct{}, len(exactIndices))
	for _, idx := range exactIndices {
		exactSet[fmt.Sprintf("%s%d", idPrefix, idx)] = struct{}{}
	}
	var found int
	for _, id := range approxIDs {
		if _, ok := exactSet[id]; ok {
			found++
		}
	}
	return float64(found) / float64(len(exactIndices))
}

// buildTestIndex creates an engine+index for the given vectors and metric, using a temp dir.
func buildTestIndex(t testing.TB, vecs [][]float32, metricType Metric, idSuffix string) (*Engine, *Index) {
	t.Helper()
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	spec := DefaultSpec(fmt.Sprintf("bench-%s-%s", metricType.String(), idSuffix), len(vecs[0]), metricType)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	return eng, idx
}

// recallConfig describes one recall evaluation scenario.
type recallConfig struct {
	n      int
	dim    int
	metric Metric
	label  string
}

// TestRecallEvaluation measures recall@10 for various dataset sizes, dimensions, and metrics.
func TestRecallEvaluation(t *testing.T) {
	const (
		nQueries = 100
		topK     = 10
	)

	configs := []recallConfig{
		{n: 1000, dim: 128, metric: MetricEuclidean, label: "1K_128dim_euclidean"},
		{n: 1000, dim: 128, metric: MetricCosine, label: "1K_128dim_cosine"},
		{n: 1000, dim: 128, metric: MetricDot, label: "1K_128dim_dot"},
		{n: 10000, dim: 128, metric: MetricEuclidean, label: "10K_128dim_euclidean"},
		{n: 10000, dim: 128, metric: MetricCosine, label: "10K_128dim_cosine"},
		{n: 10000, dim: 128, metric: MetricDot, label: "10K_128dim_dot"},
		{n: 10000, dim: 768, metric: MetricEuclidean, label: "10K_768dim_euclidean"},
		{n: 10000, dim: 768, metric: MetricCosine, label: "10K_768dim_cosine"},
		{n: 10000, dim: 768, metric: MetricDot, label: "10K_768dim_dot"},
	}

	ctx := context.Background()

	for _, cfg := range configs {
		t.Run(cfg.label, func(t *testing.T) {
			vecs := randomVectors(cfg.n, cfg.dim, 42)
			eng, idx := buildTestIndex(t, vecs, cfg.metric, cfg.label)
			defer eng.Close()

			rng := rand.New(rand.NewPCG(999, 0))
			recalls := make([]float64, nQueries)

			for q := range nQueries {
				qi := rng.IntN(cfg.n)
				query := vecs[qi]

				exact := bruteForceTopK(query, vecs, topK, cfg.metric)

				result, err := idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: topK})
				if err != nil {
					t.Fatalf("Search q=%d: %v", q, err)
				}

				approxIDs := make([]string, len(result.Hits))
				for i, h := range result.Hits {
					approxIDs[i] = string(h.ExternalID)
				}
				recalls[q] = computeRecall(approxIDs, exact, "vec-")
			}

			// Compute mean, min, p50.
			mean := 0.0
			minR := 1.0
			for _, r := range recalls {
				mean += r
				if r < minR {
					minR = r
				}
			}
			mean /= float64(nQueries)

			sorted := make([]float64, nQueries)
			copy(sorted, recalls)
			sort.Float64s(sorted)
			p50 := sorted[nQueries/2]

			t.Logf("recall@%d  mean=%.4f  min=%.4f  p50=%.4f  (n=%d dim=%d metric=%s)",
				topK, mean, minR, p50, cfg.n, cfg.dim, cfg.metric)
		})
	}
}

// BenchmarkBuild_1K_128dim measures index build throughput for 1K 128-dim vectors.
func BenchmarkBuild_1K_128dim(b *testing.B) {
	benchmarkBuild(b, 1000, 128, MetricEuclidean)
}

// BenchmarkBuild_10K_128dim measures index build throughput for 10K 128-dim vectors.
func BenchmarkBuild_10K_128dim(b *testing.B) {
	benchmarkBuild(b, 10000, 128, MetricEuclidean)
}

// BenchmarkBuild_10K_768dim measures index build throughput for 10K 768-dim vectors.
func BenchmarkBuild_10K_768dim(b *testing.B) {
	benchmarkBuild(b, 10000, 768, MetricEuclidean)
}

func benchmarkBuild(b *testing.B, n, dim int, metricType Metric) {
	b.Helper()
	ctx := context.Background()
	vecs := randomVectors(n, dim, 42)
	entries := makeVectorEntries(vecs)

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		eng, err := NewEngine(b.TempDir())
		if err != nil {
			b.Fatalf("NewEngine: %v", err)
		}
		spec := DefaultSpec(fmt.Sprintf("bench-build-%d-%d", n, dim), dim, metricType)
		b.StartTimer()

		_, err = eng.CreateIndex(ctx, spec, entries)

		b.StopTimer()
		if err != nil {
			b.Fatalf("CreateIndex: %v", err)
		}
		eng.Close()
		b.StartTimer()
	}
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/sec")
}

// BenchmarkSearch_1K_128dim_k10 measures search QPS for 1K 128-dim index.
func BenchmarkSearch_1K_128dim_k10(b *testing.B) {
	benchmarkSearch(b, 1000, 128, MetricEuclidean)
}

// BenchmarkSearch_10K_128dim_k10 measures search QPS for 10K 128-dim index.
func BenchmarkSearch_10K_128dim_k10(b *testing.B) {
	benchmarkSearch(b, 10000, 128, MetricEuclidean)
}

// BenchmarkSearch_10K_768dim_k10 measures search QPS for 10K 768-dim index.
func BenchmarkSearch_10K_768dim_k10(b *testing.B) {
	benchmarkSearch(b, 10000, 768, MetricEuclidean)
}

func benchmarkSearch(b *testing.B, n, dim int, metricType Metric) {
	b.Helper()
	ctx := context.Background()
	vecs := randomVectors(n, dim, 42)
	eng, idx := buildTestIndex(b, vecs, metricType, fmt.Sprintf("%d-%d", n, dim))
	defer eng.Close()

	// Pre-generate query vectors so random generation is not in the hot path.
	queries := randomVectors(1000, dim, 1337)
	qLen := len(queries)

	b.ResetTimer()
	for i := range b.N {
		q := queries[i%qLen]
		_, err := idx.Search(ctx, SearchRequest{VectorFP32: q, TopK: 10})
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// TestSearchLatencyProfile runs 1000 searches on a 10K 128-dim index and reports
// latency breakdown using SearchStats.
func TestSearchLatencyProfile(t *testing.T) {
	const (
		n       = 10000
		dim     = 128
		nSearch = 1000
		topK    = 10
	)

	ctx := context.Background()
	vecs := randomVectors(n, dim, 42)
	eng, idx := buildTestIndex(t, vecs, MetricEuclidean, "latency")
	defer eng.Close()

	queries := randomVectors(nSearch, dim, 77)

	latencies := make([]time.Duration, nSearch)
	var totalCandidatesScanned int
	var totalAfterTriangle int
	var totalAfterPtolemaic int
	var totalExactScored int
	var totalPartitions int

	for i, q := range queries {
		start := time.Now()
		result, err := idx.Search(ctx, SearchRequest{VectorFP32: q, TopK: topK})
		latencies[i] = time.Since(start)
		if err != nil {
			t.Fatalf("Search %d: %v", i, err)
		}
		s := result.Stats
		totalCandidatesScanned += s.CandidatesScanned
		totalAfterTriangle += s.CandidatesAfterTriangle
		totalAfterPtolemaic += s.CandidatesAfterPtolemaic
		totalExactScored += s.CandidatesExactScored
		totalPartitions += s.PartitionsSearched
	}

	// Sort for percentile calculation.
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })

	var totalTime time.Duration
	for _, l := range latencies {
		totalTime += l
	}
	mean := totalTime / time.Duration(nSearch)
	p50 := latencies[nSearch/2]
	p99 := latencies[int(float64(nSearch)*0.99)]

	t.Logf("Search latency profile (n=%d dim=%d topK=%d nSearch=%d):", n, dim, topK, nSearch)
	t.Logf("  mean=%v  p50=%v  p99=%v", mean, p50, p99)
	t.Logf("  avg candidates_scanned=%.1f  after_triangle=%.1f  after_ptolemaic=%.1f  exact_scored=%.1f",
		float64(totalCandidatesScanned)/float64(nSearch),
		float64(totalAfterTriangle)/float64(nSearch),
		float64(totalAfterPtolemaic)/float64(nSearch),
		float64(totalExactScored)/float64(nSearch),
	)
	t.Logf("  avg partitions_searched=%.2f", float64(totalPartitions)/float64(nSearch))
}

// TestMemoryProfile measures heap usage for index creation and search.
func TestMemoryProfile(t *testing.T) {
	configs := []struct {
		n   int
		dim int
	}{
		{10000, 128},
		{10000, 768},
	}

	ctx := context.Background()

	for _, cfg := range configs {
		t.Run(fmt.Sprintf("%dK_%ddim", cfg.n/1000, cfg.dim), func(t *testing.T) {
			vecs := randomVectors(cfg.n, cfg.dim, 42)

			// Measure heap before index creation.
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			eng, idx := buildTestIndex(t, vecs, MetricEuclidean, fmt.Sprintf("mem-%d-%d", cfg.n, cfg.dim))
			defer eng.Close()

			// Measure heap after index creation.
			runtime.GC()
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			heapDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
			allocDelta := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)

			t.Logf("Memory profile (n=%d dim=%d):", cfg.n, cfg.dim)
			t.Logf("  heap_inuse_delta=%d bytes (%.2f MB)", heapDelta, float64(heapDelta)/(1<<20))
			t.Logf("  total_alloc_delta=%d bytes (%.2f MB)", allocDelta, float64(allocDelta)/(1<<20))
			t.Logf("  approx bytes/vector (alloc)=%.1f", float64(allocDelta)/float64(cfg.n))

			// Measure per-query allocation.
			query := randomVectors(1, cfg.dim, 55)[0]
			runtime.GC()
			var memPreSearch runtime.MemStats
			runtime.ReadMemStats(&memPreSearch)

			const nWarmup = 100
			for range nWarmup {
				_, _ = idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: 10})
			}

			runtime.GC()
			var memPostSearch runtime.MemStats
			runtime.ReadMemStats(&memPostSearch)

			searchAllocDelta := int64(memPostSearch.TotalAlloc) - int64(memPreSearch.TotalAlloc)
			t.Logf("  per_query_alloc=%.1f bytes (avg over %d searches)", float64(searchAllocDelta)/float64(nWarmup), nWarmup)
		})
	}
}

// BenchmarkUpsert_128dim measures single-vector upsert throughput for 128-dim vectors.
func BenchmarkUpsert_128dim(b *testing.B) {
	benchmarkUpsert(b, 128)
}

// BenchmarkUpsert_768dim measures single-vector upsert throughput for 768-dim vectors.
func BenchmarkUpsert_768dim(b *testing.B) {
	benchmarkUpsert(b, 768)
}

func benchmarkUpsert(b *testing.B, dim int) {
	b.Helper()
	ctx := context.Background()

	// Seed the index with some initial vectors.
	const seed = 500
	vecs := randomVectors(seed, dim, 42)
	eng, idx := buildTestIndex(b, vecs, MetricEuclidean, fmt.Sprintf("upsert-%d", dim))
	defer eng.Close()

	newVecs := randomVectors(b.N+1, dim, 9999)

	b.ResetTimer()
	for i := range b.N {
		err := idx.Upsert(ctx, Mutation{
			TxnID:      uint64(i + 1),
			SeqID:      1,
			ExternalID: []byte(fmt.Sprintf("upsert-%d", i)),
			VectorFP32: newVecs[i],
		})
		if err != nil {
			b.Fatalf("Upsert: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "upserts/sec")
}
