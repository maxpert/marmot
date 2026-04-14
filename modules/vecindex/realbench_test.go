package vecindex

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex/pkg/benchutil"
	"github.com/stretchr/testify/require"
)

const (
	benchDataDir     = "/tmp/marmot-bench"
	benchIndexDir    = "/tmp/marmot-bench/ivf-idx"
	benchIndexID     = "dbpedia1536-100k"
	benchIndexVer    = "v1" // bump to force rebuild when index format changes
	benchCacheMB     = 512
	warmupQueries    = 200
	benchQueryCount  = 5000
	recallQueryCount = 500
	recallMinK10     = 0.85
	defaultNlist     = 256
	defaultNprobe    = 32
	vecDim           = 1536
)

// skipIfNoData skips when -short or when the dataset train.fvecs is absent.
// Returns the dataset directory path when data is present.
func skipIfNoData(t testing.TB, dataset string) string {
	t.Helper()
	if testing.Short() {
		t.Skipf("real-data benchmark skipped: -short flag set")
	}
	dir := filepath.Join(benchDataDir, dataset)
	trainPath := filepath.Join(dir, "train.fvecs")
	if _, err := os.Stat(trainPath); os.IsNotExist(err) {
		t.Skipf("real-data benchmark skipped: %s not found (run modules/vecindex/scripts/fetch_benchmark_data.py first)", trainPath)
	}
	return dir
}

// loadRealDataset loads train, test, and groundtruth from dir.
func loadRealDataset(t testing.TB, dir string) (train [][]float32, test [][]float32, groundtruth [][]int32, meta benchutil.DatasetMetadata) {
	t.Helper()
	var err error
	train, err = benchutil.ReadFvecs(filepath.Join(dir, "train.fvecs"))
	require.NoError(t, err, "ReadFvecs train")
	test, err = benchutil.ReadFvecs(filepath.Join(dir, "test.fvecs"))
	require.NoError(t, err, "ReadFvecs test")
	groundtruth, err = benchutil.ReadIvecs(filepath.Join(dir, "groundtruth.ivecs"))
	require.NoError(t, err, "ReadIvecs groundtruth")
	meta, err = benchutil.LoadMetadata(dir)
	require.NoError(t, err, "LoadMetadata")
	return
}

// metricFromMeta maps a metadata metric string to a vecindex Metric.
func metricFromMeta(meta benchutil.DatasetMetadata) Metric {
	if meta.Metric == "angular" || meta.Metric == "cosine" {
		return MetricCosine
	}
	return MetricL2
}

// buildRealDataIVFIndex bulk-loads vecs into an Engine rooted at dir and graduates to IVF.
// ExternalIDs are decimal integers equal to their train index position.
func buildRealDataIVFIndex(t testing.TB, engDir string, vecs [][]float32, m Metric, nlist, nprobe int, id string) (*Engine, *Index) {
	t.Helper()
	eng, err := NewEngine(engDir, benchCacheMB, newTestLogger())
	require.NoError(t, err, "NewEngine")

	spec := IVFSpec{
		ID:     id,
		Dim:    len(vecs[0]),
		Metric: m,
		Nlist:  nlist,
		Nprobe: nprobe,
	}

	bulk := make([]BulkEntry, len(vecs))
	for i, v := range vecs {
		bulk[i] = BulkEntry{
			ExternalID: []byte(strconv.Itoa(i)),
			Vector:     v,
		}
	}

	t.Logf("bulk loading %d vectors (dim=%d) ...", len(vecs), len(vecs[0]))
	buildStart := time.Now()
	idx, err := eng.CreateIndex(context.Background(), spec, bulk)
	require.NoError(t, err, "CreateIndex")
	t.Logf("bulk load done in %s", time.Since(buildStart).Round(time.Millisecond))

	t.Logf("graduating to IVF (nlist=%d) ...", nlist)
	gradStart := time.Now()
	err = Graduate(context.Background(), idx, nlist)
	require.NoError(t, err, "Graduate")
	t.Logf("graduation done in %s", time.Since(gradStart).Round(time.Millisecond))

	return eng, idx
}

// setupOrReuseIndex opens an existing persistent index if present, otherwise builds it.
// The index lives at benchIndexDir/<ver>/<id>. The version stamp ensures stale
// indexes are rebuilt when benchIndexVer is bumped.
func setupOrReuseIndex(tb testing.TB, dataDir string) (*Engine, *Index) {
	tb.Helper()
	engDir := filepath.Join(benchIndexDir, benchIndexVer)

	train, _, _, meta := loadRealDataset(tb, dataDir)
	require.NotEmpty(tb, train, "train set must be non-empty")

	m := metricFromMeta(meta)

	eng, err := NewEngine(engDir, benchCacheMB, newTestLogger())
	require.NoError(tb, err, "NewEngine")

	// Try to open an existing index first.
	idx, err := eng.OpenIndex(context.Background(), benchIndexID)
	if err == nil {
		tb.Logf("reusing persistent index at %s", engDir)
		return eng, idx
	}

	// Build from scratch.
	tb.Logf("persistent index not found (%v), building ...", err)
	_ = eng.Close()

	eng, idx = buildRealDataIVFIndex(tb, engDir, train, m, defaultNlist, defaultNprobe, benchIndexID)
	tb.Logf("persistent index built at %s", engDir)
	return eng, idx
}

// recallAtK computes the fraction of the top-k groundtruth neighbours found in hits.
func recallAtK(hits []SearchHit, groundTruth []int32, k int) float64 {
	bound := k
	if bound > len(groundTruth) {
		bound = len(groundTruth)
	}
	if bound == 0 {
		return 0
	}
	gtSet := make(map[int]struct{}, bound)
	for _, idx := range groundTruth[:bound] {
		gtSet[int(idx)] = struct{}{}
	}
	found := 0
	for _, h := range hits {
		id, parseErr := strconv.Atoi(string(h.ExternalID))
		if parseErr != nil {
			continue
		}
		if _, ok := gtSet[id]; ok {
			found++
		}
	}
	return float64(found) / float64(bound)
}

// percentile computes a percentile (0–100) from a sorted duration slice using
// linear interpolation.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	n := float64(len(sorted))
	rank := p / 100.0 * (n - 1)
	lo := int(math.Floor(rank))
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo] + time.Duration(frac*float64(sorted[hi]-sorted[lo]))
}

// TestRealDataRecall_DBPedia1536_100K verifies recall@10 >= 0.85 on the
// DBPedia-OpenAI-1536 dataset (100K vectors, nlist=256, nprobe=32).
// Uses t.TempDir() — correctness does not depend on cache state.
func TestRealDataRecall_DBPedia1536_100K(t *testing.T) {
	dir := skipIfNoData(t, "dbpedia-openai-1536")
	train, test, gt, meta := loadRealDataset(t, dir)
	require.NotEmpty(t, train, "train set must be non-empty")
	require.NotEmpty(t, test, "test set must be non-empty")
	require.NotEmpty(t, gt, "groundtruth must be non-empty")

	t.Logf("dataset: n_train=%d, n_test=%d, dim=%d, metric=%s, k=%d",
		len(train), len(test), len(train[0]), meta.Metric, meta.K)

	m := metricFromMeta(meta)
	engDir := filepath.Join(t.TempDir(), "eng")
	eng, idx := buildRealDataIVFIndex(t, engDir, train, m, defaultNlist, defaultNprobe, "dbpedia1536-100k-recall")
	defer eng.Close()

	stats := idx.Stats()
	t.Logf("index stats: vectors=%d, centroids=%d, epoch=%d",
		stats.VectorCount, stats.CentroidCount, stats.Epoch)

	const k = 10
	nQ := recallQueryCount
	if nQ > len(test) {
		nQ = len(test)
	}

	ctx := context.Background()
	var sumRecall float64
	for q := range nQ {
		hits, err := idx.Search(ctx, SearchRequest{Vector: test[q], K: k})
		require.NoError(t, err, fmt.Sprintf("Search q=%d", q))
		sumRecall += recallAtK(hits, gt[q], k)
	}

	avgRecall := sumRecall / float64(nQ)
	t.Logf("recall@%d=%.4f  queries=%d", k, avgRecall, nQ)

	require.GreaterOrEqual(t, avgRecall, recallMinK10,
		fmt.Sprintf("recall@%d=%.4f below minimum %.2f", k, avgRecall, recallMinK10))
}

// BenchmarkIVFSearch_Warm_DBPedia1536_100K measures steady-state search latency
// with a persistent index dir and 200-query cache warmup. Reports p50/p95/p99/QPS.
func BenchmarkIVFSearch_Warm_DBPedia1536_100K(b *testing.B) {
	dataDir := skipIfNoData(b, "dbpedia-openai-1536")
	_, test, _, _ := loadRealDataset(b, dataDir)
	if len(test) == 0 {
		b.Skip("empty test set")
	}

	eng, idx := setupOrReuseIndex(b, dataDir)
	defer eng.Close()

	ctx := context.Background()
	qLen := len(test)

	nprobeValues := []int{8, 16, 32}
	for _, nprobe := range nprobeValues {
		np := nprobe
		b.Run(fmt.Sprintf("nprobe=%d", np), func(b *testing.B) {
			// Warmup: saturate Pebble block cache and OS page cache.
			wCount := warmupQueries
			if wCount > qLen {
				wCount = qLen
			}
			for i := range wCount {
				req := SearchRequest{Vector: test[i%qLen], K: 10, NprobeOverride: np}
				_, _ = idx.Search(ctx, req)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(vecDim * 4))

			nQ := benchQueryCount
			durations := make([]time.Duration, 0, nQ)
			start := time.Now()

			for i := range b.N {
				q := test[i%qLen]
				t0 := time.Now()
				_, err := idx.Search(ctx, SearchRequest{Vector: q, K: 10, NprobeOverride: np})
				durations = append(durations, time.Since(t0))
				if err != nil {
					b.Fatalf("Search: %v", err)
				}
			}

			elapsed := time.Since(start)
			total := len(durations)
			if total == 0 {
				return
			}

			sorted := make([]time.Duration, total)
			copy(sorted, durations)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

			p50 := percentile(sorted, 50).Seconds() * 1000
			p95 := percentile(sorted, 95).Seconds() * 1000
			p99 := percentile(sorted, 99).Seconds() * 1000
			qps := float64(total) / elapsed.Seconds()

			b.ReportMetric(p50, "p50_ms")
			b.ReportMetric(p95, "p95_ms")
			b.ReportMetric(p99, "p99_ms")
			b.ReportMetric(qps, "qps")
		})
	}
}

// BenchmarkIVFSearch_Cold_DBPedia1536_100K measures cold-start search latency
// with a fresh t.TempDir() and no warmup. Used for regression-tracking the
// cold-cache overhead.
func BenchmarkIVFSearch_Cold_DBPedia1536_100K(b *testing.B) {
	dataDir := skipIfNoData(b, "dbpedia-openai-1536")
	train, test, _, meta := loadRealDataset(b, dataDir)
	if len(test) == 0 {
		b.Skip("empty test set")
	}

	m := metricFromMeta(meta)
	engDir := filepath.Join(b.TempDir(), "eng")
	eng, idx := buildRealDataIVFIndex(b, engDir, train, m, defaultNlist, defaultNprobe, "dbpedia1536-100k-cold")
	defer eng.Close()

	ctx := context.Background()
	qLen := len(test)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(vecDim * 4))

	durations := make([]time.Duration, 0, b.N)
	start := time.Now()

	for i := range b.N {
		q := test[i%qLen]
		t0 := time.Now()
		_, err := idx.Search(ctx, SearchRequest{Vector: q, K: 10})
		durations = append(durations, time.Since(t0))
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
	}

	elapsed := time.Since(start)
	total := len(durations)
	if total == 0 {
		return
	}

	sorted := make([]time.Duration, total)
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 := percentile(sorted, 50).Seconds() * 1000
	p95 := percentile(sorted, 95).Seconds() * 1000
	p99 := percentile(sorted, 99).Seconds() * 1000
	qps := float64(total) / elapsed.Seconds()

	b.ReportMetric(p50, "p50_ms")
	b.ReportMetric(p95, "p95_ms")
	b.ReportMetric(p99, "p99_ms")
	b.ReportMetric(qps, "qps")
}
