package vecindex

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
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

// BenchmarkBulkLoad_DBPedia1536_100K measures the end-to-end cost of
// CreateIndex + Graduate on 100K DBpedia vectors. A fresh b.TempDir() is used
// per sub-benchmark iteration so each run starts with a cold engine. Data
// loading happens before b.ResetTimer(), so only the build + graduation time is
// captured in the reported ns/op.
func BenchmarkBulkLoad_DBPedia1536_100K(b *testing.B) {
	dataDir := skipIfNoData(b, "dbpedia-openai-1536")
	train, _, _, meta := loadRealDataset(b, dataDir)
	if len(train) == 0 {
		b.Skip("empty train set")
	}

	m := metricFromMeta(meta)

	bulk := make([]BulkEntry, len(train))
	for i, v := range train {
		bulk[i] = BulkEntry{
			ExternalID: []byte(strconv.Itoa(i)),
			Vector:     v,
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(train)) * int64(vecDim*4))

	for range b.N {
		engDir := filepath.Join(b.TempDir(), "eng")

		b.ResetTimer()
		buildStart := time.Now()

		eng, err := NewEngine(engDir, benchCacheMB, newTestLogger())
		if err != nil {
			b.Fatalf("NewEngine: %v", err)
		}

		spec := IVFSpec{
			ID:     "bulk-bench",
			Dim:    vecDim,
			Metric: m,
			Nlist:  defaultNlist,
			Nprobe: defaultNprobe,
		}

		ctx := context.Background()
		idx, err := eng.CreateIndex(ctx, spec, bulk)
		if err != nil {
			_ = eng.Close()
			b.Fatalf("CreateIndex: %v", err)
		}

		if err = Graduate(ctx, idx, defaultNlist); err != nil {
			_ = eng.Close()
			b.Fatalf("Graduate: %v", err)
		}

		b.StopTimer()

		elapsed := time.Since(buildStart)
		vecsPerSec := float64(len(train)) / elapsed.Seconds()
		b.ReportMetric(vecsPerSec, "vecs/sec")
		b.ReportMetric(elapsed.Seconds()*1000, "build_ms")

		// Measure on-disk size.
		var diskBytes int64
		_ = filepath.Walk(engDir, func(_ string, fi os.FileInfo, err error) error {
			if err == nil && !fi.IsDir() {
				diskBytes += fi.Size()
			}
			return nil
		})
		b.ReportMetric(float64(diskBytes)/(1024*1024), "disk_MB")

		_ = eng.Close()
	}
}

// BenchmarkUpsert_SteadyState_DBPedia1536 measures single-Upsert latency after
// the index is warmed up. A temp-dir index seeded with the first 10K train
// vectors is built during setup; the bench then measures incremental Upserts
// using the last 10K test vectors as insert targets. This keeps the bench
// hermetic and avoids corrupting the persistent search index.
func BenchmarkUpsert_SteadyState_DBPedia1536(b *testing.B) {
	dataDir := skipIfNoData(b, "dbpedia-openai-1536")
	train, test, _, meta := loadRealDataset(b, dataDir)
	if len(train) == 0 || len(test) == 0 {
		b.Skip("empty dataset")
	}

	m := metricFromMeta(meta)

	// Seed index with first 10K train vectors (above flatScanThreshold so
	// Graduate succeeds, giving us a realistic IVF structure).
	const seedCount = 10_000
	seed := train
	if len(seed) > seedCount {
		seed = seed[:seedCount]
	}

	bulk := make([]BulkEntry, len(seed))
	for i, v := range seed {
		bulk[i] = BulkEntry{
			ExternalID: []byte(strconv.Itoa(i)),
			Vector:     v,
		}
	}

	engDir := filepath.Join(b.TempDir(), "eng")
	eng, err := NewEngine(engDir, benchCacheMB, newTestLogger())
	if err != nil {
		b.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	spec := IVFSpec{
		ID:     "upsert-bench",
		Dim:    vecDim,
		Metric: m,
		Nlist:  64,
		Nprobe: defaultNprobe,
	}

	idx, err := eng.CreateIndex(ctx, spec, bulk)
	if err != nil {
		b.Fatalf("CreateIndex: %v", err)
	}
	if err = Graduate(ctx, idx, 64); err != nil {
		b.Fatalf("Graduate: %v", err)
	}

	// Use test vectors as insert targets; cycle if b.N > len(test).
	insertVecs := test
	iLen := len(insertVecs)

	// 100 warmup Upserts.
	for i := range 100 {
		_ = idx.Upsert(ctx, []byte("warmup-"+strconv.Itoa(i)), insertVecs[i%iLen], uint64(i+1), 0)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(vecDim * 4))

	durations := make([]time.Duration, 0, b.N)
	start := time.Now()

	for i := range b.N {
		vec := insertVecs[i%iLen]
		extID := []byte(strconv.Itoa(seedCount + 100 + i))
		t0 := time.Now()
		if err := idx.Upsert(ctx, extID, vec, uint64(seedCount+100+i+1), 0); err != nil {
			b.Fatalf("Upsert: %v", err)
		}
		durations = append(durations, time.Since(t0))
	}

	elapsed := time.Since(start)
	total := len(durations)
	if total == 0 {
		return
	}

	sorted := make([]time.Duration, total)
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	b.ReportMetric(percentile(sorted, 50).Seconds()*1000, "p50_ms")
	b.ReportMetric(percentile(sorted, 95).Seconds()*1000, "p95_ms")
	b.ReportMetric(percentile(sorted, 99).Seconds()*1000, "p99_ms")
	b.ReportMetric(float64(total)/elapsed.Seconds(), "qps")
}

// BenchmarkMixed_ReadWrite_DBPedia1536 simulates production load with 6 search
// goroutines and 2 upsert goroutines (75/25 read/write ratio) running
// concurrently. Intended for use with -benchtime=20s. Reports per-operation
// p50/p95/p99 and QPS for both reads and writes.
//
// The bench uses the same temp-dir index seeded with 10K vectors that
// BenchmarkUpsert_SteadyState_DBPedia1536 uses — hermetic and no interference
// with the persistent search index.
func BenchmarkMixed_ReadWrite_DBPedia1536(b *testing.B) {
	dataDir := skipIfNoData(b, "dbpedia-openai-1536")
	train, test, _, meta := loadRealDataset(b, dataDir)
	if len(train) == 0 || len(test) == 0 {
		b.Skip("empty dataset")
	}

	m := metricFromMeta(meta)

	const seedCount = 10_000
	seed := train
	if len(seed) > seedCount {
		seed = seed[:seedCount]
	}

	bulk := make([]BulkEntry, len(seed))
	for i, v := range seed {
		bulk[i] = BulkEntry{
			ExternalID: []byte(strconv.Itoa(i)),
			Vector:     v,
		}
	}

	engDir := filepath.Join(b.TempDir(), "eng")
	eng, err := NewEngine(engDir, benchCacheMB, newTestLogger())
	if err != nil {
		b.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	spec := IVFSpec{
		ID:     "mixed-bench",
		Dim:    vecDim,
		Metric: m,
		Nlist:  64,
		Nprobe: defaultNprobe,
	}

	idx, err := eng.CreateIndex(ctx, spec, bulk)
	if err != nil {
		b.Fatalf("CreateIndex: %v", err)
	}
	if err = Graduate(ctx, idx, 64); err != nil {
		b.Fatalf("Graduate: %v", err)
	}

	qLen := len(test)
	iLen := len(test)

	type opResult struct {
		d       time.Duration
		isWrite bool
	}

	results := make(chan opResult, b.N+8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(vecDim * 4))

	start := time.Now()

	var opCounter atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Use atomic counter to distribute: every 4th op is a write (75/25 ratio).
			n := opCounter.Add(1)
			if n%4 == 0 {
				// Upsert worker path.
				i := int(n)
				vec := test[i%iLen]
				extID := []byte("mixed-" + strconv.Itoa(seedCount+i))
				t0 := time.Now()
				_ = idx.Upsert(ctx, extID, vec, uint64(seedCount+i+1), 0)
				results <- opResult{d: time.Since(t0), isWrite: true}
			} else {
				// Search worker path.
				i := int(n)
				vec := test[i%qLen]
				t0 := time.Now()
				_, _ = idx.Search(ctx, SearchRequest{Vector: vec, K: 10})
				results <- opResult{d: time.Since(t0), isWrite: false}
			}
		}
	})

	close(results)
	elapsed := time.Since(start)

	var readDurs, writeDurs []time.Duration
	for r := range results {
		if r.isWrite {
			writeDurs = append(writeDurs, r.d)
		} else {
			readDurs = append(readDurs, r.d)
		}
	}

	sort.Slice(readDurs, func(i, j int) bool { return readDurs[i] < readDurs[j] })
	sort.Slice(writeDurs, func(i, j int) bool { return writeDurs[i] < writeDurs[j] })

	if len(readDurs) > 0 {
		b.ReportMetric(percentile(readDurs, 50).Seconds()*1000, "search_p50_ms")
		b.ReportMetric(percentile(readDurs, 95).Seconds()*1000, "search_p95_ms")
		b.ReportMetric(percentile(readDurs, 99).Seconds()*1000, "search_p99_ms")
		b.ReportMetric(float64(len(readDurs))/elapsed.Seconds(), "search_qps")
	}
	if len(writeDurs) > 0 {
		b.ReportMetric(percentile(writeDurs, 50).Seconds()*1000, "upsert_p50_ms")
		b.ReportMetric(percentile(writeDurs, 95).Seconds()*1000, "upsert_p95_ms")
		b.ReportMetric(percentile(writeDurs, 99).Seconds()*1000, "upsert_p99_ms")
		b.ReportMetric(float64(len(writeDurs))/elapsed.Seconds(), "upsert_qps")
	}
}

// BenchmarkGraduation_100K isolates the cost of promoting a flat index to IVF
// via Graduate. The flat index is built from the first 10K train vectors (the
// minimum viable size) during setup; only the Graduate call is timed.
func BenchmarkGraduation_100K(b *testing.B) {
	dataDir := skipIfNoData(b, "dbpedia-openai-1536")
	train, _, _, meta := loadRealDataset(b, dataDir)
	if len(train) == 0 {
		b.Skip("empty train set")
	}

	m := metricFromMeta(meta)

	// Use up to 10K vectors; flatScanThreshold is 6400 so this is safely above it.
	const gradCount = 10_000
	seed := train
	if len(seed) > gradCount {
		seed = seed[:gradCount]
	}

	bulk := make([]BulkEntry, len(seed))
	for i, v := range seed {
		bulk[i] = BulkEntry{
			ExternalID: []byte(strconv.Itoa(i)),
			Vector:     v,
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(seed)) * int64(vecDim*4))

	for range b.N {
		engDir := filepath.Join(b.TempDir(), "eng")

		eng, err := NewEngine(engDir, benchCacheMB, newTestLogger())
		if err != nil {
			b.Fatalf("NewEngine: %v", err)
		}

		ctx := context.Background()
		spec := IVFSpec{
			ID:     "grad-bench",
			Dim:    vecDim,
			Metric: m,
			Nlist:  64,
			Nprobe: defaultNprobe,
		}

		idx, err := eng.CreateIndex(ctx, spec, bulk)
		if err != nil {
			_ = eng.Close()
			b.Fatalf("CreateIndex: %v", err)
		}

		b.ResetTimer()
		gradStart := time.Now()

		if err = Graduate(ctx, idx, 64); err != nil {
			_ = eng.Close()
			b.Fatalf("Graduate: %v", err)
		}

		b.StopTimer()

		elapsed := time.Since(gradStart)
		b.ReportMetric(float64(len(seed))/elapsed.Seconds(), "vecs/sec")
		b.ReportMetric(elapsed.Seconds()*1000, "grad_ms")

		_ = eng.Close()
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
