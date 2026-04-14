package vecindex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex/pkg/benchutil"
	"github.com/stretchr/testify/require"
)

const benchDataDir = "/tmp/marmot-bench"

// skipIfNoData skips the test if the dataset's train.fvecs is absent.
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
// "angular" maps to MetricCosine.
func metricFromMeta(meta benchutil.DatasetMetadata) Metric {
	if meta.Metric == "angular" || meta.Metric == "cosine" {
		return MetricCosine
	}
	return MetricL2
}

// buildRealDataIVFIndex bulk-loads vecs into an Engine and graduates to IVF.
// ExternalIDs are zero-padded decimal integers so the integer value equals DocID
// (assigned in order during bulkLoad).
func buildRealDataIVFIndex(t testing.TB, vecs [][]float32, m Metric, nlist, nprobe int, id string) (*Engine, *Index) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), id)
	eng, err := NewEngine(dir, 512, newTestLogger())
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

// recallAtK computes the fraction of the top-k groundtruth neighbours found in hits.
// groundTruth contains 0-based train indices (docIDs assigned during bulkLoad).
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
		// ExternalID is a decimal integer string; its integer value equals the
		// train index (set via strconv.Itoa(i) in buildRealDataIVFIndex).
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

// TestRealDataRecall_DBPedia1536_100K verifies recall@10 >= 0.85 on the
// DBPedia-OpenAI-1536 dataset subset of 100K vectors at nlist=256, nprobe=32.
// This test is skipped in short mode and when the data directory is absent.
func TestRealDataRecall_DBPedia1536_100K(t *testing.T) {
	dir := skipIfNoData(t, "dbpedia-openai-1536")
	train, test, gt, meta := loadRealDataset(t, dir)
	require.NotEmpty(t, train, "train set must be non-empty")
	require.NotEmpty(t, test, "test set must be non-empty")
	require.NotEmpty(t, gt, "groundtruth must be non-empty")

	t.Logf("dataset: n_train=%d, n_test=%d, dim=%d, metric=%s, k=%d",
		len(train), len(test), len(train[0]), meta.Metric, meta.K)

	m := metricFromMeta(meta)
	eng, idx := buildRealDataIVFIndex(t, train, m, 256, 32, "dbpedia1536-100k-recall")
	defer eng.Close()

	stats := idx.Stats()
	t.Logf("index stats: vectors=%d, centroids=%d, epoch=%d",
		stats.VectorCount, stats.CentroidCount, stats.Epoch)

	const (
		k         = 10
		nQueries  = 500
		minRecall = 0.85
	)

	nQ := nQueries
	if nQ > len(test) {
		nQ = len(test)
	}

	ctx := context.Background()
	var sumRecall float64
	var totalDuration time.Duration

	for q := range nQ {
		start := time.Now()
		hits, err := idx.Search(ctx, SearchRequest{Vector: test[q], K: k})
		totalDuration += time.Since(start)
		require.NoError(t, err, fmt.Sprintf("Search q=%d", q))
		sumRecall += recallAtK(hits, gt[q], k)
	}

	avgRecall := sumRecall / float64(nQ)
	avgLatencyMs := float64(totalDuration.Milliseconds()) / float64(nQ)
	qps := float64(nQ) / totalDuration.Seconds()

	t.Logf("recall@%d=%.4f  avg_latency=%.2fms  QPS=%.1f  queries=%d",
		k, avgRecall, avgLatencyMs, qps, nQ)

	require.GreaterOrEqual(t, avgRecall, minRecall,
		fmt.Sprintf("recall@%d=%.4f below minimum %.2f", k, avgRecall, minRecall))
}

// BenchmarkRealDataSearch_DBPedia1536_100K measures search throughput on the
// DBPedia-OpenAI-1536 dataset subset of 100K vectors at nlist=256, nprobe=32.
// Skipped in short mode and when the data directory is absent.
func BenchmarkRealDataSearch_DBPedia1536_100K(b *testing.B) {
	dir := skipIfNoData(b, "dbpedia-openai-1536")
	train, test, _, meta := loadRealDataset(b, dir)
	if len(test) == 0 {
		b.Skip("empty test set")
	}

	m := metricFromMeta(meta)
	eng, idx := buildRealDataIVFIndex(b, train, m, 256, 32, "dbpedia1536-100k-bench")
	defer eng.Close()

	ctx := context.Background()
	qLen := len(test)

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		q := test[i%qLen]
		_, err := idx.Search(ctx, SearchRequest{Vector: q, K: 10})
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}
