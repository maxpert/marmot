package hdindex

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/hdindex/pkg/benchutil"
)

const benchDataDir = "/tmp/marmot/benchdata"

// skipIfNoData checks for the dataset's train.fvecs and skips the test if absent.
// Returns the dataset directory path when the data is present.
func skipIfNoData(t testing.TB, dataset string) string {
	t.Helper()
	dir := filepath.Join(benchDataDir, dataset)
	trainPath := filepath.Join(dir, "train.fvecs")
	if _, err := os.Stat(trainPath); os.IsNotExist(err) {
		t.Skipf("real-data benchmark skipped: %s not found (run the Python download script to populate %s)", trainPath, dir)
	}
	return dir
}

// loadDataset loads all dataset files from dir using benchutil.
func loadDataset(t testing.TB, dir string) (train [][]float32, test [][]float32, groundtruth [][]int32, meta benchutil.DatasetMetadata) {
	t.Helper()

	var err error
	train, err = benchutil.ReadFvecs(filepath.Join(dir, "train.fvecs"))
	if err != nil {
		t.Fatalf("ReadFvecs train: %v", err)
	}
	test, err = benchutil.ReadFvecs(filepath.Join(dir, "test.fvecs"))
	if err != nil {
		t.Fatalf("ReadFvecs test: %v", err)
	}
	groundtruth, err = benchutil.ReadIvecs(filepath.Join(dir, "groundtruth.ivecs"))
	if err != nil {
		t.Fatalf("ReadIvecs groundtruth: %v", err)
	}
	meta, err = benchutil.LoadMetadata(dir)
	if err != nil {
		t.Fatalf("LoadMetadata: %v", err)
	}
	return
}

// buildRealDataIndex creates an engine and index for the given vectors.
// External IDs are decimal integer strings matching train vector indices.
func buildRealDataIndex(t testing.TB, vecs [][]float32, m Metric, name string) (*Engine, *Index) {
	t.Helper()
	eng, err := NewEngine(t.TempDir(), EngineConfig{PebbleCacheMB: 64})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	entries := make([]VectorEntry, len(vecs))
	for i, v := range vecs {
		entries[i] = VectorEntry{
			ExternalID: []byte(strconv.Itoa(i)),
			Vector:     v,
		}
	}
	spec := DefaultSpec(name, len(vecs[0]), m)
	idx, err := eng.CreateIndex(context.Background(), spec, entries)
	if err != nil {
		eng.Close()
		t.Fatalf("CreateIndex: %v", err)
	}
	return eng, idx
}

// computeRecallAtK computes the fraction of the top-k ground truth neighbours
// found in hits. groundTruth contains 0-based train indices.
func computeRecallAtK(hits []SearchHit, groundTruth []int32, k int) float64 {
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
		id, err := strconv.Atoi(string(h.ExternalID))
		if err != nil {
			// ExternalIDs are always decimal integers set by buildRealDataIndex;
			// a parse failure here indicates a bug, not a legitimate miss.
			continue
		}
		if _, ok := gtSet[id]; ok {
			found++
		}
	}
	return float64(found) / float64(bound)
}

// metricForDataset maps a metadata metric string to a Metric constant.
// "angular" is treated as cosine.
func metricForDataset(meta benchutil.DatasetMetadata) Metric {
	if meta.Metric == "angular" {
		return MetricCosine
	}
	m, ok := ParseMetric(meta.Metric)
	if !ok {
		return MetricEuclidean
	}
	return m
}

// TestRealDataRecall_SIFT128 evaluates recall@{1,10,100} on the SIFT-128 dataset
// across a range of alpha probe widths. The test reports results via t.Logf and
// never fails on low recall — it is an observation benchmark.
func TestRealDataRecall_SIFT128(t *testing.T) {
	dir := skipIfNoData(t, "sift-128")
	train, test, gt, meta := loadDataset(t, dir)

	eng, idx := buildRealDataIndex(t, train, metricForDataset(meta), "sift128-recall")
	defer eng.Close()

	alphas := []int{0, 2048, 4096, 8192}
	ctx := context.Background()

	for _, alpha := range alphas {
		var sumR1, sumR10, sumR100 float64
		var totalMs float64

		nQueries := min(len(test), 500)
		for q := range nQueries {
			qv := test[q]
			start := time.Now()
			result, err := idx.Search(ctx, SearchRequest{
				VectorFP32: qv,
				TopK:       100,
				Alpha:      alpha,
			})
			totalMs += float64(time.Since(start).Microseconds()) / 1000.0
			if err != nil {
				t.Fatalf("Search q=%d alpha=%d: %v", q, alpha, err)
			}
			sumR1 += computeRecallAtK(result.Hits, gt[q], 1)
			sumR10 += computeRecallAtK(result.Hits, gt[q], 10)
			sumR100 += computeRecallAtK(result.Hits, gt[q], 100)
		}

		n := float64(nQueries)
		t.Logf("alpha=%d  recall@1=%.4f  recall@10=%.4f  recall@100=%.4f  avg_ms=%.1f",
			alpha, sumR1/n, sumR10/n, sumR100/n, totalMs/n)
	}
}

// TestRealDataRecall_DBPedia1536 evaluates recall@{1,10,100} on the
// DBPedia-OpenAI-1536 dataset. The metric is derived from metadata.json
// ("angular" maps to cosine).
func TestRealDataRecall_DBPedia1536(t *testing.T) {
	dir := skipIfNoData(t, "dbpedia-openai-1536")
	train, test, gt, meta := loadDataset(t, dir)

	m := metricForDataset(meta)
	eng, idx := buildRealDataIndex(t, train, m, "dbpedia1536-recall")
	defer eng.Close()

	alphas := []int{0, 2048, 4096, 8192}
	ctx := context.Background()

	for _, alpha := range alphas {
		var sumR1, sumR10, sumR100 float64
		var totalMs float64

		nQueries := min(len(test), 500)
		for q := range nQueries {
			qv := test[q]
			start := time.Now()
			result, err := idx.Search(ctx, SearchRequest{
				VectorFP32: qv,
				TopK:       100,
				Alpha:      alpha,
			})
			totalMs += float64(time.Since(start).Microseconds()) / 1000.0
			if err != nil {
				t.Fatalf("Search q=%d alpha=%d: %v", q, alpha, err)
			}
			sumR1 += computeRecallAtK(result.Hits, gt[q], 1)
			sumR10 += computeRecallAtK(result.Hits, gt[q], 10)
			sumR100 += computeRecallAtK(result.Hits, gt[q], 100)
		}

		n := float64(nQueries)
		t.Logf("alpha=%d  recall@1=%.4f  recall@10=%.4f  recall@100=%.4f  avg_ms=%.1f",
			alpha, sumR1/n, sumR10/n, sumR100/n, totalMs/n)
	}
}

// BenchmarkRealDataSearch_SIFT128 measures search QPS on the SIFT-128 dataset.
func BenchmarkRealDataSearch_SIFT128(b *testing.B) {
	dir := skipIfNoData(b, "sift-128")
	train, test, _, _ := loadDataset(b, dir)

	if len(test) == 0 {
		b.Skip("empty test set")
	}

	eng, idx := buildRealDataIndex(b, train, MetricEuclidean, "sift128-bench")
	defer eng.Close()

	ctx := context.Background()
	qLen := len(test)

	b.ResetTimer()
	for i := range b.N {
		q := test[i%qLen]
		_, err := idx.Search(ctx, SearchRequest{VectorFP32: q, TopK: 10})
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkRealDataSearch_DBPedia1536 measures search QPS on the DBPedia-1536 dataset.
func BenchmarkRealDataSearch_DBPedia1536(b *testing.B) {
	dir := skipIfNoData(b, "dbpedia-openai-1536")
	train, test, _, meta := loadDataset(b, dir)

	if len(test) == 0 {
		b.Skip("empty test set")
	}

	m := metricForDataset(meta)
	eng, idx := buildRealDataIndex(b, train, m, "dbpedia1536-bench")
	defer eng.Close()

	ctx := context.Background()
	qLen := len(test)

	b.ResetTimer()
	for i := range b.N {
		q := test[i%qLen]
		_, err := idx.Search(ctx, SearchRequest{VectorFP32: q, TopK: 10})
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}
