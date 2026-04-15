package vecindex

import (
	"context"
	"fmt"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

func TestSearch_FlatScanPhase(t *testing.T) {
	t.Parallel()
	// Below the 6400-vector flat-scan threshold.
	const (
		n   = 500
		dim = 16
		k   = 5
	)
	e := newTempEngine(t)
	idx := buildTestIndex(t, e, "flat-idx", dim, n, MetricL2)

	vecs := makeRandomVectors(n, dim, 42) // same seed as buildTestIndex
	query := makeRandomVectors(1, dim, 99)[0]

	truth := bruteForceTopK(query, vecs, k, MetricL2)
	ctx := context.Background()
	hits, err := idx.Search(ctx, SearchRequest{Vector: query, K: k})
	require.NoError(t, err)
	require.Equal(t, k, len(hits), "flat scan must return exactly k hits")

	recall := computeRecall(hits, truth, k)
	require.Equal(t, float32(1.0), recall, "flat scan must be exact")
}

func TestSearch_IVFPhase_Recall(t *testing.T) {
	t.Parallel()
	const (
		n         = 10_000
		dim       = 64
		k         = 10
		nQueries  = 50
		minRecall = 0.9
	)
	e := newTempEngine(t)
	spec := IVFSpec{
		ID:     "ivf-recall",
		Dim:    dim,
		Metric: MetricL2,
		Nlist:  64,
		Nprobe: 16,
	}
	vecs := makeRandomVectors(n, dim, 7)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{
			ExternalID: []byte(fmt.Sprintf("v%d", i)),
			Vector:     v,
		}
	}
	ctx := context.Background()
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	// Graduate to IVF tier so centroids are trained.
	require.NoError(t, Graduate(ctx, idx, 64))

	queries := makeRandomVectors(nQueries, dim, 13)
	var totalRecall float32
	for _, q := range queries {
		truth := bruteForceTopK(q, vecs, k, MetricL2)
		hits, searchErr := idx.Search(ctx, SearchRequest{Vector: q, K: k})
		require.NoError(t, searchErr)
		totalRecall += computeRecall(hits, truth, k)
	}
	avgRecall := totalRecall / float32(nQueries)
	require.GreaterOrEqual(t, avgRecall, float32(minRecall),
		"IVF recall@10 must be >= %.1f, got %.3f", minRecall, avgRecall)
}

func TestSearch_NprobeOverride(t *testing.T) {
	t.Parallel()
	const (
		n   = 10_000
		dim = 32
		k   = 10
	)
	e := newTempEngine(t)
	spec := IVFSpec{ID: "nprobe-test", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 1}
	vecs := makeRandomVectors(n, dim, 1)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	ctx := context.Background()
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	query := makeRandomVectors(1, dim, 55)[0]
	truth := bruteForceTopK(query, vecs, k, MetricL2)

	hitsNarrow, err := idx.Search(ctx, SearchRequest{Vector: query, K: k, NprobeOverride: 1})
	require.NoError(t, err)
	hitsWide, err := idx.Search(ctx, SearchRequest{Vector: query, K: k, NprobeOverride: 8})
	require.NoError(t, err)

	recallNarrow := computeRecall(hitsNarrow, truth, k)
	recallWide := computeRecall(hitsWide, truth, k)
	require.LessOrEqual(t, recallNarrow, recallWide+0.05,
		"wider nprobe should not produce worse recall than nprobe=1")
	// Wide probe should cover at least as many true neighbours.
	_ = recallNarrow
	_ = recallWide
}

func TestSearch_AdaptiveMultiProbe(t *testing.T) {
	t.Parallel()
	// Craft two queries: one near a centroid boundary and one far inside.
	// The boundary query should trigger adaptive bump, yielding higher or equal recall.
	const (
		n   = 8_000
		dim = 32
		k   = 10
	)
	e := newTempEngine(t)
	spec := IVFSpec{ID: "adaptive-idx", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 1}
	vecs := makeRandomVectors(n, dim, 2)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	ctx := context.Background()
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	// Run a boundary query (between two centroids) and inspect LastQueryNprobe.
	// We can't easily craft the exact boundary vector without knowing centroids,
	// so we run 20 queries and verify that at least one triggers an nprobe bump.
	queries := makeRandomVectors(20, dim, 77)
	maxNprobe := 0
	baseNprobe := spec.Nprobe
	for _, q := range queries {
		hits, searchErr := idx.Search(ctx, SearchRequest{Vector: q, K: k})
		require.NoError(t, searchErr)
		_ = hits
		stats := idx.Stats()
		if int(stats.LastQueryNprobe) > maxNprobe {
			maxNprobe = int(stats.LastQueryNprobe)
		}
	}
	require.Greater(t, maxNprobe, baseNprobe,
		"adaptive multi-probe must bump nprobe for at least one boundary query")
}

// TestAdaptiveMultiProbe_NoRunawayCascade verifies that the one-shot adaptive bump
// keeps effective nprobe within nprobe+max(2,nprobe/4) even when the boundary
// condition fires on every query. nprobe=8 must stay <= 10.
func TestAdaptiveMultiProbe_NoRunawayCascade(t *testing.T) {
	t.Parallel()
	const (
		n      = 8_000
		dim    = 32
		k      = 10
		nprobe = 8
	)
	e := newTempEngine(t)
	spec := IVFSpec{ID: "cascade-guard", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: nprobe}
	vecs := makeRandomVectors(n, dim, 33)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	ctx := context.Background()
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	// maxAdaptive = nprobe + max(2, nprobe/4) = 8 + 2 = 10
	maxAllowed := nprobe + max(2, nprobe/4)

	queries := makeRandomVectors(100, dim, 44)
	for _, q := range queries {
		hits, searchErr := idx.Search(ctx, SearchRequest{Vector: q, K: k})
		require.NoError(t, searchErr)
		_ = hits
		got := int(idx.Stats().LastQueryNprobe)
		require.LessOrEqual(t, got, maxAllowed,
			"adaptive bump must not exceed nprobe+max(2,nprobe/4)=%d, got %d", maxAllowed, got)
	}
}

func TestSearch_EmptyIndex_ReturnsEmpty(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("empty-search", 8, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	query := makeRandomVectors(1, 8, 1)[0]
	hits, err := idx.Search(ctx, SearchRequest{Vector: query, K: 5})
	require.NoError(t, err)
	require.Empty(t, hits, "empty index must return no hits")
}

func TestSearch_KGreaterThanVectorCount(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("small-idx", 8, MetricL2)
	vecs := makeRandomVectors(5, 8, 3)
	bulk := make([]BulkEntry, 5)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	hits, err := idx.Search(ctx, SearchRequest{Vector: vecs[0], K: 10})
	require.NoError(t, err)
	require.Len(t, hits, 5, "k > n must return all n vectors")
}

func TestSearch_DotMetric_Unsupported(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	const dim = 8
	ctx := context.Background()
	spec := DefaultSpec("dot-idx", dim, MetricDot)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	// MetricDot is unsupported — Upsert and Search must return an explicit error.
	vec := makeRandomVectors(1, dim, 5)[0]
	err = idx.Upsert(ctx, []byte("ext-1"), vec, 1, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MetricDot not yet supported")

	query := makeRandomVectors(1, dim, 9)[0]
	_, err = idx.Search(ctx, SearchRequest{Vector: query, K: 3})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MetricDot not yet supported")
}

func TestUpsert_NewVector_Inserts(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("upsert-new", 8, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	vec := makeRandomVectors(1, 8, 10)[0]
	require.NoError(t, idx.Upsert(ctx, []byte("ext-1"), vec, 1, 0))

	stats := idx.Stats()
	require.Equal(t, uint64(1), stats.VectorCount)

	hits, err := idx.Search(ctx, SearchRequest{Vector: vec, K: 1})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, []byte("ext-1"), hits[0].ExternalID)
}

func TestUpsert_ExistingExtID_Updates(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("upsert-update", 8, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	oldVec := makeRandomVectors(1, 8, 1)[0]
	newVec := makeRandomVectors(1, 8, 2)[0]

	require.NoError(t, idx.Upsert(ctx, []byte("ext-x"), oldVec, 1, 0))
	require.NoError(t, idx.Upsert(ctx, []byte("ext-x"), newVec, 2, 0))

	stats := idx.Stats()
	require.Equal(t, uint64(1), stats.VectorCount, "upsert same extID must not increment count")

	// Searching with newVec should find ext-x.
	hits, err := idx.Search(ctx, SearchRequest{Vector: newVec, K: 1})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, []byte("ext-x"), hits[0].ExternalID)
}

func TestUpsert_ReassignsCluster(t *testing.T) {
	t.Parallel()
	// Build a graduated index and update a vector far from its original cluster centroid.
	// Verify the reverse map reflects the new cluster.
	const (
		n   = 8_000
		dim = 16
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "reassign-idx", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 4}
	vecs := makeRandomVectors(n, dim, 20)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	// Create a vector far from the others.
	farVec := make([]float32, dim)
	for j := range farVec {
		farVec[j] = 100.0
	}
	extID := []byte("reassign-me")
	require.NoError(t, idx.Upsert(ctx, extID, farVec, 1, 0))

	// Update to a near-origin vector — should move clusters.
	nearVec := make([]float32, dim)
	require.NoError(t, idx.Upsert(ctx, extID, nearVec, 2, 0))

	// Searching nearVec should still find extID.
	hits, err := idx.Search(ctx, SearchRequest{Vector: nearVec, K: 1})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, extID, hits[0].ExternalID)
}

func TestUpsert_WatermarkIdempotency(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("watermark-idx", 8, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	vec := makeRandomVectors(1, 8, 30)[0]

	// First insert — accepted.
	require.NoError(t, idx.Upsert(ctx, []byte("wm-ext"), vec, 5, 0))
	require.Equal(t, uint64(1), idx.Stats().VectorCount)

	// Same (txn=5, seq=0) again — must be a no-op.
	require.NoError(t, idx.Upsert(ctx, []byte("wm-ext"), vec, 5, 0))
	require.Equal(t, uint64(1), idx.Stats().VectorCount)

	// Older watermark (txn=4, seq=0) — must be rejected (no-op, no error).
	require.NoError(t, idx.Upsert(ctx, []byte("wm-ext"), vec, 4, 0))
	require.Equal(t, uint64(1), idx.Stats().VectorCount)
}

func TestDelete_RemovesVector(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("delete-idx", 8, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	vec := makeRandomVectors(1, 8, 40)[0]
	require.NoError(t, idx.Upsert(ctx, []byte("del-ext"), vec, 1, 0))
	require.Equal(t, uint64(1), idx.Stats().VectorCount)

	require.NoError(t, idx.Delete(ctx, []byte("del-ext"), 2, 0))
	require.Equal(t, uint64(0), idx.Stats().VectorCount)

	hits, err := idx.Search(ctx, SearchRequest{Vector: vec, K: 1})
	require.NoError(t, err)
	require.Empty(t, hits)
}

func TestDelete_NonExistentIsNoOp(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("del-noop", 8, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	err = idx.Delete(ctx, []byte("does-not-exist"), 1, 0)
	require.NoError(t, err)
}

func TestDelete_ThenUpsertWithOlderSeqID_Rejected(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("del-wm", 8, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	vec := makeRandomVectors(1, 8, 50)[0]
	require.NoError(t, idx.Upsert(ctx, []byte("res-ext"), vec, 5, 0))

	// Delete at txn=10.
	require.NoError(t, idx.Delete(ctx, []byte("res-ext"), 10, 0))
	require.Equal(t, uint64(0), idx.Stats().VectorCount)

	// Upsert at txn=9 (< delete watermark) — must be rejected (no-op, no error).
	require.NoError(t, idx.Upsert(ctx, []byte("res-ext"), vec, 9, 0))
	require.Equal(t, uint64(0), idx.Stats().VectorCount,
		"upsert with txnID < delete watermark must be rejected")

	// Upsert at txn=11 (> delete watermark) — must be accepted.
	require.NoError(t, idx.Upsert(ctx, []byte("res-ext"), vec, 11, 0))
	require.Equal(t, uint64(1), idx.Stats().VectorCount,
		"upsert with txnID > delete watermark must be accepted")
}

func TestStats_Accurate(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "stats-idx", Dim: 8, Metric: MetricL2, Nlist: 64, Nprobe: 4}
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	const n = 20
	vecs := makeRandomVectors(n, 8, 60)
	for i, v := range vecs {
		require.NoError(t, idx.Upsert(ctx, []byte(fmt.Sprintf("s%d", i)), v, uint64(i+1), 0))
	}

	// Delete 3 vectors.
	for i := 0; i < 3; i++ {
		require.NoError(t, idx.Delete(ctx, []byte(fmt.Sprintf("s%d", i)), uint64(n+i+1), 0))
	}

	stats := idx.Stats()
	require.Equal(t, uint64(n-3), stats.VectorCount)

	_ = metric.MetricL2 // keep metric import used
}
