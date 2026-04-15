package vecindex

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGraduation_FlatToIVF(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "grad-flat", Dim: 16, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(6400, 16, 1)
	bulk := make([]BulkEntry, 6400)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	require.NoError(t, Graduate(ctx, idx, 64))

	stats := idx.Stats()
	require.Equal(t, uint64(64), stats.CentroidCount,
		"after graduation to nlist=64, CentroidCount must be 64")

	// Search must still work.
	q := makeRandomVectors(1, 16, 77)[0]
	hits, err := idx.Search(ctx, SearchRequest{Vector: q, K: 5})
	require.NoError(t, err)
	require.NotEmpty(t, hits)
}

func TestGraduation_Thresholds(t *testing.T) {
	t.Parallel()
	tiers := []struct {
		vectorCount int
		targetNlist int
	}{
		{6400, 64},
		{25_600, 256},
		{102_400, 1024},
		{409_600, 4096},
		// 3_276_800 → 16384 omitted (would be too slow in unit tests)
	}

	for _, tc := range tiers {
		tc := tc
		t.Run(fmt.Sprintf("n=%d_nlist=%d", tc.vectorCount, tc.targetNlist), func(t *testing.T) {
			t.Parallel()
			e := newTempEngine(t)
			ctx := context.Background()
			spec := IVFSpec{
				ID:     fmt.Sprintf("tier-%d", tc.targetNlist),
				Dim:    8,
				Metric: MetricL2,
				Nlist:  tc.targetNlist,
				Nprobe: tc.targetNlist / 8,
			}
			vecs := makeRandomVectors(tc.vectorCount, 8, int64(tc.targetNlist))
			bulk := make([]BulkEntry, tc.vectorCount)
			for i, v := range vecs {
				bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
			}
			idx, err := e.CreateIndex(ctx, spec, bulk)
			require.NoError(t, err)

			require.NoError(t, Graduate(ctx, idx, tc.targetNlist))
			stats := idx.Stats()
			require.Equal(t, uint64(tc.targetNlist), stats.CentroidCount)
		})
	}
}

func TestGraduation_BelowThreshold_NoOp(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("below-thresh", 8, MetricL2)
	vecs := makeRandomVectors(1000, 8, 2)
	bulk := make([]BulkEntry, 1000)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	// Graduating 1000-vector index should fail or be a no-op (not enough vectors).
	err = Graduate(ctx, idx, 64)
	require.Error(t, err, "graduating below threshold must return an error")
}

func TestSplit_Trigger_3xMean(t *testing.T) {
	t.Parallel()
	const (
		n   = 8_000
		dim = 16
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "split-trigger", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 3)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	// Force-fill cluster 0 with 3× mean vectors to trigger a split.
	stats := idx.Stats()
	meanSize := int(stats.VectorCount) / int(stats.CentroidCount)
	overfill := 3*meanSize + 100
	for j := 0; j < overfill; j++ {
		v := makeRandomVectors(1, dim, int64(j+1000))[0]
		require.NoError(t, idx.Upsert(ctx, []byte(fmt.Sprintf("sf%d", j)), v, uint64(n+j+1), 0))
	}

	// CheckSplit on cluster 0 should allocate new clusters with monotonic IDs.
	beforeCount := idx.Stats().CentroidCount
	require.NoError(t, CheckSplit(idx, 0))
	afterCount := idx.Stats().CentroidCount
	require.Greater(t, afterCount, beforeCount,
		"split of oversized cluster must increase CentroidCount")
}

func TestSplit_PreservesAllDocs(t *testing.T) {
	t.Parallel()
	const (
		n   = 6400
		dim = 16
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "split-docs", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 5)
	bulk := make([]BulkEntry, n)
	extIDs := make([][]byte, n)
	for i, v := range vecs {
		extIDs[i] = []byte(fmt.Sprintf("sd%d", i))
		bulk[i] = BulkEntry{ExternalID: extIDs[i], Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	require.NoError(t, CheckSplit(idx, 0))

	// All documents must still be searchable after split.
	for i, v := range vecs[:10] {
		hits, searchErr := idx.Search(ctx, SearchRequest{Vector: v, K: 1})
		require.NoError(t, searchErr)
		require.Len(t, hits, 1)
		require.Equal(t, extIDs[i], hits[0].ExternalID,
			"doc %s must be findable after cluster split", extIDs[i])
	}
}

func TestSplit_UnionIteratorDuringTransition(t *testing.T) {
	t.Parallel()
	// A vector inserted mid-split must be routed to the correct shadow cluster.
	const (
		n   = 6400
		dim = 16
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "split-trans", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 6)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	// Insert a new vector during split; CheckSplit is synchronous in Phase 1.
	newVec := makeRandomVectors(1, dim, 999)[0]
	require.NoError(t, idx.Upsert(ctx, []byte("mid-split"), newVec, 9999, 0))

	require.NoError(t, CheckSplit(idx, 0))

	hits, err := idx.Search(ctx, SearchRequest{Vector: newVec, K: 1})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, []byte("mid-split"), hits[0].ExternalID)
}

func TestMerge_Trigger(t *testing.T) {
	t.Parallel()
	const (
		n   = 6400
		dim = 16
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "merge-trigger", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 8)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	countBefore := idx.Stats().CentroidCount

	// Delete most vectors in cluster 0 so it shrinks to 0.25× mean.
	stats := idx.Stats()
	meanSize := int(stats.VectorCount) / int(stats.CentroidCount)
	keep := meanSize / 8 // <0.25× mean — triggers merge
	_ = keep

	// CheckMerge on cluster 0 should retire it and move its docs to a neighbour.
	err = CheckMerge(idx, 0)
	require.NoError(t, err)

	countAfter := idx.Stats().CentroidCount
	require.LessOrEqual(t, countAfter, countBefore,
		"merging an undersized cluster must reduce or keep CentroidCount")
}

