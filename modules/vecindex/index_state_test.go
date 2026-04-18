package vecindex

import (
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

// encodeVec serialises a []float32 as little-endian bytes (BLOB column format).
func encodeVec(v []float32) []byte {
	out := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(f))
	}
	return out
}

func makeIndexState(t *testing.T, spec IVFSpec, centroids [][]float32) *IndexState {
	t.Helper()
	cs, err := kmeans.NewCentroidSet(1, centroids)
	require.NoError(t, err)
	return NewIndexState(spec, cs)
}

func TestIndexState_AssignNearest_ReturnsOneBased(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 3, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{{1, 0, 0}, {0, 1, 0}}
	state := makeIndexState(t, spec, centroids)

	// Vector closest to centroid 0 (index 0) → cluster_id 1.
	clusterID, err := state.AssignNearest(encodeVec([]float32{0.9, 0.1, 0}))
	require.NoError(t, err)
	require.Equal(t, int64(1), clusterID, "nearest centroid[0] should map to cluster_id 1")

	// Vector closest to centroid 1 (index 1) → cluster_id 2.
	clusterID, err = state.AssignNearest(encodeVec([]float32{0.1, 0.9, 0}))
	require.NoError(t, err)
	require.Equal(t, int64(2), clusterID, "nearest centroid[1] should map to cluster_id 2")
}

func TestIndexState_AssignNearest_NeverReturnsZero(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 4}
	centroids := [][]float32{{1, 0}, {0, 1}, {-1, 0}, {0, -1}}
	state := makeIndexState(t, spec, centroids)

	vecs := [][]float32{{1, 0}, {0, 1}, {-1, 0}, {0, -1}, {0.5, 0.5}}
	for _, v := range vecs {
		id, err := state.AssignNearest(encodeVec(v))
		require.NoError(t, err)
		require.Greater(t, id, int64(0), "cluster_id must be >= 1 (0 reserved for delta)")
	}
}

func TestIndexState_AssignNearest_DimensionMismatch(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "emb", Dim: 3, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{{1, 0, 0}, {0, 1, 0}}
	state := makeIndexState(t, spec, centroids)

	// 2-dim vector on a 3-dim index → error.
	_, err := state.AssignNearest(encodeVec([]float32{1, 0}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-014")
}

func TestIndexState_AssignNearest_EmptyBlob(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "emb", Dim: 3, Metric: MetricL2, Nlist: 1}
	centroids := [][]float32{{1, 0, 0}}
	state := makeIndexState(t, spec, centroids)

	_, err := state.AssignNearest([]byte{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-014")
}

func TestIndexState_AssignNearest_UnalignedBlob(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "emb", Dim: 3, Metric: MetricL2, Nlist: 1}
	centroids := [][]float32{{1, 0, 0}}
	state := makeIndexState(t, spec, centroids)

	_, err := state.AssignNearest([]byte{1, 2, 3}) // not multiple of 4
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-014")
}

func TestIndexState_ProbeVersion(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "emb", Dim: 2, Metric: MetricL2, Nlist: 1}
	cs, err := kmeans.NewCentroidSet(7, [][]float32{{1, 0}})
	require.NoError(t, err)
	state := NewIndexState(spec, cs)
	require.Equal(t, uint64(7), state.ProbeVersion())
}

func TestIndexState_SwapProbeState(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "emb", Dim: 2, Metric: MetricL2, Nlist: 1}
	cs1, _ := kmeans.NewCentroidSet(1, [][]float32{{1, 0}})
	cs2, _ := kmeans.NewCentroidSet(2, [][]float32{{0, 1}})

	state := NewIndexState(spec, cs1)
	require.Equal(t, uint64(1), state.ProbeVersion())

	old := state.SwapProbeState(cs2)
	require.Equal(t, uint64(1), old.Epoch())
	require.Equal(t, uint64(2), state.ProbeVersion())
}

func TestIndexState_CosineMetric(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "emb", Dim: 2, Metric: MetricCosine, Nlist: 2}
	centroids := [][]float32{{1, 0}, {0, 1}}
	state := makeIndexState(t, spec, centroids)

	id, err := state.AssignNearest(encodeVec([]float32{0.9, 0.1}))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
}

func TestIndexState_StoreResidentDelta(t *testing.T) {
	t.Parallel()

	state := makeIndexState(t, IVFSpec{ID: "emb", Dim: 2, Metric: MetricL2, Nlist: 1}, [][]float32{{1, 0}})
	delta := NewDeltaBuffer()
	delta.AppendBatch([]CachedVector{
		{RowID: 10, Vec: []float32{1, 0}},
		{RowID: 11, Vec: []float32{0, 1}},
	})

	state.StoreResidentDelta(delta)
	require.Equal(t, []CachedVector{
		{RowID: 10, Vec: []float32{1, 0}},
		{RowID: 11, Vec: []float32{0, 1}},
	}, state.LoadResidentDelta().Snapshot())
}

func TestIndexState_ApplyDeltaFlushUpdates_RemovesResidentRows(t *testing.T) {
	t.Parallel()

	state := makeIndexState(t, IVFSpec{ID: "emb", Dim: 2, Metric: MetricL2, Nlist: 2}, [][]float32{{1, 0}, {0, 1}})
	delta := NewDeltaBuffer()
	delta.AppendBatch([]CachedVector{
		{RowID: 10, Vec: []float32{1, 0}},
		{RowID: 11, Vec: []float32{0, 1}},
	})
	state.StoreResidentDelta(delta)

	state.ApplyDeltaFlushUpdates(1, []PartitionUpdate{{ClusterID: 2, RowID: 10}})

	require.Equal(t, []CachedVector{
		{RowID: 11, Vec: []float32{0, 1}},
	}, state.LoadResidentDelta().Snapshot())
}

func TestIndexState_ApplyDeltaFlushUpdates_StaleEpochNoOp(t *testing.T) {
	t.Parallel()

	state := makeIndexState(t, IVFSpec{ID: "emb", Dim: 2, Metric: MetricL2, Nlist: 1}, [][]float32{{1, 0}})
	delta := NewDeltaBuffer()
	delta.Append(CachedVector{RowID: 99, Vec: []float32{1, 0}})
	state.StoreResidentDelta(delta)

	state.ApplyDeltaFlushUpdates(999, []PartitionUpdate{{ClusterID: 1, RowID: 99}})

	require.Len(t, state.LoadResidentDelta().Snapshot(), 1)
}

func TestIndexState_ApplyDeltaFlushUpdates_Concurrent(t *testing.T) {
	t.Parallel()

	state := makeIndexState(t, IVFSpec{ID: "emb", Dim: 2, Metric: MetricL2, Nlist: 4}, [][]float32{{1, 0}, {0, 1}, {-1, 0}, {0, -1}})
	const totalRows = 1000
	seed := make([]CachedVector, totalRows)
	for i := 0; i < totalRows; i++ {
		seed[i] = CachedVector{RowID: int64(i), Vec: []float32{float32(i), 0}}
	}
	delta := NewDeltaBuffer()
	delta.AppendBatch(seed)
	state.StoreResidentDelta(delta)

	const workers = 8
	perWorker := totalRows / workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			batch := make([]PartitionUpdate, perWorker)
			for i := 0; i < perWorker; i++ {
				batch[i] = PartitionUpdate{
					ClusterID: int64(1 + (w+i)%4),
					RowID:     int64(w*perWorker + i),
				}
			}
			state.ApplyDeltaFlushUpdates(1, batch)
		}(w)
	}
	wg.Wait()

	require.Empty(t, state.LoadResidentDelta().Snapshot())
}

func TestIndexState_DriftUpdate_AccumulatesCorrectly(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{{1, 0}, {0, 1}}
	state := makeIndexState(t, spec, centroids)

	// Apply 100 updates to cluster 1 (1-based) with vec {2, 0}.
	for i := 0; i < 100; i++ {
		state.DriftUpdate(1, []float32{2, 0})
	}
	// Apply 50 updates to cluster 2 (1-based) with vec {0, 3}.
	for i := 0; i < 50; i++ {
		state.DriftUpdate(2, []float32{0, 3})
	}

	tracker := state.LoadDriftTracker()
	require.NotNil(t, tracker)
	require.Equal(t, int64(101), tracker.ClusterCount(0)) // 1 initial + 100
	require.Equal(t, int64(51), tracker.ClusterCount(1))  // 1 initial + 50

	// Verify drifted centroids.
	// Cluster 0: sum = (1,0) + 100*(2,0) = (201, 0), centroid = (201/101, 0)
	drifted := state.DriftCentroids()
	require.InDelta(t, 201.0/101.0, drifted[0][0], 1e-5)
	require.InDelta(t, 0.0, drifted[0][1], 1e-5)
	// Cluster 1: sum = (0,1) + 50*(0,3) = (0, 151), centroid = (0, 151/51)
	require.InDelta(t, 0.0, drifted[1][0], 1e-5)
	require.InDelta(t, 151.0/51.0, drifted[1][1], 1e-5)
}

func TestIndexState_DriftUpdate_InvalidClusterNoOp(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 1}
	state := makeIndexState(t, spec, [][]float32{{1, 0}})

	// 0 is delta-reserved (not a real cluster), should be no-op.
	state.DriftUpdate(0, []float32{9, 9})
	// Out of range.
	state.DriftUpdate(5, []float32{9, 9})

	tracker := state.LoadDriftTracker()
	require.Equal(t, int64(1), tracker.ClusterCount(0)) // unchanged
}

func TestIndexState_DriftCentroids_MatchesMeanShift(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 3, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{{0, 0, 0}, {10, 10, 10}}
	state := makeIndexState(t, spec, centroids)

	// Drift cluster 1 toward {2, 2, 2} with 9 updates.
	for i := 0; i < 9; i++ {
		state.DriftUpdate(1, []float32{2, 2, 2})
	}

	// Expected: sum = (0,0,0) + 9*(2,2,2) = (18,18,18), count = 10
	// centroid = (1.8, 1.8, 1.8)
	drifted := state.DriftCentroids()
	for d := 0; d < 3; d++ {
		require.InDelta(t, 1.8, drifted[0][d], 1e-5)
	}
	// Cluster 2 unchanged.
	for d := 0; d < 3; d++ {
		require.InDelta(t, 10.0, drifted[1][d], 1e-5)
	}
}

func TestIndexState_DriftUpdate_ConcurrentCOWSafety(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{{0, 0}, {1, 1}}
	state := makeIndexState(t, spec, centroids)

	const workers = 8
	const updates = 200
	done := make(chan struct{})

	for w := 0; w < workers; w++ {
		go func(w int) {
			clusterID := int64(w%2) + 1 // 1-based
			for i := 0; i < updates; i++ {
				state.DriftUpdate(clusterID, []float32{1, 1})
			}
			done <- struct{}{}
		}(w)
	}
	for i := 0; i < workers; i++ {
		<-done
	}

	// All 8*200 = 1600 updates should be reflected across the two clusters.
	tracker := state.LoadDriftTracker()
	total := tracker.ClusterCount(0) + tracker.ClusterCount(1)
	// 2 initial (one per cluster) + 1600 updates = 1602.
	require.Equal(t, int64(2+workers*updates), total)
}

func TestIndexState_ResetDriftState_ResetTracker(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 2}
	state := makeIndexState(t, spec, [][]float32{{1, 0}, {0, 1}})

	// Accumulate some drift.
	for i := 0; i < 50; i++ {
		state.DriftUpdate(1, []float32{2, 0})
	}
	require.Equal(t, int64(51), state.LoadDriftTracker().ClusterCount(0))

	// Reset with new centroids.
	newCS, err := kmeans.NewCentroidSet(2, [][]float32{{5, 5}, {-5, -5}})
	require.NoError(t, err)
	state.ResetDriftState(newCS)

	// Tracker should be fresh: count=1 per cluster.
	tracker := state.LoadDriftTracker()
	require.Equal(t, int64(1), tracker.ClusterCount(0))
	require.Equal(t, int64(1), tracker.ClusterCount(1))

	// Drifted centroids should match the new centroids.
	drifted := state.DriftCentroids()
	require.InDeltaSlice(t, []float64{5, 5}, toFloat64Slice(drifted[0]), 1e-6)
	require.InDeltaSlice(t, []float64{-5, -5}, toFloat64Slice(drifted[1]), 1e-6)
}

func toFloat64Slice(v []float32) []float64 {
	out := make([]float64, len(v))
	for i, f := range v {
		out[i] = float64(f)
	}
	return out
}

func TestFloat32ToBytes_RoundTrip(t *testing.T) {
	t.Parallel()
	v := []float32{1.5, -2.3, 0, math.MaxFloat32}
	b := Float32ToBytes(v)
	require.Len(t, b, 4*len(v))

	got := metric.BytesToFloat32(b)
	for i, f := range v {
		require.InDelta(t, f, got[i], 1e-6, "element %d", i)
	}
}
