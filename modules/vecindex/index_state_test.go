package vecindex

import (
	"encoding/binary"
	"math"
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
		require.Greater(t, id, int64(0), "cluster_id must be >= 1")
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

func TestIndexState_TopNprobeDotUsesQueryAugmentation(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "dot", Dim: 2, Metric: MetricDot, Nlist: 2, Nprobe: 1, MaxNorm: 1}
	centroids := [][]float32{{0, 0, 1}, {3, 0, 0}}
	state := makeIndexState(t, spec, centroids)

	ids, epoch, err := state.TopNprobeClustersWithEpoch(encodeVec([]float32{3, 0}), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), epoch)
	require.Equal(t, []int64{2}, ids)
}

func TestIndexState_HotClusters(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 4}
	state := makeIndexState(t, spec, [][]float32{{1, 0}, {0, 1}, {-1, 0}, {0, -1}})

	state.RecordClusterHits([]int64{2, 3, 3, 4, 3, 2, 4})

	require.Equal(t, []int64{3, 2, 4}, state.HotClusters(3))
	require.Equal(t, map[int64]uint64{3: 3, 2: 2}, state.HotClusterScores(2))
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
