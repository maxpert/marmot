package kmeans_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

func TestCentroidSet_New(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{{1, 2, 3}, {4, 5, 6}}
	cs, err := kmeans.NewCentroidSet(1, vecs)
	require.NoError(t, err)
	require.NotNil(t, cs)
}

func TestCentroidSet_Len(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{{1, 2}, {3, 4}, {5, 6}}
	cs, err := kmeans.NewCentroidSet(0, vecs)
	require.NoError(t, err)
	require.Equal(t, 3, cs.Len())
}

func TestCentroidSet_Get(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{{1, 2}, {3, 4}}
	cs, err := kmeans.NewCentroidSet(0, vecs)
	require.NoError(t, err)

	v, err := cs.Get(0)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2}, v)

	v, err = cs.Get(1)
	require.NoError(t, err)
	require.Equal(t, []float32{3, 4}, v)

	_, err = cs.Get(2)
	require.Error(t, err, "out-of-range cluster ID must return error")
}

func TestCentroidSet_Epoch(t *testing.T) {
	t.Parallel()
	cs, err := kmeans.NewCentroidSet(42, [][]float32{{1, 2}})
	require.NoError(t, err)
	require.Equal(t, uint64(42), cs.Epoch())
}

func TestCentroidSet_Immutable(t *testing.T) {
	t.Parallel()
	orig := []float32{1, 2, 3}
	cs, err := kmeans.NewCentroidSet(0, [][]float32{orig})
	require.NoError(t, err)

	got, err := cs.Get(0)
	require.NoError(t, err)

	// Mutate the returned slice
	got[0] = 999

	// Re-fetch: internal state must not have changed
	got2, err := cs.Get(0)
	require.NoError(t, err)
	require.Equal(t, float32(1), got2[0], "mutating returned slice must not affect CentroidSet internal state")
}

func TestCentroidSet_GetReadOnly_NoAlloc(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{{1, 2, 3}, {4, 5, 6}}
	cs, err := kmeans.NewCentroidSet(0, vecs)
	require.NoError(t, err)

	v, err := cs.GetReadOnly(0)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, v)

	// GetReadOnly must return the same pointer on repeated calls (no copy).
	v2, err := cs.GetReadOnly(0)
	require.NoError(t, err)
	require.Equal(t, &v[0], &v2[0], "GetReadOnly must return the same backing array without copying")

	// Out-of-range must error.
	_, err = cs.GetReadOnly(99)
	require.Error(t, err)
}

func TestCentroidSet_Encode_Decode(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{
		{1.1, 2.2, 3.3},
		{4.4, 5.5, 6.6},
	}
	cs, err := kmeans.NewCentroidSet(7, vecs)
	require.NoError(t, err)

	data, err := cs.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	cs2, err := kmeans.DecodeCentroidSet(data)
	require.NoError(t, err)
	require.NotNil(t, cs2)

	require.Equal(t, cs.Epoch(), cs2.Epoch())
	require.Equal(t, cs.Len(), cs2.Len())

	for i := 0; i < cs.Len(); i++ {
		v1, err := cs.Get(uint32(i))
		require.NoError(t, err)
		v2, err := cs2.Get(uint32(i))
		require.NoError(t, err)
		require.Equal(t, v1, v2, "centroid %d must round-trip through msgpack", i)
	}
}

func TestCentroidSet_AssignNearestCosineIsScaleInvariant(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(1, [][]float32{
		{1, 0},
		{0, 5},
	})
	require.NoError(t, err)

	id, _, err := cs.AssignNearest([]float32{0, 2}, metric.MetricCosine)
	require.NoError(t, err)
	require.Equal(t, uint32(1), id)
}

func TestCentroidSet_AssignTopNCosineMatchesOrder(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(1, [][]float32{
		{1, 0},
		{1, 1},
		{0, 1},
	})
	require.NoError(t, err)

	ids, _, err := cs.AssignTopN([]float32{1, 0.2}, 2, metric.MetricCosine)
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 1}, ids)
}

func TestCentroidSet_AssignTopNUntilBudgetMatchesSortedPrefix(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(1, [][]float32{{0}, {10}, {20}, {30}})
	require.NoError(t, err)

	ids, _, err := cs.AssignTopNUntilBudget([]float32{9}, []uint64{7000, 900, 500, 6000}, 8192, 1, 4, metric.MetricL2)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 0, 2}, ids)
}

func TestCentroidSet_AssignTopNUntilBudgetHonorsZeroCountsAndMinProbe(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(1, [][]float32{{0}, {10}, {20}, {30}})
	require.NoError(t, err)

	ids, _, err := cs.AssignTopNUntilBudget([]float32{9}, []uint64{0, 0, 100, 100}, 1, 3, 4, metric.MetricL2)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 0, 2}, ids)
}

func TestCentroidSet_AssignTopNUntilBudgetCosineTieBreaksByID(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(1, [][]float32{{1, 0}, {2, 0}, {0, 1}})
	require.NoError(t, err)

	ids, _, err := cs.AssignTopNUntilBudget([]float32{1, 0}, []uint64{10, 10, 10}, 1, 1, 3, metric.MetricCosine)
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, ids)
}
