package kmeans_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

func TestAssign_ReturnsNearestCentroid(t *testing.T) {
	t.Parallel()
	centroids := [][]float32{
		{0, 0},   // id=0
		{10, 0},  // id=1
		{0, 10},  // id=2
		{10, 10}, // id=3
	}
	// Query near centroid 2
	q := []float32{0.5, 9.5}
	id, _, err := kmeans.Assign(q, centroids, metric.MetricL2)
	require.NoError(t, err)
	require.Equal(t, uint32(2), id)
}

func TestAssign_TieBreaksByLowerID(t *testing.T) {
	t.Parallel()
	// Two centroids equidistant from query; lower ID wins.
	centroids := [][]float32{
		{-1, 0}, // id=0, dist=1
		{1, 0},  // id=1, dist=1
	}
	q := []float32{0, 0}
	id, _, err := kmeans.Assign(q, centroids, metric.MetricL2)
	require.NoError(t, err)
	require.Equal(t, uint32(0), id, "tie must resolve to lower cluster ID")
}

func TestAssign_EmptyCentroidsError(t *testing.T) {
	t.Parallel()
	_, _, err := kmeans.Assign([]float32{1, 2}, [][]float32{}, metric.MetricL2)
	require.Error(t, err)
}

func TestAssign_DifferentMetrics(t *testing.T) {
	t.Parallel()
	// Craft centroids so that different metrics return different nearest.
	// c0 = {2, 0}: large magnitude, dot product with q={1,0} = 2
	// c1 = {0, 1}: unit, dot product with q={1,0} = 0
	// Under MetricDot (negative dot): -2 vs 0 → c0 is closer (lower value wins).
	// Under MetricCosine: c0 aligns with q perfectly (cos dist=0), c1 is 90° away (cos dist=1) → c0 wins.
	// Under MetricL2: dist(q,c0)=1, dist(q,c1)=sqrt(2) → c0 wins.
	// So we need a case where c1 beats c0 for at least one metric.
	// Use q={1,1}/sqrt(2) normalised-ish: c0={1,0}, c1={0,1}
	// Cosine: both equidistant from q={1,1}; tie → id=0.
	// Let's do: q={3,1}, c0={1,0} (aligned roughly), c1={0,4} (closer in L2).
	// L2: d(q,c0)^2 = 4+1=5, d(q,c1)^2=9+9=18 → c0
	// Cosine: cos(q,c0)=3/sqrt(10), cos(q,c1)=4/sqrt(170) → c0 wins
	// Use separate test for MetricDot where a high-magnitude centroid wins.

	centroids := [][]float32{
		{0, 10}, // id=0: far in L2, but high dot with y-heavy query
		{1, 0},  // id=1: close in L2 to q={0,1}
	}
	q := []float32{0, 1}

	// MetricL2: d(q,c0)^2=0+81=81, d(q,c1)^2=1+1=2 → c1
	idL2, _, err := kmeans.Assign(q, centroids, metric.MetricL2)
	require.NoError(t, err)
	require.Equal(t, uint32(1), idL2, "MetricL2 should pick nearest in Euclidean space")

	// MetricDot: score = -dot; dot(q,c0)=10, dot(q,c1)=0 → -10 vs 0 → c0 wins (lower score)
	idDot, _, err := kmeans.Assign(q, centroids, metric.MetricDot)
	require.NoError(t, err)
	require.Equal(t, uint32(0), idDot, "MetricDot should pick highest inner product")

	// MetricCosine: cos(q,c0): q={0,1}, c0={0,10} → cos=1 → dist=0; c1={1,0} → cos=0 → dist=1 → c0 wins
	idCos, _, err := kmeans.Assign(q, centroids, metric.MetricCosine)
	require.NoError(t, err)
	require.Equal(t, uint32(0), idCos, "MetricCosine should pick best cosine alignment")
}

func TestAssignTopN_ReturnsKNearest(t *testing.T) {
	t.Parallel()
	centroids := make([][]float32, 10)
	for i := range centroids {
		centroids[i] = []float32{float32(i * 10), 0}
	}
	// Query near centroid 0
	q := []float32{1, 0}
	ids, dists, err := kmeans.AssignTopN(q, centroids, 3, metric.MetricL2)
	require.NoError(t, err)
	require.Len(t, ids, 3)
	require.Len(t, dists, 3)

	// Must be sorted ascending by distance
	for i := 1; i < len(dists); i++ {
		require.LessOrEqual(t, dists[i-1], dists[i], "distances must be in ascending order")
	}
	// Nearest 3 to q={1,0} are centroids at 0, 10, 20 → ids 0,1,2
	require.Equal(t, uint32(0), ids[0])
	require.Equal(t, uint32(1), ids[1])
	require.Equal(t, uint32(2), ids[2])
}

func TestAssignTopN_NGreaterThanK(t *testing.T) {
	t.Parallel()
	centroids := [][]float32{
		{1, 0},
		{0, 1},
		{1, 1},
	}
	q := []float32{0.5, 0.5}
	ids, dists, err := kmeans.AssignTopN(q, centroids, 10, metric.MetricL2)
	require.NoError(t, err)
	require.Len(t, ids, 3, "should return all centroids when n > len(centroids)")
	require.Len(t, dists, 3)

	// Verify sorted
	for i := 1; i < len(dists); i++ {
		require.LessOrEqual(t, dists[i-1], dists[i])
	}
}

func TestAssignTopN_NZero(t *testing.T) {
	t.Parallel()
	centroids := [][]float32{{1, 0}, {0, 1}}
	q := []float32{0.5, 0.5}
	ids, dists, err := kmeans.AssignTopN(q, centroids, 0, metric.MetricL2)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)
}
