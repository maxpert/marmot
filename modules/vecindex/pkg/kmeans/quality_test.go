package kmeans_test

import (
	"math/rand"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

// inertia computes the sum of squared L2 distances from each vector to its
// assigned centroid.
func inertia(vecs [][]float32, centroids [][]float32) float64 {
	var total float64
	for _, v := range vecs {
		_, d, err := kmeans.Assign(v, centroids, metric.MetricL2)
		if err != nil {
			return -1
		}
		total += float64(d)
	}
	return total
}

func TestClusterQuality_InertiaDecreases(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(77))
	vecs := make([][]float32, 500)
	for i := range vecs {
		v := make([]float32, 4)
		for d := range v {
			v[d] = float32(rng.NormFloat64() * 10)
		}
		vecs[i] = v
	}

	c2, err := kmeans.KMeansPlusPlus(vecs, 2, 1, 50)
	require.NoError(t, err)

	c4, err := kmeans.KMeansPlusPlus(vecs, 4, 1, 50)
	require.NoError(t, err)

	i2 := inertia(vecs, c2)
	i4 := inertia(vecs, c4)
	require.Greater(t, i2, i4, "inertia with k=4 must be lower than with k=2")
}

func TestClusterQuality_AssignmentBalanced(t *testing.T) {
	t.Parallel()
	const (
		n   = 1000
		dim = 4
		k   = 8
	)
	rng := rand.New(rand.NewSource(55))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for d := range v {
			v[d] = float32(rng.Float64())
		}
		vecs[i] = v
	}

	centroids, err := kmeans.KMeansPlusPlus(vecs, k, 42, 100)
	require.NoError(t, err)

	counts := make([]int, k)
	for _, v := range vecs {
		id, _, err := kmeans.Assign(v, centroids, metric.MetricL2)
		require.NoError(t, err)
		counts[id]++
	}

	minCount, maxCount := counts[0], counts[0]
	for _, c := range counts {
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
	}
	require.Greater(t, minCount, 0, "no cluster should be empty on uniform data")
	ratio := float64(maxCount) / float64(minCount)
	require.Less(t, ratio, 3.0, "cluster sizes should not differ by more than 3x")
}
