package kmeans_test

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func TestKMeans_LargeVectors_1536D(t *testing.T) {
	t.Parallel()
	const (
		n    = 10_000
		dim  = 1536
		k    = 16
		seed = 123
	)
	rng := rand.New(rand.NewSource(seed))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for d := range v {
			v[d] = float32(rng.NormFloat64())
		}
		vecs[i] = v
	}

	start := time.Now()
	centroids, err := kmeans.KMeansPlusPlus(vecs, k, seed, 20)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Len(t, centroids, k)
	require.Less(t, elapsed, 5*time.Second, "k-means on 10K×1536D must complete within 5s")

	for i, c := range centroids {
		require.Len(t, c, dim, "centroid %d has wrong dimension", i)
		for d, val := range c {
			require.False(t, math.IsNaN(float64(val)), "centroid %d dim %d is NaN", i, d)
			require.False(t, math.IsInf(float64(val), 0), "centroid %d dim %d is Inf", i, d)
		}
	}
}

func TestKMeans_DuplicateVectors(t *testing.T) {
	t.Parallel()
	const (
		n   = 1000
		dim = 4
		k   = 5
	)
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(i + 1)
	}
	vecs := make([][]float32, n)
	for i := range vecs {
		cp := make([]float32, dim)
		copy(cp, v)
		vecs[i] = cp
	}

	// Must not crash; result is implementation-defined but must return k centroids.
	centroids, err := kmeans.KMeansPlusPlus(vecs, k, 0, 20)
	require.NoError(t, err)
	require.Len(t, centroids, k)
	for i, c := range centroids {
		require.Len(t, c, dim, "centroid %d has wrong dimension", i)
		for d, val := range c {
			require.False(t, math.IsNaN(float64(val)), "centroid %d dim %d is NaN", i, d)
			require.False(t, math.IsInf(float64(val), 0), "centroid %d dim %d is Inf", i, d)
		}
	}
}

func TestKMeans_SingleDimension(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{
		{1}, {1}, {1},
		{10}, {10}, {10},
		{20}, {20}, {20},
	}
	centroids, err := kmeans.KMeansPlusPlus(vecs, 3, 0, 100)
	require.NoError(t, err)
	require.Len(t, centroids, 3)

	expected := []float32{1, 10, 20}
	const tol = 1.0
	matched := make([]bool, 3)
	for _, c := range centroids {
		require.Len(t, c, 1)
		for j, e := range expected {
			if math.Abs(float64(c[0]-e)) < tol {
				matched[j] = true
			}
		}
	}
	for j, e := range expected {
		require.True(t, matched[j], "no centroid near expected value %v", e)
	}
}
