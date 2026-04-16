package kmeans_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

// gaussianBlob generates count vectors of dimension dim centred at mean with stddev.
func gaussianBlob(rng *rand.Rand, count, dim int, mean []float32, stddev float32) [][]float32 {
	vecs := make([][]float32, count)
	for i := range vecs {
		v := make([]float32, dim)
		for d := range v {
			v[d] = mean[d] + float32(rng.NormFloat64())*stddev
		}
		vecs[i] = v
	}
	return vecs
}

func TestKMeansPlusPlus_DeterministicSeed(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42))
	vecs := make([][]float32, 100)
	for i := range vecs {
		v := make([]float32, 8)
		for d := range v {
			v[d] = float32(rng.Float64())
		}
		vecs[i] = v
	}

	c1, err := kmeans.KMeansPlusPlus(vecs, 5, 7, 20)
	require.NoError(t, err)

	c2, err := kmeans.KMeansPlusPlus(vecs, 5, 7, 20)
	require.NoError(t, err)

	require.Equal(t, len(c1), len(c2), "centroid count must match")
	for i := range c1 {
		require.Equal(t, c1[i], c2[i], "centroid %d must be byte-identical", i)
	}
}

func TestKMeansPlusPlus_DifferentSeeds(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(99))
	vecs := make([][]float32, 200)
	for i := range vecs {
		v := make([]float32, 8)
		for d := range v {
			v[d] = float32(rng.Float64() * 100)
		}
		vecs[i] = v
	}

	c1, err := kmeans.KMeansPlusPlus(vecs, 5, 1, 20)
	require.NoError(t, err)

	c2, err := kmeans.KMeansPlusPlus(vecs, 5, 999, 20)
	require.NoError(t, err)

	require.Equal(t, 5, len(c1))
	require.Equal(t, 5, len(c2))
	// Different seeds may (almost certainly) produce different centroids.
	// We just assert both are valid (correct k, correct dim).
	for _, c := range c1 {
		require.Equal(t, 8, len(c))
	}
	for _, c := range c2 {
		require.Equal(t, 8, len(c))
	}
}

func TestKMeansPlusPlus_ConvergesOnClusteredData(t *testing.T) {
	t.Parallel()
	const dim = 8
	rng := rand.New(rand.NewSource(1234))

	means := [][]float32{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{100, 100, 100, 100, 100, 100, 100, 100},
		{-100, 50, -50, 50, -50, 50, -50, 50},
	}

	var vecs [][]float32
	for _, m := range means {
		vecs = append(vecs, gaussianBlob(rng, 100, dim, m, 1.0)...)
	}

	centroids, err := kmeans.KMeansPlusPlus(vecs, 3, 42, 100)
	require.NoError(t, err)
	require.Len(t, centroids, 3)

	// Each found centroid should be within 5.0 of one of the blob means.
	const tolerance = 5.0
	matched := make([]bool, 3)
	for _, c := range centroids {
		for j, m := range means {
			var d float64
			for i := range c {
				diff := float64(c[i] - m[i])
				d += diff * diff
			}
			if math.Sqrt(d) < tolerance {
				matched[j] = true
			}
		}
	}
	for j := range matched {
		require.True(t, matched[j], "no centroid found near blob mean %d", j)
	}
}

func TestKMeansPlusPlus_K1(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{
		{1, 2, 3},
		{3, 4, 5},
		{5, 6, 7},
	}
	// mean = {3, 4, 5}
	centroids, err := kmeans.KMeansPlusPlus(vecs, 1, 0, 20)
	require.NoError(t, err)
	require.Len(t, centroids, 1)

	for i, got := range centroids[0] {
		expected := float32(i + 3)
		require.InDelta(t, expected, got, 0.01, "centroid[0][%d]", i)
	}
}

func TestKMeansPlusPlus_KEqualsN(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{
		{1, 0},
		{0, 1},
		{1, 1},
	}
	centroids, err := kmeans.KMeansPlusPlus(vecs, 3, 7, 20)
	require.NoError(t, err)
	require.Len(t, centroids, 3)

	// Each input vector should be "claimed" by some centroid.
	// With k == n, centroids should equal the input vectors (in some order).
	for _, v := range vecs {
		found := false
		for _, c := range centroids {
			if len(c) == len(v) {
				var d float32
				for i := range v {
					diff := v[i] - c[i]
					d += diff * diff
				}
				if d < 1e-6 {
					found = true
					break
				}
			}
		}
		require.True(t, found, "input vector %v not represented in centroids", v)
	}
}

func TestKMeansPlusPlus_ErrorOnKZero(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{{1, 2}, {3, 4}}
	_, err := kmeans.KMeansPlusPlus(vecs, 0, 0, 20)
	require.Error(t, err)
}

func TestKMeansPlusPlus_ErrorOnKGreaterThanN(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{{1, 2}, {3, 4}}
	_, err := kmeans.KMeansPlusPlus(vecs, 5, 0, 20)
	require.Error(t, err)
}

func TestKMeansPlusPlus_ErrorOnEmptyInput(t *testing.T) {
	t.Parallel()
	_, err := kmeans.KMeansPlusPlus([][]float32{}, 1, 0, 20)
	require.Error(t, err)
}

func TestKMeansPlusPlus_DimensionConsistency(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{
		{1, 2, 3},
		{4, 5}, // wrong dim
		{7, 8, 9},
	}
	_, err := kmeans.KMeansPlusPlus(vecs, 2, 0, 20)
	require.Error(t, err)
}

func TestKMeansPlusPlus_MaxIterZero(t *testing.T) {
	t.Parallel()
	vecs := [][]float32{{1, 2}, {3, 4}, {5, 6}}
	_, err := kmeans.KMeansPlusPlus(vecs, 2, 0, 0)
	require.Error(t, err, "maxIter=0 must return an error")
}
