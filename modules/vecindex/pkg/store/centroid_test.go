package store_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestPutGetCentroid(t *testing.T) {
	s := openStore(t)
	vec := []float32{0.1, 0.2, 0.3, 0.4}
	require.NoError(t, s.PutCentroid(7, vec, len(vec)))

	got, err := s.GetCentroid(7)
	require.NoError(t, err)
	require.Equal(t, vec, got)
}

func TestListAllCentroids(t *testing.T) {
	s := openStore(t)
	const n = 5
	for i := uint32(0); i < n; i++ {
		require.NoError(t, s.PutCentroid(i, []float32{float32(i), float32(i + 1)}, 2))
	}

	ids, vecs, err := s.ListCentroids()
	require.NoError(t, err)
	require.Len(t, ids, n)
	require.Len(t, vecs, n)

	for i := 1; i < len(ids); i++ {
		require.Less(t, ids[i-1], ids[i], "cluster IDs must be sorted ascending")
	}
}

func TestDeleteCentroid(t *testing.T) {
	s := openStore(t)
	require.NoError(t, s.PutCentroid(3, []float32{1, 2, 3}, 3))
	require.NoError(t, s.DeleteCentroid(3))

	_, err := s.GetCentroid(3)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestCentroidDimensionMismatch(t *testing.T) {
	s := openStore(t)
	// dim=4 but vec has 3 elements — must error.
	err := s.PutCentroid(1, []float32{1, 2, 3}, 4)
	require.Error(t, err)
}
