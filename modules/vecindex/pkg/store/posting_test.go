package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func makeVec(dim int, val float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = val
	}
	return v
}

func TestInsertPostingEntry(t *testing.T) {
	s := openStore(t)
	vec := []float32{1.1, 2.2, 3.3}
	require.NoError(t, s.PutPosting(5, 100, vec))

	got, err := s.GetPosting(5, 100)
	require.NoError(t, err)
	require.Equal(t, vec, got)
}

func TestRangeScanCluster(t *testing.T) {
	s := openStore(t)
	const clusterID = uint32(42)
	const n = 100

	for i := uint64(0); i < n; i++ {
		require.NoError(t, s.PutPosting(clusterID, i, makeVec(4, float32(i))))
	}

	entries, err := s.ScanCluster(clusterID)
	require.NoError(t, err)
	require.Len(t, entries, n)

	// Must be sorted by docID ascending.
	for i := 1; i < len(entries); i++ {
		require.Less(t, entries[i-1].DocID, entries[i].DocID)
	}
}

func TestRangeScanSkipsOtherClusters(t *testing.T) {
	s := openStore(t)

	for _, c := range []uint32{41, 42, 43} {
		for i := uint64(0); i < 10; i++ {
			require.NoError(t, s.PutPosting(c, i, makeVec(2, float32(c))))
		}
	}

	entries, err := s.ScanCluster(42)
	require.NoError(t, err)
	require.Len(t, entries, 10)
	for _, e := range entries {
		require.Equal(t, makeVec(2, float32(42)), e.Vector)
	}
}

func TestDeletePosting(t *testing.T) {
	s := openStore(t)
	require.NoError(t, s.PutPosting(42, 7, makeVec(3, 1.0)))
	require.NoError(t, s.DeletePosting(42, 7))

	entries, err := s.ScanCluster(42)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestRangeScanEmpty(t *testing.T) {
	s := openStore(t)
	entries, err := s.ScanCluster(999)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestInlineVectorSize(t *testing.T) {
	s := openStore(t)
	const dim = 1536
	vec := makeVec(dim, 3.14)
	require.NoError(t, s.PutPosting(1, 1, vec))

	got, err := s.GetPosting(1, 1)
	require.NoError(t, err)
	require.Len(t, got, dim)
	require.Equal(t, vec, got)
}
