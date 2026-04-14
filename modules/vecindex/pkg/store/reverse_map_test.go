package store_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestReverseMapLookup(t *testing.T) {
	s := openStore(t)
	const (
		clusterID = uint32(10)
		docID     = uint64(55)
	)

	b := s.NewBatch()
	require.NoError(t, b.BatchPutPosting(clusterID, docID, makeVec(3, 1.0)))
	require.NoError(t, b.BatchPutReverseMap(docID, clusterID))
	require.NoError(t, b.Commit())

	got, err := s.GetClusterForDoc(docID)
	require.NoError(t, err)
	require.Equal(t, clusterID, got)
}

func TestReverseMap_MissingDoc(t *testing.T) {
	s := openStore(t)
	_, err := s.GetClusterForDoc(99999)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestReverseMap_UpdateMoves(t *testing.T) {
	s := openStore(t)
	const (
		oldCluster = uint32(3)
		newCluster = uint32(7)
		docID      = uint64(42)
	)

	// Initial placement.
	require.NoError(t, s.PutReverseMap(docID, oldCluster))
	got, err := s.GetClusterForDoc(docID)
	require.NoError(t, err)
	require.Equal(t, oldCluster, got)

	// Move via batch update.
	b := s.NewBatch()
	require.NoError(t, b.BatchPutReverseMap(docID, newCluster))
	require.NoError(t, b.Commit())

	got, err = s.GetClusterForDoc(docID)
	require.NoError(t, err)
	require.Equal(t, newCluster, got)
}
