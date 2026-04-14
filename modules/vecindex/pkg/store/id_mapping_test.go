package store_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestExternalIDMapping_Bidirectional(t *testing.T) {
	s := openStore(t)
	const docID = uint64(10)
	extID := []byte("user:abc")

	require.NoError(t, s.PutExtToDoc(extID, docID))
	require.NoError(t, s.PutDocToExt(docID, extID))

	gotDoc, err := s.GetExtToDoc(extID)
	require.NoError(t, err)
	require.Equal(t, docID, gotDoc)

	gotExt, err := s.GetDocToExt(docID)
	require.NoError(t, err)
	require.Equal(t, extID, gotExt)
}

func TestExternalIDMapping_AllocNewDocID(t *testing.T) {
	s := openStore(t)
	extID := []byte("user:new")

	// First insert allocates a new doc ID.
	id1, err := s.AllocateClusterID()
	require.NoError(t, err)
	require.NoError(t, s.PutExtToDoc(extID, uint64(id1)))
	require.NoError(t, s.PutDocToExt(uint64(id1), extID))

	// Second alloc returns a different ID.
	id2, err := s.AllocateClusterID()
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	// Retrieving the ext still returns the original doc.
	gotDoc, err := s.GetExtToDoc(extID)
	require.NoError(t, err)
	require.Equal(t, uint64(id1), gotDoc)
}

func TestExternalIDMapping_DeleteRemovesBoth(t *testing.T) {
	s := openStore(t)
	const docID = uint64(20)
	extID := []byte("user:del")

	require.NoError(t, s.PutExtToDoc(extID, docID))
	require.NoError(t, s.PutDocToExt(docID, extID))
	require.NoError(t, s.DeleteExtMapping(extID, docID))

	_, err := s.GetExtToDoc(extID)
	require.ErrorIs(t, err, store.ErrNotFound)

	_, err = s.GetDocToExt(docID)
	require.ErrorIs(t, err, store.ErrNotFound)
}
