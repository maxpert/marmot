package store_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

// TestBatchInsert_Atomic verifies that a batch write spanning all relevant
// namespaces (0x02, 0x03, 0x04, 0x05, 0x06) either commits fully or not at all.
func TestBatchInsert_Atomic(t *testing.T) {
	s := openStore(t)
	const (
		clusterID  = uint32(1)
		docID      = uint64(100)
		externalID = "ext-100"
	)
	vec := makeVec(4, 1.0)
	meta := store.ClusterMeta{Size: 1, Epoch: 1, TombstoneCount: 0, State: store.ClusterStateActive}

	b := s.NewBatch()
	require.NoError(t, b.BatchPutPosting(clusterID, docID, vec))
	require.NoError(t, b.BatchPutReverseMap(docID, clusterID))
	require.NoError(t, b.BatchPutClusterMeta(clusterID, meta))
	require.NoError(t, b.BatchPutExtToDoc([]byte(externalID), docID))
	require.NoError(t, b.BatchPutDocToExt(docID, []byte(externalID)))
	require.NoError(t, b.Commit())

	// All namespaces must reflect the write.
	gotVec, err := s.GetPosting(clusterID, docID)
	require.NoError(t, err)
	require.Equal(t, vec, gotVec)

	gotCluster, err := s.GetClusterForDoc(docID)
	require.NoError(t, err)
	require.Equal(t, clusterID, gotCluster)

	gotMeta, err := s.GetClusterMeta(clusterID)
	require.NoError(t, err)
	require.Equal(t, uint32(1), gotMeta.Size)

	gotDoc, err := s.GetExtToDoc([]byte(externalID))
	require.NoError(t, err)
	require.Equal(t, docID, gotDoc)

	gotExt, err := s.GetDocToExt(docID)
	require.NoError(t, err)
	require.Equal(t, []byte(externalID), gotExt)
}

// TestBatchUpdate_MovesCluster verifies an atomic cluster reassignment:
// old posting removed, new posting added, reverse map updated, both metas updated.
func TestBatchUpdate_MovesCluster(t *testing.T) {
	s := openStore(t)
	const (
		oldCluster = uint32(3)
		newCluster = uint32(7)
		docID      = uint64(42)
	)
	vec := makeVec(4, 2.0)

	// Seed initial state.
	b0 := s.NewBatch()
	require.NoError(t, b0.BatchPutPosting(oldCluster, docID, vec))
	require.NoError(t, b0.BatchPutReverseMap(docID, oldCluster))
	require.NoError(t, b0.BatchPutClusterMeta(oldCluster, store.ClusterMeta{Size: 1, State: store.ClusterStateActive}))
	require.NoError(t, b0.BatchPutClusterMeta(newCluster, store.ClusterMeta{Size: 0, State: store.ClusterStateActive}))
	require.NoError(t, b0.Commit())

	// Move doc from oldCluster → newCluster.
	b1 := s.NewBatch()
	require.NoError(t, b1.BatchDeletePosting(oldCluster, docID))
	require.NoError(t, b1.BatchPutPosting(newCluster, docID, vec))
	require.NoError(t, b1.BatchPutReverseMap(docID, newCluster))
	require.NoError(t, b1.BatchPutClusterMeta(oldCluster, store.ClusterMeta{Size: 0, TombstoneCount: 1, State: store.ClusterStateActive}))
	require.NoError(t, b1.BatchPutClusterMeta(newCluster, store.ClusterMeta{Size: 1, State: store.ClusterStateActive}))
	require.NoError(t, b1.Commit())

	// Verify state after move.
	gotCluster, err := s.GetClusterForDoc(docID)
	require.NoError(t, err)
	require.Equal(t, newCluster, gotCluster)

	oldEntries, err := s.ScanCluster(oldCluster)
	require.NoError(t, err)
	require.Empty(t, oldEntries)

	newEntries, err := s.ScanCluster(newCluster)
	require.NoError(t, err)
	require.Len(t, newEntries, 1)
	require.Equal(t, docID, newEntries[0].DocID)

	oldMeta, err := s.GetClusterMeta(oldCluster)
	require.NoError(t, err)
	require.Equal(t, uint32(0), oldMeta.Size)
	require.Equal(t, uint32(1), oldMeta.TombstoneCount)

	newMeta, err := s.GetClusterMeta(newCluster)
	require.NoError(t, err)
	require.Equal(t, uint32(1), newMeta.Size)
}

// TestBatchDelete_RemovesAll verifies full deletion of a document across all namespaces.
func TestBatchDelete_RemovesAll(t *testing.T) {
	s := openStore(t)
	const (
		clusterID  = uint32(5)
		docID      = uint64(77)
		externalID = "ext-77"
	)
	vec := makeVec(3, 0.5)

	// Insert.
	b0 := s.NewBatch()
	require.NoError(t, b0.BatchPutPosting(clusterID, docID, vec))
	require.NoError(t, b0.BatchPutReverseMap(docID, clusterID))
	require.NoError(t, b0.BatchPutClusterMeta(clusterID, store.ClusterMeta{Size: 1, State: store.ClusterStateActive}))
	require.NoError(t, b0.BatchPutExtToDoc([]byte(externalID), docID))
	require.NoError(t, b0.BatchPutDocToExt(docID, []byte(externalID)))
	require.NoError(t, b0.Commit())

	// Delete.
	b1 := s.NewBatch()
	require.NoError(t, b1.BatchDeletePosting(clusterID, docID))
	require.NoError(t, b1.BatchDeleteExtMapping([]byte(externalID), docID))
	require.NoError(t, b1.BatchPutClusterMeta(clusterID, store.ClusterMeta{Size: 0, TombstoneCount: 1, State: store.ClusterStateActive}))
	require.NoError(t, b1.Commit())

	// Everything must be gone.
	_, err := s.GetPosting(clusterID, docID)
	require.ErrorIs(t, err, store.ErrNotFound)

	_, err = s.GetExtToDoc([]byte(externalID))
	require.ErrorIs(t, err, store.ErrNotFound)

	_, err = s.GetDocToExt(docID)
	require.ErrorIs(t, err, store.ErrNotFound)

	meta, err := s.GetClusterMeta(clusterID)
	require.NoError(t, err)
	require.Equal(t, uint32(0), meta.Size)
	require.Equal(t, uint32(1), meta.TombstoneCount)
}

// TestBatch_OrderIndependent verifies that two batches adding to the same
// cluster produce deterministic results regardless of write order.
func TestBatch_OrderIndependent(t *testing.T) {
	s := openStore(t)
	const clusterID = uint32(9)

	b1 := s.NewBatch()
	require.NoError(t, b1.BatchPutPosting(clusterID, 1, makeVec(2, 1.0)))
	require.NoError(t, b1.BatchPutPosting(clusterID, 2, makeVec(2, 2.0)))
	require.NoError(t, b1.Commit())

	b2 := s.NewBatch()
	require.NoError(t, b2.BatchPutPosting(clusterID, 2, makeVec(2, 2.0)))
	require.NoError(t, b2.BatchPutPosting(clusterID, 1, makeVec(2, 1.0)))
	require.NoError(t, b2.Commit())

	entries, err := s.ScanCluster(clusterID)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].DocID)
	require.Equal(t, uint64(2), entries[1].DocID)
}
