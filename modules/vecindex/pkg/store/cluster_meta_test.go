package store_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestClusterMeta_Roundtrip(t *testing.T) {
	s := openStore(t)
	meta := store.ClusterMeta{
		Size:           123,
		Epoch:          7,
		TombstoneCount: 4,
		State:          store.ClusterStateActive,
	}
	require.NoError(t, s.PutClusterMeta(1, meta))

	got, err := s.GetClusterMeta(1)
	require.NoError(t, err)
	require.Equal(t, meta, got)
}

func TestClusterMeta_StateTransitions(t *testing.T) {
	s := openStore(t)

	states := []store.ClusterState{
		store.ClusterStateActive,
		store.ClusterStateSplitting,
		store.ClusterStateRetired,
	}
	for i, st := range states {
		meta := store.ClusterMeta{Size: uint32(i), State: st}
		require.NoError(t, s.PutClusterMeta(uint32(i), meta))
		got, err := s.GetClusterMeta(uint32(i))
		require.NoError(t, err)
		require.Equal(t, st, got.State)
	}
}

func TestClusterMeta_IncrementCounters(t *testing.T) {
	s := openStore(t)
	const clusterID = uint32(2)

	require.NoError(t, s.PutClusterMeta(clusterID, store.ClusterMeta{Size: 10, TombstoneCount: 2, State: store.ClusterStateActive}))

	// Read-modify-write via batch to increment size.
	meta, err := s.GetClusterMeta(clusterID)
	require.NoError(t, err)
	meta.Size++
	meta.TombstoneCount++

	b := s.NewBatch()
	require.NoError(t, b.BatchPutClusterMeta(clusterID, meta))
	require.NoError(t, b.Commit())

	got, err := s.GetClusterMeta(clusterID)
	require.NoError(t, err)
	require.Equal(t, uint32(11), got.Size)
	require.Equal(t, uint32(3), got.TombstoneCount)
}

func TestClusterMeta_ListActiveOnly(t *testing.T) {
	s := openStore(t)

	require.NoError(t, s.PutClusterMeta(1, store.ClusterMeta{Size: 10, State: store.ClusterStateActive}))
	require.NoError(t, s.PutClusterMeta(2, store.ClusterMeta{Size: 5, State: store.ClusterStateSplitting}))
	require.NoError(t, s.PutClusterMeta(3, store.ClusterMeta{Size: 0, State: store.ClusterStateRetired}))
	require.NoError(t, s.PutClusterMeta(4, store.ClusterMeta{Size: 8, State: store.ClusterStateActive}))

	ids, err := s.ListActiveClusters()
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{1, 4}, ids)
}
