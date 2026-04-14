package store_test

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestCompactCluster_ReclaimsTombstones(t *testing.T) {
	s := openStore(t)
	const clusterID = uint32(10)
	const total = 100
	const toDelete = 30

	// Insert 100 docs.
	for i := uint64(0); i < total; i++ {
		require.NoError(t, s.PutPosting(clusterID, i, makeVec(4, float32(i))))
	}

	// Delete 30 docs (create tombstones).
	for i := uint64(0); i < toDelete; i++ {
		require.NoError(t, s.DeletePosting(clusterID, i))
	}

	// Compact should succeed.
	require.NoError(t, s.CompactCluster(clusterID))

	// Only 70 live entries should remain.
	entries, err := s.ScanCluster(clusterID)
	require.NoError(t, err)
	require.Len(t, entries, total-toDelete)
}

func TestCompactCluster_DoesNotAffectOtherClusters(t *testing.T) {
	s := openStore(t)

	// Populate three clusters.
	for _, c := range []uint32{41, 42, 43} {
		for i := uint64(0); i < 10; i++ {
			require.NoError(t, s.PutPosting(c, i, makeVec(2, float32(c))))
		}
	}

	// Delete all from cluster 42 then compact.
	for i := uint64(0); i < 10; i++ {
		require.NoError(t, s.DeletePosting(42, i))
	}
	require.NoError(t, s.CompactCluster(42))

	// Clusters 41 and 43 must be untouched.
	for _, c := range []uint32{41, 43} {
		entries, err := s.ScanCluster(c)
		require.NoError(t, err)
		require.Len(t, entries, 10, "cluster %d should still have 10 entries", c)
	}

	// Cluster 42 should be empty.
	entries, err := s.ScanCluster(42)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestTombstoneRatioTrigger(t *testing.T) {
	t.Parallel()

	cases := []struct {
		meta   store.ClusterMeta
		expect bool
	}{
		{store.ClusterMeta{Size: 70, TombstoneCount: 0}, false},
		{store.ClusterMeta{Size: 70, TombstoneCount: 29}, false}, // ~29.3% — below threshold
		{store.ClusterMeta{Size: 70, TombstoneCount: 30}, false}, // exactly 30% — not strictly above 0.3
		{store.ClusterMeta{Size: 69, TombstoneCount: 30}, true},  // ~30.3% — above threshold
		{store.ClusterMeta{Size: 0, TombstoneCount: 0}, false},    // empty cluster
		{store.ClusterMeta{Size: 0, TombstoneCount: 10}, true},    // all tombstones
	}

	for _, tc := range cases {
		got := store.ShouldCompact(tc.meta)
		require.Equal(t, tc.expect, got,
			"ShouldCompact(%+v) = %v, want %v", tc.meta, got, tc.expect)
	}
}
