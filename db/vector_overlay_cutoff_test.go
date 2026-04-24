package db

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func TestOverlayPreparedVectorCutoffSequenceBoundsPrefix(t *testing.T) {
	overlay, err := vecindex.OpenJournaledOverlay(filepath.Join(t.TempDir(), "overlay.journal"))
	require.NoError(t, err)
	defer overlay.Close()

	now := time.Now().UnixNano()
	require.NoError(t, overlay.ApplyCommittedBatch([]vecindex.OverlayMutation{
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 1, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{1, 0})},
		{Kind: vecindex.OverlayMutationDelete, Epoch: 1, Sequence: 2, RowID: 2, AppliedAtUnixNano: now},
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 1, Sequence: 3, ClusterID: 1, RowID: 3, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{0, 1})},
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 1, Sequence: 4, ClusterID: 1, RowID: 4, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{1, 1})},
		{Kind: vecindex.OverlayMutationDelete, Epoch: 1, Sequence: 5, RowID: 5, AppliedAtUnixNano: now},
	}))

	cutoff, rows := overlayPreparedVectorCutoffSequence(overlay.Snapshot(), 2)
	require.Equal(t, uint64(3), cutoff)
	require.Equal(t, 2, rows)

	cutoff, rows = overlayPreparedVectorCutoffSequence(overlay.Snapshot(), 10)
	require.Equal(t, uint64(5), cutoff)
	require.Equal(t, 3, rows)
}

func TestOverlayMutationCutoffSequenceBoundsPrefix(t *testing.T) {
	overlay, err := vecindex.OpenJournaledOverlay(filepath.Join(t.TempDir(), "overlay.journal"))
	require.NoError(t, err)
	defer overlay.Close()

	now := time.Now().UnixNano()
	require.NoError(t, overlay.ApplyCommittedBatch([]vecindex.OverlayMutation{
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 1, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{1, 0})},
		{Kind: vecindex.OverlayMutationDelete, Epoch: 1, Sequence: 2, RowID: 2, AppliedAtUnixNano: now},
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 1, Sequence: 3, ClusterID: 1, RowID: 3, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{0, 1})},
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 1, Sequence: 4, ClusterID: 1, RowID: 4, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{1, 1})},
	}))

	cutoff, rows := overlayMutationCutoffSequence(overlay.Snapshot(), 0, 2)
	require.Equal(t, uint64(2), cutoff)
	require.Equal(t, 2, rows)

	cutoff, rows = overlayMutationCutoffSequence(overlay.Snapshot(), 2, 10)
	require.Equal(t, uint64(4), cutoff)
	require.Equal(t, 2, rows)
}

func TestReconcileOverlayForStateRewritesEpochAndKeepsTail(t *testing.T) {
	overlay, err := vecindex.OpenJournaledOverlay(filepath.Join(t.TempDir(), "overlay.journal"))
	require.NoError(t, err)
	defer overlay.Close()

	now := time.Now().UnixNano()
	require.NoError(t, overlay.ApplyCommittedBatch([]vecindex.OverlayMutation{
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 0, Sequence: 1, ClusterID: 0, RowID: 1, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{1, 0})},
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 0, Sequence: 2, ClusterID: 0, RowID: 2, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{0, 1})},
		{Kind: vecindex.OverlayMutationUpsert, Epoch: 0, Sequence: 3, ClusterID: 0, RowID: 3, AppliedAtUnixNano: now, Vec: encodeVec(t, []float32{1, 1})},
	}))

	cs, err := kmeans.NewCentroidSet(1, [][]float32{{1, 0}})
	require.NoError(t, err)
	spec := vecindex.IVFSpec{ID: "idx", Dim: 2, Metric: vecindex.MetricL2, Nlist: 1, Nprobe: 1}
	state := vecindex.NewIndexState(spec, cs)
	state.StoreSegmentStore(&vecindex.SegmentGeneration{AppliedOverlaySeq: 2})

	require.NoError(t, reconcileOverlayForState(state, overlay, 1))

	snapshot := overlay.Snapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(1), snapshot.Epoch())
	require.Equal(t, uint64(3), snapshot.LastSequence())
	require.Equal(t, 1, snapshot.Len())
	_, ok := snapshot.RowCluster(1)
	require.False(t, ok)
	_, ok = snapshot.RowCluster(2)
	require.False(t, ok)
	clusterID, ok := snapshot.RowCluster(3)
	require.True(t, ok)
	require.Equal(t, int64(1), clusterID)
}
