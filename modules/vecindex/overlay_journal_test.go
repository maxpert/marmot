package vecindex

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOverlayBuffer_ApplyBatchMutationSemantics(t *testing.T) {
	t.Parallel()

	buffer := NewOverlayBuffer()
	err := buffer.ApplyBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 0, RowID: 10, Vec: encodeOverlayTestVec(1, 0)},
		{Kind: OverlayMutationReplace, Epoch: 1, Sequence: 2, ClusterID: 2, RowID: 11, Vec: encodeOverlayTestVec(0, 1)},
		{Kind: OverlayMutationDelete, Epoch: 1, Sequence: 3, ClusterID: 0, RowID: 10},
	})
	require.NoError(t, err)

	snapshot := buffer.Snapshot()
	require.Equal(t, uint64(1), snapshot.Epoch())
	require.Equal(t, uint64(3), snapshot.LastSequence())
	require.Equal(t, 1, snapshot.Len())
	require.True(t, snapshot.HasTombstone(10))
	require.True(t, snapshot.HasTombstone(11))
	clusterID, ok := snapshot.RowCluster(11)
	require.True(t, ok)
	require.Equal(t, int64(2), clusterID)

	var rowIDs []int64
	snapshot.VisitCluster(2, func(rowID int64, vec []byte) bool {
		rowIDs = append(rowIDs, rowID)
		require.Equal(t, encodeOverlayTestVec(0, 1), vec)
		return true
	})
	require.Equal(t, []int64{11}, rowIDs)
}

func TestOverlayBuffer_EpochAdvanceResetsPreviousState(t *testing.T) {
	t.Parallel()

	buffer := NewOverlayBuffer()
	require.NoError(t, buffer.ApplyBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 1, Vec: encodeOverlayTestVec(1, 0)},
	}))
	require.NoError(t, buffer.ApplyBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 2, Sequence: 1, ClusterID: 3, RowID: 99, Vec: encodeOverlayTestVec(0, 1)},
	}))

	snapshot := buffer.Snapshot()
	require.Equal(t, uint64(2), snapshot.Epoch())
	require.Equal(t, uint64(1), snapshot.LastSequence())
	_, ok := snapshot.RowCluster(1)
	require.False(t, ok, "epoch bump should discard prior overlay rows")
	clusterID, ok := snapshot.RowCluster(99)
	require.True(t, ok)
	require.Equal(t, int64(3), clusterID)
}

func TestOverlayBuffer_SnapshotStableAcrossMutation(t *testing.T) {
	t.Parallel()

	buffer := NewOverlayBuffer()
	require.NoError(t, buffer.ApplyBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 1, Vec: encodeOverlayTestVec(1, 0)},
	}))
	stale := buffer.Snapshot()
	require.NoError(t, buffer.ApplyBatch([]OverlayMutation{
		{Kind: OverlayMutationReplace, Epoch: 1, Sequence: 2, ClusterID: 2, RowID: 1, Vec: encodeOverlayTestVec(0, 1)},
	}))

	clusterID, ok := stale.RowCluster(1)
	require.True(t, ok)
	require.Equal(t, int64(1), clusterID)
	require.False(t, stale.HasTombstone(1))

	latest := buffer.Snapshot()
	clusterID, ok = latest.RowCluster(1)
	require.True(t, ok)
	require.Equal(t, int64(2), clusterID)
	require.True(t, latest.HasTombstone(1))
}

func TestJournaledOverlay_ReplayOnReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 7, Sequence: 1, ClusterID: 0, RowID: 10, Vec: encodeOverlayTestVec(1, 0)},
		{Kind: OverlayMutationReplace, Epoch: 7, Sequence: 2, ClusterID: 2, RowID: 11, Vec: encodeOverlayTestVec(0, 1)},
	}))
	require.NoError(t, overlay.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	require.Equal(t, uint64(7), snapshot.Epoch())
	require.Equal(t, uint64(2), snapshot.LastSequence())
	clusterID, ok := snapshot.RowCluster(10)
	require.True(t, ok)
	require.Equal(t, int64(0), clusterID)
	clusterID, ok = snapshot.RowCluster(11)
	require.True(t, ok)
	require.Equal(t, int64(2), clusterID)
	require.True(t, snapshot.HasTombstone(11))
}

func TestOverlayJournal_TruncatedTailIgnoredOnOpen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 3, Sequence: 1, ClusterID: 1, RowID: 1, Vec: encodeOverlayTestVec(1, 0)},
		{Kind: OverlayMutationReplace, Epoch: 3, Sequence: 2, ClusterID: 2, RowID: 2, Vec: encodeOverlayTestVec(0, 1)},
	}))
	require.NoError(t, overlay.Close())

	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = file.Write([]byte{0xaa, 0xbb, 0xcc})
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	require.Equal(t, uint64(3), snapshot.Epoch())
	require.Equal(t, uint64(2), snapshot.LastSequence())
	clusterID, ok := snapshot.RowCluster(1)
	require.True(t, ok)
	require.Equal(t, int64(1), clusterID)
	clusterID, ok = snapshot.RowCluster(2)
	require.True(t, ok)
	require.Equal(t, int64(2), clusterID)
}

func TestOverlayJournal_ResetCompactsState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 1, Vec: encodeOverlayTestVec(1, 0)},
	}))
	require.NoError(t, overlay.Reset(5))
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 5, Sequence: 1, ClusterID: 2, RowID: 22, Vec: encodeOverlayTestVec(0, 1)},
	}))
	require.NoError(t, overlay.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	require.Equal(t, uint64(5), snapshot.Epoch())
	require.Equal(t, uint64(1), snapshot.LastSequence())
	_, ok := snapshot.RowCluster(1)
	require.False(t, ok)
	clusterID, ok := snapshot.RowCluster(22)
	require.True(t, ok)
	require.Equal(t, int64(2), clusterID)
}

func TestJournaledOverlay_CompactAfterPreservesHighWatermark(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 4, Sequence: 1, ClusterID: 1, RowID: 1, Vec: encodeOverlayTestVec(1, 0)},
		{Kind: OverlayMutationDelete, Epoch: 4, Sequence: 2, RowID: 1},
	}))
	require.NoError(t, overlay.CompactAfter(2))
	require.NoError(t, overlay.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	require.Equal(t, uint64(4), snapshot.Epoch())
	require.Equal(t, uint64(2), snapshot.LastSequence())
	require.Equal(t, 0, snapshot.Len())

	require.NoError(t, reopened.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 4, Sequence: 3, ClusterID: 2, RowID: 2, Vec: encodeOverlayTestVec(0, 1)},
	}))
	snapshot = reopened.Snapshot()
	require.Equal(t, uint64(3), snapshot.LastSequence())
	clusterID, ok := snapshot.RowCluster(2)
	require.True(t, ok)
	require.Equal(t, int64(2), clusterID)
}

func TestOverlayJournal_RejectsSequenceRegression(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, overlay.Close()) })

	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 2, ClusterID: 1, RowID: 1, Vec: encodeOverlayTestVec(1, 0)},
	}))
	err = overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 2, ClusterID: 1, RowID: 2, Vec: encodeOverlayTestVec(0, 1)},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "sequence")
}

func TestOverlayJournal_RejectsCorruptInteriorRecord(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 1, Vec: encodeOverlayTestVec(1, 0)},
	}))
	require.NoError(t, overlay.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), overlayJournalHeaderSize+8)

	recordLen := int(binary.LittleEndian.Uint32(data[overlayJournalHeaderSize : overlayJournalHeaderSize+4]))
	payloadStart := overlayJournalHeaderSize + 4
	require.Greater(t, recordLen, 0)
	data[payloadStart+recordLen-1] ^= 0xff
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = OpenJournaledOverlay(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum")
}

func encodeOverlayTestVec(values ...float32) []byte {
	return Float32ToBytes(values)
}
