package vecindex

import (
	"encoding/binary"
	"hash/crc32"
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

func TestJournaledOverlay_ReplayPreservesCommitMetadata(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{
			Kind:              OverlayMutationUpsert,
			Epoch:             9,
			Sequence:          1,
			ClusterID:         3,
			RowID:             10,
			AppliedAtUnixNano: 111,
			CommitTxnID:       101,
			CommitSeqNum:      202,
			Vec:               encodeOverlayTestVec(1, 0),
		},
		{
			Kind:              OverlayMutationDelete,
			Epoch:             9,
			Sequence:          2,
			RowID:             11,
			AppliedAtUnixNano: 222,
			CommitTxnID:       103,
			CommitSeqNum:      204,
		},
	}))
	require.NoError(t, overlay.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	mutations := reopened.Snapshot().MutationsAfter(0)
	require.Len(t, mutations, 2)
	require.Equal(t, uint64(101), mutations[0].CommitTxnID)
	require.Equal(t, uint64(202), mutations[0].CommitSeqNum)
	require.Equal(t, uint64(103), mutations[1].CommitTxnID)
	require.Equal(t, uint64(204), mutations[1].CommitSeqNum)
}

func TestJournaledOverlay_ReplayPreservesVectorEncoding(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{
			Kind:        OverlayMutationUpsert,
			Epoch:       11,
			Sequence:    1,
			ClusterID:   3,
			RowID:       42,
			VecEncoding: OverlayResidualInt8,
			Vec:         []byte{1, 2, 3, 4},
		},
	}))
	require.NoError(t, overlay.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	var seen bool
	reopened.Snapshot().VisitClusterEncodedAfter(3, 0, func(rowID int64, encoding OverlayVecEncoding, vec []byte) bool {
		require.Equal(t, int64(42), rowID)
		require.Equal(t, OverlayResidualInt8, encoding)
		require.Equal(t, []byte{1, 2, 3, 4}, vec)
		seen = true
		return true
	})
	require.True(t, seen)
	mutations := reopened.Snapshot().MutationsAfter(0)
	require.Len(t, mutations, 1)
	require.Equal(t, OverlayResidualInt8, mutations[0].VecEncoding)
}

func TestJournaledOverlay_RejectsUnknownVectorEncoding(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, overlay.Close()) })

	err = overlay.ApplyCommittedBatch([]OverlayMutation{
		{
			Kind:        OverlayMutationUpsert,
			Epoch:       1,
			Sequence:    1,
			ClusterID:   1,
			RowID:       1,
			VecEncoding: OverlayVecEncoding(99),
			Vec:         []byte{1, 2, 3},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown overlay vector encoding")
}

func TestOverlayJournal_ReplayLegacyV2WithoutCommitMetadata(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay-v2.journal")
	vec := encodeOverlayTestVec(4, 5)
	payload := make([]byte, overlayJournalRecordFloorV2+len(vec))
	payload[0] = byte(OverlayMutationUpsert)
	binary.LittleEndian.PutUint64(payload[1:9], 4)
	binary.LittleEndian.PutUint64(payload[9:17], 1)
	binary.LittleEndian.PutUint64(payload[17:25], 2)
	binary.LittleEndian.PutUint64(payload[25:33], 99)
	binary.LittleEndian.PutUint64(payload[33:41], 1234)
	binary.LittleEndian.PutUint32(payload[41:45], uint32(len(vec)))
	copy(payload[overlayJournalRecordFloorV2:], vec)

	var header [overlayJournalHeaderSize]byte
	copy(header[:8], overlayJournalMagic)
	binary.LittleEndian.PutUint32(header[8:12], 2)
	binary.LittleEndian.PutUint64(header[12:20], 4)
	binary.LittleEndian.PutUint64(header[20:28], 1)

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	crc := crc32.Checksum(payload, overlayJournalCRCTable)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)

	file, err := os.Create(path)
	require.NoError(t, err)
	_, err = file.Write(header[:])
	require.NoError(t, err)
	_, err = file.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = file.Write(payload)
	require.NoError(t, err)
	_, err = file.Write(crcBuf[:])
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	require.Equal(t, uint64(4), snapshot.Epoch())
	require.Equal(t, uint64(1), snapshot.LastSequence())
	clusterID, ok := snapshot.RowCluster(99)
	require.True(t, ok)
	require.Equal(t, int64(2), clusterID)
	got, err := snapshot.ReadVec(99)
	require.NoError(t, err)
	require.Equal(t, vec, got)
	mutations := snapshot.MutationsAfter(0)
	require.Len(t, mutations, 1)
	require.Zero(t, mutations[0].CommitTxnID)
	require.Zero(t, mutations[0].CommitSeqNum)
}

func TestJournaledOverlay_SnapshotStoresJournalRefs(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, overlay.Close()) })

	vec := encodeOverlayTestVec(1, 2)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 10, Vec: vec},
	}))
	vec[0] ^= 0xff

	snapshot := overlay.Snapshot()
	row := snapshot.byCluster[1][10]
	require.Empty(t, row.vec.inline)
	require.Positive(t, row.vec.offset)
	require.Equal(t, len(encodeOverlayTestVec(1, 2)), row.vec.length)

	got, err := snapshot.ReadVec(10)
	require.NoError(t, err)
	require.Equal(t, encodeOverlayTestVec(1, 2), got)
}

func TestJournaledOverlay_ReplayStoresJournalRefs(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 7, Sequence: 1, ClusterID: 2, RowID: 11, Vec: encodeOverlayTestVec(3, 4)},
	}))
	require.NoError(t, overlay.Close())

	reopened, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	row := snapshot.byCluster[2][11]
	require.Empty(t, row.vec.inline)
	require.Positive(t, row.vec.offset)
	got, err := snapshot.ReadVec(11)
	require.NoError(t, err)
	require.Equal(t, encodeOverlayTestVec(3, 4), got)
}

func TestOverlayVecCacheEvictsAtByteCap(t *testing.T) {
	t.Parallel()

	cache := newOverlayVecCache(8)
	cache.Put(overlayVecKey{sequence: 1, rowID: 1}, []byte{1, 2, 3, 4})
	cache.Put(overlayVecKey{sequence: 2, rowID: 2}, []byte{5, 6, 7, 8})
	_, ok := cache.Get(overlayVecKey{sequence: 1, rowID: 1})
	require.True(t, ok)
	cache.Put(overlayVecKey{sequence: 3, rowID: 3}, []byte{9, 10, 11, 12})

	_, ok = cache.Get(overlayVecKey{sequence: 2, rowID: 2})
	require.False(t, ok)
	_, ok = cache.Get(overlayVecKey{sequence: 1, rowID: 1})
	require.True(t, ok)
	_, ok = cache.Get(overlayVecKey{sequence: 3, rowID: 3})
	require.True(t, ok)
}

func TestOverlaySnapshot_NewestUnixNanoAfter(t *testing.T) {
	t.Parallel()

	buffer := NewOverlayBuffer()
	require.NoError(t, buffer.ApplyBatch([]OverlayMutation{
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 1, ClusterID: 1, RowID: 1, AppliedAtUnixNano: 100, Vec: encodeOverlayTestVec(1, 0)},
		{Kind: OverlayMutationUpsert, Epoch: 1, Sequence: 2, ClusterID: 2, RowID: 2, AppliedAtUnixNano: 200, Vec: encodeOverlayTestVec(0, 1)},
		{Kind: OverlayMutationDelete, Epoch: 1, Sequence: 3, RowID: 3, AppliedAtUnixNano: 300},
	}))

	snapshot := buffer.Snapshot()
	require.Equal(t, int64(300), snapshot.NewestUnixNanoAfter(0))
	require.Equal(t, int64(300), snapshot.NewestUnixNanoAfter(2))
	require.Zero(t, snapshot.NewestUnixNanoAfter(3))
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

func TestOverlayJournal_OpenRemovesStaleTemp(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "overlay.journal")
	require.NoError(t, os.WriteFile(path+".tmp", []byte("stale"), 0o644))
	overlay, err := OpenJournaledOverlay(path)
	require.NoError(t, err)
	require.NoError(t, overlay.Close())

	_, err = os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err), "stale temp file should be removed on open")
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
