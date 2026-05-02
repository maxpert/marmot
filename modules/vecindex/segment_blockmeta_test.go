package vecindex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func TestSegmentBlockMetaStoreRoundTripAllowsSegmentClusterOrder(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 8, Metric: MetricL2, Nlist: 2, Nprobe: 1}
	cs, err := kmeans.NewCentroidSet(7, [][]float32{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{1, 1, 1, 1, 1, 1, 1, 1},
	})
	require.NoError(t, err)
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "blocks", "gen.blk")
	writer, err := CreateSegmentBlockMetaWriter(path, spec, codec, 2, 2, 7, 11)
	require.NoError(t, err)

	offset := uint64(4096)
	for _, row := range []struct {
		clusterID int64
		rowID     int64
		vec       []float32
	}{
		{clusterID: 2, rowID: 20, vec: []float32{1.1, 1, 1, 1, 1, 1, 1, 1}},
		{clusterID: 2, rowID: 21, vec: []float32{1, 1.1, 1, 1, 1, 1, 1, 1}},
		{clusterID: 1, rowID: 10, vec: []float32{0.1, 0, 0, 0, 0, 0, 0, 0}},
	} {
		_, encoded, err := codec.Encode(row.clusterID, Float32ToBytes(row.vec))
		require.NoError(t, err)
		require.NoError(t, writer.Append(row.clusterID, row.rowID, offset, 8+len(encoded), encoded))
		offset += uint64(8 + len(encoded))
	}

	store, err := writer.Close()
	require.NoError(t, err)
	defer store.Close()

	require.Equal(t, uint64(2), store.RecordCount())
	clusterTwo, err := store.ReadClusterBlocks([]int64{2})
	require.NoError(t, err)
	require.Len(t, clusterTwo, 1)
	require.Equal(t, int64(2), clusterTwo[0].ClusterID)
	require.Equal(t, uint64(2), clusterTwo[0].RowCount)
	require.Equal(t, int64(20), clusterTwo[0].MinRowID)
	require.Equal(t, int64(21), clusterTwo[0].MaxRowID)

	clusterOne, err := store.ReadClusterBlocks([]int64{1})
	require.NoError(t, err)
	require.Len(t, clusterOne, 1)
	require.Equal(t, int64(1), clusterOne[0].ClusterID)
	require.Equal(t, uint64(1), clusterOne[0].RowCount)
}

func TestSegmentBlockMetaWriterRejectsReopenedCluster(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 8, Metric: MetricL2, Nlist: 2, Nprobe: 1}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{1, 1, 1, 1, 1, 1, 1, 1},
	})
	require.NoError(t, err)
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
	require.NoError(t, err)
	writer, err := CreateSegmentBlockMetaWriter(filepath.Join(t.TempDir(), "gen.blk"), spec, codec, 4, 2, 1, 1)
	require.NoError(t, err)
	defer writer.Abort()

	_, first, err := codec.Encode(2, Float32ToBytes([]float32{1, 1, 1, 1, 1, 1, 1, 1}))
	require.NoError(t, err)
	_, second, err := codec.Encode(1, Float32ToBytes([]float32{0, 0, 0, 0, 0, 0, 0, 0}))
	require.NoError(t, err)
	require.NoError(t, writer.Append(2, 20, 0, 8+len(first), first))
	require.NoError(t, writer.Append(1, 10, uint64(8+len(first)), 8+len(second), second))
	require.ErrorContains(t, writer.Append(2, 21, uint64(16+len(first)+len(second)), 8+len(first), first), "cannot be reopened")
}

func TestSegmentBlockMetaStoreRejectsTornFile(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 8, Metric: MetricL2, Nlist: 1, Nprobe: 1}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{{0, 0, 0, 0, 0, 0, 0, 0}})
	require.NoError(t, err)
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "gen.blk")
	writer, err := CreateSegmentBlockMetaWriter(path, spec, codec, 4, 1, 1, 1)
	require.NoError(t, err)
	_, encoded, err := codec.Encode(1, Float32ToBytes([]float32{0.1, 0, 0, 0, 0, 0, 0, 0}))
	require.NoError(t, err)
	require.NoError(t, writer.Append(1, 1, 0, 8+len(encoded), encoded))
	store, err := writer.Close()
	require.NoError(t, err)
	require.NoError(t, store.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, info.Size()-1))
	reopened, err := OpenSegmentBlockMetaStore(path)
	if reopened != nil {
		require.NoError(t, reopened.Close())
	}
	require.ErrorContains(t, err, "invalid block meta file size")
}

func TestSegmentBlockMetaStoreValidateCoverage(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 8, Metric: MetricL2, Nlist: 2, Nprobe: 1}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{1, 1, 1, 1, 1, 1, 1, 1},
	})
	require.NoError(t, err)
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
	require.NoError(t, err)

	dir := t.TempDir()
	dataWriter, err := CreateSegmentDataWriter(filepath.Join(dir, "gen.dat"), spec.InternalMetric(), codec.Encoding(), spec.Dim, spec.InternalDim(), codec.EncodedSize(), 2, 1, 1)
	require.NoError(t, err)
	blockWriter, err := CreateSegmentBlockMetaWriter(filepath.Join(dir, "gen.blk"), spec, codec, 4, 2, 1, 1)
	require.NoError(t, err)

	for _, row := range []struct {
		clusterID int64
		rowID     int64
		vec       []float32
	}{
		{clusterID: 1, rowID: 10, vec: []float32{0.1, 0, 0, 0, 0, 0, 0, 0}},
		{clusterID: 2, rowID: 20, vec: []float32{1.1, 1, 1, 1, 1, 1, 1, 1}},
	} {
		_, encoded, err := codec.Encode(row.clusterID, Float32ToBytes(row.vec))
		require.NoError(t, err)
		offset := dataWriter.NextOffset()
		require.NoError(t, dataWriter.Append(row.clusterID, row.rowID, encoded))
		require.NoError(t, blockWriter.Append(row.clusterID, row.rowID, offset, dataWriter.EntrySize(), encoded))
	}

	dataStore, err := dataWriter.Close()
	require.NoError(t, err)
	defer dataStore.Close()
	blockStore, err := blockWriter.Close()
	require.NoError(t, err)
	defer blockStore.Close()

	require.NoError(t, blockStore.ValidateCoverage(dataStore))
}

func TestSegmentBlockMetaStoreValidateCoverageRejectsMissingCluster(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 8, Metric: MetricL2, Nlist: 2, Nprobe: 1}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{1, 1, 1, 1, 1, 1, 1, 1},
	})
	require.NoError(t, err)
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
	require.NoError(t, err)

	dir := t.TempDir()
	dataWriter, err := CreateSegmentDataWriter(filepath.Join(dir, "gen.dat"), spec.InternalMetric(), codec.Encoding(), spec.Dim, spec.InternalDim(), codec.EncodedSize(), 2, 1, 1)
	require.NoError(t, err)
	blockWriter, err := CreateSegmentBlockMetaWriter(filepath.Join(dir, "gen.blk"), spec, codec, 4, 2, 1, 1)
	require.NoError(t, err)

	_, first, err := codec.Encode(1, Float32ToBytes([]float32{0, 0, 0, 0, 0, 0, 0, 0}))
	require.NoError(t, err)
	firstOffset := dataWriter.NextOffset()
	require.NoError(t, dataWriter.Append(1, 10, first))
	require.NoError(t, blockWriter.Append(1, 10, firstOffset, dataWriter.EntrySize(), first))

	_, second, err := codec.Encode(2, Float32ToBytes([]float32{1, 1, 1, 1, 1, 1, 1, 1}))
	require.NoError(t, err)
	require.NoError(t, dataWriter.Append(2, 20, second))

	dataStore, err := dataWriter.Close()
	require.NoError(t, err)
	defer dataStore.Close()
	blockStore, err := blockWriter.Close()
	require.NoError(t, err)
	defer blockStore.Close()

	require.ErrorContains(t, blockStore.ValidateCoverage(dataStore), "missing cluster 2")
}
