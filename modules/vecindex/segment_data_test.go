package vecindex

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPlanSegmentReadBatchesCoalescesNearbySpans(t *testing.T) {
	t.Parallel()

	spans := []segmentClusterSpan{
		{clusterID: 1, offset: 128, bytes: 256, count: 1},
		{clusterID: 2, offset: 448, bytes: 256, count: 1}, // gap = 64
		{clusterID: 3, offset: 1408, bytes: 256, count: 1},
	}

	batches := planSegmentReadBatches(spans, 2048, 128)
	if len(batches) != 2 {
		t.Fatalf("batch count = %d, want 2", len(batches))
	}
	if got := []int64{
		batches[0].spans[0].clusterID,
		batches[0].spans[1].clusterID,
	}; !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("first batch clusters = %v, want [1 2]", got)
	}
	if got := batches[0].end - batches[0].start; got != 576 {
		t.Fatalf("first batch bytes = %d, want 576", got)
	}
	if got := []int64{batches[1].spans[0].clusterID}; !reflect.DeepEqual(got, []int64{3}) {
		t.Fatalf("second batch clusters = %v, want [3]", got)
	}
}

func TestPlanSegmentReadBatchesRespectsMaxBatchBytes(t *testing.T) {
	t.Parallel()

	spans := []segmentClusterSpan{
		{clusterID: 1, offset: 0, bytes: 1024, count: 1},
		{clusterID: 2, offset: 1024, bytes: 1024, count: 1},
		{clusterID: 3, offset: 2048, bytes: 1024, count: 1},
	}

	batches := planSegmentReadBatches(spans, 2048, 0)
	if len(batches) != 2 {
		t.Fatalf("batch count = %d, want 2", len(batches))
	}
	if got := len(batches[0].spans); got != 2 {
		t.Fatalf("first batch spans = %d, want 2", got)
	}
	if got := len(batches[1].spans); got != 1 {
		t.Fatalf("second batch spans = %d, want 1", got)
	}
}

func BenchmarkPlanSegmentReadBatches(b *testing.B) {
	spans := make([]segmentClusterSpan, 0, 64)
	var offset int64 = 4096
	for i := 0; i < 64; i++ {
		spans = append(spans, segmentClusterSpan{
			clusterID: int64(i + 1),
			offset:    offset,
			bytes:     128 << 10,
			count:     32,
		})
		offset += 128 << 10
		if i%3 == 0 {
			offset += 64 << 10
		}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = planSegmentReadBatches(spans, segmentReadBatch, segmentReadGap)
	}
}

func TestOpenSegmentDataStoreRejectsUnknownEncoding(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/segment.dat"
	writer, err := CreateSegmentDataWriter(path, MetricCosine, MemberEncodingResidualInt8, 2, 2, 4, 1, 1, 1)
	if err != nil {
		t.Fatalf("CreateSegmentDataWriter: %v", err)
	}
	if err := writer.Append(1, 11, make([]byte, 4)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	store, err := writer.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 13); err != nil {
		_ = f.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close file: %v", err)
	}
	if _, err := OpenSegmentDataStore(path); err == nil {
		t.Fatal("OpenSegmentDataStore succeeded with invalid encoding")
	}
}

func TestCreateSegmentDataWriterRejectsRetiredRawEncoding(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "segment.dat")
	_, err := CreateSegmentDataWriter(path, MetricCosine, MemberEncodingRawPreparedF32, 2, 2, 8, 1, 1, 1)
	if err == nil {
		t.Fatal("CreateSegmentDataWriter accepted raw stable encoding")
	}
}

func TestCreateSegmentDataWriterRejectsHighDimResidualInt8(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "segment.dat")
	_, err := CreateSegmentDataWriter(path, MetricCosine, MemberEncodingResidualInt8, StablePQMinInternalDim, StablePQMinInternalDim, StablePQMinInternalDim+4, 1, 1, 1)
	if err == nil {
		t.Fatal("CreateSegmentDataWriter accepted high-dimensional residual-int8 stable encoding")
	}
}

func TestCreateSegmentDataWriterAcceptsLowDimResidualInt8(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "segment.dat")
	vecBytes := 4
	writer, err := CreateSegmentDataWriter(path, MetricCosine, MemberEncodingResidualInt8, 2, 2, vecBytes, 1, 1, 1)
	if err != nil {
		t.Fatalf("CreateSegmentDataWriter: %v", err)
	}
	writer.Abort()
}

func TestSegmentDataStoreScanClustersFileOrderRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "segment.dat")
	writer, err := CreateSegmentDataWriter(path, MetricCosine, MemberEncodingResidualInt8, 2, 2, 4, 3, 1, 1)
	if err != nil {
		t.Fatalf("CreateSegmentDataWriter: %v", err)
	}
	rows := []struct {
		clusterID int64
		rowID     int64
		vec       []byte
	}{
		{clusterID: 1, rowID: 11, vec: []byte{1, 2, 3, 4}},
		{clusterID: 2, rowID: 21, vec: []byte{7, 8, 9, 10}},
		{clusterID: 2, rowID: 22, vec: []byte{13, 14, 15, 16}},
	}
	for _, row := range rows {
		if err := writer.Append(row.clusterID, row.rowID, row.vec); err != nil {
			t.Fatalf("Append(%+v): %v", row, err)
		}
	}
	store, err := writer.Close()
	if err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	defer store.Close()

	var got []int64
	if err := store.ScanClustersFileOrder([]int64{2, 1}, func(clusterID, rowID int64, _ []byte) bool {
		got = append(got, clusterID, rowID)
		return true
	}); err != nil {
		t.Fatalf("ScanClustersFileOrder: %v", err)
	}
	want := []int64{1, 11, 2, 21, 2, 22}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("file-order scan = %v, want %v", got, want)
	}
}

func TestSegmentDataStoreScanClustersFileOrderSpansRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "segment.dat")
	writer, err := CreateSegmentDataWriter(path, MetricCosine, MemberEncodingResidualInt8, 2, 2, 4, 3, 1, 1)
	if err != nil {
		t.Fatalf("CreateSegmentDataWriter: %v", err)
	}
	rows := []struct {
		clusterID int64
		rowID     int64
		vec       []byte
	}{
		{clusterID: 1, rowID: 11, vec: []byte{1, 2, 3, 4}},
		{clusterID: 2, rowID: 21, vec: []byte{7, 8, 9, 10}},
		{clusterID: 2, rowID: 22, vec: []byte{13, 14, 15, 16}},
	}
	for _, row := range rows {
		if err := writer.Append(row.clusterID, row.rowID, row.vec); err != nil {
			t.Fatalf("Append(%+v): %v", row, err)
		}
	}
	store, err := writer.Close()
	if err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	defer store.Close()

	var got []int64
	if err := store.ScanClustersFileOrderSpans([]int64{2, 1}, func(clusterID int64, rows []byte, count uint64, entrySize int) bool {
		cursor := 0
		for i := uint64(0); i < count; i++ {
			got = append(got, clusterID, int64(binary.LittleEndian.Uint64(rows[cursor:cursor+8])))
			cursor += entrySize
		}
		return true
	}); err != nil {
		t.Fatalf("ScanClustersFileOrderSpans: %v", err)
	}
	want := []int64{1, 11, 2, 21, 2, 22}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("file-order span scan = %v, want %v", got, want)
	}
}
