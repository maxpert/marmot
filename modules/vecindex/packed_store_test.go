package vecindex

import (
	"math"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPackedPartitionStoreRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "test.vecpack")
	w, err := CreatePackedPartitionStoreWriter(path, 2, 4)
	if err != nil {
		t.Fatalf("CreatePackedPartitionStoreWriter: %v", err)
	}
	rows := []struct {
		clusterID int64
		rowid     int64
		vec       []byte
	}{
		{1, 11, float32sToLE(1, 0)},
		{1, 12, float32sToLE(0.5, 0.5)},
		{3, 31, float32sToLE(0, 1)},
	}
	for _, row := range rows {
		if err := w.Append(row.clusterID, row.rowid, row.vec); err != nil {
			t.Fatalf("Append(%d,%d): %v", row.clusterID, row.rowid, err)
		}
	}
	store, err := w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	defer store.Close()

	if got := store.Dim(); got != 2 {
		t.Fatalf("Dim()=%d want 2", got)
	}

	var got1 []int64
	store.ScanCluster(1, func(rowid int64, vecBytes []byte) bool {
		got1 = append(got1, rowid)
		return true
	})
	if !reflect.DeepEqual(got1, []int64{11, 12}) {
		t.Fatalf("cluster 1 rowids = %v want [11 12]", got1)
	}

	var gotAll []int64
	store.ScanClusters([]int64{1, 2, 3}, func(rowid int64, vecBytes []byte) bool {
		gotAll = append(gotAll, rowid)
		return true
	})
	if !reflect.DeepEqual(gotAll, []int64{11, 12, 31}) {
		t.Fatalf("ScanClusters rowids = %v want [11 12 31]", gotAll)
	}
}

func TestPackedPartitionStoreRejectsOutOfOrderClusters(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "test.vecpack")
	w, err := CreatePackedPartitionStoreWriter(path, 2, 4)
	if err != nil {
		t.Fatalf("CreatePackedPartitionStoreWriter: %v", err)
	}
	defer w.Abort()
	if err := w.Append(2, 21, float32sToLE(1, 0)); err != nil {
		t.Fatalf("Append(2): %v", err)
	}
	if err := w.Append(1, 11, float32sToLE(0, 1)); err == nil {
		t.Fatal("expected out-of-order cluster append to fail")
	}
}

func float32sToLE(v ...float32) []byte {
	out := make([]byte, len(v)*4)
	for i, f := range v {
		bits := math.Float32bits(f)
		out[i*4] = byte(bits)
		out[i*4+1] = byte(bits >> 8)
		out[i*4+2] = byte(bits >> 16)
		out[i*4+3] = byte(bits >> 24)
	}
	return out
}
