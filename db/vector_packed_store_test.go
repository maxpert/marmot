package db

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
)

func TestRebuildPackedPartitionStore(t *testing.T) {
	t.Parallel()

	tdb := openTestDBWithMeta(t, filepath.Join(t.TempDir(), "packed.db"))
	db := tdb.DB
	idx := "packed_idx"
	mt := vecindex.MembersTable(idx)
	_, err := db.Exec(`CREATE TABLE "` + mt + `" (
		cluster_id INTEGER NOT NULL,
		rowid INTEGER NOT NULL,
		vec BLOB NOT NULL,
		PRIMARY KEY (cluster_id, rowid)
	) WITHOUT ROWID`)
	if err != nil {
		t.Fatalf("create members: %v", err)
	}
	for _, row := range []struct {
		clusterID int64
		rowid     int64
		vec       []byte
	}{
		{0, 1, float32sToBlob([]float32{9, 9})},
		{1, 11, float32sToBlob([]float32{1, 0})},
		{1, 12, float32sToBlob([]float32{0.5, 0.5})},
		{2, 21, float32sToBlob([]float32{0, 1})},
	} {
		if _, err := db.Exec(`INSERT INTO "`+mt+`" (cluster_id, rowid, vec) VALUES (?, ?, ?)`, row.clusterID, row.rowid, row.vec); err != nil {
			t.Fatalf("seed row %+v: %v", row, err)
		}
	}

	store, err := RebuildPackedPartitionStore(context.Background(), db, tdb.dbPath, common.VectorIndexMeta{
		IndexName: idx,
	}, vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricCosine})
	if err != nil {
		t.Fatalf("RebuildPackedPartitionStore: %v", err)
	}
	defer store.Close()

	var got []int64
	store.ScanClusters([]int64{1, 2}, func(rowid int64, vec []byte) bool {
		got = append(got, rowid)
		return true
	})
	if !reflect.DeepEqual(got, []int64{11, 12, 21}) {
		t.Fatalf("packed rowids = %v want [11 12 21]", got)
	}
}
