package db

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

func TestRebuildSegmentGeneration(t *testing.T) {
	t.Parallel()

	tdb := openTestDBWithMeta(t, filepath.Join(t.TempDir(), "segment.db"))
	db := tdb.DB
	idx := "segment_idx"
	baseTable := "docs"
	_, err := db.Exec(`CREATE TABLE docs (embed BLOB)`)
	if err != nil {
		t.Fatalf("create base table: %v", err)
	}
	for _, row := range []struct {
		rowid int64
		vec   []float32
	}{
		{11, []float32{1, 0}},
		{12, []float32{0.5, 0.5}},
		{21, []float32{0, 1}},
	} {
		if _, err := db.Exec(`INSERT INTO docs(rowid, embed) VALUES (?, ?)`, row.rowid, float32sToBlob(row.vec)); err != nil {
			t.Fatalf("insert base row %+v: %v", row, err)
		}
	}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{{1, 0}, {0, 1}})
	if err != nil {
		t.Fatalf("new centroid set: %v", err)
	}

	generation, err := RebuildSegmentGeneration(context.Background(), db, tdb.dbPath, common.VectorIndexMeta{
		IndexName:  idx,
		TableName:  baseTable,
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "cosine",
		Dim:        2,
		CreatedAt:  1,
	}, vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricCosine, Nlist: 2, Nprobe: 1}, cs, 0, map[int64]uint64{2: 5})
	if err != nil {
		t.Fatalf("RebuildSegmentGeneration: %v", err)
	}
	defer generation.Close()
	if !reflect.DeepEqual(generation.LayoutHotClusters, []int64{2}) {
		t.Fatalf("layout hot clusters = %v, want [2]", generation.LayoutHotClusters)
	}
	if got := generation.Data.Encoding(); got != vecindex.MemberEncodingResidualInt8 {
		t.Fatalf("segment encoding = %d, want residual-int8", got)
	}
	if _, want := vecindex.StableMemberEncodingSpec(vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricCosine, Nlist: 2, Nprobe: 1}); generation.Data.VecBytes() != want {
		t.Fatalf("segment vec bytes = %d, want %d", generation.Data.VecBytes(), want)
	}

	var got []int64
	if err := generation.Data.ScanClusters([]int64{1, 2}, func(_clusterID, rowid int64, _vec []byte) bool {
		got = append(got, rowid)
		return true
	}); err != nil {
		t.Fatalf("ScanClusters: %v", err)
	}
	if !reflect.DeepEqual(got, []int64{11, 12, 21}) {
		t.Fatalf("segment rowids = %v want [11 12 21]", got)
	}

	if loc, ok, err := generation.RowMap.Lookup(21); err != nil {
		t.Fatalf("RowMap.Lookup: %v", err)
	} else if !ok || loc.ClusterID != 2 {
		t.Fatalf("rowmap lookup = %+v, %v want cluster 2", loc, ok)
	}
}

func TestOrderClustersByProjection(t *testing.T) {
	t.Parallel()

	order := orderClustersByProjection(
		[][]float32{
			{0},
			{2},
			{-1},
		},
		[]float32{1},
		nil,
	)
	if !reflect.DeepEqual(order, []int64{3, 1, 2}) {
		t.Fatalf("order = %v, want [3 1 2]", order)
	}
}

func TestOrderClustersByProjection_PrefersHotClusters(t *testing.T) {
	t.Parallel()

	order := orderClustersByProjection(
		[][]float32{
			{0},
			{2},
			{-1},
		},
		[]float32{1},
		map[int64]uint64{2: 9},
	)
	if !reflect.DeepEqual(order, []int64{2, 3, 1}) {
		t.Fatalf("order = %v, want [2 3 1]", order)
	}
}

func TestDeterministicSegmentProjectionStable(t *testing.T) {
	t.Parallel()

	got1 := deterministicSegmentProjection(42, 4)
	got2 := deterministicSegmentProjection(42, 4)
	if !reflect.DeepEqual(got1, got2) {
		t.Fatalf("projection mismatch: %v vs %v", got1, got2)
	}
	if len(got1) != 4 {
		t.Fatalf("projection len = %d, want 4", len(got1))
	}
}

func TestRebuildSegmentGeneration_DotMetricUsesInternalL2(t *testing.T) {
	t.Parallel()

	tdb := openTestDBWithMeta(t, filepath.Join(t.TempDir(), "segment-dot.db"))
	db := tdb.DB
	idx := "segment_dot_idx"
	_, err := db.Exec(`CREATE TABLE docs (embed BLOB)`)
	if err != nil {
		t.Fatalf("create base table: %v", err)
	}
	for _, row := range []struct {
		rowid int64
		vec   []float32
	}{
		{11, []float32{1, 0}},
		{21, []float32{0, 1}},
	} {
		if _, err := db.Exec(`INSERT INTO docs(rowid, embed) VALUES (?, ?)`, row.rowid, float32sToBlob(row.vec)); err != nil {
			t.Fatalf("insert base row %+v: %v", row, err)
		}
	}
	c0, err := metric.AugmentData([]float32{1, 0}, 2, nil)
	if err != nil {
		t.Fatalf("augment c0: %v", err)
	}
	c1, err := metric.AugmentData([]float32{0, 1}, 2, nil)
	if err != nil {
		t.Fatalf("augment c1: %v", err)
	}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{c0, c1})
	if err != nil {
		t.Fatalf("new centroid set: %v", err)
	}

	spec := vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricDot, Nlist: 2, Nprobe: 1, MaxNorm: 2}
	generation, err := RebuildSegmentGeneration(context.Background(), db, tdb.dbPath, common.VectorIndexMeta{
		IndexName:  idx,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "dot",
		Dim:        2,
		MaxNorm:    2,
		CreatedAt:  1,
	}, spec, cs, 0, nil)
	if err != nil {
		t.Fatalf("RebuildSegmentGeneration: %v", err)
	}
	defer generation.Close()
	if got := generation.Data.Metric(); got != vecindex.MetricL2 {
		t.Fatalf("segment metric = %v, want internal l2", got)
	}
	if got := generation.Data.Encoding(); got != vecindex.MemberEncodingResidualInt8 {
		t.Fatalf("segment encoding = %d, want residual-int8", got)
	}
}
