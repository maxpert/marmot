package db

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
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

func TestOpenSegmentGenerationResidualPQ8(t *testing.T) {
	t.Parallel()

	tdb := openTestDBWithMeta(t, filepath.Join(t.TempDir(), "segment-pq.db"))
	idx := "segment_pq_idx"
	meta := common.VectorIndexMeta{
		IndexName:  idx,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "l2",
		Dim:        vecindex.StablePQMinInternalDim,
		CreatedAt:  1,
	}
	spec := vecindex.IVFSpec{ID: idx, Dim: vecindex.StablePQMinInternalDim, Metric: vecindex.MetricL2, Nlist: 1, Nprobe: 1}
	centroid := make([]float32, spec.InternalDim())
	cs, err := kmeans.NewCentroidSet(1, [][]float32{centroid})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	residuals := make([][]float32, 32)
	for i := range residuals {
		residuals[i] = make([]float32, spec.InternalDim())
		for d := range residuals[i] {
			residuals[i][d] = float32(((i+1)*(d+3))%31) * 0.01
		}
	}
	pq, err := quantize.TrainPQ8(residuals, spec.InternalDim(), quantize.PQ8Options{M: 4, MaxIter: 3, Seed: 5})
	if err != nil {
		t.Fatalf("TrainPQ8: %v", err)
	}
	codec, err := vecindex.NewStableMemberCodec(spec, cs, vecindex.MemberEncodingResidualPQ8, pq)
	if err != nil {
		t.Fatalf("NewStableMemberCodec: %v", err)
	}
	codecBlob, err := vecindex.EncodeStableMemberCodecBlob(codec)
	if err != nil {
		t.Fatalf("EncodeStableMemberCodecBlob: %v", err)
	}

	dir := vecindex.SegmentStoreDir(tdb.dbPath, idx)
	generation := uint64(1)
	dataWriter, err := vecindex.CreateSegmentDataWriter(
		vecindex.SegmentDataPath(dir, generation),
		spec.InternalMetric(),
		codec.Encoding(),
		spec.Dim,
		spec.InternalDim(),
		codec.EncodedSize(),
		1,
		cs.Epoch(),
		generation,
	)
	if err != nil {
		t.Fatalf("CreateSegmentDataWriter: %v", err)
	}
	rowMapWriter, err := vecindex.CreateSegmentRowMapWriter(vecindex.SegmentRowMapPath(dir, generation), cs.Epoch(), generation)
	if err != nil {
		t.Fatalf("CreateSegmentRowMapWriter: %v", err)
	}
	vec := make([]float32, spec.InternalDim())
	for i := range vec {
		vec[i] = residuals[17][i]
	}
	_, encoded, err := codec.Encode(1, vecindex.Float32ToBytes(vec))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	offset := dataWriter.NextOffset()
	if err := dataWriter.Append(1, 11, encoded); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := rowMapWriter.Append(11, 1, offset); err != nil {
		t.Fatalf("Append rowmap: %v", err)
	}
	dataStore, err := dataWriter.Close()
	if err != nil {
		t.Fatalf("Close data: %v", err)
	}
	rowMapStore, err := rowMapWriter.Close()
	if err != nil {
		t.Fatalf("Close rowmap: %v", err)
	}
	if err := rowMapStore.Close(); err != nil {
		t.Fatalf("Close rowmap store: %v", err)
	}
	if err := dataStore.Close(); err != nil {
		t.Fatalf("Close data store: %v", err)
	}
	manifest := vecindex.SegmentManifest{
		Version:               vecindex.SegmentStoreVersion,
		Database:              meta.Database,
		IndexName:             meta.IndexName,
		IndexCreatedAt:        meta.CreatedAt,
		Metric:                meta.Metric,
		Dim:                   uint32(meta.Dim),
		InternalDim:           uint32(spec.InternalDim()),
		ProbeCentroidEpoch:    cs.Epoch(),
		ProbeCentroidBlob:     mustCentroidBlob(cs),
		StableCentroidEpoch:   cs.Epoch(),
		StableCentroidBlob:    mustCentroidBlob(cs),
		StableMemberCodecBlob: codecBlob,
		AppliedOverlaySeq:     7,
		Generation:            generation,
		MaxCluster:            1,
		RowCount:              1,
		ClusterRowCounts:      []uint64{0, 1},
		ClusterVectorSums:     [][]float32{nil, append([]float32(nil), vec...)},
		CreatedAtUnixNano:     1,
	}
	if err := publishSegmentGeneration(dir, manifest, dataStore.Path(), rowMapStore.Path()); err != nil {
		t.Fatalf("publishSegmentGeneration: %v", err)
	}

	opened, err := openSegmentGeneration(dir, meta, spec, cs.Epoch())
	if err != nil {
		t.Fatalf("openSegmentGeneration: %v", err)
	}
	defer opened.Close()
	if got := opened.Data.Encoding(); got != vecindex.MemberEncodingResidualPQ8 {
		t.Fatalf("encoding = %d, want PQ", got)
	}
	if got := opened.Data.VecBytes(); got != codec.EncodedSize() {
		t.Fatalf("vec bytes = %d, want %d", got, codec.EncodedSize())
	}
	loc, ok, err := opened.RowMap.Lookup(11)
	if err != nil || !ok {
		t.Fatalf("RowMap.Lookup = %+v %v %v", loc, ok, err)
	}
	if loc.Offset != offset {
		t.Fatalf("row offset = %d, want %d", loc.Offset, offset)
	}
	decoded, err := opened.StableCodec.DecodePrepared(1, encoded)
	if err != nil {
		t.Fatalf("DecodePrepared: %v", err)
	}
	if len(decoded) != spec.InternalDim() {
		t.Fatalf("decoded dim = %d, want %d", len(decoded), spec.InternalDim())
	}
}

func TestBuildIncrementalSegmentGeneration_RewritesTouchedClustersOnly(t *testing.T) {
	t.Parallel()

	tdb := openTestDBWithMeta(t, filepath.Join(t.TempDir(), "segment-incremental.db"))
	db := tdb.DB
	idx := "segment_incremental_idx"
	_, err := db.Exec(`CREATE TABLE docs (embed BLOB)`)
	if err != nil {
		t.Fatalf("create base table: %v", err)
	}
	for _, row := range []struct {
		rowid int64
		vec   []float32
	}{
		{11, []float32{1, 0}},
		{12, []float32{0.8, 0.2}},
		{21, []float32{0, 1}},
		{31, []float32{-1, 0}},
	} {
		if _, err := db.Exec(`INSERT INTO docs(rowid, embed) VALUES (?, ?)`, row.rowid, float32sToBlob(row.vec)); err != nil {
			t.Fatalf("insert base row %+v: %v", row, err)
		}
	}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{{1, 0}, {0, 1}, {-1, 0}})
	if err != nil {
		t.Fatalf("new centroid set: %v", err)
	}

	meta := common.VectorIndexMeta{
		IndexName:  idx,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "cosine",
		Dim:        2,
		CreatedAt:  1,
	}
	spec := vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricCosine, Nlist: 3, Nprobe: 1}
	base, err := RebuildSegmentGeneration(context.Background(), db, tdb.dbPath, meta, spec, cs, 0, nil)
	if err != nil {
		t.Fatalf("RebuildSegmentGeneration: %v", err)
	}
	defer base.Close()

	overlay := vecindex.NewOverlayBuffer()
	row12, err := materializeVectorBlob(float32sToBlob([]float32{0.1, 0.9}), spec.Metric, spec.Dim, spec.MaxNorm)
	if err != nil {
		t.Fatalf("materialize row12: %v", err)
	}
	row41, err := materializeVectorBlob(float32sToBlob([]float32{0.9, 0.1}), spec.Metric, spec.Dim, spec.MaxNorm)
	if err != nil {
		t.Fatalf("materialize row41: %v", err)
	}
	err = overlay.ApplyBatch([]vecindex.OverlayMutation{
		{Kind: vecindex.OverlayMutationReplace, Epoch: cs.Epoch(), Sequence: 1, ClusterID: 2, RowID: 12, Vec: row12},
		{Kind: vecindex.OverlayMutationDelete, Epoch: cs.Epoch(), Sequence: 2, RowID: 21},
		{Kind: vecindex.OverlayMutationUpsert, Epoch: cs.Epoch(), Sequence: 3, ClusterID: 1, RowID: 41, Vec: row41},
	})
	if err != nil {
		t.Fatalf("overlay apply: %v", err)
	}

	stats, err := buildCutoffClusterStats(context.Background(), spec, base, overlay.Snapshot(), 3, nil, nil)
	if err != nil {
		t.Fatalf("buildCutoffClusterStats: %v", err)
	}

	pending, err := BuildIncrementalSegmentGeneration(
		context.Background(),
		db,
		tdb.dbPath,
		meta,
		spec,
		cs,
		cs,
		base,
		overlay.Snapshot(),
		3,
		stats.Counts,
		stats.Sums,
		nil,
	)
	if err != nil {
		t.Fatalf("BuildIncrementalSegmentGeneration: %v", err)
	}
	defer pending.Close()
	if _, err := os.Stat(pending.stagingDir); err != nil {
		t.Fatalf("pending staging dir missing before publish: %v", err)
	}
	if err := pending.Publish(); err != nil {
		t.Fatalf("publish incremental generation: %v", err)
	}
	if _, err := os.Stat(pending.stagingDir); !os.IsNotExist(err) {
		t.Fatalf("pending staging dir exists after publish: %v", err)
	}
	next := pending.generation
	pending.generation = nil
	defer next.Close()

	if got := next.AppliedOverlaySeq; got != 3 {
		t.Fatalf("AppliedOverlaySeq = %d, want 3", got)
	}
	if !reflect.DeepEqual(next.ClusterRowCounts, []uint64{0, 2, 1, 1}) {
		t.Fatalf("cluster row counts = %v, want [0 2 1 1]", next.ClusterRowCounts)
	}

	var rowIDs []int64
	if err := next.Data.ScanClusters([]int64{1, 2, 3}, func(_clusterID, rowid int64, _vec []byte) bool {
		rowIDs = append(rowIDs, rowid)
		return true
	}); err != nil {
		t.Fatalf("ScanClusters: %v", err)
	}
	sort.Slice(rowIDs, func(i, j int) bool { return rowIDs[i] < rowIDs[j] })
	if !reflect.DeepEqual(rowIDs, []int64{11, 12, 31, 41}) {
		t.Fatalf("segment rowids = %v, want [11 12 31 41]", rowIDs)
	}

	loc12, ok, err := next.RowMap.Lookup(12)
	if err != nil {
		t.Fatalf("rowmap lookup 12: %v", err)
	}
	if !ok || loc12.ClusterID != 2 {
		t.Fatalf("row 12 location = %+v, %v want cluster 2", loc12, ok)
	}
	loc31, ok, err := next.RowMap.Lookup(31)
	if err != nil {
		t.Fatalf("rowmap lookup 31: %v", err)
	}
	if !ok || loc31.ClusterID != 3 {
		t.Fatalf("row 31 location = %+v, %v want cluster 3", loc31, ok)
	}
	if _, ok, err := next.RowMap.Lookup(21); err != nil {
		t.Fatalf("rowmap lookup 21: %v", err)
	} else if ok {
		t.Fatalf("row 21 should be absent after delete")
	}
}

func TestOpenAndStoreOverlay_RehydratesMaintenanceStateFromOverlay(t *testing.T) {
	t.Parallel()

	tdb := openTestDBWithMeta(t, filepath.Join(t.TempDir(), "segment-reopen.db"))
	db := tdb.DB
	idx := "segment_reopen_idx"
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
	cs, err := kmeans.NewCentroidSet(1, [][]float32{{1, 0}, {0, 1}})
	if err != nil {
		t.Fatalf("new centroid set: %v", err)
	}
	meta := common.VectorIndexMeta{
		IndexName:  idx,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "cosine",
		Dim:        2,
		CreatedAt:  1,
	}
	spec := vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricCosine, Nlist: 2, Nprobe: 1}
	base, err := RebuildSegmentGeneration(context.Background(), db, tdb.dbPath, meta, spec, cs, 0, nil)
	if err != nil {
		t.Fatalf("RebuildSegmentGeneration: %v", err)
	}
	defer base.Close()

	state := vecindex.NewIndexState(spec, cs)
	state.StoreSegmentStore(base)

	dir := vecindex.SegmentStoreDir(tdb.dbPath, idx)
	overlay, err := vecindex.OpenJournaledOverlay(vecindex.OverlayJournalPath(dir))
	if err != nil {
		t.Fatalf("OpenJournaledOverlay: %v", err)
	}
	row31, err := materializeVectorBlob(float32sToBlob([]float32{0.2, 0.8}), spec.Metric, spec.Dim, spec.MaxNorm)
	if err != nil {
		t.Fatalf("materialize row31: %v", err)
	}
	if err := overlay.ApplyCommittedBatch([]vecindex.OverlayMutation{
		{Kind: vecindex.OverlayMutationUpsert, Epoch: cs.Epoch(), Sequence: 1, ClusterID: 2, RowID: 31, Vec: row31},
	}); err != nil {
		t.Fatalf("overlay apply: %v", err)
	}
	if err := overlay.Close(); err != nil {
		t.Fatalf("overlay close: %v", err)
	}

	if err := openAndStoreOverlay(tdb.dbPath, idx, state, cs.Epoch()); err != nil {
		t.Fatalf("openAndStoreOverlay: %v", err)
	}

	counts := state.LoadMaintenanceState().LiveClusterRowCounts()
	if !reflect.DeepEqual(counts, []uint64{0, 1, 2}) {
		t.Fatalf("live cluster row counts = %v, want [0 1 2]", counts)
	}
}
