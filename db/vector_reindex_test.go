package db

import (
	"context"
	"database/sql"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

func setupReindexDB(t *testing.T, nVec int) (*sql.DB, *vecindex.Engine, vecindex.IVFSpec) {
	t.Helper()

	engine := vecindex.NewEngine()
	SetVectorUDFProvider(engine)
	t.Cleanup(func() { SetVectorUDFProvider(nil) })

	db := openVecDB(t)
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS __marmot_vector_indexes (
		index_name TEXT PRIMARY KEY,
		table_name TEXT NOT NULL,
		column_name TEXT NOT NULL,
		database_name TEXT NOT NULL,
		metric TEXT NOT NULL,
		dim INTEGER NOT NULL,
		nlist INTEGER NOT NULL,
		nprobe INTEGER NOT NULL,
		auto_nlist INTEGER NOT NULL DEFAULT 0,
		auto_nprobe INTEGER NOT NULL DEFAULT 0,
			target_partition_size INTEGER NOT NULL DEFAULT 512,
		max_norm REAL NOT NULL,
		status TEXT NOT NULL,
		created_at INTEGER NOT NULL
	)`)
	require.NoError(t, err)

	createdAt := time.Now().UnixNano()
	_, err = db.Exec(`INSERT INTO __marmot_vector_indexes
		(index_name, table_name, column_name, database_name, metric, dim,
		 nlist, nprobe, auto_nlist, auto_nprobe, target_partition_size,
		 max_norm, status, created_at)
			VALUES (?, 'docs', 'embed', 'test', 'l2', 4, 4, 2, 0, 0, ?, 0, 'ready', ?)`,
		"embeddings", defaultTargetPartitionSize, createdAt)
	require.NoError(t, err)

	spec := vecindex.IVFSpec{ID: "embeddings", Dim: 4, Metric: vecindex.MetricL2, Nlist: 4, Nprobe: 2, Seed: 42}
	rng := rand.New(rand.NewSource(99))
	for i := 1; i <= nVec; i++ {
		v := make([]float32, 4)
		for j := range v {
			v[j] = rng.Float32()
		}
		insertTestVec(t, db, i, v)
	}
	require.NoError(t, BulkPopulate(context.Background(), db, engine, createdAt, "docs", "embed", spec, defaultTargetPartitionSize))
	return db, engine, spec
}

func TestReindex_BasicComplete(t *testing.T) {
	db, engine, spec := setupReindexDB(t, 500)
	ctx := context.Background()

	oldState, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	oldEpoch := oldState.ProbeVersion()

	meta, newState, err := Reindex(ctx, db, engine, testMeta(spec.ID), 0, time.Now().UnixNano())
	require.NoError(t, err)

	currentState, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	require.Same(t, oldState, currentState, "reindex prepare must not publish into the engine")
	require.Greater(t, newState.ProbeVersion(), oldEpoch, "epoch must increment")
	require.NotNil(t, newState.ProbeState())
	require.Greater(t, newState.ProbeState().Len(), 0)
	require.Equal(t, spec.Nlist, meta.Nlist)
	require.Equal(t, spec.Nprobe, meta.Nprobe)

	var status string
	require.NoError(t, db.QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name=?`, spec.ID,
	).Scan(&status))
	require.Equal(t, "ready", status)
}

func TestReindex_RetunesAutoTunedParams(t *testing.T) {
	db, engine, spec := setupReindexDB(t, 100)
	ctx := context.Background()

	_, err := db.Exec(`UPDATE __marmot_vector_indexes
		SET auto_nlist = 1, auto_nprobe = 1
		WHERE index_name = ?`, spec.ID)
	require.NoError(t, err)

	meta := testMeta(spec.ID)
	meta.AutoTuneNlist = true
	meta.AutoTuneNprobe = true

	retunedMeta, newState, err := Reindex(ctx, db, engine, meta, 0, time.Now().UnixNano())
	require.NoError(t, err)

	var nlist, nprobe int
	require.NoError(t, db.QueryRow(
		`SELECT nlist, nprobe FROM __marmot_vector_indexes WHERE index_name=?`, spec.ID,
	).Scan(&nlist, &nprobe))
	require.Equal(t, spec.Nlist, nlist)
	require.Equal(t, spec.Nprobe, nprobe)

	state, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	require.Equal(t, spec.Nlist, state.Spec().Nlist)
	require.Equal(t, spec.Nprobe, state.Spec().Nprobe)
	require.Equal(t, autoTuneNlist(100), retunedMeta.Nlist)
	require.Equal(t, autoTuneNprobe(retunedMeta.Nlist), retunedMeta.Nprobe)
	require.Equal(t, retunedMeta.Nlist, newState.Spec().Nlist)
	require.Equal(t, retunedMeta.Nprobe, newState.Spec().Nprobe)
}

func TestReindex_PreservesExplicitParams(t *testing.T) {
	db, engine, spec := setupReindexDB(t, 100)
	ctx := context.Background()

	meta, newState, err := Reindex(ctx, db, engine, testMeta(spec.ID), 0, time.Now().UnixNano())
	require.NoError(t, err)

	var nlist, nprobe int
	require.NoError(t, db.QueryRow(
		`SELECT nlist, nprobe FROM __marmot_vector_indexes WHERE index_name=?`, spec.ID,
	).Scan(&nlist, &nprobe))
	require.Equal(t, spec.Nlist, nlist)
	require.Equal(t, spec.Nprobe, nprobe)

	state, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	require.Equal(t, spec.Nlist, state.Spec().Nlist)
	require.Equal(t, spec.Nprobe, state.Spec().Nprobe)
	require.Equal(t, spec.Nlist, meta.Nlist)
	require.Equal(t, spec.Nprobe, meta.Nprobe)
	require.Equal(t, spec.Nlist, newState.Spec().Nlist)
	require.Equal(t, spec.Nprobe, newState.Spec().Nprobe)
}

func TestReindex_DriftIsolation(t *testing.T) {
	db, engine, spec := setupReindexDB(t, 100)
	ctx := context.Background()

	_, newState, err := Reindex(ctx, db, engine, testMeta(spec.ID), 0, time.Now().UnixNano())
	require.NoError(t, err)

	state, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	probe := newState.ProbeState()
	require.NotNil(t, probe)
	require.NotSame(t, newState, state, "prepared reindex state must remain unpublished until the hook commits it")
}

func TestSelectPromotionSplitSources(t *testing.T) {
	got := selectPromotionSplitSources([]uint64{0, 1536, 900, 400, 1300}, 4, 7, 512)
	require.Equal(t, []promotionSplitSource{
		{clusterID: 1, count: 1536, splits: 2},
		{clusterID: 4, count: 1300, splits: 1},
	}, got)
}

func TestCapPromotionSplitSourcesByRowBudget(t *testing.T) {
	got := capPromotionSplitSourcesByRowBudget([]promotionSplitSource{
		{clusterID: 1, count: 100, splits: 2},
		{clusterID: 2, count: 80, splits: 1},
		{clusterID: 3, count: 70, splits: 1},
	}, 180)
	require.Equal(t, []promotionSplitSource{
		{clusterID: 1, count: 100, splits: 2},
		{clusterID: 2, count: 80, splits: 1},
	}, got)

	require.Nil(t, capPromotionSplitSourcesByRowBudget([]promotionSplitSource{
		{clusterID: 1, count: 200, splits: 2},
	}, 100))
}

func TestRepairClusterSets(t *testing.T) {
	overfull, underfull, total := repairClusterSets([]uint64{0, 1200, 32, 900, 0, 500}, 512)

	require.Equal(t, uint64(2632), total)
	require.Equal(t, []promotionSplitSource{
		{clusterID: 1, count: 1200, splits: 2},
		{clusterID: 3, count: 900, splits: 1},
	}, overfull)
	require.Equal(t, []int64{4, 2, 5}, underfull)
}

func TestAssignPromotionRowsBalancedCapsFamilySkew(t *testing.T) {
	rows := make([]promotionRow, 0, 10)
	for i := 0; i < 10; i++ {
		rows = append(rows, promotionRow{rowID: int64(i + 1), vec: []float32{float32(i), 0}})
	}
	centroids := [][]float32{{0, 0}, {9, 0}}
	_, counts, _ := assignPromotionRowsBalanced(rows, centroids, vecindex.MetricL2, 4)

	require.LessOrEqual(t, counts[0], uint64(5))
	require.LessOrEqual(t, counts[1], uint64(5))
	require.Equal(t, uint64(10), counts[0]+counts[1])
}

func TestPromotionWarmStartCentroids_SplitsHeavyClusters(t *testing.T) {
	tdb, engine := func(t *testing.T) (*testDBWithMetaStore, *vecindex.Engine) {
		t.Helper()
		engine := vecindex.NewEngine()
		SetVectorUDFProvider(engine)
		t.Cleanup(func() { SetVectorUDFProvider(nil) })

		tdb := openTestDBWithMeta(t, t.TempDir()+"/promotion.db")
		db := tdb.DB
		_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
		require.NoError(t, err)
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS __marmot_vector_indexes (
			index_name TEXT PRIMARY KEY,
			table_name TEXT NOT NULL,
			column_name TEXT NOT NULL,
			database_name TEXT NOT NULL,
			metric TEXT NOT NULL,
			dim INTEGER NOT NULL,
			nlist INTEGER NOT NULL,
			nprobe INTEGER NOT NULL,
			auto_nlist INTEGER NOT NULL DEFAULT 0,
			auto_nprobe INTEGER NOT NULL DEFAULT 0,
			target_partition_size INTEGER NOT NULL DEFAULT 512,
			max_norm REAL NOT NULL,
			status TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO __marmot_vector_indexes
			(index_name, table_name, column_name, database_name, metric, dim, nlist, nprobe, auto_nlist, auto_nprobe, target_partition_size, max_norm, status, created_at)
			VALUES ('embeddings', 'docs', 'embed', 'test', 'l2', 2, 2, 1, 0, 0, 4, 0, 'ready', ?)`, time.Now().UnixNano())
		require.NoError(t, err)
		return tdb, engine
	}(t)
	db := tdb.DB

	spec := vecindex.IVFSpec{ID: "embeddings", Dim: 2, Metric: vecindex.MetricL2, Nlist: 2, Nprobe: 1, Seed: 42}
	rowID := 1
	for _, vec := range [][]float32{
		{0, 0}, {0.2, 0.1}, {-0.1, 0.2}, {0.1, -0.1},
		{5, 5}, {5.2, 5.1}, {4.8, 5.1}, {5.1, 4.9},
		{10, 10}, {10.2, 9.8}, {9.8, 10.3}, {10.1, 10.1},
		{30, 30}, {30.2, 29.9}, {29.8, 30.1}, {30.1, 30.2},
	} {
		insertTestVec(t, db, rowID, vec)
		rowID++
	}
	require.NoError(t, BulkPopulate(context.Background(), db, engine, time.Now().UnixNano(), "docs", "embed", spec, 4))

	state, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	generation, err := RebuildSegmentGeneration(context.Background(), db, tdb.dbPath, VectorIndexMeta{
		IndexName:           spec.ID,
		TableName:           "docs",
		ColumnName:          "embed",
		Database:            "test",
		Metric:              "l2",
		Dim:                 2,
		Nlist:               2,
		Nprobe:              1,
		TargetPartitionSize: 4,
		CreatedAt:           time.Now().UnixNano(),
	}, spec, state.ProbeState(), 0, nil)
	require.NoError(t, err)
	state.StoreSegmentStore(generation)
	base := state.ProbeState().Snapshot()
	expanded, err := promotionWarmStartCentroids(state, vecindex.IVFSpec{
		ID:     spec.ID,
		Dim:    spec.Dim,
		Metric: spec.Metric,
		Nlist:  4,
		Nprobe: 1,
		Seed:   spec.Seed,
	}, base, 4)
	require.NoError(t, err)
	require.Len(t, expanded, 4)

	extras := expanded[len(base):]
	require.Len(t, extras, 2)
	for _, extra := range extras {
		best := float32(math.MaxFloat32)
		for _, centroid := range base {
			dist := metric.Distance(metric.MetricL2, extra, centroid)
			if dist < best {
				best = dist
			}
		}
		require.Greater(t, best, float32(1.0))
	}
}

func testMeta(idx string) VectorIndexMeta {
	return VectorIndexMeta{
		IndexName:           idx,
		TableName:           "docs",
		ColumnName:          "embed",
		Database:            "test",
		Metric:              "l2",
		Dim:                 4,
		Nlist:               4,
		Nprobe:              2,
		TargetPartitionSize: defaultTargetPartitionSize,
		Status:              "reindexing",
	}
}
