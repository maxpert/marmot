package db

import (
	"context"
	"database/sql"
	"math/rand"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex"
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
		target_partition_size INTEGER NOT NULL DEFAULT 100,
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
		VALUES (?, 'docs', 'embed', 'test', 'l2', 4, 4, 2, 0, 0, 100, 0, 'ready', ?)`,
		"embeddings", createdAt)
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
	require.NoError(t, BulkPopulate(context.Background(), db, engine, createdAt, "docs", "embed", spec))
	return db, engine, spec
}

func TestReindex_BasicComplete(t *testing.T) {
	db, engine, spec := setupReindexDB(t, 500)
	ctx := context.Background()

	oldState, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	oldEpoch := oldState.ProbeVersion()

	meta, newState, err := Reindex(ctx, db, engine, testMeta(spec.ID), 100, time.Now().UnixNano())
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

	retunedMeta, newState, err := Reindex(ctx, db, engine, meta, 50, time.Now().UnixNano())
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

	meta, newState, err := Reindex(ctx, db, engine, testMeta(spec.ID), 50, time.Now().UnixNano())
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

	_, newState, err := Reindex(ctx, db, engine, testMeta(spec.ID), 50, time.Now().UnixNano())
	require.NoError(t, err)

	state, ok := engine.Lookup(spec.ID)
	require.True(t, ok)
	probe := newState.ProbeState()
	drift := newState.DriftState()
	require.NotNil(t, probe)
	require.NotNil(t, drift)
	require.Equal(t, probe.Epoch(), drift.Epoch(), "probe and drift must share epoch after REINDEX")
	require.NotSame(t, newState, state, "prepared reindex state must remain unpublished until the hook commits it")
}

func testMeta(idx string) VectorIndexMeta {
	return VectorIndexMeta{
		IndexName:  idx,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "l2",
		Dim:        4,
		Nlist:      4,
		Nprobe:     2,
		Status:     "reindexing",
	}
}
