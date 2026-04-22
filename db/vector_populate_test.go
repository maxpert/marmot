package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/stretchr/testify/require"
)

// setupPopulateDB creates an in-memory SQLite DB with the UDF engine wired in,
// a base docs table, and vector-index metadata for indexName.
func setupPopulateDB(t *testing.T, indexName string) (*sql.DB, *vecindex.Engine) {
	t.Helper()

	engine := vecindex.NewEngine()
	SetVectorUDFProvider(engine)
	t.Cleanup(func() { SetVectorUDFProvider(nil) })

	db := openVecDB(t)

	// Base table with INTEGER PRIMARY KEY (design R6).
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	// Metadata table for the status='ready' update inside populateMembers.
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS __marmot_vector_indexes (
		index_name    TEXT PRIMARY KEY,
		table_name    TEXT NOT NULL,
		column_name   TEXT NOT NULL,
		database_name TEXT NOT NULL,
		metric        TEXT NOT NULL,
		dim           INTEGER NOT NULL,
		nlist         INTEGER NOT NULL,
		nprobe        INTEGER NOT NULL,
		auto_nlist    INTEGER NOT NULL DEFAULT 0,
		auto_nprobe   INTEGER NOT NULL DEFAULT 0,
		target_partition_size INTEGER NOT NULL DEFAULT 100,
		max_norm      REAL NOT NULL DEFAULT 0,
		status        TEXT NOT NULL DEFAULT 'building',
		created_at    INTEGER NOT NULL DEFAULT 0
	)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT OR IGNORE INTO __marmot_vector_indexes
		(index_name, table_name, column_name, database_name, metric, dim,
		 nlist, nprobe, auto_nlist, auto_nprobe, target_partition_size, status)
		VALUES (?, 'docs', 'embed', 'test', 'l2', 3, 2, 1, 0, 0, 100, 'building')`, indexName)
	require.NoError(t, err)

	return db, engine
}

func insertTestVec(t *testing.T, db *sql.DB, id int, v []float32) {
	t.Helper()
	_, err := db.Exec("INSERT INTO docs (id, embed) VALUES (?, ?)", id, encodeVec(t, v))
	require.NoError(t, err)
}

func TestBulkPopulate_Basic(t *testing.T) {
	ctx := context.Background()
	idx := "embeddings"
	db, engine := setupPopulateDB(t, idx)

	insertTestVec(t, db, 1, []float32{1, 0, 0})
	insertTestVec(t, db, 2, []float32{0, 1, 0})
	insertTestVec(t, db, 3, []float32{0, 0, 1})
	insertTestVec(t, db, 4, []float32{0.9, 0.1, 0})

	spec := vecindex.IVFSpec{ID: idx, Dim: 3, Metric: vecindex.MetricL2, Nlist: 2, Seed: 42}
	err := BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec, 100)
	require.NoError(t, err)

	state, ok := engine.Lookup(idx)
	require.True(t, ok)
	require.Equal(t, idx, state.Spec().ID)
	require.Equal(t, uint64(1), state.ProbeVersion())
	require.NotNil(t, state.ProbeState())
	require.Equal(t, 2, state.ProbeState().Len())

	var status string
	require.NoError(t, db.QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`, idx,
	).Scan(&status))
	require.Equal(t, "ready", status)
}

func TestBulkPopulate_EmptyTable(t *testing.T) {
	ctx := context.Background()
	idx := "emb"
	db, engine := setupPopulateDB(t, idx)

	spec := vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricL2, Nlist: 4, Seed: 1}
	err := BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec, 100)
	require.NoError(t, err)

	state, ok := engine.Lookup(idx)
	require.True(t, ok, "engine must register empty state for online inserts")
	require.Zero(t, state.ProbeVersion(), "empty bootstrap starts without centroids until the lifecycle hook promotes it")
	var derivedObjects int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE ?`,
		"%marmot_vec_"+idx+"%",
	).Scan(&derivedObjects))
	require.Zero(t, derivedObjects, "empty bootstrap should not create SQLite vector payload artifacts")

	var status string
	require.NoError(t, db.QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`, idx,
	).Scan(&status))
	require.Equal(t, "ready", status, "empty index must become queryable immediately")
}

func TestBulkPopulate_NlistCappedToSampleCount(t *testing.T) {
	// nlist=10 but only 3 vectors → should succeed with k capped at 3.
	ctx := context.Background()
	idx := "emb"
	db, engine := setupPopulateDB(t, idx)

	insertTestVec(t, db, 1, []float32{1, 0})
	insertTestVec(t, db, 2, []float32{0, 1})
	insertTestVec(t, db, 3, []float32{0.5, 0.5})

	spec := vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricL2, Nlist: 10, Seed: 1}
	err := BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec, 100)
	require.NoError(t, err)

	state, ok := engine.Lookup(idx)
	require.True(t, ok)
	require.NotNil(t, state.ProbeState())
	require.Equal(t, 3, state.ProbeState().Len())
}

func TestComputeCentroids_WarmStartExpandsToRequestedK(t *testing.T) {
	ctx := context.Background()
	db := openVecDB(t)
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	for i, vec := range [][]float32{
		{0, 0}, {0.1, 0},
		{10, 10}, {10.1, 10},
		{-10, 10}, {-10.1, 10},
		{10, -10}, {10.1, -10},
	} {
		insertTestVec(t, db, i+1, vec)
	}

	cs, err := computeCentroids(ctx, db, "docs", "embed", vecindex.IVFSpec{
		ID:     "emb",
		Dim:    2,
		Metric: vecindex.MetricL2,
		Nlist:  4,
		Seed:   7,
	}, 2, [][]float32{{0, 0}, {10, 10}})
	require.NoError(t, err)
	require.NotNil(t, cs)
	require.Equal(t, 4, cs.Len())
}

func TestComputeCentroids_CapsToIndexableRows(t *testing.T) {
	ctx := context.Background()
	db := openVecDB(t)
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	insertTestVec(t, db, 1, []float32{0, 0})
	insertTestVec(t, db, 2, []float32{0, 0})
	insertTestVec(t, db, 3, []float32{1, 0})
	insertTestVec(t, db, 4, []float32{0, 1})

	cs, err := computeCentroids(ctx, db, "docs", "embed", vecindex.IVFSpec{
		ID:     "emb",
		Dim:    2,
		Metric: vecindex.MetricCosine,
		Nlist:  4,
		Seed:   11,
	}, 2, nil)
	require.NoError(t, err)
	require.NotNil(t, cs)
	require.Equal(t, 2, cs.Len())
}

func TestDecodeVecBlob_RoundTrip(t *testing.T) {
	t.Parallel()
	v := []float32{1.5, -2.3, 0, 42.0}
	blob := encodeVec(t, v)
	got, err := decodeVec(blob)
	require.NoError(t, err)
	require.InDeltaSlice(t, v, got, 1e-6)
}

func TestDecodeVecBlob_Empty(t *testing.T) {
	t.Parallel()
	_, err := decodeVec(nil)
	require.Error(t, err)
}

func TestDecodeVecBlob_Unaligned(t *testing.T) {
	t.Parallel()
	_, err := decodeVec([]byte{1, 2, 3})
	require.Error(t, err)
}

func TestQuoteIdent(t *testing.T) {
	t.Parallel()
	require.Equal(t, `"foo"`, quoteIdent("foo"))
	require.Equal(t, `"fo""o"`, quoteIdent(`fo"o`))
	require.Equal(t, `"embed_idx"`, quoteIdent("embed_idx"))
}
