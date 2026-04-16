package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/stretchr/testify/require"
)

// setupPopulateDB creates an in-memory SQLite DB with the UDF engine wired in,
// a base docs table, and the centroids + members tables for indexName.
func setupPopulateDB(t *testing.T, indexName string) (*sql.DB, *vecindex.Engine) {
	t.Helper()

	engine := vecindex.NewEngine()
	SetVectorUDFProvider(engine)
	t.Cleanup(func() { SetVectorUDFProvider(nil) })

	db := openVecDB(t)

	// Base table with INTEGER PRIMARY KEY (design R6).
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	ct := vecindex.CentroidsTable(indexName)
	_, err = db.Exec(`CREATE TABLE "` + ct + `" (
		index_id    INTEGER PRIMARY KEY,
		version     INTEGER NOT NULL,
		updated_at  INTEGER NOT NULL,
		nlist       INTEGER NOT NULL,
		compression TEXT    NOT NULL,
		centroids   BLOB    NOT NULL,
		last_n      INTEGER NOT NULL
	)`)
	require.NoError(t, err)

	mt := vecindex.MembersTable(indexName)
	_, err = db.Exec(`CREATE TABLE "` + mt + `" (
		cluster_id INTEGER NOT NULL,
		rowid      INTEGER NOT NULL,
		PRIMARY KEY (cluster_id, rowid)
	) WITHOUT ROWID`)
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
		max_norm      REAL NOT NULL DEFAULT 0,
		status        TEXT NOT NULL DEFAULT 'building',
		created_at    INTEGER NOT NULL DEFAULT 0
	)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT OR IGNORE INTO __marmot_vector_indexes
		(index_name, table_name, column_name, database_name, metric, dim, nlist, nprobe, status)
		VALUES (?, 'docs', 'embed', 'test', 'l2', 3, 2, 1, 'building')`, indexName)
	require.NoError(t, err)

	return db, engine
}

func insertTestVec(t *testing.T, db *sql.DB, id int, v []float32) {
	t.Helper()
	_, err := db.Exec("INSERT INTO docs (id, embed) VALUES (?, ?)", id, encodeVec(t, v))
	require.NoError(t, err)
}

func membersCount(t *testing.T, db *sql.DB, indexName string) int {
	t.Helper()
	mt := vecindex.MembersTable(indexName)
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "`+mt+`"`).Scan(&n))
	return n
}

func membersMinCluster(t *testing.T, db *sql.DB, indexName string) int {
	t.Helper()
	mt := vecindex.MembersTable(indexName)
	var n int
	require.NoError(t, db.QueryRow(`SELECT MIN(cluster_id) FROM "`+mt+`"`).Scan(&n))
	return n
}

// TestBulkPopulate_NoDuplicateRowidsWithDelta locks the HIGH-1 fix:
// populateMembers must clear any pre-existing cluster_id=0 delta rows
// (simulating AFTER INSERT triggers that fired between DDL commit and
// populate start) so each base-table rowid appears exactly ONCE in the
// members table.
func TestBulkPopulate_NoDuplicateRowidsWithDelta(t *testing.T) {
	ctx := context.Background()
	idx := "embeddings"
	db, engine := setupPopulateDB(t, idx)

	insertTestVec(t, db, 1, []float32{1, 0, 0})
	insertTestVec(t, db, 2, []float32{0, 1, 0})
	insertTestVec(t, db, 3, []float32{0, 0, 1})
	insertTestVec(t, db, 4, []float32{0.9, 0.1, 0})

	// Simulate the AFTER INSERT trigger having already fired for every row
	// during the DDL → populate window: every rowid exists in members with
	// cluster_id=0. A broken populate would leave these and add (k, rowid)
	// as well, producing duplicate rowids.
	mt := vecindex.MembersTable(idx)
	for _, id := range []int{1, 2, 3, 4} {
		_, err := db.Exec(`INSERT INTO "`+mt+`" (cluster_id, rowid) VALUES (0, ?)`, id)
		require.NoError(t, err)
	}

	spec := vecindex.IVFSpec{ID: idx, Dim: 3, Metric: vecindex.MetricL2, Nlist: 2, Seed: 42}
	require.NoError(t, BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec))

	// Exactly one entry per rowid, and no residual cluster_id=0 delta.
	require.Equal(t, 4, membersCount(t, db, idx), "one row per base-table rowid")

	var maxDup int
	require.NoError(t, db.QueryRow(
		`SELECT COALESCE(MAX(c), 0) FROM (SELECT COUNT(*) c FROM "`+mt+`" GROUP BY rowid)`,
	).Scan(&maxDup))
	require.Equal(t, 1, maxDup, "no rowid may appear twice in members")

	var deltaLeft int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM "`+mt+`" WHERE cluster_id = 0`,
	).Scan(&deltaLeft))
	require.Equal(t, 0, deltaLeft, "stale delta rows must be cleared by populate")
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
	err := BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec)
	require.NoError(t, err)

	state, ok := engine.Lookup(idx)
	require.True(t, ok)
	require.Equal(t, idx, state.Spec().ID)
	require.Equal(t, 4, membersCount(t, db, idx))
	require.GreaterOrEqual(t, membersMinCluster(t, db, idx), 1, "cluster_id=0 reserved for delta")
}

func TestBulkPopulate_EmptyTable(t *testing.T) {
	ctx := context.Background()
	idx := "emb"
	db, engine := setupPopulateDB(t, idx)

	spec := vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricL2, Nlist: 4, Seed: 1}
	err := BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec)
	require.NoError(t, err)

	_, ok := engine.Lookup(idx)
	require.False(t, ok, "engine must not register state for empty table")
	require.Equal(t, 0, membersCount(t, db, idx))
}

func TestBulkPopulate_ReplicaPath(t *testing.T) {
	// Run BulkPopulate once on the "origin" (inserts centroid row).
	// Then create a fresh engine + empty members table and run again → replica path.
	ctx := context.Background()
	idx := "emb"
	db, engine := setupPopulateDB(t, idx)

	insertTestVec(t, db, 1, []float32{1, 0})
	insertTestVec(t, db, 2, []float32{0, 1})
	insertTestVec(t, db, 3, []float32{0.5, 0.5})

	spec := vecindex.IVFSpec{ID: idx, Dim: 2, Metric: vecindex.MetricL2, Nlist: 2, Seed: 7}

	// First call: origin path — computes centroids, writes row, populates members.
	err := BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec)
	require.NoError(t, err)
	require.Equal(t, 3, membersCount(t, db, idx))

	// Simulate replica: clear members, swap engine, call again.
	engine2 := vecindex.NewEngine()
	SetVectorUDFProvider(engine2)
	mt := vecindex.MembersTable(idx)
	_, err = db.Exec(`DELETE FROM "` + mt + `"`)
	require.NoError(t, err)

	// Second call: centroids row exists → replica path loads them.
	err = BulkPopulate(ctx, db, engine2, 999, "docs", "embed", spec)
	require.NoError(t, err)

	state, ok := engine2.Lookup(idx)
	require.True(t, ok)
	require.Equal(t, uint64(1), state.ProbeVersion())
	require.Equal(t, 3, membersCount(t, db, idx))
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
	err := BulkPopulate(ctx, db, engine, 1000, "docs", "embed", spec)
	require.NoError(t, err)

	_, ok := engine.Lookup(idx)
	require.True(t, ok)
	require.Equal(t, 3, membersCount(t, db, idx))
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
	require.Equal(t, `"__marmot_vec_emb_members"`, quoteIdent("__marmot_vec_emb_members"))
}
