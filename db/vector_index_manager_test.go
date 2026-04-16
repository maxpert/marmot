//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/stretchr/testify/require"
)

// setupVecIndexTestDB opens an in-memory DB and runs schema migration.
func setupVecIndexTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := openInMemorySQLite(t)
	require.NoError(t, MigrateVectorIndexesSchema(db))
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB, title TEXT)`)
	require.NoError(t, err)
	return db
}

// objectExists checks whether a SQLite object (table or trigger) with the given
// name is recorded in sqlite_master.
func objectExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, name).Scan(&n)
	require.NoError(t, err)
	return n > 0
}

// metaRow returns the status of an index from __marmot_vector_indexes or "" if absent.
func metaStatus(t *testing.T, db *sql.DB, indexName string) string {
	t.Helper()
	var status string
	err := db.QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`, indexName,
	).Scan(&status)
	if err == sql.ErrNoRows {
		return ""
	}
	require.NoError(t, err)
	return status
}

func TestCreateVectorIndex_DDL(t *testing.T) {
	db := setupVecIndexTestDB(t)
	ctx := context.Background()

	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      64,
		Nprobe:     8,
		CreatedAt:  time.Now().UnixNano(),
	}

	mgr := &VectorIndexManager{}

	err := mgr.execCreateDDL(ctx, db, meta)
	require.NoError(t, err)

	// Centroids table (single underscore → CDC-replicated).
	require.True(t, objectExists(t, db, vecindex.CentroidsTable("embeddings")),
		"centroids table must exist")

	// Members shadow table (double underscore → local).
	require.True(t, objectExists(t, db, vecindex.MembersTable("embeddings")),
		"members table must exist")

	// Members rowid index.
	require.True(t, objectExists(t, db, vecindex.MembersRowidIndex("embeddings")),
		"members rowid index must exist")

	// Base-table triggers.
	require.True(t, objectExists(t, db, vecindex.TriggerInsert("embeddings")), "insert trigger")
	require.True(t, objectExists(t, db, vecindex.TriggerUpdate("embeddings")), "update trigger")
	require.True(t, objectExists(t, db, vecindex.TriggerDelete("embeddings")), "delete trigger")

	// Centroid-change triggers.
	require.True(t, objectExists(t, db, vecindex.TriggerCentroidChange("embeddings")), "centroid insert trigger")
	require.True(t, objectExists(t, db, vecindex.TriggerCentroidsVersionUpdate("embeddings")), "centroid update trigger")

	// Metadata row present with status='building'.
	require.Equal(t, "building", metaStatus(t, db, "embeddings"))
}

func TestCreateVectorIndex_Idempotent(t *testing.T) {
	db := setupVecIndexTestDB(t)
	ctx := context.Background()

	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      64,
		Nprobe:     8,
		CreatedAt:  time.Now().UnixNano(),
	}
	mgr := &VectorIndexManager{}

	require.NoError(t, mgr.execCreateDDL(ctx, db, meta))
	// Second call with IF NOT EXISTS guards must not error.
	require.NoError(t, mgr.execCreateDDL(ctx, db, meta))
}

func TestDropVectorIndex_DDL(t *testing.T) {
	db := setupVecIndexTestDB(t)
	ctx := context.Background()

	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      64,
		Nprobe:     8,
		CreatedAt:  time.Now().UnixNano(),
	}
	mgr := &VectorIndexManager{}

	require.NoError(t, mgr.execCreateDDL(ctx, db, meta))
	require.NoError(t, mgr.execDropDDL(ctx, db, "embeddings"))

	// All generated objects must be gone.
	require.False(t, objectExists(t, db, vecindex.CentroidsTable("embeddings")), "centroids table must be dropped")
	require.False(t, objectExists(t, db, vecindex.MembersTable("embeddings")), "members table must be dropped")
	require.False(t, objectExists(t, db, vecindex.TriggerInsert("embeddings")), "insert trigger must be dropped")
	require.False(t, objectExists(t, db, vecindex.TriggerUpdate("embeddings")), "update trigger must be dropped")
	require.False(t, objectExists(t, db, vecindex.TriggerDelete("embeddings")), "delete trigger must be dropped")
	require.False(t, objectExists(t, db, vecindex.TriggerCentroidChange("embeddings")), "centroid insert trigger must be dropped")
	require.False(t, objectExists(t, db, vecindex.TriggerCentroidsVersionUpdate("embeddings")), "centroid update trigger must be dropped")

	// Metadata row must be removed.
	require.Equal(t, "", metaStatus(t, db, "embeddings"), "metadata row must be deleted")
}

func TestInsertTrigger_AddsToDelta(t *testing.T) {
	db := setupVecIndexTestDB(t)
	ctx := context.Background()

	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      64,
		Nprobe:     8,
		CreatedAt:  time.Now().UnixNano(),
	}
	mgr := &VectorIndexManager{}
	require.NoError(t, mgr.execCreateDDL(ctx, db, meta))

	// Insert a row with a non-NULL embed.
	embed := []byte{0, 0, 0x80, 0x3f} // float32(1.0) little-endian
	_, err := db.Exec(`INSERT INTO docs (id, embed) VALUES (1, ?)`, embed)
	require.NoError(t, err)

	// The trigger must have inserted (cluster_id=0, rowid=1) into members.
	var clusterID, rowid int64
	err = db.QueryRow(
		fmt.Sprintf(`SELECT cluster_id, rowid FROM "%s" WHERE rowid = 1`,
			vecindex.MembersTable("embeddings")),
	).Scan(&clusterID, &rowid)
	require.NoError(t, err)
	require.Equal(t, int64(0), clusterID, "new row must enter delta (cluster_id=0)")
	require.Equal(t, int64(1), rowid)
}

func TestDeleteTrigger_RemovesFromMembers(t *testing.T) {
	db := setupVecIndexTestDB(t)
	ctx := context.Background()

	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      64,
		Nprobe:     8,
		CreatedAt:  time.Now().UnixNano(),
	}
	mgr := &VectorIndexManager{}
	require.NoError(t, mgr.execCreateDDL(ctx, db, meta))

	embed := []byte{0, 0, 0x80, 0x3f}
	_, err := db.Exec(`INSERT INTO docs (id, embed) VALUES (2, ?)`, embed)
	require.NoError(t, err)

	_, err = db.Exec(`DELETE FROM docs WHERE id = 2`)
	require.NoError(t, err)

	var n int
	err = db.QueryRow(
		fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE rowid = 2`, vecindex.MembersTable("embeddings")),
	).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 0, n, "deleted row must be removed from members")
}

func TestAutoTuneNlist(t *testing.T) {
	t.Parallel()
	cases := []struct {
		n    int64
		want int
	}{
		{0, 64},
		{100, 64},       // 4*10=40 < 64 → clamp to 64
		{1000, 126},     // int(4*31.62)=126
		{100000, 1264},  // int(4*316.22)=1264
		{1000000, 2048}, // int(4*1000)=4000 > 2048 → clamp to 2048
	}
	for _, tc := range cases {
		got := autoTuneNlist(tc.n)
		require.Equal(t, tc.want, got, "n=%d", tc.n)
	}
}

func TestAutoTuneNprobe(t *testing.T) {
	t.Parallel()
	cases := []struct {
		nlist int
		want  int
	}{
		{64, 8},   // sqrt(64)=8
		{256, 16}, // sqrt(256)=16
		{4, 8},    // sqrt(4)=2 < 8 → clamped to 8
	}
	for _, tc := range cases {
		got := autoTuneNprobe(tc.nlist)
		require.Equal(t, tc.want, got, "nlist=%d", tc.nlist)
	}
}

// setupManagerWithDB creates a VectorIndexManager wired to a single in-memory
// database. It uses package-internal execCreateDDL to bypass DatabaseManager
// (which we don't need for cache tests).
func setupManagerWithDB(t *testing.T) (*VectorIndexManager, *sql.DB) {
	t.Helper()
	db := setupVecIndexTestDB(t)
	mgr := NewVectorIndexManager(nil) // nil dbMgr — we call loadExistingIndexes manually
	return mgr, db
}

// loadMetaIntoCache inserts a row into __marmot_vector_indexes and then calls
// loadExistingIndexes to warm the cache.  It uses a one-database stub that
// returns the provided *sql.DB.
func seedManagerCache(t *testing.T, mgr *VectorIndexManager, db *sql.DB, meta common.VectorIndexMeta) {
	t.Helper()
	// Insert metadata row directly (execCreateDDL already did this in the DDL test).
	_, err := db.Exec(`INSERT INTO __marmot_vector_indexes
		(index_name, table_name, column_name, database_name, metric, dim, nlist, nprobe, max_norm, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?)
		ON CONFLICT(index_name) DO NOTHING`,
		meta.IndexName, meta.TableName, meta.ColumnName, meta.Database,
		meta.Metric, meta.Dim, meta.Nlist, meta.Nprobe, meta.MaxNorm, meta.CreatedAt,
	)
	require.NoError(t, err)

	// Warm cache by scanning the row.
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, `
		SELECT index_name, table_name, column_name, database_name,
		       metric, dim, nlist, nprobe, max_norm, status
		FROM __marmot_vector_indexes`)
	require.NoError(t, err)
	for rows.Next() {
		var m common.VectorIndexMeta
		require.NoError(t, rows.Scan(
			&m.IndexName, &m.TableName, &m.ColumnName, &m.Database,
			&m.Metric, &m.Dim, &m.Nlist, &m.Nprobe, &m.MaxNorm, &m.Status,
		))
		key := indexCacheKey{database: m.Database, table: m.TableName, column: m.ColumnName}
		mc := m
		mgr.cacheMu.Lock()
		mgr.indexCache[key] = &mc
		mgr.cacheMu.Unlock()
	}
	rows.Close()
}

func TestGetIndexByColumn_Found(t *testing.T) {
	t.Parallel()
	mgr, db := setupManagerWithDB(t)
	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "cosine",
		Dim:        4, Nlist: 64, Nprobe: 8,
		CreatedAt: time.Now().UnixNano(),
	}
	seedManagerCache(t, mgr, db, meta)

	got, ok := mgr.GetIndexByColumn("testdb", "docs", "embed")
	require.True(t, ok)
	require.NotNil(t, got)
	require.Equal(t, "embeddings", got.IndexName)
	require.Equal(t, "cosine", got.Metric)
}

func TestGetIndexByColumn_EmptyDatabase_UniqueMatch(t *testing.T) {
	t.Parallel()
	mgr, db := setupManagerWithDB(t)
	meta := common.VectorIndexMeta{
		IndexName: "embeddings", TableName: "docs", ColumnName: "embed",
		Database: "testdb", Metric: "l2", Dim: 4, Nlist: 64, Nprobe: 8,
		CreatedAt: time.Now().UnixNano(),
	}
	seedManagerCache(t, mgr, db, meta)

	// Empty database → unique scan.
	got, ok := mgr.GetIndexByColumn("", "docs", "embed")
	require.True(t, ok)
	require.Equal(t, "embeddings", got.IndexName)
}

func TestGetIndexByColumn_NotFound(t *testing.T) {
	t.Parallel()
	mgr, _ := setupManagerWithDB(t)
	got, ok := mgr.GetIndexByColumn("testdb", "unknown_table", "embed")
	require.False(t, ok)
	require.Nil(t, got)
}

func TestGetIndexByColumn_AfterDrop(t *testing.T) {
	t.Parallel()
	mgr, db := setupManagerWithDB(t)
	meta := common.VectorIndexMeta{
		IndexName: "embeddings", TableName: "docs", ColumnName: "embed",
		Database: "testdb", Metric: "l2", Dim: 4, Nlist: 64, Nprobe: 8,
		CreatedAt: time.Now().UnixNano(),
	}
	seedManagerCache(t, mgr, db, meta)

	_, ok := mgr.GetIndexByColumn("testdb", "docs", "embed")
	require.True(t, ok)

	// Simulate drop: remove from cache.
	mgr.cacheMu.Lock()
	delete(mgr.indexCache, indexCacheKey{database: "testdb", table: "docs", column: "embed"})
	mgr.cacheMu.Unlock()

	got, ok := mgr.GetIndexByColumn("testdb", "docs", "embed")
	require.False(t, ok)
	require.Nil(t, got)
}
