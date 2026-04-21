//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/stretchr/testify/require"
)

type failingCreateHook struct {
	dbMgr *DatabaseManager
	err   error
}

func (h failingCreateHook) OnIndexCreated(_ context.Context, meta common.VectorIndexMeta) error {
	if h.dbMgr != nil {
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err == nil {
			dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
			_ = os.MkdirAll(dir, 0o755)
			_ = os.WriteFile(filepath.Join(dir, "partial"), []byte("partial"), 0o644)
		}
	}
	return h.err
}

type recordingEngineProvider struct {
	removed []string
}

func (p *recordingEngineProvider) RemoveIndex(indexName string) func() {
	p.removed = append(p.removed, indexName)
	return func() {}
}

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

	var derivedObjects int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE ?`,
		"%marmot_vec_embeddings%",
	).Scan(&derivedObjects))
	require.Zero(t, derivedObjects, "vector index DDL should not create SQLite-side payload tables or triggers")

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

	// Metadata row must be removed.
	require.Equal(t, "", metaStatus(t, db, "embeddings"), "metadata row must be deleted")
}

func TestCreateIndex_EmptyTableAutoBootstrapsOnInsert(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase("test"))

	vecMgr := NewVectorIndexManager(dbMgr)
	dbMgr.SetVectorIndexManager(vecMgr)

	engine := vecindex.NewEngine()
	SetVectorUDFProvider(engine)
	t.Cleanup(func() { SetVectorUDFProvider(nil) })

	hook := NewEngineHook(engine, dbMgr)
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)
	require.NoError(t, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection("test")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

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
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	state, ok := engine.Lookup(meta.IndexName)
	require.True(t, ok)
	require.Zero(t, state.ProbeVersion(), "empty create may start without centroids, but must bootstrap automatically")

	const bootstrapRows = 4096
	for i := 0; i < bootstrapRows; i++ {
		_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, i+1, encodeVec(t, []float32{
			1, float32(i % 7), float32((i + 1) % 11), float32((i + 2) % 13),
		}))
		require.NoError(t, err)
	}

	ok = false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		state, found := engine.Lookup(meta.IndexName)
		if !found || state.ProbeVersion() == 0 || state.LoadSegmentStore() == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ok = true
		break
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		state, stateOK := engine.Lookup(meta.IndexName)
		probeVersion := uint64(0)
		hasSegment := false
		if stateOK {
			probeVersion = state.ProbeVersion()
			hasSegment = state.LoadSegmentStore() != nil
		}
		t.Fatalf("empty-table index did not auto-bootstrap: probeVersion=%d hasSegment=%v", probeVersion, hasSegment)
	}
}

func TestCreateIndex_RollsBackMetadataAndCacheOnHookFailure(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase("test"))

	vecMgr := NewVectorIndexManager(dbMgr)
	provider := &recordingEngineProvider{}
	vecMgr.SetEngineProvider(provider)
	vecMgr.SetLifecycleHook(failingCreateHook{dbMgr: dbMgr, err: errors.New("boom")})
	require.NoError(t, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection("test")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

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

	err = vecMgr.CreateIndex(context.Background(), meta)
	require.Error(t, err)
	require.ErrorContains(t, err, "populate failed")
	require.ErrorContains(t, err, "boom")

	require.Equal(t, "", metaStatus(t, conn, meta.IndexName), "metadata row must be removed after hook failure")

	got, ok := vecMgr.GetIndexByColumn(meta.Database, meta.TableName, meta.ColumnName)
	require.False(t, ok, "index cache must be cleared after hook failure")
	require.Nil(t, got)
	require.Equal(t, []string{meta.IndexName}, provider.removed, "failed create must remove any partially-registered engine state")

	dbPath, err := dbMgr.GetDatabasePath(meta.Database)
	require.NoError(t, err)
	_, statErr := os.Stat(vecindex.SegmentStoreDir(dbPath, meta.IndexName))
	require.ErrorIs(t, statErr, os.ErrNotExist, "failed create must remove partial local files")
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
		(index_name, table_name, column_name, database_name, metric, dim,
		 nlist, nprobe, auto_nlist, auto_nprobe, target_partition_size,
		 max_norm, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?)
		ON CONFLICT(index_name) DO NOTHING`,
		meta.IndexName, meta.TableName, meta.ColumnName, meta.Database,
		meta.Metric, meta.Dim, meta.Nlist, meta.Nprobe,
		boolToInt(meta.AutoTuneNlist), boolToInt(meta.AutoTuneNprobe),
		meta.TargetPartitionSize, meta.MaxNorm, meta.CreatedAt,
	)
	require.NoError(t, err)

	// Warm cache by scanning the row.
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, `
		SELECT index_name, table_name, column_name, database_name,
		       metric, dim, nlist, nprobe, auto_nlist, auto_nprobe,
		       target_partition_size, max_norm, status, created_at
		FROM __marmot_vector_indexes`)
	require.NoError(t, err)
	for rows.Next() {
		var (
			m          common.VectorIndexMeta
			autoNlist  int64
			autoNprobe int64
		)
		require.NoError(t, rows.Scan(
			&m.IndexName, &m.TableName, &m.ColumnName, &m.Database,
			&m.Metric, &m.Dim, &m.Nlist, &m.Nprobe,
			&autoNlist, &autoNprobe, &m.TargetPartitionSize,
			&m.MaxNorm, &m.Status, &m.CreatedAt,
		))
		m.AutoTuneNlist = autoNlist != 0
		m.AutoTuneNprobe = autoNprobe != 0
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
	require.Equal(t, meta.CreatedAt, got.CreatedAt)
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
