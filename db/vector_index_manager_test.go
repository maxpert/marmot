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

func insertDocRows(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(`INSERT INTO docs(id) VALUES (?)`)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		_, err = stmt.Exec(i)
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())
}

func insertDocVectorRows(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(`INSERT INTO docs(id, embed) VALUES (?, ?)`)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		vec := []float32{
			float32(i % 11),
			float32((i * 3) % 17),
			float32((i * 5) % 19),
			float32((i * 7) % 23),
		}
		_, err = stmt.Exec(i, encodeVec(t, vec))
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())
}

type overlayMirrorRow struct {
	rowID int64
	vec   []float32
}

func mirrorRowsToOverlay(t *testing.T, hook *EngineHook, meta common.VectorIndexMeta, rows []overlayMirrorRow) {
	t.Helper()

	state, spec, err := hook.ensureIndexState(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, state)

	overlay := state.LoadOverlay()
	require.NotNil(t, overlay)
	snapshot := overlay.Snapshot()
	nextSequence := uint64(1)
	if snapshot != nil {
		nextSequence = snapshot.LastSequence() + 1
	}

	mutations := make([]vecindex.OverlayMutation, 0, len(rows))
	appliedAtUnixNano := time.Now().UnixNano()
	for _, row := range rows {
		mutation, err := buildUpsertMutation(state, spec, state.ProbeVersion(), row.rowID, encodeVec(t, row.vec), nextSequence)
		require.NoError(t, err)
		mutation.AppliedAtUnixNano = appliedAtUnixNano
		mutations = append(mutations, mutation)
		nextSequence++
	}

	require.NoError(t, overlay.ApplyCommittedBatch(mutations))
	state.RecordRowsModified(uint64(countUniqueMutationRows(mutations)))
}

func cleanupIndexWatchers(t *testing.T, hook *EngineHook, dbMgr *DatabaseManager, database, indexName string) {
	t.Helper()
	if hook == nil || dbMgr == nil {
		return
	}

	hook.stopBootstrapWatcher(indexName)
	hook.stopMaintenanceWatcher(indexName)

	dbPath, err := dbMgr.GetDatabasePath(database)
	require.NoError(t, err)
	segmentDir := vecindex.SegmentStoreDir(dbPath, indexName)
	require.Eventually(t, func() bool {
		_ = os.RemoveAll(segmentDir)
		_, statErr := os.Stat(segmentDir)
		return os.IsNotExist(statErr)
	}, 5*time.Second, 50*time.Millisecond)
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
	hook.BindVectorIndexManager(vecMgr)
	t.Cleanup(func() {
		cleanupIndexWatchers(t, hook, dbMgr, "test", "embeddings")
	})
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
	mirrorRows := make([]overlayMirrorRow, 0, bootstrapRows)
	for i := 0; i < bootstrapRows; i++ {
		vec := []float32{
			1, float32(i % 7), float32((i + 1) % 11), float32((i + 2) % 13),
		}
		_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, i+1, encodeVec(t, vec))
		require.NoError(t, err)
		mirrorRows = append(mirrorRows, overlayMirrorRow{rowID: int64(i + 1), vec: vec})
	}
	mirrorRowsToOverlay(t, hook, meta, mirrorRows)

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
		overlayEpoch := uint64(0)
		overlayRows := 0
		overlaySeq := uint64(0)
		if stateOK {
			probeVersion = state.ProbeVersion()
			hasSegment = state.LoadSegmentStore() != nil
			if overlay := state.LoadOverlay(); overlay != nil && overlay.Snapshot() != nil {
				snapshot := overlay.Snapshot()
				overlayEpoch = snapshot.Epoch()
				overlayRows = snapshot.Len()
				overlaySeq = snapshot.LastSequence()
			}
		}
		t.Fatalf(
			"empty-table index did not auto-bootstrap: probeVersion=%d hasSegment=%v overlayEpoch=%d overlayRows=%d overlaySeq=%d",
			probeVersion,
			hasSegment,
			overlayEpoch,
			overlayRows,
			overlaySeq,
		)
	}
}

func TestCreateIndex_EmptyTableAutoTuneBootstrapsAtTargetPartitionFloor(t *testing.T) {
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
	hook.BindVectorIndexManager(vecMgr)
	t.Cleanup(func() {
		cleanupIndexWatchers(t, hook, dbMgr, "test", "embeddings")
	})
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)
	require.NoError(t, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection("test")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	meta := common.VectorIndexMeta{
		IndexName:           "embeddings",
		TableName:           "docs",
		ColumnName:          "embed",
		Database:            "test",
		Metric:              "cosine",
		Dim:                 4,
		TargetPartitionSize: 128,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	state, ok := engine.Lookup(meta.IndexName)
	require.True(t, ok)
	require.Zero(t, state.ProbeVersion(), "empty create may start without centroids, but must bootstrap automatically")

	const bootstrapRows = 64 * 128
	mirrorRows := make([]overlayMirrorRow, 0, bootstrapRows)
	for i := 0; i < bootstrapRows-1; i++ {
		vec := []float32{
			1, float32(i % 5), float32((i + 1) % 7), float32((i + 2) % 11),
		}
		_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, i+1, encodeVec(t, vec))
		require.NoError(t, err)
		mirrorRows = append(mirrorRows, overlayMirrorRow{rowID: int64(i + 1), vec: vec})
	}
	mirrorRowsToOverlay(t, hook, meta, mirrorRows)

	time.Sleep(750 * time.Millisecond)

	state, ok = engine.Lookup(meta.IndexName)
	require.True(t, ok)
	require.Zero(t, state.ProbeVersion(), "auto bootstrap should wait for the minimum target-sized training set")
	require.Nil(t, state.LoadSegmentStore(), "segment store should not publish before the bootstrap floor is reached")

	lastVec := []float32{1, 0, 1, 2}
	_, err = conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, bootstrapRows, encodeVec(t, lastVec))
	require.NoError(t, err)
	mirrorRows = append(mirrorRows, overlayMirrorRow{rowID: int64(bootstrapRows), vec: lastVec})
	mirrorRowsToOverlay(t, hook, meta, mirrorRows)

	require.Eventually(t, func() bool {
		state, ok := engine.Lookup(meta.IndexName)
		return ok && state.ProbeVersion() > 0 && state.LoadSegmentStore() != nil
	}, 30*time.Second, 100*time.Millisecond, "empty-table auto-tuned create should bootstrap when the target-partition floor is reached")

	var nlist, nprobe int
	require.NoError(t, conn.QueryRow(
		`SELECT nlist, nprobe FROM __marmot_vector_indexes WHERE index_name = ?`, meta.IndexName,
	).Scan(&nlist, &nprobe))
	require.Equal(t, 64, nlist)
	require.Equal(t, autoTuneNprobeForTarget(nlist, meta.TargetPartitionSize), nprobe)

	state, ok = engine.Lookup(meta.IndexName)
	require.True(t, ok)
	require.Equal(t, nlist, state.Spec().Nlist)
	require.Equal(t, nprobe, state.Spec().Nprobe)
}

func TestCreateIndex_EmptyTableAutoTuneWaitsForStrongBootstrapFloor(t *testing.T) {
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
	hook.BindVectorIndexManager(vecMgr)
	t.Cleanup(func() {
		cleanupIndexWatchers(t, hook, dbMgr, "test", "embeddings")
	})
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)
	require.NoError(t, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection("test")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	meta := common.VectorIndexMeta{
		IndexName:           "embeddings",
		TableName:           "docs",
		ColumnName:          "embed",
		Database:            "test",
		Metric:              "cosine",
		Dim:                 4,
		TargetPartitionSize: 64,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	weakRows := (bootstrapMinTargetPartitions * meta.TargetPartitionSize) - 1
	mirrorRows := make([]overlayMirrorRow, 0, weakRows)
	for i := 0; i < weakRows; i++ {
		vec := []float32{1, float32(i % 3), float32((i + 1) % 5), float32((i + 2) % 7)}
		_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, i+1, encodeVec(t, vec))
		require.NoError(t, err)
		mirrorRows = append(mirrorRows, overlayMirrorRow{rowID: int64(i + 1), vec: vec})
	}
	mirrorRowsToOverlay(t, hook, meta, mirrorRows)

	time.Sleep(bootstrapQuiesceDuration + 250*time.Millisecond)

	state, ok := engine.Lookup(meta.IndexName)
	require.True(t, ok)
	require.Zero(t, state.ProbeVersion(), "auto bootstrap should not publish at the old 8-partition floor")
	require.Nil(t, state.LoadSegmentStore())
}

func TestVectorIndexManagerSetHooksBindsEngineHook(t *testing.T) {
	t.Parallel()

	clock := hlc.NewClock(1)
	dbMgr, err := NewDatabaseManager(t.TempDir(), 1, clock)
	require.NoError(t, err)
	vecMgr := NewVectorIndexManager(dbMgr)
	hook := NewEngineHook(vecindex.NewEngine(), dbMgr)

	vecMgr.SetLifecycleHook(hook)
	require.Same(t, vecMgr, hook.indexMgr)

	hook.indexMgr = nil
	vecMgr.SetReindexHook(hook)
	require.Same(t, vecMgr, hook.indexMgr)

	hook.indexMgr = nil
	vecMgr.SetEngineProvider(hook)
	require.Same(t, vecMgr, hook.indexMgr)
}

func TestBootstrapOnce_RetriesAfterSegmentPublishFailure(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase("test"))

	conn, err := dbMgr.GetDatabaseConnection("test")
	require.NoError(t, err)
	require.NoError(t, MigrateVectorIndexesSchema(conn))
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)
	insertDocVectorRows(t, conn, 32)

	engine := vecindex.NewEngine()
	hook := NewEngineHook(engine, dbMgr)

	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      8,
		Nprobe:     8,
		CreatedAt:  time.Now().UnixNano(),
	}
	spec := vecindex.IVFSpec{
		ID:      meta.IndexName,
		Dim:     meta.Dim,
		Metric:  vecindex.MetricCosine,
		Nlist:   meta.Nlist,
		Nprobe:  meta.Nprobe,
		Seed:    StableIndexSeed(meta),
		MaxNorm: meta.MaxNorm,
	}
	_, err = conn.Exec(`INSERT INTO __marmot_vector_indexes
		(index_name, table_name, column_name, database_name, metric, dim, nlist, nprobe, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'building', ?)`,
		meta.IndexName, meta.TableName, meta.ColumnName, meta.Database, meta.Metric, meta.Dim, meta.Nlist, meta.Nprobe, meta.CreatedAt)
	require.NoError(t, err)

	require.NoError(t, BulkPopulate(context.Background(), conn, engine, 0, meta.TableName, meta.ColumnName, spec, meta.TargetPartitionSize))
	mirrorRows := make([]overlayMirrorRow, 0, 32)
	for i := 1; i <= 32; i++ {
		mirrorRows = append(mirrorRows, overlayMirrorRow{
			rowID: int64(i),
			vec: []float32{
				float32(i % 11),
				float32((i * 3) % 17),
				float32((i * 5) % 19),
				float32((i * 7) % 23),
			},
		})
	}
	mirrorRowsToOverlay(t, hook, meta, mirrorRows)

	state, ok := engine.Lookup(meta.IndexName)
	require.True(t, ok)
	require.NotZero(t, state.ProbeVersion())
	require.Nil(t, state.LoadSegmentStore())

	dbPath, err := dbMgr.GetDatabasePath(meta.Database)
	require.NoError(t, err)
	segmentDir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	blockerPath := filepath.Join(segmentDir, "manifest")
	require.NoError(t, os.WriteFile(blockerPath, []byte("blocker"), 0o644))

	retry := hook.bootstrapOnce(context.Background(), meta, spec)
	require.False(t, retry, "bootstrap should keep retrying after a transient segment publish failure")
	require.Nil(t, state.LoadSegmentStore(), "failed publish must not install a partial segment generation")

	require.NoError(t, os.Remove(blockerPath))
	require.True(t, hook.bootstrapOnce(context.Background(), meta, spec), "bootstrap should complete once the publish blocker is removed")
	require.NotNil(t, state.LoadSegmentStore())
	hook.stopMaintenanceWatcher(meta.IndexName)
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
		{100, 64},       // ceil(100/512)=1 < 64 → clamp to 64
		{1000, 64},      // ceil(1000/512)=2 < 64 → clamp to 64
		{32768, 64},     // ceil(32768/512)=64
		{100000, 196},   // ceil(100000/512)=196
		{1000000, 1954}, // ceil(1000000/512)=1954
		{1048576, 2048}, // ceil(1048576/512)=2048
		{2000000, 2048}, // ceil(2000000/512)=3907 > 2048 → clamp to 2048
	}
	for _, tc := range cases {
		got := autoTuneNlist(tc.n)
		require.Equal(t, tc.want, got, "n=%d", tc.n)
	}
}

func TestCreateIndex_AutoTuneDefaultsTargetPartitionSizeTo512(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase("test"))

	vecMgr := NewVectorIndexManager(dbMgr)
	require.NoError(t, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection("test")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)
	insertDocVectorRows(t, conn, 32769)

	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "cosine",
		Dim:        4,
		CreatedAt:  time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	var nlist, nprobe, targetPartitionSize int
	var autoNlist, autoNprobe int64
	require.NoError(t, conn.QueryRow(`
		SELECT nlist, nprobe, auto_nlist, auto_nprobe, target_partition_size
		FROM __marmot_vector_indexes
		WHERE index_name = ?`,
		meta.IndexName,
	).Scan(&nlist, &nprobe, &autoNlist, &autoNprobe, &targetPartitionSize))
	require.Equal(t, 65, nlist)
	require.Equal(t, autoTuneNprobe(nlist), nprobe)
	require.Zero(t, autoNlist)
	require.EqualValues(t, 1, autoNprobe)
	require.Equal(t, defaultTargetPartitionSize, targetPartitionSize)
}

func TestCreateIndex_AutoTuneUsesExplicitTargetPartitionSize(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase("test"))

	vecMgr := NewVectorIndexManager(dbMgr)
	require.NoError(t, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection("test")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)
	insertDocVectorRows(t, conn, 8200)

	meta := common.VectorIndexMeta{
		IndexName:           "embeddings",
		TableName:           "docs",
		ColumnName:          "embed",
		Database:            "test",
		Metric:              "cosine",
		Dim:                 4,
		TargetPartitionSize: 128,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	var nlist, nprobe, targetPartitionSize int
	require.NoError(t, conn.QueryRow(`
		SELECT nlist, nprobe, target_partition_size
		FROM __marmot_vector_indexes
		WHERE index_name = ?`,
		meta.IndexName,
	).Scan(&nlist, &nprobe, &targetPartitionSize))
	require.Equal(t, 65, nlist)
	require.Equal(t, autoTuneNprobeForTarget(nlist, meta.TargetPartitionSize), nprobe)
	require.Equal(t, meta.TargetPartitionSize, targetPartitionSize)
}

func TestAutoTuneNprobe(t *testing.T) {
	t.Parallel()
	cases := []struct {
		nlist  int
		target int
		want   int
	}{
		{64, defaultTargetPartitionSize, 16},
		{256, defaultTargetPartitionSize, 16},
		{977, defaultTargetPartitionSize, 32},
		{4, defaultTargetPartitionSize, 4},
		{65, 128, 64},
		{8, 128, 8},
	}
	for _, tc := range cases {
		got := autoTuneNprobeForTarget(tc.nlist, tc.target)
		require.Equal(t, tc.want, got, "nlist=%d target=%d", tc.nlist, tc.target)
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
