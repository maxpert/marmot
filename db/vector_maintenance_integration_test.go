//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/stretchr/testify/require"
)

func TestIncrementalMerge_PublishesNewGenerationAndAdvancesAppliedSeq(t *testing.T) {
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
		TargetPartitionSize: 32,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	const bootstrapRows = bootstrapMinTargetPartitions * 32
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

	var state *vecindex.IndexState
	require.Eventually(t, func() bool {
		var ok bool
		state, ok = engine.Lookup(meta.IndexName)
		return ok && state.ProbeVersion() > 0 && state.LoadSegmentStore() != nil
	}, 30*time.Second, 100*time.Millisecond)

	oldEpoch := state.ProbeVersion()
	oldGeneration := state.LoadSegmentStore().Data.Generation()
	hook.stopMaintenanceWatcher(meta.IndexName)

	raw := encodeVec(t, []float32{0.1, 0.9, 0.2, 0.8})
	_, err = conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, bootstrapRows+1, raw)
	require.NoError(t, err)
	overlay := state.LoadOverlay()
	require.NotNil(t, overlay)
	snapshotBefore := overlay.Snapshot()
	require.NotNil(t, snapshotBefore)
	mutation, err := buildUpsertMutation(state, state.Spec(), state.ProbeVersion(), bootstrapRows+1, raw, snapshotBefore.LastSequence()+1)
	require.NoError(t, err)
	require.NoError(t, overlay.ApplyCommittedBatch([]vecindex.OverlayMutation{mutation}))
	newCluster, newVec, err := maintenancePreparedCluster(state, state.Spec(), raw)
	require.NoError(t, err)
	state.RecordClusterMutation(0, nil, newCluster, newVec)
	state.RecordRowsModified(1)

	require.Eventually(t, func() bool {
		state, _ = engine.Lookup(meta.IndexName)
		overlay := state.LoadOverlay()
		return overlay != nil && overlay.Snapshot() != nil && overlay.Snapshot().Len() > 0
	}, 10*time.Second, 50*time.Millisecond)

	dbPath, err := dbMgr.GetDatabasePath("test")
	require.NoError(t, err)

	state, _ = engine.Lookup(meta.IndexName)
	require.NoError(t, hook.runIncrementalMerge(context.Background(), conn, dbPath, meta, state.Spec(), state))

	state, _ = engine.Lookup(meta.IndexName)
	require.Equal(t, oldEpoch, state.ProbeVersion())
	require.NotNil(t, state.LoadSegmentStore())
	require.Greater(t, state.LoadSegmentStore().Data.Generation(), oldGeneration)
	require.Equal(t, state.ProbeVersion(), state.LoadSegmentStore().ProbeCentroids.Epoch())
	require.Equal(t, state.ProbeVersion(), state.LoadSegmentStore().StableCentroids.Epoch())

	overlay = state.LoadOverlay()
	require.NotNil(t, overlay)
	snapshot := overlay.Snapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, state.ProbeVersion(), snapshot.Epoch())
	backlogRows, _, _ := snapshot.BacklogStats(state.LoadSegmentStore().AppliedOverlaySeq)
	require.Zero(t, backlogRows)
	overlayInfo, err := os.Stat(vecindex.OverlayJournalPath(vecindex.SegmentStoreDir(dbPath, meta.IndexName)))
	require.NoError(t, err)
	require.LessOrEqual(t, overlayInfo.Size(), int64(64))

	loc, ok, err := state.LoadSegmentStore().RowMap.Lookup(int64(bootstrapRows + 1))
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, loc.ClusterID, int64(0))
}

func TestBootstrapPublishesBoundedPrefixAndPreservesOverlayTail(t *testing.T) {
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
		TargetPartitionSize: 8,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	const rows = 300
	mirrorRows := make([]overlayMirrorRow, 0, rows)
	for i := 0; i < rows; i++ {
		vec := []float32{
			1, float32(i % 7), float32((i + 1) % 11), float32((i + 2) % 13),
		}
		_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, i+1, encodeVec(t, vec))
		require.NoError(t, err)
		mirrorRows = append(mirrorRows, overlayMirrorRow{rowID: int64(i + 1), vec: vec})
	}
	mirrorRowsToOverlay(t, hook, meta, mirrorRows)

	var state *vecindex.IndexState
	require.Eventually(t, func() bool {
		var ok bool
		state, ok = engine.Lookup(meta.IndexName)
		return ok && state.ProbeVersion() > 0 && state.LoadSegmentStore() != nil
	}, 30*time.Second, 100*time.Millisecond)
	hook.stopMaintenanceWatcher(meta.IndexName)

	expectedPublished := bootstrapInitialPublishRows(meta, rows)
	segments := state.LoadSegmentStore()
	require.NotNil(t, segments)
	require.Equal(t, uint64(expectedPublished), segments.AppliedOverlaySeq)
	require.Equal(t, uint64(expectedPublished), segments.Data.RowCount())

	overlay := state.LoadOverlay()
	require.NotNil(t, overlay)
	snapshot := overlay.Snapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, rows-expectedPublished, snapshot.Len())
	require.Equal(t, state.ProbeVersion(), snapshot.Epoch())

	_, ok, err := segments.RowMap.Lookup(int64(expectedPublished))
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = segments.RowMap.Lookup(int64(expectedPublished + 1))
	require.NoError(t, err)
	require.False(t, ok)
	_, ok = snapshot.RowCluster(int64(expectedPublished + 1))
	require.True(t, ok)
}

func TestCatchUpRebuildPromotesOverlayTailInOnePublish(t *testing.T) {
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
		TargetPartitionSize: 4,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	const rows = 300
	mirrorRows := make([]overlayMirrorRow, 0, rows)
	for i := 0; i < rows; i++ {
		group := i / 4
		vec := []float32{
			1,
			float32(group % 19),
			float32((group / 19) % 17),
			float32(i % 4),
		}
		_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, i+1, encodeVec(t, vec))
		require.NoError(t, err)
		mirrorRows = append(mirrorRows, overlayMirrorRow{rowID: int64(i + 1), vec: vec})
	}
	mirrorRowsToOverlay(t, hook, meta, mirrorRows)

	var state *vecindex.IndexState
	require.Eventually(t, func() bool {
		var ok bool
		state, ok = engine.Lookup(meta.IndexName)
		if ok && state.ProbeVersion() > 0 && state.LoadSegmentStore() != nil {
			hook.stopMaintenanceWatcher(meta.IndexName)
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	initialGeneration := state.LoadSegmentStore().Data.Generation()
	ranCatchUp := false
	if state.Spec().Nlist < desiredClusterCount(rows, meta.TargetPartitionSize) {
		require.NotZero(t, state.LoadOverlay().Snapshot().Len())
		refreshedMeta, err := loadIndexMetaByName(context.Background(), conn, meta.IndexName)
		require.NoError(t, err)
		dbPath, err := dbMgr.GetDatabasePath("test")
		require.NoError(t, err)
		require.NoError(t, hook.runCatchUpRebuild(context.Background(), conn, dbPath, *refreshedMeta, state.Spec(), state))
		ranCatchUp = true
	}

	state, _ = engine.Lookup(meta.IndexName)
	require.Equal(t, desiredClusterCount(rows, meta.TargetPartitionSize), state.Spec().Nlist)
	if ranCatchUp {
		require.Greater(t, state.LoadSegmentStore().Data.Generation(), initialGeneration)
	}
	require.Equal(t, uint64(rows), state.LoadSegmentStore().Data.RowCount())
	require.Equal(t, uint64(rows), state.LoadSegmentStore().AppliedOverlaySeq)
	require.Zero(t, state.LoadOverlay().Snapshot().Len())

	loc, ok, err := state.LoadSegmentStore().RowMap.Lookup(rows)
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, loc.ClusterID, int64(0))
}

func TestIncrementalMerge_PreservesOverlayTailAcrossPublish(t *testing.T) {
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
		TargetPartitionSize: 32,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	const bootstrapRows = bootstrapMinTargetPartitions * 32
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

	var state *vecindex.IndexState
	require.Eventually(t, func() bool {
		var ok bool
		state, ok = engine.Lookup(meta.IndexName)
		return ok && state.ProbeVersion() > 0 && state.LoadSegmentStore() != nil
	}, 30*time.Second, 100*time.Millisecond)

	hook.stopMaintenanceWatcher(meta.IndexName)
	oldEpoch := state.ProbeVersion()

	applyTail := func(rowID int64, vec []byte) {
		_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, rowID, vec)
		require.NoError(t, err)
		overlay := state.LoadOverlay()
		require.NotNil(t, overlay)
		snapshot := overlay.Snapshot()
		require.NotNil(t, snapshot)
		mutation, err := buildUpsertMutation(state, state.Spec(), state.ProbeVersion(), rowID, vec, snapshot.LastSequence()+1)
		require.NoError(t, err)
		require.NoError(t, overlay.ApplyCommittedBatch([]vecindex.OverlayMutation{mutation}))
		newCluster, newVec, err := maintenancePreparedCluster(state, state.Spec(), vec)
		require.NoError(t, err)
		state.RecordClusterMutation(0, nil, newCluster, newVec)
		state.RecordRowsModified(1)
	}

	firstRaw := encodeVec(t, []float32{0.1, 0.9, 0.2, 0.8})
	applyTail(bootstrapRows+1, firstRaw)

	dbPath, err := dbMgr.GetDatabasePath("test")
	require.NoError(t, err)

	plan, err := hook.prepareIncrementalMerge(context.Background(), conn, dbPath, meta, state.Spec(), state)
	require.NoError(t, err)
	require.NotNil(t, plan)
	defer plan.Close()

	tailRaw := encodeVec(t, []float32{0.2, 0.8, 0.4, 0.6})
	applyTail(bootstrapRows+2, tailRaw)

	require.NoError(t, hook.publishIncrementalMerge(dbPath, meta, plan))

	state, _ = engine.Lookup(meta.IndexName)
	require.Equal(t, oldEpoch, state.ProbeVersion())
	require.Equal(t, oldEpoch, plan.currentEpoch)

	overlay := state.LoadOverlay()
	require.NotNil(t, overlay)
	snapshot := overlay.Snapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, state.ProbeVersion(), snapshot.Epoch())
	backlogRows, _, _ := snapshot.BacklogStats(state.LoadSegmentStore().AppliedOverlaySeq)
	require.Equal(t, 1, backlogRows)
	clusterID, ok := snapshot.RowClusterAfter(int64(bootstrapRows+2), state.LoadSegmentStore().AppliedOverlaySeq)
	require.True(t, ok)
	require.Greater(t, clusterID, int64(0))
	_, ok = snapshot.RowClusterAfter(int64(bootstrapRows+1), state.LoadSegmentStore().AppliedOverlaySeq)
	require.False(t, ok)

	loc, ok, err := state.LoadSegmentStore().RowMap.Lookup(int64(bootstrapRows + 1))
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, loc.ClusterID, int64(0))
}

func TestIncrementalPromotion_PublishesLargerClusterLayout(t *testing.T) {
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
		Metric:              "l2",
		Dim:                 4,
		Nlist:               8,
		Nprobe:              3,
		TargetPartitionSize: 4,
		CreatedAt:           time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))

	rows := make([]overlayMirrorRow, 0, 64)
	bases := [][]float32{
		{0, 0, 0, 0},
		{5, 5, 5, 5},
		{10, 10, 10, 10},
		{15, 15, 15, 15},
		{20, 20, 20, 20},
		{25, 25, 25, 25},
		{30, 30, 30, 30},
		{35, 35, 35, 35},
	}
	rowID := 1
	for baseIdx, base := range bases {
		groupRows := 8
		if baseIdx == 0 {
			groupRows = 12
		}
		for i := 0; i < groupRows; i++ {
			vec := []float32{
				base[0] + float32(i%2),
				base[1] + float32((i/2)%2),
				base[2] + float32((i/4)%2),
				base[3] + float32(i%3),
			}
			_, err := conn.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, rowID, encodeVec(t, vec))
			require.NoError(t, err)
			rows = append(rows, overlayMirrorRow{rowID: int64(rowID), vec: vec})
			rowID++
		}
	}
	mirrorRowsToOverlay(t, hook, meta, rows)

	var state *vecindex.IndexState
	require.Eventually(t, func() bool {
		var ok bool
		state, ok = engine.Lookup(meta.IndexName)
		return ok && state.ProbeVersion() > 0 && state.LoadSegmentStore() != nil
	}, 30*time.Second, 100*time.Millisecond)

	hook.stopMaintenanceWatcher(meta.IndexName)
	oldEpoch := state.ProbeVersion()
	oldGeneration := state.LoadSegmentStore().Data.Generation()

	dbPath, err := dbMgr.GetDatabasePath("test")
	require.NoError(t, err)
	require.NoError(t, hook.runIncrementalPromotion(context.Background(), conn, dbPath, meta, state.Spec(), state, 10))

	state, _ = engine.Lookup(meta.IndexName)
	require.Equal(t, oldEpoch+1, state.ProbeVersion())
	require.NotNil(t, state.LoadSegmentStore())
	require.Greater(t, state.LoadSegmentStore().Data.Generation(), oldGeneration)
	require.Equal(t, 10, state.Spec().Nlist)
	require.Equal(t, 10, state.LoadSegmentStore().ProbeCentroids.Len())
	require.Equal(t, 10, state.LoadSegmentStore().StableCentroids.Len())

	overlay := state.LoadOverlay()
	require.NotNil(t, overlay)
	snapshot := overlay.Snapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, state.ProbeVersion(), snapshot.Epoch())
	require.Zero(t, snapshot.Len())

	var nlist int
	require.NoError(t, conn.QueryRow(`SELECT nlist FROM __marmot_vector_indexes WHERE index_name = ?`, meta.IndexName).Scan(&nlist))
	require.Equal(t, 10, nlist)
}
