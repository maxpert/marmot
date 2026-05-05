package db

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func TestHierarchicalCatchUpRelabelsRowsByCurrentParent(t *testing.T) {
	tdb := openTestDBWithMeta(t, t.TempDir()+"/catchup.db")
	db := tdb.DB
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	meta := common.VectorIndexMeta{
		IndexName:           "catchup_idx",
		TableName:           "docs",
		ColumnName:          "embed",
		Database:            "testdb",
		Metric:              "l2",
		Dim:                 2,
		Nlist:               4,
		Nprobe:              1,
		TargetPartitionSize: 6,
		CreatedAt:           1,
	}
	baseSpec := vecindex.IVFSpec{ID: meta.IndexName, Dim: 2, Metric: vecindex.MetricL2, Nlist: 4, Nprobe: 1, Seed: 99}
	currentParents, err := kmeans.NewCentroidSet(7, [][]float32{
		{0, 0},
		{100, 0},
		{200, 0},
		{300, 0},
	})
	require.NoError(t, err)
	badStoredParents, err := kmeans.NewCentroidSet(7, [][]float32{
		{300, 0},
		{100, 0},
		{200, 0},
		{0, 0},
	})
	require.NoError(t, err)
	guards := catchUpParentGuardFamilies(currentParents, baseSpec, catchUpDefaultParentGuardFamilies)
	require.NotContains(t, guards[1], 4)

	rowID := 1
	for i := 0; i < 12; i++ {
		_, err := db.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, rowID, encodeVec(t, []float32{float32(i % 3), 0}))
		require.NoError(t, err)
		rowID++
	}
	for _, x := range []float32{100, 101, 102, 103} {
		_, err := db.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, rowID, encodeVec(t, []float32{x, 0}))
		require.NoError(t, err)
		rowID++
	}
	for _, x := range []float32{200, 201, 202, 203} {
		_, err := db.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, rowID, encodeVec(t, []float32{x, 0}))
		require.NoError(t, err)
		rowID++
	}
	wrongBaseRowID := int64(rowID)
	for _, x := range []float32{300, 301} {
		_, err := db.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, rowID, encodeVec(t, []float32{x, 0}))
		require.NoError(t, err)
		rowID++
	}

	base, err := RebuildSegmentGeneration(context.Background(), db, tdb.dbPath, meta, baseSpec, badStoredParents, 0, nil)
	require.NoError(t, err)
	defer base.Close()
	staleLoc, ok, err := base.RowMap.Lookup(wrongBaseRowID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), staleLoc.ClusterID)

	overlayVec := []float32{302, 0}
	overlayRowID := int64(rowID)
	_, err = db.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, rowID, encodeVec(t, overlayVec))
	require.NoError(t, err)
	overlayPrepared, err := materializeVectorBlob(encodeVec(t, overlayVec), baseSpec.Metric, baseSpec.Dim, baseSpec.MaxNorm)
	require.NoError(t, err)
	overlay := vecindex.NewOverlayBuffer()
	require.NoError(t, overlay.ApplyBatch([]vecindex.OverlayMutation{
		{
			Kind:      vecindex.OverlayMutationUpsert,
			Epoch:     currentParents.Epoch(),
			Sequence:  1,
			ClusterID: 1,
			RowID:     overlayRowID,
			Vec:       overlayPrepared,
		},
	}))

	nextMeta := meta
	nextMeta.Nlist = 5
	nextSpec := baseSpec
	nextSpec.Nlist = 5
	_, pending, err := BuildHierarchicalCatchUpSegmentGeneration(
		context.Background(),
		db,
		tdb.dbPath,
		nextMeta,
		nextSpec,
		currentParents,
		base,
		overlay.Snapshot(),
		0,
		1,
		currentParents.Epoch()+1,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, pending)
	defer pending.Close()

	next := pending.generation
	for _, id := range []int64{wrongBaseRowID, overlayRowID} {
		loc, ok, err := next.RowMap.Lookup(id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(4), loc.ClusterID)
	}
}
