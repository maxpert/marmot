package db

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func TestDecodeCDCBytesFastPathBorrowsMsgpackBinPayload(t *testing.T) {
	raw, err := encoding.Marshal([]byte{1, 2, 3, 4})
	require.NoError(t, err)

	values := map[string][]byte{"embed": raw}
	got, ok, err := decodeCDCBytes(values, "embed")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3, 4}, got)

	raw[len(raw)-1] = 9
	require.Equal(t, byte(9), got[len(got)-1])
}

func TestDecodeCDCBytesFallsBackForString(t *testing.T) {
	raw, err := encoding.Marshal("abcd")
	require.NoError(t, err)

	got, ok, err := decodeCDCBytes(map[string][]byte{"embed": raw}, "embed")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("abcd"), got)
}

func TestRecordMaintenanceDeltasRepeatedUpdateUsesBatchCurrentRow(t *testing.T) {
	spec := vecindex.IVFSpec{ID: "docs_embed_idx", Dim: 1, Metric: vecindex.MetricL2, Nlist: 2, Nprobe: 2}
	centroids, err := kmeans.NewCentroidSet(1, [][]float32{{0}, {10}})
	require.NoError(t, err)

	state := vecindex.NewIndexState(spec, centroids)
	state.StoreMaintenanceState(&vecindex.MaintenanceState{
		ClusterRowCounts:  []uint64{0, 1, 0},
		ClusterVectorSums: [][]float32{nil, {1}, {0}},
	})

	overlay, err := vecindex.OpenJournaledOverlay(t.TempDir() + "/overlay.journal")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, overlay.Close()) })

	now := time.Now().UnixNano()
	require.NoError(t, overlay.ApplyCommittedBatch([]vecindex.OverlayMutation{{
		Kind:              vecindex.OverlayMutationUpsert,
		Epoch:             1,
		Sequence:          1,
		ClusterID:         1,
		RowID:             7,
		AppliedAtUnixNano: now,
		Vec:               encodeVec(t, []float32{1}),
	}}))

	meta := common.VectorIndexMeta{
		IndexName:  "docs_embed_idx",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "l2",
		Dim:        1,
		Nlist:      2,
		Nprobe:     2,
	}
	entries := []common.CDCEntry{
		{
			Table: "docs",
			OldValues: encodeTestValues(map[string]interface{}{
				"id":    int64(7),
				"embed": encodeVec(t, []float32{1}),
			}),
			NewValues: encodeTestValues(map[string]interface{}{
				"id":    int64(7),
				"embed": encodeVec(t, []float32{10}),
			}),
		},
		{
			Table: "docs",
			OldValues: encodeTestValues(map[string]interface{}{
				"id":    int64(7),
				"embed": encodeVec(t, []float32{10}),
			}),
			NewValues: encodeTestValues(map[string]interface{}{
				"id":    int64(7),
				"embed": encodeVec(t, []float32{12}),
			}),
		},
	}

	require.NoError(t, recordMaintenanceDeltas(meta, state, spec, "id", overlay.Snapshot(), entries))

	maintenance := state.LoadMaintenanceState()
	require.Equal(t, []uint64{0, 0, 1}, maintenance.LiveClusterRowCounts())
	require.Equal(t, [][]float32{nil, {0}, {12}}, maintenance.LiveClusterVectorSums())
}
