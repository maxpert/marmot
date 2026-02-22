package verify

import (
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/maxpert/marmot/modules/freshann/pkg/graphv2"
	"github.com/maxpert/marmot/modules/freshann/pkg/segment"
	"github.com/maxpert/marmot/modules/freshann/pkg/storagev2"
	"github.com/stretchr/testify/require"
)

func TestRunBasic(t *testing.T) {
	store, err := storagev2.Open(t.TempDir(), storagev2.OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	_, _, err = store.ApplyUpsert(api.ApplyToken{TxnID: 1, SeqID: 1}, []byte("a"), storagev2.VectorRecord{VectorFP32: []float32{1, 2}}, nil)
	require.NoError(t, err)
	report, err := RunBasic(api.IndexSpec{Dim: 2}, store)
	require.NoError(t, err)
	require.True(t, report.Healthy)
}

func TestRunComprehensiveDeepGraphDangling(t *testing.T) {
	root := t.TempDir()
	store, err := storagev2.Open(filepath.Join(root, "meta.pebble"), storagev2.OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	_, _, err = store.ApplyUpsert(api.ApplyToken{TxnID: 1, SeqID: 1}, []byte("a"), storagev2.VectorRecord{VectorFP32: []float32{1, 2}}, nil)
	require.NoError(t, err)

	require.NoError(t, store.SaveGraphState(graphv2.State{
		Metric: api.MetricDot,
		R:      2,
		Start:  []uint64{1},
		Adj: map[uint64][]uint64{
			1: {2},
			2: {1},
		},
	}, nil))

	manifest := segment.Manifest{ActiveSegment: ""}
	report, err := RunComprehensive(api.IndexSpec{Dim: 2, Metric: api.MetricDot}, store, manifest, root, true)
	require.NoError(t, err)
	require.False(t, report.Healthy)
}
