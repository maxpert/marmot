package verify

import (
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/maxpert/marmot/modules/freshann/pkg/graph"
	"github.com/maxpert/marmot/modules/freshann/pkg/segment"
	"github.com/maxpert/marmot/modules/freshann/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRunBasic(t *testing.T) {
	store, err := storage.Open(t.TempDir(), storage.OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.PutVector([]byte("a"), storage.VectorRecord{VectorFP32: []float32{1, 2}}, nil))
	report, err := RunBasic(api.IndexSpec{Dim: 2}, store)
	require.NoError(t, err)
	require.True(t, report.Healthy)
}

func TestRunComprehensiveDeepGraphDangling(t *testing.T) {
	root := t.TempDir()
	store, err := storage.Open(filepath.Join(root, "meta.pebble"), storage.OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.PutVector([]byte("a"), storage.VectorRecord{VectorFP32: []float32{1, 2}}, nil))

	require.NoError(t, store.SaveGraphState(graph.State{
		Metric: api.MetricDot,
		R:      2,
		Start:  []string{"a"},
		Adj: map[string][]string{
			"a":       []string{"missing"},
			"missing": []string{"a"},
		},
	}, nil))

	manifest := segment.Manifest{ActiveSegment: ""}
	report, err := RunComprehensive(api.IndexSpec{Dim: 2, Metric: api.MetricDot}, store, manifest, root, true)
	require.NoError(t, err)
	require.False(t, report.Healthy)
}
