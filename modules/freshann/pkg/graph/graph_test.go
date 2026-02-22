package graph

import (
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestBuildAndSearch(t *testing.T) {
	g := New(api.MetricDot, 2)
	vectors := map[string][]float32{
		"a": {1, 0},
		"b": {0.9, 0.1},
		"c": {0, 1},
		"d": {-1, 0},
	}
	require.NoError(t, g.Build(vectors))
	ids, err := g.Search([]float32{1, 0}, 2, 32, 8,
		func(id string) ([]float32, bool) { v, ok := vectors[id]; return v, ok },
		func(id string) bool { return true },
	)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	require.Equal(t, "a", ids[0])
}

func TestSnapshotRoundTrip(t *testing.T) {
	g := New(api.MetricDot, 2)
	vectors := map[string][]float32{"a": {1, 0}, "b": {0, 1}}
	require.NoError(t, g.Build(vectors))
	st := g.SnapshotState()
	g2 := FromState(st)
	ids, err := g2.Search([]float32{1, 0}, 1, 16, 4,
		func(id string) ([]float32, bool) { v, ok := vectors[id]; return v, ok },
		func(id string) bool { return true },
	)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
}

func TestFromStateDefaults(t *testing.T) {
	g := FromState(State{Metric: api.MetricDot})
	require.NotNil(t, g)
	ids, err := g.Search([]float32{1, 0}, 1, 16, 4,
		func(id string) ([]float32, bool) { return nil, false },
		func(id string) bool { return true },
	)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestBuildAndSearchEuclidean(t *testing.T) {
	g := New(api.MetricEuclidean, 2)
	vectors := map[string][]float32{
		"a": {0, 0},
		"b": {1, 1},
		"c": {10, 10},
	}
	require.NoError(t, g.Build(vectors))
	ids, err := g.Search([]float32{0, 0}, 1, 32, 8,
		func(id string) ([]float32, bool) { v, ok := vectors[id]; return v, ok },
		func(id string) bool { return true },
	)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	require.Equal(t, "a", ids[0])
}
