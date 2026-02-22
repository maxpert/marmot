package graphv2

import (
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestBuildAndSearch(t *testing.T) {
	g := New(api.MetricDot, 4)
	vectors := map[uint64][]float32{
		1: {1, 0},
		2: {0.9, 0.1},
		3: {0, 1},
	}
	require.NoError(t, g.Build(vectors))

	ids, steps, err := g.Search([]float32{1, 0}, 2, 32, 8,
		func(id uint64) ([]float32, bool) { v, ok := vectors[id]; return v, ok },
		func(id uint64) bool { return true },
	)
	require.NoError(t, err)
	require.Greater(t, steps, 0)
	require.NotEmpty(t, ids)
	require.Equal(t, uint64(1), ids[0])
}

func TestInsertRemove(t *testing.T) {
	g := New(api.MetricDot, 2)
	vectors := map[uint64][]float32{1: {1, 0}, 2: {0, 1}}
	require.NoError(t, g.Build(vectors))

	vectors[3] = []float32{0.95, 0.05}
	g.Insert(3, vectors[3], 32, 8, func(id uint64) ([]float32, bool) { v, ok := vectors[id]; return v, ok })
	ids, _, err := g.Search([]float32{1, 0}, 2, 32, 8,
		func(id uint64) ([]float32, bool) { v, ok := vectors[id]; return v, ok },
		func(id uint64) bool { return true },
	)
	require.NoError(t, err)
	require.Contains(t, ids, uint64(3))

	g.RemoveNode(3)
	delete(vectors, 3)
	ids, _, err = g.Search([]float32{1, 0}, 2, 32, 8,
		func(id uint64) ([]float32, bool) { v, ok := vectors[id]; return v, ok },
		func(id uint64) bool { return true },
	)
	require.NoError(t, err)
	for _, id := range ids {
		require.NotEqual(t, uint64(3), id)
	}
}

func TestFromStateRoundTrip(t *testing.T) {
	state := State{
		Metric: api.MetricDot,
		R:      2,
		Start:  []uint64{1},
		Adj: map[uint64][]uint64{
			1: {2},
			2: {1},
		},
	}
	g := FromState(state)
	snap := g.SnapshotState()
	require.Equal(t, state.Metric, snap.Metric)
	require.Equal(t, state.R, snap.R)
	require.Equal(t, state.Start, snap.Start)
	require.Equal(t, state.Adj, snap.Adj)
}
