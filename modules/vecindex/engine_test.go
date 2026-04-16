package vecindex

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func makeEngine(t *testing.T) *Engine {
	t.Helper()
	return NewEngine()
}

func makeState(t *testing.T, id string, dim int, centroids [][]float32) *IndexState {
	t.Helper()
	spec := IVFSpec{ID: id, Dim: dim, Metric: MetricL2, Nlist: len(centroids)}
	cs, err := kmeans.NewCentroidSet(1, centroids)
	require.NoError(t, err)
	return NewIndexState(spec, cs)
}

func TestEngine_RegisterAndLookup(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	state := makeState(t, "emb", 3, [][]float32{{1, 0, 0}, {0, 1, 0}})

	e.Register("emb", state)

	got, ok := e.Lookup("emb")
	require.True(t, ok)
	require.Same(t, state, got)
}

func TestEngine_LookupUnknown(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	_, ok := e.Lookup("nonexistent")
	require.False(t, ok)
}

func TestEngine_Unregister(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	state := makeState(t, "emb", 2, [][]float32{{1, 0}})
	e.Register("emb", state)
	e.Unregister("emb")
	_, ok := e.Lookup("emb")
	require.False(t, ok)
}

func TestEngine_UnregisterNoop(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	require.NotPanics(t, func() { e.Unregister("nothing") })
}

func TestEngine_AssignNearest_ReturnsOneBased(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	state := makeState(t, "emb", 3, [][]float32{{1, 0, 0}, {0, 1, 0}})
	e.Register("emb", state)

	id, err := e.AssignNearest("emb", encodeVec([]float32{0.9, 0.1, 0}))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	id, err = e.AssignNearest("emb", encodeVec([]float32{0.1, 0.9, 0}))
	require.NoError(t, err)
	require.Equal(t, int64(2), id)
}

func TestEngine_AssignNearest_UnknownIndex(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	_, err := e.AssignNearest("noindex", encodeVec([]float32{1, 0}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-013")
}

func TestEngine_AssignNearest_DimMismatch(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	state := makeState(t, "emb", 3, [][]float32{{1, 0, 0}})
	e.Register("emb", state)

	_, err := e.AssignNearest("emb", encodeVec([]float32{1, 0})) // 2-dim on 3-dim index
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-014")
}

func TestEngine_NotifyCentroidChange_NoOp(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	// No listener installed — must not return an error regardless of registration state.
	require.NoError(t, e.NotifyCentroidChange("any", 99))
	require.NoError(t, e.NotifyCentroidChange("", 0))
}

func TestEngine_RegisterWithCentroidSet(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	spec := IVFSpec{ID: "emb", Dim: 2, Metric: MetricL2, Nlist: 2}
	cs, err := kmeans.NewCentroidSet(5, [][]float32{{1, 0}, {0, 1}})
	require.NoError(t, err)

	state := e.RegisterWithCentroidSet("emb", spec, cs)
	require.NotNil(t, state)
	require.Equal(t, uint64(5), state.ProbeVersion())

	got, ok := e.Lookup("emb")
	require.True(t, ok)
	require.Same(t, state, got)
}

func TestEngine_ImplementsVectorUDFProvider(t *testing.T) {
	t.Parallel()
	// Compile-time check that Engine satisfies VectorUDFProvider.
	var _ VectorUDFProvider = (*Engine)(nil)
}

func TestEngine_ConcurrentRegisterLookup(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	const n = 64
	done := make(chan struct{})

	for i := 0; i < n; i++ {
		go func(i int) {
			state := makeState(t, "emb", 2, [][]float32{{float32(i), 0}})
			e.Register("emb", state)
			e.Lookup("emb")
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < n; i++ {
		<-done
	}
	_, ok := e.Lookup("emb")
	require.True(t, ok)
}
