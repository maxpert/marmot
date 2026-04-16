package vecindex

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func makeListenerEngine(t *testing.T, indexName string, epoch uint64) (*Engine, *IndexState) {
	t.Helper()
	e := NewEngine()
	centroids := [][]float32{{1, 0}, {0, 1}}
	spec := IVFSpec{ID: indexName, Dim: 2, Metric: MetricL2, Nlist: 2}
	cs, err := kmeans.NewCentroidSet(epoch, centroids)
	require.NoError(t, err)
	state := NewIndexState(spec, cs)
	e.Register(indexName, state)
	return e, state
}

func makeTestBlob(t *testing.T, epoch uint64) []byte {
	t.Helper()
	cs, err := kmeans.NewCentroidSet(epoch, [][]float32{{0.5, 0.5}, {-0.5, -0.5}})
	require.NoError(t, err)
	blob, err := EncodeCentroidBlob(cs)
	require.NoError(t, err)
	return blob
}

func TestReplicaListener_HigherVersion_Rebuilds(t *testing.T) {
	t.Parallel()
	engine, state := makeListenerEngine(t, "emb", 1)

	blob := makeTestBlob(t, 5)
	var rebuildCalled atomic.Int32

	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		return blob, 5, nil
	}
	rebuilder := func(_ context.Context, _ string, _ *kmeans.CentroidSet) error {
		rebuildCalled.Add(1)
		return nil
	}

	listener := NewReplicaListener(engine, 16, loader, rebuilder, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	listener.Notify("emb", 5)

	require.Eventually(t, func() bool {
		return state.ProbeVersion() == 5
	}, 2*time.Second, 5*time.Millisecond, "probeState should be updated to version 5")

	require.Equal(t, int32(1), rebuildCalled.Load())
}

func TestReplicaListener_SameVersion_Skipped(t *testing.T) {
	t.Parallel()
	engine, state := makeListenerEngine(t, "emb", 5)

	var loaderCalled atomic.Int32
	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		loaderCalled.Add(1)
		return nil, 0, nil
	}

	listener := NewReplicaListener(engine, 16, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	// Notify with same version as current probeState.
	listener.Notify("emb", 5)

	// Give the worker time to process.
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, uint64(5), state.ProbeVersion(), "probeState should remain unchanged")
	require.Equal(t, int32(0), loaderCalled.Load(), "loader should not be called for same version")
}

func TestReplicaListener_LowerVersion_Skipped(t *testing.T) {
	t.Parallel()
	engine, state := makeListenerEngine(t, "emb", 10)

	var loaderCalled atomic.Int32
	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		loaderCalled.Add(1)
		return nil, 0, nil
	}

	listener := NewReplicaListener(engine, 16, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	listener.Notify("emb", 3)
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, uint64(10), state.ProbeVersion())
	require.Equal(t, int32(0), loaderCalled.Load())
}

func TestReplicaListener_ConcurrentNotifications_Coalesced(t *testing.T) {
	t.Parallel()
	engine, _ := makeListenerEngine(t, "emb", 1)

	var mu sync.Mutex
	loadCount := 0
	blob := makeTestBlob(t, 100)

	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		mu.Lock()
		loadCount++
		mu.Unlock()
		// Simulate slow load to allow coalescing.
		time.Sleep(10 * time.Millisecond)
		return blob, 100, nil
	}

	// Buffer size 1 — forces coalescing.
	listener := NewReplicaListener(engine, 1, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	// Fire many notifications rapidly.
	for i := 0; i < 100; i++ {
		listener.Notify("emb", int64(i+2))
	}

	require.Eventually(t, func() bool {
		s, ok := engine.Lookup("emb")
		return ok && s.ProbeVersion() >= 100
	}, 2*time.Second, 5*time.Millisecond)

	mu.Lock()
	loads := loadCount
	mu.Unlock()

	// With a buffer of 1, many notifications should be coalesced.
	// The exact count depends on timing, but should be much less than 100.
	require.Less(t, loads, 50, "notifications should be coalesced (got %d loads)", loads)
}

func TestReplicaListener_OriginSelfNotify_Skipped(t *testing.T) {
	t.Parallel()
	// Simulate origin: probeState already at version 5 (just finished own REINDEX).
	engine, state := makeListenerEngine(t, "emb", 5)

	var loaderCalled atomic.Int32
	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		loaderCalled.Add(1)
		return nil, 0, nil
	}

	listener := NewReplicaListener(engine, 16, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	// Origin's own trigger fires with version 5 — should be skipped.
	listener.Notify("emb", 5)
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, uint64(5), state.ProbeVersion())
	require.Equal(t, int32(0), loaderCalled.Load())
}

func TestReplicaListener_UnknownIndex_NoPanic(t *testing.T) {
	t.Parallel()
	engine := NewEngine()
	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		t.Fatal("loader should not be called for unknown index")
		return nil, 0, nil
	}

	listener := NewReplicaListener(engine, 16, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	// Should not panic or call loader.
	listener.Notify("nonexistent", 99)
	time.Sleep(50 * time.Millisecond)
}

func TestReplicaListener_DriftStateReset(t *testing.T) {
	t.Parallel()
	engine, state := makeListenerEngine(t, "emb", 1)

	blob := makeTestBlob(t, 5)
	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		return blob, 5, nil
	}

	listener := NewReplicaListener(engine, 16, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	listener.Notify("emb", 5)

	require.Eventually(t, func() bool {
		return state.ProbeVersion() == 5
	}, 2*time.Second, 5*time.Millisecond)

	// DriftState should also be reset to the new centroid set.
	driftCS := state.DriftState()
	require.NotNil(t, driftCS)
	require.Equal(t, uint64(5), driftCS.Epoch())
}

func TestEngine_NotifyCentroidChange_WithListener(t *testing.T) {
	t.Parallel()
	engine, state := makeListenerEngine(t, "emb", 1)

	blob := makeTestBlob(t, 7)
	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		return blob, 7, nil
	}

	listener := NewReplicaListener(engine, 16, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Start(ctx)
	defer listener.Stop()

	engine.SetReplicaListener(listener)

	// Use the VectorUDFProvider interface method.
	err := engine.NotifyCentroidChange("emb", 7)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return state.ProbeVersion() == 7
	}, 2*time.Second, 5*time.Millisecond)
}

func TestEngine_NotifyCentroidChange_NoListener(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	// No listener installed — should be a no-op, no panic.
	err := e.NotifyCentroidChange("any", 99)
	require.NoError(t, err)
}

func TestReplicaListener_GracefulShutdown(t *testing.T) {
	t.Parallel()
	engine := NewEngine()
	loader := func(_ context.Context, _ string) ([]byte, int64, error) {
		return nil, 0, nil
	}

	listener := NewReplicaListener(engine, 16, loader, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)

	cancel()

	done := make(chan struct{})
	go func() {
		listener.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not shut down within timeout")
	}
}
