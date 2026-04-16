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

// stubStats returns a ClusterStatsFunc that returns the given stats.
func stubStats(stats ClusterStats) ClusterStatsFunc {
	return func(_ context.Context, _ string, _ int) (ClusterStats, error) {
		return stats, nil
	}
}

// reindexRecorder records calls to the reindex function.
type reindexRecorder struct {
	mu    sync.Mutex
	calls []string
	// block controls whether reindexFn blocks until unblocked.
	block   chan struct{}
	blocked chan struct{} // closed when reindexFn is blocked
}

func newReindexRecorder() *reindexRecorder {
	return &reindexRecorder{}
}

func (r *reindexRecorder) fn() ReindexFunc {
	return func(_ context.Context, indexName string) error {
		if r.block != nil {
			// Signal that we're blocked.
			select {
			case <-r.blocked:
			default:
				close(r.blocked)
			}
			<-r.block
		}
		r.mu.Lock()
		r.calls = append(r.calls, indexName)
		r.mu.Unlock()
		return nil
	}
}

func (r *reindexRecorder) getCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]string, len(r.calls))
	copy(cp, r.calls)
	return cp
}

func makeMonitorEngine(t *testing.T, indexName string, nlist int) *Engine {
	t.Helper()
	e := NewEngine()
	centroids := make([][]float32, nlist)
	for i := range centroids {
		centroids[i] = []float32{float32(i), 0}
	}
	spec := IVFSpec{ID: indexName, Dim: 2, Metric: MetricL2, Nlist: nlist}
	cs, err := kmeans.NewCentroidSet(1, centroids)
	require.NoError(t, err)
	e.Register(indexName, NewIndexState(spec, cs))
	return e
}

func TestRetrainMonitor_GrowthTrigger(t *testing.T) {
	t.Parallel()
	engine := makeMonitorEngine(t, "emb", 4)
	rec := newReindexRecorder()

	// 4 clusters, total=400, even=100 each. Cluster 0 has 200 → growth=2.0 >= 1.5.
	stats := ClusterStats{
		Counts: []int64{200, 100, 50, 50},
		DeltaN: 0,
		TotalN: 400,
	}

	cfg := DefaultRetrainConfig()
	cfg.CheckInterval = 10 * time.Millisecond

	mon := NewRetrainMonitor(engine, stubStats(stats), rec.fn(), nil, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)
	defer mon.Stop()

	require.Eventually(t, func() bool {
		return len(rec.getCalls()) > 0
	}, 2*time.Second, 5*time.Millisecond, "monitor should trigger reindex on growth")

	require.Equal(t, "emb", rec.getCalls()[0])
}

func TestRetrainMonitor_DeltaTrigger(t *testing.T) {
	t.Parallel()
	engine := makeMonitorEngine(t, "emb", 4)
	rec := newReindexRecorder()

	// delta_ratio = 250/1000 = 0.25 >= 0.2 threshold.
	stats := ClusterStats{
		Counts: []int64{200, 200, 200, 150},
		DeltaN: 250,
		TotalN: 1000,
	}

	cfg := DefaultRetrainConfig()
	cfg.CheckInterval = 10 * time.Millisecond

	mon := NewRetrainMonitor(engine, stubStats(stats), rec.fn(), nil, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)
	defer mon.Stop()

	require.Eventually(t, func() bool {
		return len(rec.getCalls()) > 0
	}, 2*time.Second, 5*time.Millisecond, "monitor should trigger reindex on delta ratio")
}

func TestRetrainMonitor_Hysteresis(t *testing.T) {
	t.Parallel()
	engine := makeMonitorEngine(t, "emb", 4)

	rec := newReindexRecorder()
	rec.block = make(chan struct{})
	rec.blocked = make(chan struct{})

	// Strong growth trigger.
	stats := ClusterStats{
		Counts: []int64{400, 100, 100, 100},
		DeltaN: 0,
		TotalN: 700,
	}

	cfg := DefaultRetrainConfig()
	cfg.CheckInterval = 10 * time.Millisecond

	mon := NewRetrainMonitor(engine, stubStats(stats), rec.fn(), nil, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)
	defer mon.Stop()

	// Wait for the first reindex to start (it will block).
	<-rec.blocked
	require.True(t, mon.IsRebuilding("emb"), "index should be marked as rebuilding")

	// Let several more ticks pass — monitor should skip due to hysteresis.
	// No additional calls should accumulate while the first is blocked.
	time.Sleep(50 * time.Millisecond)
	callsWhileBlocked := len(rec.getCalls())
	require.Equal(t, 0, callsWhileBlocked, "no reindex calls should complete while blocked")

	// Unblock.
	close(rec.block)

	// Wait for the blocked call to complete.
	require.Eventually(t, func() bool {
		return !mon.IsRebuilding("emb")
	}, 2*time.Second, 5*time.Millisecond)

	// Exactly one call should have completed.
	require.Equal(t, 1, len(rec.getCalls()), "hysteresis should prevent duplicate reindex calls")
}

func TestRetrainMonitor_OptOut(t *testing.T) {
	t.Parallel()
	engine := makeMonitorEngine(t, "emb", 4)
	rec := newReindexRecorder()

	// Would trigger growth if enabled.
	stats := ClusterStats{
		Counts: []int64{400, 100, 100, 100},
		DeltaN: 0,
		TotalN: 700,
	}

	cfg := DefaultRetrainConfig()
	cfg.Enabled = false
	cfg.CheckInterval = 10 * time.Millisecond

	mon := NewRetrainMonitor(engine, stubStats(stats), rec.fn(), nil, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)

	// Let several ticks pass.
	time.Sleep(80 * time.Millisecond)
	mon.Stop()

	require.Empty(t, rec.getCalls(), "disabled monitor should not trigger reindex")
}

func TestRetrainMonitor_BelowThreshold(t *testing.T) {
	t.Parallel()
	engine := makeMonitorEngine(t, "emb", 4)
	rec := newReindexRecorder()

	// Even distribution, no delta — below both thresholds.
	stats := ClusterStats{
		Counts: []int64{100, 100, 100, 100},
		DeltaN: 0,
		TotalN: 400,
	}

	cfg := DefaultRetrainConfig()
	cfg.CheckInterval = 10 * time.Millisecond

	mon := NewRetrainMonitor(engine, stubStats(stats), rec.fn(), nil, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)
	time.Sleep(80 * time.Millisecond)
	mon.Stop()

	require.Empty(t, rec.getCalls(), "even distribution should not trigger reindex")
}

func TestRetrainMonitor_EmptyIndex(t *testing.T) {
	t.Parallel()
	engine := makeMonitorEngine(t, "emb", 4)
	rec := newReindexRecorder()

	stats := ClusterStats{
		Counts: []int64{0, 0, 0, 0},
		DeltaN: 0,
		TotalN: 0,
	}

	cfg := DefaultRetrainConfig()
	cfg.CheckInterval = 10 * time.Millisecond

	mon := NewRetrainMonitor(engine, stubStats(stats), rec.fn(), nil, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	mon.Stop()

	require.Empty(t, rec.getCalls(), "empty index should not trigger reindex")
}

func TestRetrainMonitor_TryBeginRetrain_Concurrent(t *testing.T) {
	t.Parallel()
	engine := NewEngine()
	cfg := DefaultRetrainConfig()
	mon := NewRetrainMonitor(engine, nil, nil, nil, cfg)

	var won int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if mon.TryBeginRetrain("emb") {
				atomic.AddInt64(&won, 1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), won, "exactly one goroutine should win TryBeginRetrain")
	require.True(t, mon.IsRebuilding("emb"))

	mon.EndRetrain("emb")
	require.False(t, mon.IsRebuilding("emb"))
}

func TestRetrainMonitor_GracefulShutdown(t *testing.T) {
	t.Parallel()
	engine := makeMonitorEngine(t, "emb", 2)

	stats := ClusterStats{
		Counts: []int64{100, 100},
		DeltaN: 0,
		TotalN: 200,
	}

	cfg := DefaultRetrainConfig()
	cfg.CheckInterval = 10 * time.Millisecond

	mon := NewRetrainMonitor(engine, stubStats(stats), func(_ context.Context, _ string) error {
		return nil
	}, nil, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	mon.Start(ctx)

	// Cancel context — monitor should shut down cleanly.
	cancel()

	done := make(chan struct{})
	go func() {
		mon.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not shut down within timeout")
	}
}
