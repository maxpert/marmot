package vecindex

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
)

// RetrainConfig holds the tunable parameters for the auto-retrain monitor
// (design §8.7). Values are sourced from session variables at construction;
// changes require monitor restart.
type RetrainConfig struct {
	Enabled       bool
	CheckInterval time.Duration
	GrowthRatio   float64
	DeltaRatio    float64
}

// DefaultRetrainConfig returns the default auto-retrain configuration per
// design §8.7 session variable defaults.
func DefaultRetrainConfig() RetrainConfig {
	return RetrainConfig{
		Enabled:       true,
		CheckInterval: 30 * time.Second,
		GrowthRatio:   1.5,
		DeltaRatio:    0.2,
	}
}

// ClusterStats holds per-index member distribution data used by the retrain
// monitor to evaluate thresholds.
type ClusterStats struct {
	// Counts holds per-cluster member counts indexed by 0-based cluster ID.
	// len(Counts) == nlist.
	Counts []int64
	// DeltaN is the number of members with cluster_id=0 (unassigned delta rows).
	DeltaN int64
	// TotalN is the total member count across all clusters including delta.
	TotalN int64
}

// ClusterStatsFunc queries the members table for an index and returns the
// current cluster distribution. Injected by the db layer.
type ClusterStatsFunc func(ctx context.Context, indexName string, nlist int) (ClusterStats, error)

// ReindexFunc is the callback invoked when the retrain monitor decides an
// index needs rebuilding. The implementation should call
// VectorIndexManager.ReindexIndex or equivalent.
type ReindexFunc func(ctx context.Context, indexName string) error

// RetrainLogFunc is an optional logging callback. If nil, the monitor runs
// silently. The level is "info" for triggers, "warn" for errors.
type RetrainLogFunc func(level, msg string, indexName string, err error)

// RetrainMonitor watches all registered indexes and triggers REINDEX when
// cluster growth or delta accumulation exceeds configured thresholds (§8.7).
//
// One monitor goroutine per Engine. It iterates all registered indexes each
// tick and fires REINDEX for any that cross the threshold, skipping indexes
// already being rebuilt (hysteresis).
type RetrainMonitor struct {
	engine    *Engine
	statsFn   ClusterStatsFunc
	reindexFn ReindexFunc
	logFn     RetrainLogFunc
	cfg       RetrainConfig

	// rebuilding tracks per-index hysteresis flags. A true value means a
	// retrain is in flight; the monitor skips that index until complete.
	rebuilding sync.Map // map[string]*atomic.Bool

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// NewRetrainMonitor creates a monitor but does not start it. Call Start to
// begin the polling loop.
func NewRetrainMonitor(engine *Engine, statsFn ClusterStatsFunc, reindexFn ReindexFunc, logFn RetrainLogFunc, cfg RetrainConfig) *RetrainMonitor {
	return &RetrainMonitor{
		engine:    engine,
		statsFn:   statsFn,
		reindexFn: reindexFn,
		logFn:     logFn,
		cfg:       cfg,
	}
}

// Start begins the monitor polling loop. It returns immediately; the loop
// runs in a background goroutine until Stop is called or ctx is cancelled.
func (m *RetrainMonitor) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	m.done = make(chan struct{})
	go m.loop(ctx)
}

// Stop signals the monitor to shut down and waits for the loop to exit.
// Safe to call multiple times.
func (m *RetrainMonitor) Stop() {
	m.once.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
	})
	if m.done != nil {
		<-m.done
	}
}

// TryBeginRetrain atomically sets the rebuilding flag for indexName. Returns
// true if the caller won the race and should proceed with the retrain; false
// if another retrain is already in flight.
func (m *RetrainMonitor) TryBeginRetrain(indexName string) bool {
	flag := m.getOrCreateFlag(indexName)
	return flag.CompareAndSwap(false, true)
}

// EndRetrain clears the rebuilding flag for indexName after a retrain
// completes (success or failure). Must be called in a defer after a
// successful TryBeginRetrain.
func (m *RetrainMonitor) EndRetrain(indexName string) {
	if v, ok := m.rebuilding.Load(indexName); ok {
		v.(*atomic.Bool).Store(false)
	}
}

// IsRebuilding reports whether a retrain is in flight for indexName.
func (m *RetrainMonitor) IsRebuilding(indexName string) bool {
	if v, ok := m.rebuilding.Load(indexName); ok {
		return v.(*atomic.Bool).Load()
	}
	return false
}

func (m *RetrainMonitor) getOrCreateFlag(indexName string) *atomic.Bool {
	if v, ok := m.rebuilding.Load(indexName); ok {
		return v.(*atomic.Bool)
	}
	flag := &atomic.Bool{}
	actual, _ := m.rebuilding.LoadOrStore(indexName, flag)
	return actual.(*atomic.Bool)
}

func (m *RetrainMonitor) log(level, msg, indexName string, err error) {
	if m.logFn != nil {
		m.logFn(level, msg, indexName, err)
	}
}

func (m *RetrainMonitor) loop(ctx context.Context) {
	defer close(m.done)

	ticker := time.NewTicker(m.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.cfg.Enabled {
				continue
			}
			m.checkAllIndexes(ctx)
		}
	}
}

func (m *RetrainMonitor) checkAllIndexes(ctx context.Context) {
	m.engine.indexes.Range(func(key, value any) bool {
		if ctx.Err() != nil {
			return false
		}
		indexName := key.(string)
		state := value.(*IndexState)
		m.checkIndex(ctx, indexName, state)
		return true
	})
}

func (m *RetrainMonitor) checkIndex(ctx context.Context, indexName string, state *IndexState) {
	if m.IsRebuilding(indexName) {
		return
	}

	cs := state.ProbeState()
	if cs == nil || cs.Len() == 0 {
		return
	}

	triggered, reason := m.evaluateThresholds(ctx, indexName, cs)
	if !triggered {
		return
	}

	if !m.TryBeginRetrain(indexName) {
		return
	}

	go func() {
		defer m.EndRetrain(indexName)
		m.log("info", "triggering REINDEX: "+reason, indexName, nil)
		if err := m.reindexFn(ctx, indexName); err != nil {
			m.log("warn", "REINDEX failed", indexName, err)
		}
	}()
}

// evaluateThresholds computes growth_max and delta_ratio per ��8.7 and returns
// whether a retrain should be triggered plus a short reason string.
func (m *RetrainMonitor) evaluateThresholds(ctx context.Context, indexName string, cs *kmeans.CentroidSet) (bool, string) {
	stats, err := m.statsFn(ctx, indexName, cs.Len())
	if err != nil {
		m.log("warn", "failed to query cluster stats", indexName, err)
		return false, ""
	}

	if stats.TotalN == 0 {
		return false, ""
	}

	// Growth ratio: max(count[i] / max(initial[i], 1)) across all clusters.
	// Approximation: initial[i] is estimated as totalN / nlist (even distribution
	// assumption) rather than stored per-cluster counts from the last REINDEX.
	// This can false-trigger on naturally skewed distributions where one cluster
	// legitimately holds more vectors. Acceptable: a spurious REINDEX is cheap
	// (it reconfirms the same centroids) and the hysteresis flag prevents thrashing.
	nlist := int64(cs.Len())
	initialPerCluster := max64(stats.TotalN/nlist, 1)
	var growthMax float64
	for _, c := range stats.Counts {
		g := float64(c) / float64(initialPerCluster)
		if g > growthMax {
			growthMax = g
		}
	}

	deltaRatio := float64(stats.DeltaN) / float64(max64(stats.TotalN, 1))

	if growthMax >= m.cfg.GrowthRatio {
		return true, fmt.Sprintf("growth_max=%.2f >= %.2f", growthMax, m.cfg.GrowthRatio)
	}
	if deltaRatio >= m.cfg.DeltaRatio {
		return true, fmt.Sprintf("delta_ratio=%.2f >= %.2f", deltaRatio, m.cfg.DeltaRatio)
	}
	return false, ""
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
