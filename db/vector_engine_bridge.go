package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
)

// Ensure EngineHook implements both lifecycle hooks.
var (
	_ IndexLifecycleHook = (*EngineHook)(nil)
	_ IndexReindexHook   = (*EngineHook)(nil)
	_ EngineProvider     = (*EngineHook)(nil)
)

// EngineHook bridges *vecindex.Engine to the db-layer interfaces
// IndexLifecycleHook and EngineProvider defined in vector_index_manager.go.
// It owns the step-10 flow from design §8.1: centroid check → k-means or
// load → register → bulk populate → flip status='ready'.
type EngineHook struct {
	engine *vecindex.Engine
	dbMgr  *DatabaseManager
}

// NewEngineHook creates an EngineHook. Call mgr.SetLifecycleHook(h) and
// mgr.SetEngineProvider(h) to wire it into VectorIndexManager.
func NewEngineHook(engine *vecindex.Engine, dbMgr *DatabaseManager) *EngineHook {
	return &EngineHook{engine: engine, dbMgr: dbMgr}
}

// OnIndexCreated implements IndexLifecycleHook.
// Called after the CREATE VECTOR INDEX DDL transaction commits.
// Handles design §8.1 step 10: centroid check, k-means, bulk populate,
// status flip to 'ready'.
func (h *EngineHook) OnIndexCreated(ctx context.Context, meta common.VectorIndexMeta) error {
	conn, err := h.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return fmt.Errorf("engine hook: get db %s: %w", meta.Database, err)
	}

	metric, err := metricFromString(meta.Metric)
	if err != nil {
		return fmt.Errorf("engine hook: parse metric: %w", err)
	}

	spec := vecindex.IVFSpec{
		ID:      meta.IndexName,
		Dim:     meta.Dim,
		Metric:  metric,
		Nlist:   meta.Nlist,
		Nprobe:  meta.Nprobe,
		MaxNorm: meta.MaxNorm,
		// Seed is derived from stable identity (TableName, ColumnName, Dim,
		// Metric, Nlist) so two nodes running CREATE concurrently converge on
		// byte-identical centroids. CreatedAt is node-local (HLC) and would
		// make the LWW loser's compute a write-off if used here.
		Seed: StableIndexSeed(meta),
	}

	updatedAt := time.Now().UnixNano()
	if err := BulkPopulate(ctx, conn, h.engine, updatedAt, meta.TableName, meta.ColumnName, spec); err != nil {
		return fmt.Errorf("engine hook: bulk populate: %w", err)
	}

	// Status flip to 'ready' is now inside populateMembers' txn (MEDIUM-6 fix).
	// For empty tables BulkPopulate skips populate and status stays 'building'
	// until the delta flush assigns the first vectors.

	// Start the delta flush goroutine for this index.
	h.engine.StartFlush(meta.IndexName, meta.TableName, meta.ColumnName)

	return nil
}

// OnIndexReindex implements IndexReindexHook.
// Called by VectorIndexManager.ReindexIndex after flipping status='reindexing'.
// Runs the full §8.3 shadow-swap pipeline: warm-start k-means, chunked
// populate of the staging table, and atomic swap including the in-memory
// probeState swap.
func (h *EngineHook) OnIndexReindex(ctx context.Context, meta common.VectorIndexMeta) error {
	conn, err := h.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return fmt.Errorf("engine hook reindex: get db %s: %w", meta.Database, err)
	}
	updatedAt := time.Now().UnixNano()
	return Reindex(ctx, conn, h.engine, meta, 0, updatedAt)
}

// RemoveIndex implements EngineProvider.
// Called by VectorIndexManager before the DROP DDL transaction begins so
// concurrent queries fail fast with MARMOT-VEC-013. Returns a restore
// function that re-registers the state if the DDL fails (MEDIUM-7 fix).
func (h *EngineHook) RemoveIndex(indexName string) func() {
	state, ok := h.engine.Lookup(indexName)
	h.engine.StopFlush(indexName)
	h.engine.Unregister(indexName)
	if !ok {
		return func() {}
	}
	return func() {
		h.engine.Register(indexName, state)
	}
}

// metricFromString converts a DDL metric string to the vecindex.Metric enum.
func metricFromString(s string) (vecindex.Metric, error) {
	switch strings.ToLower(s) {
	case "l2", "":
		return vecindex.MetricL2, nil
	case "cosine":
		return vecindex.MetricCosine, nil
	case "dot":
		return vecindex.MetricDot, nil
	default:
		return 0, fmt.Errorf("MARMOT-VEC-016: unknown metric %q (valid: l2, cosine, dot)", s)
	}
}
