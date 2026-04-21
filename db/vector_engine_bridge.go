package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/rs/zerolog/log"
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

	bootstrapMu       sync.Mutex
	bootstrapSeq      uint64
	bootstrapWatchers map[string]bootstrapWatcher
	localChangeMu     sync.Mutex
}

type bootstrapWatcher struct {
	cancel context.CancelFunc
	seq    uint64
}

// NewEngineHook creates an EngineHook. Call mgr.SetLifecycleHook(h) and
// mgr.SetEngineProvider(h) to wire it into VectorIndexManager.
func NewEngineHook(engine *vecindex.Engine, dbMgr *DatabaseManager) *EngineHook {
	return &EngineHook{
		engine:            engine,
		dbMgr:             dbMgr,
		bootstrapWatchers: make(map[string]bootstrapWatcher),
	}
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
	if state, ok := h.engine.Lookup(meta.IndexName); ok {
		if dbPath, pathErr := h.dbMgr.GetDatabasePath(meta.Database); pathErr == nil {
			if err := openAndStoreOverlay(dbPath, meta.IndexName, state, state.ProbeVersion()); err != nil {
				return fmt.Errorf("engine hook: open overlay: %w", err)
			}
		}
		if state.ProbeVersion() == 0 {
			h.startBootstrapWatcher(meta, spec)
		} else if dbPath, pathErr := h.dbMgr.GetDatabasePath(meta.Database); pathErr == nil {
			if err := buildAndStoreSegmentGeneration(ctx, conn, dbPath, state, meta, spec); err != nil {
				return fmt.Errorf("engine hook: build segment generation: %w", err)
			}
		}
	}

	return nil
}

// OnIndexReindex implements IndexReindexHook.
// Called by VectorIndexManager.ReindexIndex after flipping status='reindexing'.
// Runs the full §8.3 shadow-swap pipeline: warm-start k-means, chunked
// populate of the staging table, and atomic swap including the in-memory
// probeState swap.
func (h *EngineHook) OnIndexReindex(ctx context.Context, meta common.VectorIndexMeta) error {
	h.stopBootstrapWatcher(meta.IndexName)
	h.localChangeMu.Lock()
	defer h.localChangeMu.Unlock()

	conn, err := h.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return fmt.Errorf("engine hook reindex: get db %s: %w", meta.Database, err)
	}
	updatedAt := time.Now().UnixNano()
	oldState, ok := h.engine.Lookup(meta.IndexName)
	if !ok {
		return fmt.Errorf("engine hook reindex: index %q not registered", meta.IndexName)
	}
	meta, newState, err := Reindex(ctx, conn, h.engine, meta, 0, updatedAt)
	if err != nil {
		return err
	}
	publishOK := false
	defer func() {
		if publishOK {
			return
		}
		newState.ClearOverlay()
		newState.ClearSegmentStore()
		if oldState.ProbeVersion() == 0 {
			h.startBootstrapWatcher(meta, oldState.Spec())
		}
	}()

	dbPath, pathErr := h.dbMgr.GetDatabasePath(meta.Database)
	if pathErr != nil {
		return fmt.Errorf("engine hook reindex: get db path: %w", pathErr)
	}
	if newState.ProbeVersion() != 0 {
		if err := buildAndStoreSegmentGeneration(ctx, conn, dbPath, newState, meta, newState.Spec()); err != nil {
			return fmt.Errorf("engine hook reindex: build segment generation: %w", err)
		}
	}
	if _, err := conn.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET nlist=?, nprobe=?, status='ready' WHERE index_name=?`,
		meta.Nlist, meta.Nprobe, meta.IndexName,
	); err != nil {
		return fmt.Errorf("engine hook reindex: publish metadata: %w", err)
	}
	h.engine.Register(meta.IndexName, newState)
	oldState.ClearOverlay()
	oldState.ClearSegmentStore()
	if newState.ProbeVersion() == 0 {
		h.startBootstrapWatcher(meta, newState.Spec())
	}
	publishOK = true
	return nil
}

// RemoveIndex implements EngineProvider.
// Called by VectorIndexManager before the DROP DDL transaction begins so
// concurrent queries fail fast with MARMOT-VEC-013. Returns a restore
// function that re-registers the state if the DDL fails (MEDIUM-7 fix).
func (h *EngineHook) RemoveIndex(indexName string) func() {
	h.stopBootstrapWatcher(indexName)
	state, ok := h.engine.Lookup(indexName)
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

func (h *EngineHook) startBootstrapWatcher(meta common.VectorIndexMeta, spec vecindex.IVFSpec) {
	if h == nil || h.engine == nil || h.dbMgr == nil {
		return
	}

	h.bootstrapMu.Lock()
	if _, ok := h.bootstrapWatchers[meta.IndexName]; ok {
		h.bootstrapMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.bootstrapSeq++
	watch := bootstrapWatcher{cancel: cancel, seq: h.bootstrapSeq}
	h.bootstrapWatchers[meta.IndexName] = watch
	h.bootstrapMu.Unlock()

	go h.bootstrapIndexWhenRowsAppear(ctx, meta, spec, watch.seq)
}

func (h *EngineHook) stopBootstrapWatcher(indexName string) {
	if h == nil {
		return
	}
	h.bootstrapMu.Lock()
	watch, ok := h.bootstrapWatchers[indexName]
	delete(h.bootstrapWatchers, indexName)
	h.bootstrapMu.Unlock()
	if ok && watch.cancel != nil {
		watch.cancel()
	}
}

func (h *EngineHook) clearBootstrapWatcher(indexName string, seq uint64) {
	if h == nil {
		return
	}
	h.bootstrapMu.Lock()
	if current, ok := h.bootstrapWatchers[indexName]; ok && current.seq == seq {
		delete(h.bootstrapWatchers, indexName)
	}
	h.bootstrapMu.Unlock()
}

func (h *EngineHook) bootstrapIndexWhenRowsAppear(
	ctx context.Context,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	seq uint64,
) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	defer h.clearBootstrapWatcher(meta.IndexName, seq)

	for {
		if ctx.Err() != nil {
			return
		}
		if h.bootstrapOnce(ctx, meta, spec) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *EngineHook) bootstrapOnce(ctx context.Context, meta common.VectorIndexMeta, spec vecindex.IVFSpec) bool {
	state, ok := h.engine.Lookup(meta.IndexName)
	if !ok {
		return true
	}
	conn, err := h.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: get db failed")
		return false
	}
	if state.ProbeVersion() != 0 {
		if state.LoadSegmentStore() != nil {
			return true
		}
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: get db path failed")
			return true
		}
		if err := buildAndStoreSegmentGeneration(ctx, conn, dbPath, state, meta, spec); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: build segment generation failed")
			return true
		}
		log.Info().Str("index", meta.IndexName).Msg("engine hook bootstrap: segment generation published")
		return true
	}

	currentN, err := countIndexableRows(ctx, conn, meta.TableName, meta.ColumnName, spec)
	if err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: row-count probe failed")
		return false
	}
	if currentN == 0 {
		return false
	}
	if meta.AutoTuneNlist {
		bootstrapFloor := int64(meta.TargetPartitionSize * max(meta.Nlist, 64))
		if bootstrapFloor < 4096 {
			bootstrapFloor = 4096
		}
		if currentN < bootstrapFloor {
			return false
		}
	}
	log.Info().Int64("rows", currentN).Str("index", meta.IndexName).Msg("engine hook bootstrap: bootstrap threshold reached")

	if retunedMeta, retunedSpec, changed, err := retuneBootstrapMeta(ctx, conn, meta, spec); err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: retune failed")
		return false
	} else {
		meta = retunedMeta
		spec = retunedSpec
		if changed {
			log.Info().
				Str("index", meta.IndexName).
				Int("nlist", meta.Nlist).
				Int("nprobe", meta.Nprobe).
				Msg("engine hook bootstrap: retuned auto parameters before initial centroid build")
		}
	}

	oldState, _ := h.engine.Lookup(meta.IndexName)
	updatedAt := time.Now().UnixNano()
	if err := BulkPopulate(ctx, conn, h.engine, updatedAt, meta.TableName, meta.ColumnName, spec); err != nil {
		if oldState != nil {
			h.engine.Register(meta.IndexName, oldState)
		}
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: bulk populate failed")
		return false
	}

	newState, ok := h.engine.Lookup(meta.IndexName)
	if !ok || newState.ProbeVersion() == 0 {
		log.Warn().Str("index", meta.IndexName).Msg("engine hook bootstrap: populate finished without centroids")
		return false
	}
	dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
	if err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: get db path failed after populate")
		return true
	}
	if err := openAndStoreOverlay(dbPath, meta.IndexName, newState, newState.ProbeVersion()); err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: open overlay failed")
		return true
	}
	if err := buildAndStoreSegmentGeneration(ctx, conn, dbPath, newState, meta, spec); err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("engine hook bootstrap: build segment generation failed after populate")
		return true
	}
	log.Info().Str("index", meta.IndexName).Msg("engine hook bootstrap: automatic bootstrap complete")
	return true
}

func retuneBootstrapMeta(
	ctx context.Context,
	conn *sql.DB,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
) (common.VectorIndexMeta, vecindex.IVFSpec, bool, error) {
	if !meta.AutoTuneNlist && !meta.AutoTuneNprobe {
		return meta, spec, false, nil
	}
	currentN, err := countIndexableRows(ctx, conn, meta.TableName, meta.ColumnName, spec)
	if err != nil {
		return meta, spec, false, err
	}
	if currentN <= 0 {
		return meta, spec, false, nil
	}
	oldNlist, oldNprobe := meta.Nlist, meta.Nprobe
	meta, spec = retuneReindexMeta(meta, spec, currentN)
	if meta.Nlist == oldNlist && meta.Nprobe == oldNprobe {
		return meta, spec, false, nil
	}
	if _, err := conn.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET nlist=?, nprobe=? WHERE index_name=?`,
		meta.Nlist, meta.Nprobe, meta.IndexName,
	); err != nil {
		return meta, spec, false, err
	}
	return meta, spec, true, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
