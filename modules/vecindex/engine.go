package vecindex

import (
	"context"
	"fmt"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
)

// Engine manages in-memory state for all active vector indexes.
// It implements VectorUDFProvider so it can be installed as the global
// provider via db.SetVectorUDFProvider(engine).
//
// All methods are safe for concurrent use.
type Engine struct {
	indexes sync.Map // map[string]*IndexState

	flushDB  DeltaFlushDB
	flushCfg DeltaFlushConfig

	flusherMu sync.Mutex
	flushers  map[string]*flushHandle
}

type flushHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// NewEngine creates a new Engine with no registered indexes.
func NewEngine() *Engine {
	return &Engine{
		flushCfg: DefaultDeltaFlushConfig(),
		flushers: make(map[string]*flushHandle),
	}
}

// SetFlushDB installs the DeltaFlushDB implementation used by delta flush
// workers. Must be called before StartFlush.
func (e *Engine) SetFlushDB(db DeltaFlushDB) {
	e.flushDB = db
}

// SetFlushConfig overrides the default delta flush configuration.
func (e *Engine) SetFlushConfig(cfg DeltaFlushConfig) {
	e.flushCfg = cfg
}

// StartFlush launches a delta flush goroutine for the named index.
// The index must already be registered. No-op if flushDB is nil or
// the index is not found.
func (e *Engine) StartFlush(indexName, database, tableName, columnName string) {
	if e.flushDB == nil {
		return
	}
	state, ok := e.Lookup(indexName)
	if !ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	e.flusherMu.Lock()
	old := e.flushers[indexName]
	e.flushers[indexName] = &flushHandle{cancel: cancel, done: done}
	e.flusherMu.Unlock()
	if old != nil {
		old.cancel()
		<-old.done
	}

	go func() {
		defer close(done)
		deltaFlushLoop(ctx, e.flushCfg, state, e.flushDB, database, indexName, tableName, columnName)
	}()
}

// StopFlush cancels the delta flush goroutine for the named index.
// No-op if no flusher is running.
func (e *Engine) StopFlush(indexName string) {
	e.flusherMu.Lock()
	if h, ok := e.flushers[indexName]; ok {
		h.cancel()
		delete(e.flushers, indexName)
	}
	e.flusherMu.Unlock()
}

// StopFlushAndWait cancels the delta flush goroutine for the named index and
// blocks until the worker has fully exited.
func (e *Engine) StopFlushAndWait(indexName string) {
	e.flusherMu.Lock()
	h := e.flushers[indexName]
	if h != nil {
		delete(e.flushers, indexName)
	}
	e.flusherMu.Unlock()
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
}

// Register stores the IndexState for indexName in the engine, replacing any
// existing state for the same name.
func (e *Engine) Register(indexName string, state *IndexState) {
	e.indexes.Store(indexName, state)
}

// Unregister removes the IndexState for indexName from the engine.
// It is a no-op if indexName is not registered. Clears the in-memory vector
// cache first so pending searches observe a nil cache before losing the state.
func (e *Engine) Unregister(indexName string) {
	if val, ok := e.indexes.Load(indexName); ok {
		state := val.(*IndexState)
		state.CacheClear()
		state.ClearPackedStore()
	}
	e.indexes.Delete(indexName)
}

// LookupCache returns the active VectorCache for indexName, or nil when no
// cache is installed. Used by the coordinator's Go-side ranking path to
// bypass SQLite when cache coverage is available.
func (e *Engine) LookupCache(indexName string) *VectorCache {
	state, ok := e.Lookup(indexName)
	if !ok {
		return nil
	}
	return state.LoadCache()
}

// Lookup returns the IndexState for indexName and whether it was found.
func (e *Engine) Lookup(indexName string) (*IndexState, bool) {
	val, ok := e.indexes.Load(indexName)
	if !ok {
		return nil, false
	}
	return val.(*IndexState), true
}

// RegisterWithCentroidSet is a convenience wrapper that creates a new
// IndexState from spec+cs and registers it. Returns the created state.
func (e *Engine) RegisterWithCentroidSet(indexName string, spec IVFSpec, cs *kmeans.CentroidSet) *IndexState {
	state := NewIndexState(spec, cs)
	e.Register(indexName, state)
	return state
}

// AssignNearest implements VectorUDFProvider.
// Returns the 1-based cluster ID for the nearest centroid (0 is reserved for
// delta rows). Returns MARMOT-VEC-013 if the index is unknown.
func (e *Engine) AssignNearest(indexName string, vec []byte) (int64, error) {
	state, ok := e.Lookup(indexName)
	if !ok {
		return 0, fmt.Errorf("MARMOT-VEC-013: vector index %q not registered in engine", indexName)
	}
	return state.AssignNearest(vec)
}

// NotifyCentroidChange implements VectorUDFProvider as a no-op. The
// centroid-change trigger fires into this method from the UDF layer; no
// replica listener is currently wired, so the call returns immediately.
func (e *Engine) NotifyCentroidChange(_ string, _ int64) error {
	return nil
}

// TopNprobeClusters implements VectorUDFProvider. Thin wrapper over
// TopNprobeClustersWithEpoch that discards the epoch.
func (e *Engine) TopNprobeClusters(indexName string, vec []byte, n int) ([]int64, error) {
	ids, _, err := e.TopNprobeClustersWithEpoch(indexName, vec, n)
	return ids, err
}

// TopNprobeClustersWithEpoch is TopNprobeClusters + the probe-state epoch the
// cluster IDs were computed against. Used by the coordinator's cache path to
// detect when cluster IDs produced under an older probe would be indexed into
// a post-reindex cache at a different epoch.
func (e *Engine) TopNprobeClustersWithEpoch(indexName string, vec []byte, n int) ([]int64, uint64, error) {
	state, ok := e.Lookup(indexName)
	if !ok {
		return nil, 0, fmt.Errorf("MARMOT-VEC-013: vector index %q not registered in engine", indexName)
	}
	return state.TopNprobeClustersWithEpoch(vec, n)
}
