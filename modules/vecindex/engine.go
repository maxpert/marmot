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
	indexes  sync.Map // map[string]*IndexState
	listener *ReplicaListener

	flushDB  DeltaFlushDB
	flushCfg DeltaFlushConfig

	flusherMu sync.Mutex
	flushers  map[string]context.CancelFunc
}

// NewEngine creates a new Engine with no registered indexes.
func NewEngine() *Engine {
	return &Engine{
		flushCfg: DefaultDeltaFlushConfig(),
		flushers: make(map[string]context.CancelFunc),
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
func (e *Engine) StartFlush(indexName, tableName, columnName string) {
	if e.flushDB == nil {
		return
	}
	state, ok := e.Lookup(indexName)
	if !ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.flusherMu.Lock()
	if old, exists := e.flushers[indexName]; exists {
		old()
	}
	e.flushers[indexName] = cancel
	e.flusherMu.Unlock()

	go deltaFlushLoop(ctx, e.flushCfg, state, e.flushDB, indexName, tableName, columnName)
}

// StopFlush cancels the delta flush goroutine for the named index.
// No-op if no flusher is running.
func (e *Engine) StopFlush(indexName string) {
	e.flusherMu.Lock()
	if cancel, ok := e.flushers[indexName]; ok {
		cancel()
		delete(e.flushers, indexName)
	}
	e.flusherMu.Unlock()
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
		val.(*IndexState).CacheClear()
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

// SetReplicaListener installs the listener that receives centroid-change
// notifications. Must be called before any triggers fire.
func (e *Engine) SetReplicaListener(l *ReplicaListener) {
	e.listener = l
}

// NotifyCentroidChange implements VectorUDFProvider.
// Enqueues a non-blocking rebuild request on the replica listener channel.
// Returns immediately so the writer transaction is not held (design §8.8).
func (e *Engine) NotifyCentroidChange(indexName string, version int64) error {
	if l := e.listener; l != nil {
		l.Notify(indexName, version)
	}
	return nil
}

// TopNprobeClusters implements VectorUDFProvider.
// Returns the nearest n 1-based cluster IDs for the query vector against the
// named index's probeState. Returns MARMOT-VEC-013 if the index is unknown.
func (e *Engine) TopNprobeClusters(indexName string, vec []byte, n int) ([]int64, error) {
	state, ok := e.Lookup(indexName)
	if !ok {
		return nil, fmt.Errorf("MARMOT-VEC-013: vector index %q not registered in engine", indexName)
	}
	return state.TopNprobeClusters(vec, n)
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
