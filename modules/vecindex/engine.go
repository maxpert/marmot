package vecindex

import (
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
}

// NewEngine creates a new Engine with no registered indexes.
func NewEngine() *Engine {
	return &Engine{}
}

// Register stores the IndexState for indexName in the engine, replacing any
// existing state for the same name.
func (e *Engine) Register(indexName string, state *IndexState) {
	e.indexes.Store(indexName, state)
}

// Unregister removes the IndexState for indexName from the engine.
// It is a no-op if indexName is not registered.
func (e *Engine) Unregister(indexName string) {
	if val, ok := e.indexes.Load(indexName); ok {
		state := val.(*IndexState)
		state.ClearOverlay()
		state.ClearSegmentStore()
	}
	e.indexes.Delete(indexName)
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
