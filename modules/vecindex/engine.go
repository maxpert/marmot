package vecindex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

// Engine opens and manages a collection of IVF vector indexes backed by Pebble.
type Engine struct {
	rootDir string
	cacheMB int
	logger  zerolog.Logger
	indexes sync.Map // map[string]*Index
	specs   sync.Map // map[string]IVFSpec
}

// NewEngine creates an Engine that stores index data under rootDir.
// cacheMB controls the Pebble block cache size; <= 0 uses a 64 MB default.
func NewEngine(rootDir string, cacheMB int, logger zerolog.Logger) (*Engine, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("vecindex: create engine root %s: %w", rootDir, err)
	}
	return &Engine{rootDir: rootDir, cacheMB: cacheMB, logger: logger}, nil
}

// CreateIndex builds a new IVF index from bulk entries and persists it.
func (e *Engine) CreateIndex(ctx context.Context, spec IVFSpec, bulk []BulkEntry) (*Index, error) {
	if err := validateSpec(spec); err != nil {
		return nil, err
	}

	if _, loaded := e.indexes.Load(spec.ID); loaded {
		return nil, fmt.Errorf("vecindex: index %q already exists", spec.ID)
	}

	dir := e.indexDir(spec.ID)
	if _, err := os.Stat(dir); err == nil {
		return nil, fmt.Errorf("vecindex: index directory %s already exists", dir)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("vecindex: create index dir: %w", err)
	}

	st, err := store.New(dir, e.pebbleOptions())
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("vecindex: open store for %s: %w", spec.ID, err)
	}

	if err := persistSpec(st, spec); err != nil {
		_ = st.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("vecindex: persist spec: %w", err)
	}

	idx := newIndex(spec, st, e.logger)

	if len(bulk) > 0 {
		if err := idx.bulkLoad(ctx, bulk); err != nil {
			_ = st.Close()
			_ = os.RemoveAll(dir)
			return nil, fmt.Errorf("vecindex: bulk load: %w", err)
		}
	}

	e.specs.Store(spec.ID, spec)
	e.indexes.Store(spec.ID, idx)
	return idx, nil
}

// OpenIndex re-opens an existing index by ID.
func (e *Engine) OpenIndex(ctx context.Context, id string) (*Index, error) {
	if v, ok := e.indexes.Load(id); ok {
		return v.(*Index), nil
	}

	dir := e.indexDir(id)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("vecindex: index %q not found", id)
	}

	st, err := store.New(dir, e.pebbleOptions())
	if err != nil {
		return nil, fmt.Errorf("vecindex: open store for %s: %w", id, err)
	}

	spec, err := loadSpec(st)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("vecindex: load spec for %s: %w", id, err)
	}

	idx := newIndex(spec, st, e.logger)
	if err := idx.loadCentroids(); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("vecindex: load centroids for %s: %w", id, err)
	}

	actual, loaded := e.indexes.LoadOrStore(id, idx)
	if loaded {
		// Another goroutine opened concurrently — close ours and return the winner.
		_ = st.Close()
		return actual.(*Index), nil
	}
	e.specs.Store(id, spec)
	return idx, nil
}

// SpecOf returns the IVFSpec for an open index, or false if not loaded.
func (e *Engine) SpecOf(id string) (IVFSpec, bool) {
	v, ok := e.specs.Load(id)
	if !ok {
		return IVFSpec{}, false
	}
	return v.(IVFSpec), true
}

// DropIndex permanently removes an index and its backing data.
func (e *Engine) DropIndex(ctx context.Context, id string) error {
	if v, loaded := e.indexes.LoadAndDelete(id); loaded {
		_ = v.(*Index).Close()
	}
	e.specs.Delete(id)
	dir := e.indexDir(id)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("vecindex: drop index %s: %w", id, err)
	}
	return nil
}

// Close releases all resources held by the engine and its open indexes.
func (e *Engine) Close() error {
	var firstErr error
	e.indexes.Range(func(k, v any) bool {
		if err := v.(*Index).Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.indexes.Delete(k)
		return true
	})
	return firstErr
}

func (e *Engine) indexDir(id string) string {
	return filepath.Join(e.rootDir, id)
}

// validateSpec checks that spec fields are valid before creating an index.
// DefaultSpec initialises Nlist=256, but CreateIndex accepts any spec — callers
// that supply Nlist=0 via DefaultSpec must go through lifecycle.Graduate first.
// The one hard rule: Dim must be > 0, Metric must be a known value.
// Nlist=0 is NOT valid when supplied directly in a non-default spec (Nprobe > 0).
func validateSpec(spec IVFSpec) error {
	if spec.Dim <= 0 {
		return errors.New("vecindex: spec.Dim must be > 0")
	}
	if spec.Metric != MetricL2 && spec.Metric != MetricDot && spec.Metric != MetricCosine {
		return fmt.Errorf("vecindex: unknown metric %d", spec.Metric)
	}
	if spec.Metric == MetricDot && spec.MaxNorm <= 0 {
		return errors.New("vecindex: MaxNorm must be > 0 for MetricDot")
	}
	// Reject specs where Nprobe is set but Nlist is 0 — that's an inconsistent state.
	if spec.Nprobe > 0 && spec.Nlist == 0 {
		return errors.New("vecindex: Nlist must be > 0 when Nprobe > 0")
	}
	return nil
}

// persistSpec serialises the IVFSpec via msgpack and writes it under the 0x07 key.
func persistSpec(st *store.Store, spec IVFSpec) error {
	data, err := msgpack.Marshal(spec)
	if err != nil {
		return err
	}
	return st.DB().Set(store.EncodeSpecKey(), data, pebble.NoSync)
}

// loadSpec reads and deserialises the IVFSpec from the 0x07 key.
func loadSpec(st *store.Store) (IVFSpec, error) {
	val, closer, err := st.DB().Get(store.EncodeSpecKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return IVFSpec{}, errors.New("vecindex: spec not found in store")
	}
	if err != nil {
		return IVFSpec{}, err
	}
	defer closer.Close()
	var spec IVFSpec
	if err := msgpack.Unmarshal(val, &spec); err != nil {
		return IVFSpec{}, fmt.Errorf("vecindex: decode spec: %w", err)
	}
	// Back-compat: old persisted specs predate MaxNorm; apply default.
	if spec.Metric == MetricDot && spec.MaxNorm <= 0 {
		spec.MaxNorm = defaultMaxNorm
	}
	return spec, nil
}

// pebbleOptions returns pebble.Options configured with the engine's cache size and a bloom filter.
// cacheMB <= 0 defaults to 64 MB.
func (e *Engine) pebbleOptions() *pebble.Options {
	mb := e.cacheMB
	if mb <= 0 {
		mb = 64
	}
	cache := pebble.NewCache(int64(mb) << 20)
	// pebble.Options.Cache takes its own reference; release ours so the cache is
	// freed when the DB closes (MR-04: prevent one reference leak per open call).
	defer cache.Unref()
	return &pebble.Options{
		Cache: cache,
		Levels: []pebble.LevelOptions{
			{FilterPolicy: bloom.FilterPolicy(BloomBitsPerKey)},
		},
	}
}
