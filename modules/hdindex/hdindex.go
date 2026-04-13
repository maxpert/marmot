package hdindex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	vmsgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/maxpert/marmot/modules/hdindex/pkg/hilbert"
	"github.com/maxpert/marmot/modules/hdindex/pkg/metric"
	"github.com/maxpert/marmot/modules/hdindex/pkg/rdb"
	"github.com/maxpert/marmot/modules/hdindex/pkg/refobj"
	"github.com/maxpert/marmot/modules/hdindex/pkg/vecstore"
)

const (
	metaKeySpec     = "meta/spec"
	metaKeyRefDists = "meta/ref_dists"
	metaKeyRefCount = "meta/ref_count"
	metaKeyRefPfx   = "ref/"
)

// VectorEntry represents a single vector with its external ID for bulk loading.
type VectorEntry struct {
	ExternalID []byte
	Vector     []float32
}

// Engine manages HD-Index lifecycle (create, open, drop, list, close).
type Engine struct {
	rootDir string
	config  EngineConfig
	mu      sync.RWMutex
	indexes map[string]*Index
}

// NewEngine creates an engine that stores indexes under rootDir.
func NewEngine(rootDir string, config EngineConfig) (*Engine, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("hdindex: create root dir: %w", err)
	}
	return &Engine{
		rootDir: rootDir,
		config:  config,
		indexes: make(map[string]*Index),
	}, nil
}

// CreateIndex builds a new HD-Index from scratch using the provided vectors.
func (e *Engine) CreateIndex(ctx context.Context, spec HDIndexSpec, vectors []VectorEntry) (*Index, error) {
	if spec.ID == "" {
		return nil, errors.New("hdindex: spec.ID must not be empty")
	}
	if spec.Dim <= 0 {
		return nil, errors.New("hdindex: spec.Dim must be > 0")
	}
	if len(vectors) == 0 {
		return nil, errors.New("hdindex: vectors must not be empty")
	}
	if spec.RefCount > len(vectors) {
		return nil, fmt.Errorf("hdindex: RefCount %d > number of vectors %d", spec.RefCount, len(vectors))
	}
	if len(vectors) < spec.RefCount*2 {
		return nil, fmt.Errorf("hdindex: need at least %d vectors (2x RefCount), got %d", spec.RefCount*2, len(vectors))
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.indexes[spec.ID]; exists {
		return nil, fmt.Errorf("hdindex: index %q already open", spec.ID)
	}

	idxDir := filepath.Join(e.rootDir, spec.ID)
	if _, err := os.Stat(idxDir); err == nil {
		return nil, fmt.Errorf("hdindex: index directory %q already exists", idxDir)
	}

	db, err := openPebble(idxDir, e.config.PebbleCacheMB)
	if err != nil {
		return nil, fmt.Errorf("hdindex: open pebble: %w", err)
	}

	idx, err := buildIndex(ctx, db, spec, vectors)
	if err != nil {
		db.Close()
		os.RemoveAll(idxDir)
		return nil, err
	}

	e.indexes[spec.ID] = idx
	return idx, nil
}

// OpenIndex opens an existing index by ID from disk.
func (e *Engine) OpenIndex(ctx context.Context, id string) (*Index, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if idx, exists := e.indexes[id]; exists {
		return idx, nil
	}

	idxDir := filepath.Join(e.rootDir, id)
	if _, err := os.Stat(idxDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("hdindex: index %q not found", id)
	}

	db, err := openPebble(idxDir, e.config.PebbleCacheMB)
	if err != nil {
		return nil, fmt.Errorf("hdindex: open pebble: %w", err)
	}

	idx, err := loadIndex(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("hdindex: load index metadata: %w", err)
	}

	e.indexes[id] = idx
	return idx, nil
}

// DropIndex closes and deletes an index.
func (e *Engine) DropIndex(ctx context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if idx, exists := e.indexes[id]; exists {
		if err := idx.db.Close(); err != nil {
			return fmt.Errorf("hdindex: close index: %w", err)
		}
		delete(e.indexes, id)
	}

	idxDir := filepath.Join(e.rootDir, id)
	if err := os.RemoveAll(idxDir); err != nil {
		return fmt.Errorf("hdindex: remove index dir: %w", err)
	}
	return nil
}

// Close closes all open indexes and the engine.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var firstErr error
	for id, idx := range e.indexes {
		if err := idx.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(e.indexes, id)
	}
	return firstErr
}

// buildIndex builds an HD-Index from scratch given a db and initial vectors.
func buildIndex(ctx context.Context, db *pebble.DB, spec HDIndexSpec, vectors []VectorEntry) (*Index, error) {
	// 1. Transform all vectors.
	rawVecs := make([][]float32, len(vectors))
	for i, ve := range vectors {
		rawVecs[i] = ve.Vector
	}

	// For Dot metric, find max norm first and set NormMax.
	if spec.Metric == MetricDot {
		var maxNorm float64
		for _, v := range rawVecs {
			n := float64(metric.Norm(v))
			if n > maxNorm {
				maxNorm = n
			}
		}
		spec.NormMax = maxNorm * 1.5
	}

	transformedVecs := make([][]float32, len(vectors))
	for i, v := range rawVecs {
		tv, err := transformVectorWithSpec(spec, v)
		if err != nil {
			return nil, fmt.Errorf("hdindex: transform vector %d: %w", i, err)
		}
		transformedVecs[i] = tv
	}

	// 2. Compute DomainMin/DomainMax from all transformed dimensions.
	internalDim := spec.InternalDim
	domainMin := make([]float32, internalDim)
	domainMax := make([]float32, internalDim)
	for d := range internalDim {
		domainMin[d] = float32(math.MaxFloat32)
		domainMax[d] = -float32(math.MaxFloat32)
	}
	for _, tv := range transformedVecs {
		for d, val := range tv {
			if val < domainMin[d] {
				domainMin[d] = val
			}
			if val > domainMax[d] {
				domainMax[d] = val
			}
		}
	}
	// Ensure no degenerate ranges (min == max -> expand by epsilon).
	for d := range internalDim {
		if domainMin[d] == domainMax[d] {
			domainMin[d] -= 1e-6
			domainMax[d] += 1e-6
		}
	}
	spec.DomainMin = domainMin
	spec.DomainMax = domainMax

	// 3. Select reference objects using the spec seed for determinism across replicas.
	refs, err := refobj.SelectSSS(transformedVecs, spec.RefCount, 0.3, spec.Seed)
	if err != nil {
		return nil, fmt.Errorf("hdindex: select references: %w", err)
	}

	// 4. Persist spec and reference data.
	batch := db.NewBatch()
	if err := persistMeta(batch, spec, refs); err != nil {
		batch.Close()
		return nil, fmt.Errorf("hdindex: persist metadata: %w", err)
	}

	// 5. Set up stores.
	rdbStore := rdb.Open(db, spec.RefCount)
	vsStore := vecstore.Open(db, spec.Dim)

	// 6. Index all vectors.
	for i, ve := range vectors {
		if err := ctx.Err(); err != nil {
			batch.Close()
			return nil, err
		}

		docID, err := vsStore.NextDocID(batch)
		if err != nil {
			batch.Close()
			return nil, fmt.Errorf("hdindex: alloc doc id for vector %d: %w", i, err)
		}

		tv := transformedVecs[i]
		refDists := refs.ComputeRefDists(tv)
		hilbertKeys := computeHilbertKeysFromSpec(spec, tv)
		concatenatedHilbert := concatHilbertKeys(hilbertKeys)

		if err := vsStore.PutVector(batch, docID, ve.Vector); err != nil {
			batch.Close()
			return nil, fmt.Errorf("hdindex: put vector %d: %w", i, err)
		}
		if err := vsStore.PutIDMapping(batch, ve.ExternalID, docID); err != nil {
			batch.Close()
			return nil, fmt.Errorf("hdindex: put id mapping %d: %w", i, err)
		}
		if err := vsStore.PutReverseHilbert(batch, docID, concatenatedHilbert); err != nil {
			batch.Close()
			return nil, fmt.Errorf("hdindex: put reverse hilbert %d: %w", i, err)
		}
		for p, hk := range hilbertKeys {
			if err := rdbStore.Put(batch, p, hk, docID, refDists); err != nil {
				batch.Close()
				return nil, fmt.Errorf("hdindex: put rdb vector %d partition %d: %w", i, p, err)
			}
		}

		// Flush batch periodically to avoid huge batches.
		// Use NoSync for intermediate flushes; only the final commit syncs.
		if (i+1)%1000 == 0 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				batch.Close()
				return nil, fmt.Errorf("hdindex: commit batch at %d: %w", i, err)
			}
			batch = db.NewBatch()
		}
	}

	// Set final vector count.
	if err := vsStore.SetVectorCount(batch, uint64(len(vectors))); err != nil {
		batch.Close()
		return nil, fmt.Errorf("hdindex: set vector count: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		batch.Close()
		return nil, fmt.Errorf("hdindex: commit final batch: %w", err)
	}

	return &Index{
		spec:     spec,
		refs:     refs,
		rdbStore: rdbStore,
		vecStore: vsStore,
		db:       db,
	}, nil
}

// loadIndex reconstructs an Index from persisted metadata in a Pebble DB.
func loadIndex(db *pebble.DB) (*Index, error) {
	spec, err := loadSpec(db)
	if err != nil {
		return nil, fmt.Errorf("load spec: %w", err)
	}

	refs, err := loadRefs(db, spec.RefCount)
	if err != nil {
		return nil, fmt.Errorf("load refs: %w", err)
	}

	return &Index{
		spec:     spec,
		refs:     refs,
		rdbStore: rdb.Open(db, spec.RefCount),
		vecStore: vecstore.Open(db, spec.Dim),
		db:       db,
	}, nil
}

// persistMeta writes spec and reference objects to the batch.
func persistMeta(batch *pebble.Batch, spec HDIndexSpec, refs *refobj.ReferenceSet) error {
	specBytes, err := vmsgpack.Marshal(&spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	// Batch.Set write options are ignored by Pebble — durability is controlled
	// by the batch.Commit call in buildIndex. Use NoSync for clarity.
	if err := batch.Set([]byte(metaKeySpec), specBytes, pebble.NoSync); err != nil {
		return err
	}

	// Store reference vectors.
	for i, rv := range refs.Vectors {
		key := fmt.Sprintf("%s%d", metaKeyRefPfx, i)
		val := encodeFloat32Slice(rv)
		if err := batch.Set([]byte(key), val, pebble.NoSync); err != nil {
			return err
		}
	}

	// Store pairwise distances.
	if err := batch.Set([]byte(metaKeyRefDists), encodeFloat32Slice(refs.PairDists), pebble.NoSync); err != nil {
		return err
	}

	// Store ref count for loading.
	rcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(rcBytes, uint32(refs.M))
	if err := batch.Set([]byte(metaKeyRefCount), rcBytes, pebble.NoSync); err != nil {
		return err
	}

	return nil
}

// loadSpec reads and decodes the HDIndexSpec from Pebble.
func loadSpec(db *pebble.DB) (HDIndexSpec, error) {
	val, closer, err := db.Get([]byte(metaKeySpec))
	if err != nil {
		return HDIndexSpec{}, fmt.Errorf("get spec: %w", err)
	}
	defer closer.Close()

	var spec HDIndexSpec
	if err := vmsgpack.Unmarshal(val, &spec); err != nil {
		return HDIndexSpec{}, fmt.Errorf("unmarshal spec: %w", err)
	}
	return spec, nil
}

// loadRefs reads and reconstructs the ReferenceSet from Pebble.
func loadRefs(db *pebble.DB, m int) (*refobj.ReferenceSet, error) {
	// Load ref count from DB to be safe.
	rcVal, rcCloser, err := db.Get([]byte(metaKeyRefCount))
	if err != nil {
		return nil, fmt.Errorf("get ref count: %w", err)
	}
	storedM := int(binary.BigEndian.Uint32(rcVal))
	rcCloser.Close()
	if storedM != m {
		return nil, fmt.Errorf("ref count mismatch: spec=%d stored=%d", m, storedM)
	}

	refs := &refobj.ReferenceSet{
		Vectors: make([][]float32, m),
		M:       m,
	}

	for i := range m {
		key := fmt.Sprintf("%s%d", metaKeyRefPfx, i)
		val, closer, err := db.Get([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("get ref %d: %w", i, err)
		}
		refs.Vectors[i] = decodeFloat32Slice(val)
		closer.Close()
	}

	pairVal, pairCloser, err := db.Get([]byte(metaKeyRefDists))
	if err != nil {
		return nil, fmt.Errorf("get pair dists: %w", err)
	}
	refs.PairDists = decodeFloat32Slice(pairVal)
	pairCloser.Close()

	return refs, nil
}

// openPebble opens a Pebble DB at the given path with sensible options.
func openPebble(dir string, cacheMB int) (*pebble.DB, error) {
	opts := &pebble.Options{
		MaxOpenFiles: 256,
	}
	if cacheMB > 0 {
		cache := pebble.NewCache(int64(cacheMB) << 20)
		defer cache.Unref()
		opts.Cache = cache
	}
	return pebble.Open(dir, opts)
}

// transformVectorWithSpec applies the metric transformation defined by spec.
func transformVectorWithSpec(spec HDIndexSpec, v []float32) ([]float32, error) {
	switch spec.Metric {
	case MetricCosine:
		return metric.NormalizeCopy(v), nil
	case MetricDot:
		return metric.AugmentForMIPS(v, spec.NormMax)
	default:
		out := make([]float32, len(v))
		copy(out, v)
		return out, nil
	}
}

// computeHilbertKeysFromSpec computes tau Hilbert keys from a transformed vector.
func computeHilbertKeysFromSpec(spec HDIndexSpec, transformed []float32) [][]byte {
	keys := make([][]byte, spec.Tau)
	etaDims := spec.Eta
	for i := range spec.Tau {
		start := i * etaDims
		end := start + etaDims
		partSlice := transformed[start:end]
		domainMin := spec.DomainMin[start:end]
		domainMax := spec.DomainMax[start:end]
		coords := metric.QuantizeDims(partSlice, domainMin, domainMax, spec.Omega)
		keys[i] = hilbert.Encode(coords, spec.Omega)
	}
	return keys
}

// encodeFloat32Slice encodes []float32 as little-endian bytes.
func encodeFloat32Slice(v []float32) []byte {
	out := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(f))
	}
	return out
}

// decodeFloat32Slice decodes little-endian bytes into []float32.
func decodeFloat32Slice(b []byte) []float32 {
	n := len(b) / 4
	out := make([]float32, n)
	for i := range n {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}
