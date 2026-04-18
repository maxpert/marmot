package vecindex

import (
	"context"
	"fmt"

	"github.com/maypok86/otter/v2"
)

// PartitionLoader is the data-source contract a PartitionCache needs to
// populate missing partitions. Implementations are provided by the db package
// (which issues `SELECT rowid, vec FROM members WHERE cluster_id IN (?...)`).
//
// BulkLoad MUST return an entry for every requested clusterID even when the
// partition is empty — otherwise otter treats the key as "not loaded" and
// issues a fresh load on every query, defeating caching for empty clusters.
type PartitionLoader interface {
	BulkLoad(ctx context.Context, clusterIDs []int64) (map[int64]CachedPartition, error)
}

// PartitionCache is a byte-budgeted W-TinyLFU cache of cluster_id →
// CachedPartition. Each partition is loaded on first probe and evicted under
// memory pressure; readers always see consistent snapshots because eviction
// only drops the map entry — the backing arrays stay live as long as any
// goroutine still holds the CachedPartition value.
//
// One PartitionCache exists per vector index. The epoch field pins it to a
// specific CentroidSet; a reindex swaps the entire PartitionCache atomically
// and discards the old one.
type PartitionCache struct {
	cache  *otter.Cache[int64, CachedPartition]
	loader PartitionLoader
	epoch  uint64
	dim    int
}

// PartitionCacheOptions controls cache sizing and epoch identity.
type PartitionCacheOptions struct {
	// MaxBytes is the byte budget for cached partitions (excludes DeltaBuffer).
	// Must be > 0. Typical: 1 GiB.
	MaxBytes uint64
	// Dim is the vector dimension — used by the byte-weight calculation.
	Dim int
	// Epoch identifies the CentroidSet this cache is pinned to. Readers check
	// it against plan.ProbeEpoch to detect a stale cache after reindex.
	Epoch uint64
	// Loader is the SQL fallback for cache misses. Required.
	Loader PartitionLoader
}

// NewPartitionCache builds a PartitionCache from opts. Returns an error if
// Loader is nil, MaxBytes is 0, or Dim is 0.
func NewPartitionCache(opts PartitionCacheOptions) (*PartitionCache, error) {
	if opts.Loader == nil {
		return nil, fmt.Errorf("partition cache: loader is required")
	}
	if opts.MaxBytes == 0 {
		return nil, fmt.Errorf("partition cache: MaxBytes must be > 0")
	}
	if opts.Dim <= 0 {
		return nil, fmt.Errorf("partition cache: Dim must be > 0")
	}

	weigher := func(_ int64, part CachedPartition) uint32 {
		return cachedPartitionBytes(part)
	}

	c, err := otter.New(&otter.Options[int64, CachedPartition]{
		MaximumWeight: opts.MaxBytes,
		Weigher:       weigher,
	})
	if err != nil {
		return nil, fmt.Errorf("partition cache: otter new: %w", err)
	}

	return &PartitionCache{
		cache:  c,
		loader: opts.Loader,
		epoch:  opts.Epoch,
		dim:    opts.Dim,
	}, nil
}

// Epoch returns the CentroidSet epoch this cache is pinned to.
func (p *PartitionCache) Epoch() uint64 { return p.epoch }

// Dim returns the vector dimension this cache was built for.
func (p *PartitionCache) Dim() int { return p.dim }

// BulkGet returns the CachedVectors for every requested clusterID. Entries
// are loaded from the backing store on miss in a single BulkLoad call.
// Duplicate keys in clusterIDs are de-duplicated by otter.
//
// The returned partitions are shared with the cache and MUST NOT be mutated
// by callers. A missing clusterID (loader returned nothing) maps to an empty
// CachedPartition.
func (p *PartitionCache) BulkGet(ctx context.Context, clusterIDs []int64) (map[int64]CachedPartition, error) {
	if len(clusterIDs) == 0 {
		return nil, nil
	}
	return p.cache.BulkGet(ctx, clusterIDs, bulkLoaderAdapter{inner: p.loader})
}

// Invalidate drops a single partition from the cache. Used by delete hooks
// when a row is removed — the next probe will reload from SQL and miss the
// deleted row. Safe to call for keys that are not resident.
func (p *PartitionCache) Invalidate(clusterID int64) {
	p.cache.Invalidate(clusterID)
}

// InvalidateAll drops every cached partition. Used at reindex to rebuild the
// cache against the new CentroidSet (though typically the caller discards
// the whole PartitionCache and constructs a new one under the new epoch).
func (p *PartitionCache) InvalidateAll() {
	p.cache.InvalidateAll()
}

// EstimatedSize returns the number of partitions currently resident.
func (p *PartitionCache) EstimatedSize() int {
	return p.cache.EstimatedSize()
}

// FindAndInvalidate drops the partition that contains rowID (if resident).
// Returns true iff an invalidation happened. Scans only currently-resident
// partitions — non-resident partitions are on disk and reflect the
// post-delete state anyway, so there is nothing to purge.
//
// Intended for CDC delete hooks: the caller holds only the rowID; we scan
// loaded partitions to locate the owning cluster, then invalidate so the
// next probe reloads from SQL without the deleted row.
func (p *PartitionCache) FindAndInvalidate(rowID int64) bool {
	for cid := range p.cache.Keys() {
		part, ok := p.cache.GetIfPresent(cid)
		if !ok {
			continue
		}
		for _, rid := range part.RowIDs {
			if rid == rowID {
				p.cache.Invalidate(cid)
				return true
			}
		}
	}
	return false
}

// bulkLoaderAdapter adapts our PartitionLoader interface to otter's BulkLoader.
type bulkLoaderAdapter struct {
	inner PartitionLoader
}

func (a bulkLoaderAdapter) BulkLoad(ctx context.Context, keys []int64) (map[int64]CachedPartition, error) {
	return a.inner.BulkLoad(ctx, keys)
}

func (a bulkLoaderAdapter) BulkReload(ctx context.Context, keys []int64, _ []CachedPartition) (map[int64]CachedPartition, error) {
	return a.inner.BulkLoad(ctx, keys)
}

// partitionOverheadBytes covers the outer CachedPartition struct, the otter
// internal node, map entry, and alignment padding. Deliberately over-estimates
// so the byte budget under-fills rather than over-fills.
const partitionOverheadBytes uint32 = 192

func cachedPartitionBytes(part CachedPartition) uint32 {
	return uint32(len(part.RowIDs))*8 + uint32(len(part.Vecs))*4 + partitionOverheadBytes
}
