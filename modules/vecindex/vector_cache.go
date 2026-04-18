package vecindex

import "context"

// CachedVector is one entry kept in the in-memory vector cache. Vec is a
// decoded, privately owned copy of the little-endian float32 bytes stored in
// the base table — the search path can compute distances against it without a
// SQLite cursor (design task #16).
type CachedVector struct {
	RowID int64
	Vec   []float32
}

// CachedPartition is the contiguous cache representation for one cluster.
// RowIDs[i] corresponds to the vector stored in Vecs[i*dim : (i+1)*dim].
// Vecs is flattened so each partition is a single backing array rather than
// one heap object per vector.
type CachedPartition struct {
	RowIDs []int64
	Vecs   []float32
}

// Len returns the number of vectors in the partition.
func (p CachedPartition) Len() int { return len(p.RowIDs) }

// Empty reports whether the partition contains no vectors.
func (p CachedPartition) Empty() bool { return len(p.RowIDs) == 0 }

// Vector returns the i-th vector slice. The returned slice aliases Vecs.
func (p CachedPartition) Vector(i, dim int) []float32 {
	start := i * dim
	return p.Vecs[start : start+dim]
}

// CacheEntry is the input form used by CacheInsertBatch — a (clusterID,
// rowid, vec) triple. Distinct from CachedVector because batch callers carry
// the cluster assignment alongside the vector.
type CacheEntry struct {
	ClusterID int64
	RowID     int64
	Vec       []float32
}

// VectorCache is the index-level cache snapshot consisting of an LRU-managed
// PartitionCache (per-cluster on-demand loading) plus an always-resident
// DeltaBuffer for cluster_id=0 rows. Epoch is pinned to a CentroidSet so
// readers detect reindex drift and fall back to the SQL candidate path.
//
// Instances are atomically swapped at reindex via IndexState.StoreCache.
type VectorCache struct {
	partitions *PartitionCache
	delta      *DeltaBuffer
	epoch      uint64
}

// NewVectorCache wraps a PartitionCache + DeltaBuffer under a single epoch.
// partitions and delta must be non-nil.
func NewVectorCache(epoch uint64, partitions *PartitionCache, delta *DeltaBuffer) *VectorCache {
	return &VectorCache{epoch: epoch, partitions: partitions, delta: delta}
}

// Epoch returns the CentroidSet epoch this cache is pinned to.
func (c *VectorCache) Epoch() uint64 {
	if c == nil {
		return 0
	}
	return c.epoch
}

// Partitions returns the per-cluster LRU cache. Nil on a nil cache.
func (c *VectorCache) Partitions() *PartitionCache {
	if c == nil {
		return nil
	}
	return c.partitions
}

// Delta returns the always-resident cluster_id=0 buffer. Nil on a nil cache.
func (c *VectorCache) Delta() *DeltaBuffer {
	if c == nil {
		return nil
	}
	return c.delta
}

// BulkGetPartitions is the hot-path accessor used by query rank. Loads any
// missing clusterIDs through the PartitionCache's BulkLoader and returns a
// shared map of cluster → vectors. Returned slices MUST NOT be mutated.
func (c *VectorCache) BulkGetPartitions(ctx context.Context, clusterIDs []int64) (map[int64]CachedPartition, error) {
	if c == nil || c.partitions == nil {
		return nil, nil
	}
	return c.partitions.BulkGet(ctx, clusterIDs)
}

// DeltaSnapshot returns a stable read-only view of the cluster_id=0 buffer.
// Empty slice (never nil) on a nil cache or buffer.
func (c *VectorCache) DeltaSnapshot() []CachedVector {
	if c == nil || c.delta == nil {
		return nil
	}
	return c.delta.Snapshot()
}
