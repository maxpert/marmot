package vecindex

// CachedVector is one entry kept in the in-memory vector cache. Vec is a
// decoded, privately owned copy of the little-endian float32 bytes stored in
// the base table — the search path can compute distances against it without a
// SQLite cursor (design task #16).
type CachedVector struct {
	RowID int64
	Vec   []float32
}

// CacheEntry is the input form used by CacheInsertBatch — a (clusterID,
// rowid, vec) triple. Distinct from CachedVector because batch callers carry
// the cluster assignment alongside the vector.
type CacheEntry struct {
	ClusterID int64
	RowID     int64
	Vec       []float32
}

// VectorCache is an immutable per-index snapshot of cluster → entries. It is
// epoch-tagged so readers can detect drift against the active probeState and
// skip when incoherent (epoch is set to the CentroidSet epoch at build time).
//
// Instances are treated as copy-on-write: all mutations produce a new
// *VectorCache and are installed via atomic.Pointer swap on IndexState.
type VectorCache struct {
	epoch     uint64
	byCluster map[int64][]CachedVector
}

// NewVectorCache builds a VectorCache from the given cluster-keyed entry map.
// The caller transfers ownership of entries — the cache does not defensively
// copy the outer map or the inner slices. Individual Vec slices must also be
// owned by the cache (not aliases into transient read buffers).
func NewVectorCache(epoch uint64, entries map[int64][]CachedVector) *VectorCache {
	if entries == nil {
		entries = make(map[int64][]CachedVector)
	}
	return &VectorCache{epoch: epoch, byCluster: entries}
}

// Epoch returns the centroid-set epoch the cache was built for. Searches use
// it to detect a reindex that has not yet replaced the cache.
func (c *VectorCache) Epoch() uint64 {
	if c == nil {
		return 0
	}
	return c.epoch
}

// Cluster returns the entries assigned to clusterID. The returned slice is
// shared with the cache and MUST NOT be mutated. Nil slice for unknown IDs.
func (c *VectorCache) Cluster(clusterID int64) []CachedVector {
	if c == nil {
		return nil
	}
	return c.byCluster[clusterID]
}

// Len returns the total number of cached vectors across all clusters.
func (c *VectorCache) Len() int {
	if c == nil {
		return 0
	}
	n := 0
	for _, entries := range c.byCluster {
		n += len(entries)
	}
	return n
}

// ClusterCount returns the number of distinct cluster IDs present.
func (c *VectorCache) ClusterCount() int {
	if c == nil {
		return 0
	}
	return len(c.byCluster)
}

// cloneBuckets produces a shallow copy of byCluster where only the keyed
// slices listed in touched are cloned; others alias the original. Enables
// COW updates that touch a small set of clusters without full map rebuilds.
func (c *VectorCache) cloneBuckets(touched map[int64]struct{}) map[int64][]CachedVector {
	out := make(map[int64][]CachedVector, len(c.byCluster))
	for cid, entries := range c.byCluster {
		if _, ok := touched[cid]; ok {
			cp := make([]CachedVector, len(entries), len(entries)+4)
			copy(cp, entries)
			out[cid] = cp
		} else {
			out[cid] = entries
		}
	}
	for cid := range touched {
		if _, exists := out[cid]; !exists {
			out[cid] = make([]CachedVector, 0, 4)
		}
	}
	return out
}

// withBatchInsert returns a new VectorCache containing all entries of c plus
// the supplied batch. cluster assignment comes from each CacheEntry. Only
// touched clusters are deep-copied; others alias the prior cache's slices.
func (c *VectorCache) withBatchInsert(entries []CacheEntry) *VectorCache {
	if len(entries) == 0 {
		return c
	}
	touched := make(map[int64]struct{}, 8)
	for _, e := range entries {
		touched[e.ClusterID] = struct{}{}
	}
	buckets := c.cloneBuckets(touched)
	for _, e := range entries {
		buckets[e.ClusterID] = append(buckets[e.ClusterID], CachedVector{
			RowID: e.RowID,
			Vec:   e.Vec,
		})
	}
	return &VectorCache{epoch: c.epoch, byCluster: buckets}
}

// withDelete returns a new VectorCache with the given rowid removed from
// whichever cluster holds it. Returns c unchanged if the rowid is absent.
// O(n) over the full cache — acceptable for row-scale DELETE triggers; bulk
// purges should replace the whole cache via StoreCache instead.
func (c *VectorCache) withDelete(rowid int64) *VectorCache {
	if c == nil {
		return nil
	}
	var target int64 = -1
	var idx int = -1
	for cid, entries := range c.byCluster {
		for i, entry := range entries {
			if entry.RowID == rowid {
				target = cid
				idx = i
				break
			}
		}
		if target >= 0 {
			break
		}
	}
	if target < 0 {
		return c
	}
	touched := map[int64]struct{}{target: {}}
	buckets := c.cloneBuckets(touched)
	entries := buckets[target]
	entries = append(entries[:idx], entries[idx+1:]...)
	buckets[target] = entries
	return &VectorCache{epoch: c.epoch, byCluster: buckets}
}

// Clusters returns the sorted list of cluster IDs present. Primarily for
// tests and diagnostics; hot search paths address clusters by known ID.
func (c *VectorCache) Clusters() []int64 {
	if c == nil {
		return nil
	}
	out := make([]int64, 0, len(c.byCluster))
	for cid := range c.byCluster {
		out = append(out, cid)
	}
	return out
}
