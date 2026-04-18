package vecindex

import "sync/atomic"

// CachedVector is one resident queryable vector kept in memory.
type CachedVector struct {
	RowID int64
	Vec   []float32
}

// DeltaBuffer is the always-resident buffer for cluster_id=0, which holds
// rows that have been inserted since the last reindex but not yet assigned
// to a centroid-specific partition. Every query MUST scan the delta buffer
// in addition to its probed partitions, so it cannot participate in the
// LRU cache — we pin it in a dedicated atomic.Pointer instead.
//
// Updates are copy-on-write: a new slice is allocated on append/delete and
// atomically swapped. Readers hold a stable slice snapshot, so concurrent
// updates never corrupt an in-flight rank scan.
type DeltaBuffer struct {
	vecs atomic.Pointer[[]CachedVector]
}

// NewDeltaBuffer returns an empty buffer.
func NewDeltaBuffer() *DeltaBuffer {
	b := &DeltaBuffer{}
	empty := make([]CachedVector, 0)
	b.vecs.Store(&empty)
	return b
}

// Snapshot returns the current slice. The returned slice MUST NOT be
// mutated — it is shared with the cache and with any other concurrent
// readers. Always returns a non-nil slice; empty buffer → zero-length.
func (b *DeltaBuffer) Snapshot() []CachedVector {
	if b == nil {
		return nil
	}
	p := b.vecs.Load()
	if p == nil {
		return nil
	}
	return *p
}

// Len reports the current entry count.
func (b *DeltaBuffer) Len() int {
	return len(b.Snapshot())
}

// Append appends a single entry via CAS. Retries on contention; the loop
// terminates because each retry reads a fresh base and allocates a fresh
// copy, so progress is guaranteed.
func (b *DeltaBuffer) Append(v CachedVector) {
	for {
		oldP := b.vecs.Load()
		old := *oldP
		next := make([]CachedVector, len(old)+1)
		copy(next, old)
		next[len(old)] = v
		if b.vecs.CompareAndSwap(oldP, &next) {
			return
		}
	}
}

// AppendBatch appends many entries atomically under CAS. Used by bulk
// insert paths to avoid N individual swaps.
func (b *DeltaBuffer) AppendBatch(vs []CachedVector) {
	if len(vs) == 0 {
		return
	}
	for {
		oldP := b.vecs.Load()
		old := *oldP
		next := make([]CachedVector, len(old)+len(vs))
		copy(next, old)
		copy(next[len(old):], vs)
		if b.vecs.CompareAndSwap(oldP, &next) {
			return
		}
	}
}

// Remove drops the first entry with the given rowid via CAS. Returns true
// if a matching entry was removed. O(n) scan; delta is bounded so this is
// acceptable.
func (b *DeltaBuffer) Remove(rowID int64) bool {
	for {
		oldP := b.vecs.Load()
		old := *oldP
		idx := -1
		for i, v := range old {
			if v.RowID == rowID {
				idx = i
				break
			}
		}
		if idx < 0 {
			return false
		}
		next := make([]CachedVector, 0, len(old)-1)
		next = append(next, old[:idx]...)
		next = append(next, old[idx+1:]...)
		if b.vecs.CompareAndSwap(oldP, &next) {
			return true
		}
	}
}

// Reset atomically empties the buffer. Called after delta flush, when all
// resident rows have been reassigned to their centroid-specific partitions
// and written to the members table.
func (b *DeltaBuffer) Reset() {
	empty := make([]CachedVector, 0)
	b.vecs.Store(&empty)
}
