package coordinator

// topKHeap is a bounded max-heap over rankItem keyed by dist. The root holds
// the largest dist so the farthest candidate can be evicted in O(log k) when
// a strictly closer one arrives. Capacity is fixed at construction; Push is
// allocation-free once the heap is non-empty. Not safe for concurrent use.
type topKHeap struct {
	items []rankItem
	k     int
}

// newTopKHeap returns a heap bounded to the k closest items. A non-positive k
// yields a heap that silently drops every Push. The backing slice is pre-sized
// to cap=k so Push never reallocates.
func newTopKHeap(k int) *topKHeap {
	if k < 0 {
		k = 0
	}
	return &topKHeap{
		items: make([]rankItem, 0, k),
		k:     k,
	}
}

// Len reports the current number of items in the heap.
func (h *topKHeap) Len() int { return len(h.items) }

// Push offers (rowid, dist) to the heap. If the heap is not yet full the item
// is appended and sifted up. If full, the item replaces the current max only
// when strictly closer; equal-dist candidates are rejected so ties preserve
// insertion order among the first k arrivals.
func (h *topKHeap) Push(rowid int64, dist float32) {
	if h.k == 0 {
		return
	}
	if len(h.items) < h.k {
		h.items = append(h.items, rankItem{rowid: rowid, dist: dist})
		h.siftUp(len(h.items) - 1)
		return
	}
	if dist >= h.items[0].dist {
		return
	}
	h.items[0] = rankItem{rowid: rowid, dist: dist}
	h.siftDown(0)
}

// Drain returns the heap contents sorted by ascending dist and empties the
// heap logically. The returned slice aliases the heap's backing array, so it
// remains valid only until the next Push or Reset. Drain on an empty heap
// returns nil. The backing capacity is retained across Drain.
func (h *topKHeap) Drain() []rankItem {
	n := len(h.items)
	if n == 0 {
		return nil
	}
	// In-place heap-sort: repeatedly swap the root (current max) with the
	// tail, shrink the logical heap by one, and sift the new root down over
	// the remaining prefix. After n-1 swaps the array is ascending by dist.
	buf := h.items[:n:n]
	for end := n - 1; end > 0; end-- {
		buf[0], buf[end] = buf[end], buf[0]
		h.items = h.items[:end]
		h.siftDown(0)
	}
	h.items = buf[:0]
	return buf
}

// Reset clears the logical length while preserving the backing array so the
// heap can be reused across queries without reallocating.
func (h *topKHeap) Reset() {
	h.items = h.items[:0]
}

// siftUp restores the max-heap invariant after appending at index i.
func (h *topKHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[i].dist <= h.items[parent].dist {
			return
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

// siftDown restores the max-heap invariant after replacing the root at i.
func (h *topKHeap) siftDown(i int) {
	n := len(h.items)
	for {
		left := 2*i + 1
		if left >= n {
			return
		}
		largest := left
		if right := left + 1; right < n && h.items[right].dist > h.items[left].dist {
			largest = right
		}
		if h.items[i].dist >= h.items[largest].dist {
			return
		}
		h.items[i], h.items[largest] = h.items[largest], h.items[i]
		i = largest
	}
}
