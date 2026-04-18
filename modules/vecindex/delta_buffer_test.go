package vecindex

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeltaBuffer_EmptyByDefault(t *testing.T) {
	b := NewDeltaBuffer()
	assert.Equal(t, 0, b.Len())
	assert.NotNil(t, b.Snapshot(), "snapshot must never be nil")
	assert.Len(t, b.Snapshot(), 0)
}

func TestDeltaBuffer_Append(t *testing.T) {
	b := NewDeltaBuffer()
	b.Append(CachedVector{RowID: 1, Vec: []float32{1, 2}})
	b.Append(CachedVector{RowID: 2, Vec: []float32{3, 4}})

	snap := b.Snapshot()
	require.Len(t, snap, 2)
	assert.Equal(t, int64(1), snap[0].RowID)
	assert.Equal(t, int64(2), snap[1].RowID)
}

func TestDeltaBuffer_AppendBatch(t *testing.T) {
	b := NewDeltaBuffer()
	b.AppendBatch([]CachedVector{
		{RowID: 10, Vec: []float32{1}},
		{RowID: 11, Vec: []float32{2}},
		{RowID: 12, Vec: []float32{3}},
	})
	assert.Equal(t, 3, b.Len())

	// Empty batch is a no-op.
	b.AppendBatch(nil)
	assert.Equal(t, 3, b.Len())
}

func TestDeltaBuffer_Remove(t *testing.T) {
	b := NewDeltaBuffer()
	b.AppendBatch([]CachedVector{
		{RowID: 1, Vec: []float32{1}},
		{RowID: 2, Vec: []float32{2}},
		{RowID: 3, Vec: []float32{3}},
	})

	assert.True(t, b.Remove(2))
	snap := b.Snapshot()
	require.Len(t, snap, 2)
	assert.Equal(t, int64(1), snap[0].RowID)
	assert.Equal(t, int64(3), snap[1].RowID)

	assert.False(t, b.Remove(999), "missing rowid returns false")
	assert.Equal(t, 2, b.Len())
}

func TestDeltaBuffer_Reset(t *testing.T) {
	b := NewDeltaBuffer()
	b.AppendBatch([]CachedVector{
		{RowID: 1, Vec: []float32{1}},
		{RowID: 2, Vec: []float32{2}},
	})
	require.Equal(t, 2, b.Len())

	b.Reset()
	assert.Equal(t, 0, b.Len())
	assert.NotNil(t, b.Snapshot())
}

func TestDeltaBuffer_SnapshotIsStableAcrossMutation(t *testing.T) {
	// Copy-on-write guarantee: a snapshot taken before a mutation must not
	// observe the mutation. Otherwise a concurrent scan would race.
	b := NewDeltaBuffer()
	b.Append(CachedVector{RowID: 1, Vec: []float32{1}})

	stale := b.Snapshot()
	b.Append(CachedVector{RowID: 2, Vec: []float32{2}})
	b.Remove(1)

	assert.Len(t, stale, 1, "stale snapshot must retain the original length")
	assert.Equal(t, int64(1), stale[0].RowID)
	assert.Equal(t, 1, b.Len(), "mutated buffer reflects current state")
}

func TestDeltaBuffer_ConcurrentAppendRemove(t *testing.T) {
	b := NewDeltaBuffer()
	const writers = 8
	const perWriter = 1000

	var wg sync.WaitGroup
	var appended atomic.Int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				rid := int64(w*perWriter + i)
				b.Append(CachedVector{RowID: rid, Vec: []float32{float32(rid)}})
				appended.Add(1)
			}
		}(w)
	}
	wg.Wait()

	require.Equal(t, int64(writers*perWriter), appended.Load())
	assert.Equal(t, writers*perWriter, b.Len())

	// Every rowid in [0, writers*perWriter) must be present exactly once.
	seen := make(map[int64]bool, writers*perWriter)
	for _, v := range b.Snapshot() {
		require.False(t, seen[v.RowID], "duplicate rowid %d", v.RowID)
		seen[v.RowID] = true
	}
	assert.Equal(t, writers*perWriter, len(seen))

	// Remove everything.
	for rid := int64(0); rid < int64(writers*perWriter); rid++ {
		require.True(t, b.Remove(rid))
	}
	assert.Equal(t, 0, b.Len())
}
