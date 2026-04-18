package vecindex

import (
	"context"
	"sync"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newState builds an IndexState with a one-centroid set for cache tests.
func newCacheTestState(t *testing.T) *IndexState {
	t.Helper()
	spec := IVFSpec{ID: "t", Dim: 3, Metric: MetricL2, Nlist: 1}
	cs, err := kmeans.NewCentroidSet(7, [][]float32{{0, 0, 0}})
	require.NoError(t, err)
	return NewIndexState(spec, cs)
}

// makeTestCache builds a VectorCache preloaded from seed. The backing
// PartitionCache uses a fakeLoader so any cache miss (post-invalidate or
// post-eviction) reloads from seed.
func makeTestCache(t *testing.T, epoch uint64, dim int, seed map[int64]CachedPartition) *VectorCache {
	t.Helper()
	loader := &fakeLoader{data: map[int64]CachedPartition{}}
	for cid, part := range seed {
		loader.data[cid] = part
	}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 16 << 20, Dim: dim, Epoch: epoch, Loader: loader,
	})
	require.NoError(t, err)
	return NewVectorCache(epoch, pc, NewDeltaBuffer())
}

func TestVectorCache_NilSafety(t *testing.T) {
	t.Parallel()
	var c *VectorCache
	assert.Equal(t, uint64(0), c.Epoch())
	assert.Nil(t, c.Partitions())
	assert.Nil(t, c.Delta())
	assert.Nil(t, c.DeltaSnapshot())
	got, err := c.BulkGetPartitions(context.Background(), []int64{1})
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestVectorCache_BulkGetPartitionsAndDelta(t *testing.T) {
	t.Parallel()
	c := makeTestCache(t, 5, 3, map[int64]CachedPartition{
		1: makePartition(CachedVector{RowID: 10, Vec: []float32{1, 2, 3}}),
		2: makePartition(
			CachedVector{RowID: 20, Vec: []float32{4, 5, 6}},
			CachedVector{RowID: 21, Vec: []float32{7, 8, 9}},
		),
	})
	c.Delta().Append(CachedVector{RowID: 99, Vec: []float32{0, 0, 1}})

	got, err := c.BulkGetPartitions(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	assert.Equal(t, 1, got[1].Len())
	assert.Equal(t, 2, got[2].Len())
	assert.Equal(t, uint64(5), c.Epoch())

	d := c.DeltaSnapshot()
	require.Len(t, d, 1)
	assert.Equal(t, int64(99), d[0].RowID)
}

func TestIndexState_StoreAndLoadCache(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	require.Nil(t, s.LoadCache())

	c := makeTestCache(t, 7, 3, map[int64]CachedPartition{
		1: makePartition(CachedVector{RowID: 1, Vec: []float32{1, 0, 0}}),
	})
	s.StoreCache(c)
	require.Same(t, c, s.LoadCache())

	s.CacheClear()
	require.Nil(t, s.LoadCache())
}

func TestIndexState_StoreResidentDelta(t *testing.T) {
	t.Parallel()

	s := newCacheTestState(t)
	delta := NewDeltaBuffer()
	delta.AppendBatch([]CachedVector{
		{RowID: 10, Vec: []float32{1, 0, 0}},
		{RowID: 11, Vec: []float32{0, 1, 0}},
	})

	s.StoreResidentDelta(delta)

	got := s.LoadResidentDelta()
	require.NotNil(t, got)
	assert.Equal(t, []CachedVector{
		{RowID: 10, Vec: []float32{1, 0, 0}},
		{RowID: 11, Vec: []float32{0, 1, 0}},
	}, got.Snapshot())
}

func TestIndexState_CacheInsertBatch_RemovesResidentDeltaWithoutCache(t *testing.T) {
	t.Parallel()

	s := newCacheTestState(t)
	delta := NewDeltaBuffer()
	delta.AppendBatch([]CachedVector{
		{RowID: 10, Vec: []float32{1, 0, 0}},
		{RowID: 11, Vec: []float32{0, 1, 0}},
	})
	s.StoreResidentDelta(delta)

	s.CacheInsertBatch(7, []CacheEntry{
		{ClusterID: 2, RowID: 10, Vec: []float32{1, 0, 0}},
	})

	require.Equal(t, []CachedVector{
		{RowID: 11, Vec: []float32{0, 1, 0}},
	}, s.LoadResidentDelta().Snapshot())
}

func TestIndexState_CacheInsertBatch_InvalidatesPartitionsAndDelta(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	c := makeTestCache(t, 7, 3, map[int64]CachedPartition{
		1: makePartition(CachedVector{RowID: 10, Vec: []float32{1, 0, 0}}),
		2: makePartition(CachedVector{RowID: 20, Vec: []float32{0, 1, 0}}),
	})
	s.StoreCache(c)

	// Seed delta with rowids that will get flushed.
	c.Delta().AppendBatch([]CachedVector{
		{RowID: 10, Vec: []float32{1, 0, 0}},
		{RowID: 11, Vec: []float32{0, 1, 0}},
	})

	// Warm partitions 1 and 2.
	_, err := c.BulkGetPartitions(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, 2, c.Partitions().EstimatedSize())

	// Post-flush entries: rowid 10 moved to cluster 1, rowid 11 moved to cluster 2.
	s.CacheInsertBatch(7, []CacheEntry{
		{ClusterID: 1, RowID: 10, Vec: []float32{1, 0, 0}},
		{ClusterID: 2, RowID: 11, Vec: []float32{0, 1, 0}},
	})

	// Delta rowids removed.
	d := c.DeltaSnapshot()
	assert.Empty(t, d, "delta rowids must be removed after flush batch")

	// Both partitions invalidated — next probe reloads via BulkLoader.
	assert.Equal(t, 0, c.Partitions().EstimatedSize(),
		"touched partitions should be evicted so next probe reloads fresh")
}

func TestIndexState_CacheInsertBatch_StaleEpochNoOp(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	c := makeTestCache(t, 7, 3, map[int64]CachedPartition{
		1: makePartition(CachedVector{RowID: 10, Vec: []float32{1, 0, 0}}),
	})
	s.StoreCache(c)
	c.Delta().Append(CachedVector{RowID: 99, Vec: []float32{1, 1, 1}})

	// Wrong epoch (simulates late flush after reindex).
	s.CacheInsertBatch(6, []CacheEntry{
		{ClusterID: 1, RowID: 99, Vec: []float32{1, 1, 1}},
	})

	require.Len(t, c.DeltaSnapshot(), 1,
		"stale-epoch batch must not mutate delta buffer")
}

func TestIndexState_CacheDelete_FromDelta(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	c := makeTestCache(t, 7, 3, nil)
	s.StoreCache(c)
	c.Delta().AppendBatch([]CachedVector{
		{RowID: 10, Vec: []float32{1, 0, 0}},
		{RowID: 11, Vec: []float32{0, 1, 0}},
	})

	s.CacheDelete(11)
	snap := c.DeltaSnapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, int64(10), snap[0].RowID)

	// Missing rowid: no panic.
	s.CacheDelete(9999)
	assert.Len(t, c.DeltaSnapshot(), 1)
}

func TestIndexState_CacheDelete_FromPartition(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	c := makeTestCache(t, 7, 3, map[int64]CachedPartition{
		1: makePartition(
			CachedVector{RowID: 10, Vec: []float32{1, 0, 0}},
			CachedVector{RowID: 11, Vec: []float32{0, 1, 0}},
		),
		2: makePartition(CachedVector{RowID: 20, Vec: []float32{0, 0, 1}}),
	})
	s.StoreCache(c)

	// Warm both partitions.
	_, err := c.BulkGetPartitions(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, 2, c.Partitions().EstimatedSize())

	// Delete rowid 11 — lives in partition 1, which should get invalidated.
	s.CacheDelete(11)
	assert.Equal(t, 1, c.Partitions().EstimatedSize(),
		"partition 1 should be evicted; partition 2 stays resident")
}

func TestIndexState_CacheDelete_NoCacheIsNoOp(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	require.Nil(t, s.LoadCache())

	// Must not panic.
	s.CacheDelete(42)
}

// TestIndexState_CacheInsertBatch_Concurrent drives parallel post-flush
// batches and asserts the final state is consistent. Runs under -race.
func TestIndexState_CacheInsertBatch_Concurrent(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	c := makeTestCache(t, 7, 3, map[int64]CachedPartition{
		1: makePartition(CachedVector{RowID: 1, Vec: []float32{1, 0, 0}}),
		2: makePartition(CachedVector{RowID: 2, Vec: []float32{0, 1, 0}}),
		3: makePartition(CachedVector{RowID: 3, Vec: []float32{0, 0, 1}}),
		4: makePartition(CachedVector{RowID: 4, Vec: []float32{1, 1, 0}}),
	})
	s.StoreCache(c)

	// Seed delta with 1000 rowids that all concurrent flushes will remove.
	const totalRows = 1000
	seed := make([]CachedVector, totalRows)
	for i := 0; i < totalRows; i++ {
		seed[i] = CachedVector{RowID: int64(i), Vec: []float32{float32(i), 0, 0}}
	}
	c.Delta().AppendBatch(seed)
	require.Equal(t, totalRows, c.Delta().Len())

	const workers = 8
	perWorker := totalRows / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			batch := make([]CacheEntry, perWorker)
			for i := 0; i < perWorker; i++ {
				batch[i] = CacheEntry{
					ClusterID: int64(1 + (w+i)%4),
					RowID:     int64(w*perWorker + i),
				}
			}
			s.CacheInsertBatch(7, batch)
		}(w)
	}
	wg.Wait()

	assert.Empty(t, c.DeltaSnapshot(), "all delta entries should be removed after concurrent flushes")
}

func TestEngine_UnregisterClearsCache(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	state := makeState(t, "emb", 3, [][]float32{{1, 0, 0}})
	e.Register("emb", state)
	state.StoreCache(makeTestCache(t, 1, 3, map[int64]CachedPartition{
		1: makePartition(CachedVector{RowID: 1, Vec: []float32{1, 0, 0}}),
	}))
	require.NotNil(t, state.LoadCache())
	e.Unregister("emb")
	require.Nil(t, state.LoadCache())
}

func TestEngine_LookupCache(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	require.Nil(t, e.LookupCache("missing"))

	state := makeState(t, "emb", 3, [][]float32{{1, 0, 0}})
	e.Register("emb", state)
	require.Nil(t, e.LookupCache("emb"))

	c := makeTestCache(t, 1, 3, map[int64]CachedPartition{
		1: makePartition(CachedVector{RowID: 1, Vec: []float32{1, 0, 0}}),
	})
	state.StoreCache(c)
	require.Same(t, c, e.LookupCache("emb"))
}
