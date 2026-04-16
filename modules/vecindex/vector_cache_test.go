package vecindex

import (
	"sync"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
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

func TestVectorCache_EmptyLookup(t *testing.T) {
	t.Parallel()
	c := NewVectorCache(1, nil)
	require.Zero(t, c.Len())
	require.Nil(t, c.Cluster(42))
}

func TestVectorCache_ClusterRetrieval(t *testing.T) {
	t.Parallel()
	entries := map[int64][]CachedVector{
		1: {{RowID: 10, Vec: []float32{1, 2, 3}}},
		2: {{RowID: 20, Vec: []float32{4, 5, 6}}, {RowID: 21, Vec: []float32{7, 8, 9}}},
	}
	c := NewVectorCache(5, entries)
	require.Equal(t, 3, c.Len())
	require.Equal(t, 2, c.ClusterCount())
	require.Equal(t, int64(10), c.Cluster(1)[0].RowID)
	require.Len(t, c.Cluster(2), 2)
	require.Equal(t, uint64(5), c.Epoch())
}

func TestIndexState_StoreAndLoadCache(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	require.Nil(t, s.LoadCache())

	c := NewVectorCache(7, map[int64][]CachedVector{
		1: {{RowID: 1, Vec: []float32{1, 0, 0}}},
	})
	s.StoreCache(c)
	require.Same(t, c, s.LoadCache())

	s.CacheClear()
	require.Nil(t, s.LoadCache())
}

func TestIndexState_CacheInsertBatch_EpochGate(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	s.StoreCache(NewVectorCache(7, map[int64][]CachedVector{}))

	// Correct epoch: inserts land.
	s.CacheInsertBatch(7, []CacheEntry{
		{ClusterID: 1, RowID: 10, Vec: []float32{1, 0, 0}},
		{ClusterID: 1, RowID: 11, Vec: []float32{0, 1, 0}},
		{ClusterID: 2, RowID: 12, Vec: []float32{0, 0, 1}},
	})
	c := s.LoadCache()
	require.Equal(t, 3, c.Len())
	require.Len(t, c.Cluster(1), 2)
	require.Len(t, c.Cluster(2), 1)

	// Stale epoch: no-op (simulates post-reindex late flush).
	s.CacheInsertBatch(6, []CacheEntry{
		{ClusterID: 1, RowID: 99, Vec: []float32{0.5, 0.5, 0}},
	})
	require.Equal(t, 3, s.LoadCache().Len())
}

func TestIndexState_CacheDelete(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	s.StoreCache(NewVectorCache(7, map[int64][]CachedVector{
		1: {{RowID: 10, Vec: []float32{1, 0, 0}}, {RowID: 11, Vec: []float32{0, 1, 0}}},
		2: {{RowID: 20, Vec: []float32{0, 0, 1}}},
	}))

	s.CacheDelete(11)
	require.Equal(t, 2, s.LoadCache().Len())
	require.Len(t, s.LoadCache().Cluster(1), 1)

	// Missing rowid: no-op.
	s.CacheDelete(9999)
	require.Equal(t, 2, s.LoadCache().Len())
}

// TestIndexState_CacheInsertBatch_ConcurrentCOW drives parallel insert batches
// into the same cache and asserts all entries land. Runs under -race to flag
// any data race between COW readers and the atomic.Pointer swap.
func TestIndexState_CacheInsertBatch_ConcurrentCOW(t *testing.T) {
	t.Parallel()
	s := newCacheTestState(t)
	s.StoreCache(NewVectorCache(7, map[int64][]CachedVector{}))

	const workers = 8
	const perWorker = 125 // 8*125 = 1000 entries

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			batch := make([]CacheEntry, perWorker)
			for i := 0; i < perWorker; i++ {
				batch[i] = CacheEntry{
					ClusterID: 1 + (base+int64(i))%4,
					RowID:     base*1000 + int64(i),
					Vec:       []float32{float32(i), 0, 0},
				}
			}
			s.CacheInsertBatch(7, batch)
		}(int64(w))
	}

	// Concurrent readers should observe monotonically non-decreasing Len.
	reader := make(chan int, 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-reader:
				return
			default:
				c := s.LoadCache()
				if c != nil {
					_ = c.Len()
				}
			}
		}
	}()

	wg.Wait()
	close(reader)
	<-done

	require.Equal(t, workers*perWorker, s.LoadCache().Len())
}

func TestEngine_UnregisterClearsCache(t *testing.T) {
	t.Parallel()
	e := makeEngine(t)
	state := makeState(t, "emb", 3, [][]float32{{1, 0, 0}})
	e.Register("emb", state)
	state.StoreCache(NewVectorCache(1, map[int64][]CachedVector{
		1: {{RowID: 1, Vec: []float32{1, 0, 0}}},
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

	c := NewVectorCache(1, map[int64][]CachedVector{
		1: {{RowID: 1, Vec: []float32{1, 0, 0}}},
	})
	state.StoreCache(c)
	require.Same(t, c, e.LookupCache("emb"))
}
