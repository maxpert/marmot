package vecindex

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLoader is a deterministic in-memory PartitionLoader. It records the
// number of BulkLoad calls and the full sequence of keys requested, so tests
// can assert single-flight behaviour and eviction-triggered reloads.
type fakeLoader struct {
	mu       sync.Mutex
	data     map[int64]CachedPartition
	calls    int32
	keyHist  [][]int64
	blockCh  chan struct{}      // if non-nil, BulkLoad blocks on it (for concurrency tests)
	loadFunc func(keys []int64) // optional side effect per call
}

func (f *fakeLoader) BulkLoad(_ context.Context, keys []int64) (map[int64]CachedPartition, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.blockCh != nil {
		<-f.blockCh
	}
	f.mu.Lock()
	snapshot := append([]int64(nil), keys...)
	f.keyHist = append(f.keyHist, snapshot)
	if f.loadFunc != nil {
		f.loadFunc(snapshot)
	}
	out := make(map[int64]CachedPartition, len(keys))
	for _, k := range keys {
		// Always return an entry (even empty) so otter caches it.
		part, ok := f.data[k]
		if !ok {
			out[k] = CachedPartition{}
			continue
		}
		out[k] = part
	}
	f.mu.Unlock()
	return out, nil
}

func (f *fakeLoader) callCount() int32 { return atomic.LoadInt32(&f.calls) }

func makeVec(rowID int64, dim int, fill float32) CachedVector {
	v := make([]float32, dim)
	for i := range v {
		v[i] = fill
	}
	return CachedVector{RowID: rowID, Vec: v}
}

func makePartition(vecs ...CachedVector) CachedPartition {
	part := CachedPartition{
		RowIDs: make([]int64, 0, len(vecs)),
	}
	if len(vecs) == 0 {
		return part
	}
	dim := len(vecs[0].Vec)
	part.Vecs = make([]float32, 0, len(vecs)*dim)
	for _, v := range vecs {
		part.RowIDs = append(part.RowIDs, v.RowID)
		part.Vecs = append(part.Vecs, v.Vec...)
	}
	return part
}

func TestPartitionCache_ConstructorValidation(t *testing.T) {
	loader := &fakeLoader{}
	_, err := NewPartitionCache(PartitionCacheOptions{MaxBytes: 1 << 20, Dim: 4})
	assert.Error(t, err, "missing loader should fail")

	_, err = NewPartitionCache(PartitionCacheOptions{MaxBytes: 0, Dim: 4, Loader: loader})
	assert.Error(t, err, "zero MaxBytes should fail")

	_, err = NewPartitionCache(PartitionCacheOptions{MaxBytes: 1 << 20, Dim: 0, Loader: loader})
	assert.Error(t, err, "zero Dim should fail")
}

func TestPartitionCache_BulkGetPopulatesFromLoader(t *testing.T) {
	dim := 4
	loader := &fakeLoader{data: map[int64]CachedPartition{
		1: makePartition(makeVec(101, dim, 1), makeVec(102, dim, 1)),
		2: makePartition(makeVec(201, dim, 2)),
		3: {}, // empty partition
	}}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20,
		Dim:      dim,
		Epoch:    7,
		Loader:   loader,
	})
	require.NoError(t, err)

	got, err := pc.BulkGet(context.Background(), []int64{1, 2, 3})
	require.NoError(t, err)
	assert.Equal(t, 2, got[1].Len())
	assert.Equal(t, 1, got[2].Len())
	assert.True(t, got[3].Empty())
	assert.Equal(t, int32(1), loader.callCount(), "one BulkLoad call for three keys")
}

func TestPartitionCache_HitsSkipLoader(t *testing.T) {
	dim := 4
	loader := &fakeLoader{data: map[int64]CachedPartition{
		1: makePartition(makeVec(101, dim, 1)),
		2: makePartition(makeVec(201, dim, 2)),
	}}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	_, err = pc.BulkGet(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, int32(1), loader.callCount())

	_, err = pc.BulkGet(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	assert.Equal(t, int32(1), loader.callCount(), "second call should be a full cache hit")
}

func TestPartitionCache_PartialHitLoadsOnlyMisses(t *testing.T) {
	dim := 4
	loader := &fakeLoader{data: map[int64]CachedPartition{
		1: makePartition(makeVec(101, dim, 1)),
		2: makePartition(makeVec(201, dim, 2)),
		3: makePartition(makeVec(301, dim, 3)),
	}}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	_, err = pc.BulkGet(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, int32(1), loader.callCount())

	_, err = pc.BulkGet(context.Background(), []int64{2, 3})
	require.NoError(t, err)
	require.Equal(t, int32(2), loader.callCount())

	// Last BulkLoad should have asked only for key 3 (miss).
	loader.mu.Lock()
	lastKeys := loader.keyHist[len(loader.keyHist)-1]
	loader.mu.Unlock()
	assert.Equal(t, []int64{3}, lastKeys)
}

func TestPartitionCache_EvictionUnderByteBudget(t *testing.T) {
	// Dim=256 → each vector ≈ 1056 bytes (8+24+1024). 100 vectors per partition
	// ≈ 105.6 KB + 192 overhead. 3 partitions ≈ 316 KB. Cap at 200 KB forces
	// eviction of at least one after loading 3.
	dim := 256
	vecsPer := 100
	loader := &fakeLoader{data: map[int64]CachedPartition{}}
	for cid := int64(1); cid <= 3; cid++ {
		vecs := make([]CachedVector, 0, vecsPer)
		for r := 0; r < vecsPer; r++ {
			vecs = append(vecs, makeVec(cid*1000+int64(r), dim, float32(cid)))
		}
		loader.data[cid] = makePartition(vecs...)
	}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 200 * 1024, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	_, err = pc.BulkGet(context.Background(), []int64{1, 2, 3})
	require.NoError(t, err)

	// Otter's maintenance work is asynchronous — give it a moment to enforce
	// the byte cap. We poll because exact timing is implementation-defined.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pc.EstimatedSize() <= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.LessOrEqual(t, pc.EstimatedSize(), 2,
		"cache should have evicted at least one partition under 200KB budget")
}

func TestPartitionCache_EvictionTriggersReload(t *testing.T) {
	dim := 256
	vecsPer := 100
	loader := &fakeLoader{data: map[int64]CachedPartition{}}
	for cid := int64(1); cid <= 3; cid++ {
		vecs := make([]CachedVector, 0, vecsPer)
		for r := 0; r < vecsPer; r++ {
			vecs = append(vecs, makeVec(cid*1000+int64(r), dim, float32(cid)))
		}
		loader.data[cid] = makePartition(vecs...)
	}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 200 * 1024, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = pc.BulkGet(ctx, []int64{1, 2, 3})
	require.NoError(t, err)
	// Force eviction to settle.
	time.Sleep(100 * time.Millisecond)

	// Re-probe all 3; at least one should be a miss and trigger a reload.
	callsBefore := loader.callCount()
	_, err = pc.BulkGet(ctx, []int64{1, 2, 3})
	require.NoError(t, err)
	assert.Greater(t, loader.callCount(), callsBefore,
		"reloading evicted partition should call loader again")
}

func TestPartitionCache_Invalidate(t *testing.T) {
	dim := 4
	loader := &fakeLoader{data: map[int64]CachedPartition{
		1: makePartition(makeVec(101, dim, 1)),
	}}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = pc.BulkGet(ctx, []int64{1})
	require.NoError(t, err)
	require.Equal(t, int32(1), loader.callCount())

	pc.Invalidate(1)

	_, err = pc.BulkGet(ctx, []int64{1})
	require.NoError(t, err)
	assert.Equal(t, int32(2), loader.callCount(), "invalidated key should reload")
}

func TestPartitionCache_SliceStaysLiveAfterEviction(t *testing.T) {
	// Memory-safety guarantee: a reader who grabbed a slice before eviction
	// still owns it (Go GC keeps it alive). We simulate this by holding the
	// slice across an eviction-inducing load and verifying the data is intact.
	dim := 256
	vecsPer := 100
	loader := &fakeLoader{data: map[int64]CachedPartition{}}
	for cid := int64(1); cid <= 5; cid++ {
		vecs := make([]CachedVector, 0, vecsPer)
		for r := 0; r < vecsPer; r++ {
			vecs = append(vecs, makeVec(cid*1000+int64(r), dim, float32(cid)))
		}
		loader.data[cid] = makePartition(vecs...)
	}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 200 * 1024, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	ctx := context.Background()
	first, err := pc.BulkGet(ctx, []int64{1})
	require.NoError(t, err)
	held := first[1]
	require.Equal(t, vecsPer, held.Len())

	// Probe other partitions until 1 is evicted.
	_, err = pc.BulkGet(ctx, []int64{2, 3, 4, 5})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Our held slice must still be readable with the original contents.
	for i, rid := range held.RowIDs {
		v := held.Vector(i, dim)
		require.Equal(t, int64(1*1000+int64(i)), rid)
		require.Len(t, v, dim)
		assert.InDelta(t, float32(1), v[0], 0)
	}
}

func TestPartitionCache_ConcurrentBulkGet(t *testing.T) {
	dim := 8
	data := map[int64]CachedPartition{}
	for cid := int64(0); cid < 32; cid++ {
		data[cid] = makePartition(makeVec(cid, dim, float32(cid)))
	}
	loader := &fakeLoader{data: data}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	const workers = 16
	const iters = 500
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < iters; i++ {
				base := int64((w + i) % 32)
				keys := []int64{base, (base + 1) % 32, (base + 2) % 32}
				got, err := pc.BulkGet(ctx, keys)
				require.NoError(t, err)
				for _, k := range keys {
					require.Equal(t, 1, got[k].Len())
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestPartitionCache_EmptyKeySetReturnsNil(t *testing.T) {
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20, Dim: 4, Loader: &fakeLoader{data: map[int64]CachedPartition{}},
	})
	require.NoError(t, err)

	got, err := pc.BulkGet(context.Background(), nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestPartitionCache_FindAndInvalidate(t *testing.T) {
	dim := 4
	loader := &fakeLoader{data: map[int64]CachedPartition{
		1: makePartition(makeVec(101, dim, 1), makeVec(102, dim, 1)),
		2: makePartition(makeVec(201, dim, 2), makeVec(202, dim, 2)),
	}}
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20, Dim: dim, Loader: loader,
	})
	require.NoError(t, err)

	ctx := context.Background()
	// Partitions not resident yet — search returns false with no side effects.
	assert.False(t, pc.FindAndInvalidate(101))

	// Load them, then find-and-invalidate rowid 202 (in partition 2).
	_, err = pc.BulkGet(ctx, []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, int32(1), loader.callCount())

	assert.True(t, pc.FindAndInvalidate(202))

	// Re-probing partition 2 should miss and reload; partition 1 still cached.
	_, err = pc.BulkGet(ctx, []int64{1, 2})
	require.NoError(t, err)
	assert.Equal(t, int32(2), loader.callCount(),
		"partition 2 should reload after invalidation; partition 1 stays cached")

	loader.mu.Lock()
	lastKeys := loader.keyHist[len(loader.keyHist)-1]
	loader.mu.Unlock()
	assert.Equal(t, []int64{2}, lastKeys)
}

func TestPartitionCache_EpochAndDim(t *testing.T) {
	pc, err := NewPartitionCache(PartitionCacheOptions{
		MaxBytes: 1 << 20, Dim: 128, Epoch: 42, Loader: &fakeLoader{},
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(42), pc.Epoch())
	assert.Equal(t, 128, pc.Dim())
}
