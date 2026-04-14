package store_test

import (
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestAllocClusterID_Monotonic(t *testing.T) {
	s := openStore(t)
	prev, err := s.AllocateClusterID()
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		next, err := s.AllocateClusterID()
		require.NoError(t, err)
		require.Greater(t, next, prev, "AllocateClusterID must be strictly increasing")
		prev = next
	}
}

func TestAllocClusterID_Persists(t *testing.T) {
	dir := t.TempDir()

	s, err := store.New(dir, &pebble.Options{})
	require.NoError(t, err)

	var last uint32
	for i := 0; i < 5; i++ {
		last, err = s.AllocateClusterID()
		require.NoError(t, err)
	}
	require.NoError(t, s.Close())

	// Reopen — next alloc must be > last.
	s2, err := store.New(dir, &pebble.Options{})
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	next, err := s2.AllocateClusterID()
	require.NoError(t, err)
	require.Greater(t, next, last, "cluster ID must not restart after reopen")
}

func TestAllocClusterID_Concurrent(t *testing.T) {
	s := openStore(t)
	const goroutines = 100

	ids := make([]uint32, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			id, err := s.AllocateClusterID()
			require.NoError(t, err)
			ids[i] = id
		}()
	}
	wg.Wait()

	// All IDs must be distinct.
	seen := make(map[uint32]struct{}, goroutines)
	for _, id := range ids {
		_, dup := seen[id]
		require.False(t, dup, "duplicate cluster ID %d", id)
		seen[id] = struct{}{}
	}
	require.Len(t, seen, goroutines)
}
