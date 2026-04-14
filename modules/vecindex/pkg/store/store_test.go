package store_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenStore_CreatesDB(t *testing.T) {
	dir := t.TempDir()
	s, err := store.New(dir, &pebble.Options{})
	require.NoError(t, err)

	fi, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, fi.IsDir())

	require.NoError(t, s.Close())
}

func TestOpenStore_ReopenSameDir(t *testing.T) {
	dir := t.TempDir()

	s, err := store.New(dir, &pebble.Options{})
	require.NoError(t, err)
	require.NoError(t, s.PutCentroid(1, []float32{1.0, 2.0, 3.0}, 3))
	require.NoError(t, s.Close())

	s2, err := store.New(dir, &pebble.Options{})
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	vec, err := s2.GetCentroid(1)
	require.NoError(t, err)
	require.Equal(t, []float32{1.0, 2.0, 3.0}, vec)
}

func TestKeyEncoding_Roundtrip(t *testing.T) {
	t.Parallel()

	centroidKey := store.EncodeCentroidKey(42)
	require.Equal(t, store.KeyPrefixCentroid, centroidKey[0])
	require.Len(t, centroidKey, 5)

	postingKey := store.EncodePostingKey(42, 99)
	require.Equal(t, store.KeyPrefixPosting, postingKey[0])
	require.Equal(t, uint32(42), store.DecodeClusterIDFromPostingKey(postingKey))
	require.Equal(t, uint64(99), store.DecodeDocIDFromPostingKey(postingKey))

	reverseKey := store.EncodeReverseKey(99)
	require.Equal(t, store.KeyPrefixReverseMap, reverseKey[0])
	require.Equal(t, uint64(99), store.DecodeDocIDFromReverseKey(reverseKey))
}

func TestKeyEncoding_PrefixIsolation(t *testing.T) {
	t.Parallel()

	// Same numeric ID in different namespaces must not collide.
	ck := store.EncodeCentroidKey(1)
	pk := store.EncodePostingKey(1, 0)
	rk := store.EncodeReverseKey(1)
	mk := store.EncodeClusterMetaKey(1)
	e2d := store.EncodeExtToDocKey([]byte{0x00, 0x00, 0x00, 0x01})
	d2e := store.EncodeDocToExtKey(1)
	spec := store.EncodeSpecKey()

	keys := [][]byte{ck, pk, rk, mk, e2d, d2e, spec}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			require.False(t, bytes.Equal(keys[i], keys[j]),
				"keys[%d] and keys[%d] collide", i, j)
		}
	}
}

func TestKeyEncoding_SortOrder(t *testing.T) {
	t.Parallel()

	// Posting keys for the same cluster must sort contiguously and numerically by docID.
	k0 := store.EncodePostingKey(10, 0)
	k1 := store.EncodePostingKey(10, 1)
	k2 := store.EncodePostingKey(10, 255)
	k3 := store.EncodePostingKey(10, 256)
	k4 := store.EncodePostingKey(10, 1<<32)

	require.True(t, bytes.Compare(k0, k1) < 0)
	require.True(t, bytes.Compare(k1, k2) < 0)
	require.True(t, bytes.Compare(k2, k3) < 0)
	require.True(t, bytes.Compare(k3, k4) < 0)

	// cluster 10 keys must all be less than cluster 11 keys.
	kNext := store.EncodePostingKey(11, 0)
	require.True(t, bytes.Compare(k4, kNext) < 0)
}

func TestScanClusterFunc_MatchesScanCluster(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	vecs := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
	}
	for i, v := range vecs {
		require.NoError(t, s.PutPosting(1, uint64(i), v))
	}

	// Collect via ScanCluster (reference).
	entries, err := s.ScanCluster(1)
	require.NoError(t, err)
	require.Len(t, entries, len(vecs))

	// Collect via ScanClusterFunc.
	type row struct {
		docID    uint64
		vecBytes []byte
	}
	var rows []row
	require.NoError(t, s.ScanClusterFunc(1, func(docID uint64, vecBytes []byte) error {
		// Must copy: vecBytes is iterator-owned.
		cp := make([]byte, len(vecBytes))
		copy(cp, vecBytes)
		rows = append(rows, row{docID: docID, vecBytes: cp})
		return nil
	}))
	require.Len(t, rows, len(vecs))

	for i, e := range entries {
		require.Equal(t, e.DocID, rows[i].docID)
		require.Equal(t, len(e.Vector)*4, len(rows[i].vecBytes))
	}
}

func TestScanClusterFunc_EarlyAbort(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	for i := range 5 {
		require.NoError(t, s.PutPosting(2, uint64(i), []float32{float32(i)}))
	}

	count := 0
	sentinel := errors.New("stop")
	err := s.ScanClusterFunc(2, func(_ uint64, _ []byte) error {
		count++
		if count == 2 {
			return sentinel
		}
		return nil
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 2, count)
}
