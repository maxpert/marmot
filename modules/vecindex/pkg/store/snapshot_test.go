package store_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint_CreatesHardLinkedDir(t *testing.T) {
	s := openStore(t)

	// Populate with some data.
	require.NoError(t, s.PutCentroid(1, makeVec(4, 1.0), 4))
	require.NoError(t, s.PutCentroid(2, makeVec(4, 2.0), 4))

	destDir := t.TempDir()
	// Remove the dir so Checkpoint can create it (pebble.Checkpoint requires non-existing dest).
	require.NoError(t, os.RemoveAll(destDir))

	require.NoError(t, s.Checkpoint(destDir))

	fi, err := os.Stat(destDir)
	require.NoError(t, err)
	require.True(t, fi.IsDir())

	// Destination must contain at least one file (SST or MANIFEST).
	entries, err := os.ReadDir(destDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "checkpoint directory should not be empty")
}

func TestCheckpoint_OpensAsReadOnly(t *testing.T) {
	s := openStore(t)

	require.NoError(t, s.PutCentroid(10, makeVec(3, 7.0), 3))
	require.NoError(t, s.PutCentroid(20, makeVec(3, 8.0), 3))

	destDir := t.TempDir()
	require.NoError(t, os.RemoveAll(destDir))
	require.NoError(t, s.Checkpoint(destDir))

	// Open the checkpoint as a fresh store.
	s2, err := store.New(destDir, &pebble.Options{})
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	ids, vecs, err := s2.ListCentroids()
	require.NoError(t, err)
	require.Len(t, ids, 2)
	require.Len(t, vecs, 2)

	require.Equal(t, uint32(10), ids[0])
	require.Equal(t, makeVec(3, 7.0), vecs[0])
	require.Equal(t, uint32(20), ids[1])
	require.Equal(t, makeVec(3, 8.0), vecs[1])
}
