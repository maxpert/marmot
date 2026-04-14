package vecindex

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotIndex_WritesTarGz(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("snap-hdr", 8, MetricL2)
	vecs := makeRandomVectors(10, 8, 1)
	bulk := make([]BulkEntry, 10)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	_, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, e.SnapshotIndex(ctx, "snap-hdr", &buf))
	require.NotEmpty(t, buf.Bytes(), "snapshot must write bytes")

	// Verify tar.gz header.
	gr, err := gzip.NewReader(&buf)
	require.NoError(t, err, "snapshot must be valid gzip")
	tr := tar.NewReader(gr)
	_, err = tr.Next()
	require.NoError(t, err, "snapshot must contain at least one tar entry")
}

func TestRestoreIndex_IntoBlankEngine(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Engine A — source.
	eA := newTempEngine(t)
	spec := DefaultSpec("snap-restore", 8, MetricL2)
	vecs := makeRandomVectors(20, 8, 2)
	bulk := make([]BulkEntry, 20)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	_, err := eA.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, eA.SnapshotIndex(ctx, "snap-restore", &buf))

	// Engine B — destination.
	eB := newTempEngine(t)
	require.NoError(t, eB.RestoreIndex(ctx, "snap-restore", bytes.NewReader(buf.Bytes())))

	idxB, err := eB.OpenIndex(ctx, "snap-restore")
	require.NoError(t, err)

	query := vecs[0]
	hitsB, err := idxB.Search(ctx, SearchRequest{Vector: query, K: 1})
	require.NoError(t, err)
	require.Len(t, hitsB, 1)
	require.Equal(t, []byte("v0"), hitsB[0].ExternalID,
		"restored index must return same hits as original")
}

func TestSnapshot_AtomicConsistency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newTempEngine(t)

	spec := DefaultSpec("snap-atomic", 8, MetricL2)
	vecs := makeRandomVectors(50, 8, 3)
	bulk := make([]BulkEntry, 50)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// Concurrent upserts during snapshot.
	for j := 0; j < 20; j++ {
		j := j
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := makeRandomVectors(1, 8, int64(j+100))[0]
			if upsertErr := idx.Upsert(ctx, []byte(fmt.Sprintf("race%d", j)), v, uint64(1000+j), 0); upsertErr != nil {
				errCh <- upsertErr
			}
		}()
	}

	var buf bytes.Buffer
	snapErr := e.SnapshotIndex(ctx, "snap-atomic", &buf)
	wg.Wait()
	close(errCh)

	require.NoError(t, snapErr, "snapshot must not error during concurrent upserts")
	for err := range errCh {
		require.NoError(t, err)
	}

	// Restore and verify snapshot is valid (no corruption).
	eR := newTempEngine(t)
	require.NoError(t, eR.RestoreIndex(ctx, "snap-atomic", bytes.NewReader(buf.Bytes())))
	idxR, openErr := eR.OpenIndex(ctx, "snap-atomic")
	require.NoError(t, openErr)

	// The restored index must be searchable without panicking.
	q := makeRandomVectors(1, 8, 77)[0]
	hits, searchErr := idxR.Search(ctx, SearchRequest{Vector: q, K: 5})
	require.NoError(t, searchErr)
	_ = hits
}

func TestRestore_OverExistingIndex_Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newTempEngine(t)

	spec := DefaultSpec("snap-over", 8, MetricL2)
	_, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, e.SnapshotIndex(ctx, "snap-over", &buf))

	// Restore onto same engine where the index already exists.
	err = e.RestoreIndex(ctx, "snap-over", bytes.NewReader(buf.Bytes()))
	require.Error(t, err, "RestoreIndex must error when index already exists")
}
