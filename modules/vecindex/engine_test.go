package vecindex

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEngine_CreatesRootDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	subDir := filepath.Join(dir, "engine-root")

	e, err := NewEngine(subDir, newTestLogger())
	require.NoError(t, err)
	require.NoError(t, e.Close())

	_, statErr := os.Stat(subDir)
	require.NoError(t, statErr, "engine root dir must exist after NewEngine")
}

func TestCreateIndex_Empty(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("empty-idx", 16, MetricL2)
	idx, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)
	require.NotNil(t, idx)

	stats := idx.Stats()
	require.Equal(t, uint64(0), stats.VectorCount)
}

func TestCreateIndex_WithBulk1k(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)

	idx := buildTestIndex(t, e, "bulk-idx", 64, 1000, MetricL2)
	stats := idx.Stats()
	require.Equal(t, uint64(1000), stats.VectorCount)
}

func TestCreateIndex_DuplicateIDRejected(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("dup-idx", 8, MetricL2)
	_, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	_, err2 := e.CreateIndex(ctx, spec, nil)
	require.Error(t, err2, "creating same index ID twice must return an error")
}

func TestOpenIndex_AfterClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	e1, err := NewEngine(dir, newTestLogger())
	require.NoError(t, err)

	spec := DefaultSpec("reopen-idx", 8, MetricL2)
	_, createErr := e1.CreateIndex(ctx, spec, nil)
	require.NoError(t, createErr)
	require.NoError(t, e1.Close())

	e2, err := NewEngine(dir, newTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = e2.Close() })

	idx, openErr := e2.OpenIndex(ctx, "reopen-idx")
	require.NoError(t, openErr)
	require.NotNil(t, idx)
}

func TestDropIndex_RemovesFiles(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	spec := DefaultSpec("drop-idx", 8, MetricL2)
	_, err := e.CreateIndex(ctx, spec, nil)
	require.NoError(t, err)

	require.NoError(t, e.DropIndex(ctx, "drop-idx"))

	_, openErr := e.OpenIndex(ctx, "drop-idx")
	require.Error(t, openErr, "OpenIndex after DropIndex must fail")
}

func TestEngineClose_ClosesAllIndexes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	e1, err := NewEngine(dir, newTestLogger())
	require.NoError(t, err)

	for _, id := range []string{"a", "b", "c"} {
		spec := DefaultSpec(id, 8, MetricL2)
		_, createErr := e1.CreateIndex(ctx, spec, nil)
		require.NoError(t, createErr)
	}
	require.NoError(t, e1.Close())

	// Engine must be re-openable after clean close.
	e2, err2 := NewEngine(dir, newTestLogger())
	require.NoError(t, err2)
	require.NoError(t, e2.Close())
}

func TestCreateIndex_InvalidSpec(t *testing.T) {
	t.Parallel()
	e := newTempEngine(t)
	ctx := context.Background()

	cases := []struct {
		name string
		spec IVFSpec
	}{
		{"dim=0", IVFSpec{ID: "bad-dim", Dim: 0, Metric: MetricL2, Nlist: 64, Nprobe: 4}},
		{"nlist=0", IVFSpec{ID: "bad-nlist", Dim: 8, Metric: MetricL2, Nlist: 0, Nprobe: 4}},
		{"metric=255", IVFSpec{ID: "bad-metric", Dim: 8, Metric: Metric(255), Nlist: 64, Nprobe: 4}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := e.CreateIndex(ctx, tc.spec, nil)
			require.Error(t, err, "invalid spec must be rejected")
		})
	}
}
