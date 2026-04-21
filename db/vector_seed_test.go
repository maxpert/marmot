package db

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/stretchr/testify/require"
)

// TestStableIndexSeed_SameInputsSameSeed locks the deterministic contract:
// two identical VectorIndexMeta identities must produce the same seed. Any
// drift here breaks the concurrent-CREATE convergence story from design §8.1.
func TestStableIndexSeed_SameInputsSameSeed(t *testing.T) {
	t.Parallel()

	meta := common.VectorIndexMeta{
		IndexName:  "idx_a",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "db1",
		Metric:     "cosine",
		Dim:        128,
		Nlist:      64,
		Nprobe:     8,
		CreatedAt:  12345, // must NOT influence seed
	}
	seed1 := StableIndexSeed(meta)

	// Flip node-local fields that have no part in the identity hash.
	other := meta
	other.IndexName = "different_name"
	other.Database = "other_db"
	other.Nprobe = 999
	other.Status = "ready"
	other.CreatedAt = 999999
	other.MaxNorm = 42.0
	seed2 := StableIndexSeed(other)

	require.Equal(t, seed1, seed2,
		"StableIndexSeed must ignore node-local fields; only (TableName, ColumnName, Dim, Metric, Nlist) define identity")
}

// TestStableIndexSeed_DifferentInputsDifferentSeeds varies each identity field
// in turn and asserts the seed changes. This is the inverse contract: identity
// fields MUST move the hash so two distinct indexes do not collide to the same
// centroid layout by accident.
func TestStableIndexSeed_DifferentInputsDifferentSeeds(t *testing.T) {
	t.Parallel()

	base := common.VectorIndexMeta{
		TableName:  "docs",
		ColumnName: "embed",
		Metric:     "l2",
		Dim:        8,
		Nlist:      4,
	}
	baseSeed := StableIndexSeed(base)

	cases := []struct {
		name  string
		apply func(*common.VectorIndexMeta)
	}{
		{"TableName", func(m *common.VectorIndexMeta) { m.TableName = "other" }},
		{"ColumnName", func(m *common.VectorIndexMeta) { m.ColumnName = "vec" }},
		{"Metric", func(m *common.VectorIndexMeta) { m.Metric = "cosine" }},
		{"Dim", func(m *common.VectorIndexMeta) { m.Dim = 16 }},
		{"Nlist", func(m *common.VectorIndexMeta) { m.Nlist = 8 }},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := base
			tc.apply(&m)
			got := StableIndexSeed(m)
			require.NotEqual(t, baseSeed, got,
				"varying %s must change the seed (base=%d, got=%d)", tc.name, baseSeed, got)
		})
	}
}

// TestStableIndexSeed_NoSeparatorCollision guards against the classic hash
// pitfall of field-boundary ambiguity: without a non-ASCII separator, the
// concatenations ("ab","c") and ("a","bc") would hash to the same value. The
// implementation uses a 0x00 separator; this test pins that property.
func TestStableIndexSeed_NoSeparatorCollision(t *testing.T) {
	t.Parallel()

	a := common.VectorIndexMeta{TableName: "ab", ColumnName: "c", Metric: "l2", Dim: 1, Nlist: 1}
	b := common.VectorIndexMeta{TableName: "a", ColumnName: "bc", Metric: "l2", Dim: 1, Nlist: 1}

	require.NotEqual(t, StableIndexSeed(a), StableIndexSeed(b),
		"concatenation ambiguity must be resolved by the field separator")
}

// TestStableIndexSeed_ConcurrentCreateConvergence simulates the two-node
// concurrent-CREATE path from design §8.1 L: two independent engines see the
// same base-table vectors and run BulkPopulate with the same IVFSpec seed
// derived from VectorIndexMeta identity. With byte-identical inputs and a
// stable seed, both engines must compute byte-identical centroid blobs so
// HLC-LWW replication of the losing row becomes a no-op.
func TestStableIndexSeed_ConcurrentCreateConvergence(t *testing.T) {
	ctx := context.Background()
	idx := "conv_idx"

	meta := common.VectorIndexMeta{
		IndexName:  idx,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "test",
		Metric:     "l2",
		Dim:        3,
		Nlist:      2,
		Nprobe:     1,
		CreatedAt:  0,
	}

	// Two nodes see the same 4 rows in the same rowid order (CDC replay is
	// rowid-ordered; task #17 determinism contract).
	vecs := [][]float32{
		{1, 0, 0},
		{0.9, 0.1, 0},
		{0, 1, 0},
		{0, 0.9, 0.1},
	}

	run := func(t *testing.T, nodeCreatedAt int64) []byte {
		t.Helper()
		db1, engine := setupPopulateDB(t, idx)
		for i, v := range vecs {
			insertTestVec(t, db1, i+1, v)
		}
		nodeMeta := meta
		nodeMeta.CreatedAt = nodeCreatedAt // node-local, MUST NOT affect centroids
		spec := vecindex.IVFSpec{
			ID:     nodeMeta.IndexName,
			Dim:    nodeMeta.Dim,
			Metric: vecindex.MetricL2,
			Nlist:  nodeMeta.Nlist,
			Nprobe: nodeMeta.Nprobe,
			Seed:   StableIndexSeed(nodeMeta),
		}
		require.NoError(t, BulkPopulate(ctx, db1, engine, 1000, "docs", "embed", spec))

		state, ok := engine.Lookup(idx)
		require.True(t, ok)
		require.NotNil(t, state.ProbeState())
		blob, err := vecindex.EncodeCentroidBlob(state.ProbeState())
		require.NoError(t, err)
		require.NotEmpty(t, blob)
		return blob
	}

	// Two subtests so the t.TempDir()/engine pairs are independent (real
	// two-node scenario: separate databases, separate engines, different HLC).
	var blobA, blobB []byte
	t.Run("node_A", func(t *testing.T) { blobA = run(t, 111111) })
	t.Run("node_B", func(t *testing.T) { blobB = run(t, 222222) })

	require.Equal(t, blobA, blobB,
		"concurrent CREATE on two nodes with the same base vectors must produce byte-identical centroid blobs")
}
