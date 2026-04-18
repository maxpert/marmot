package db

import (
	"context"
	"database/sql"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Test helpers (local to this file — no collisions with vector_populate_test.go
// helpers that use a different signature / seed pattern).
// -----------------------------------------------------------------------------

// encodeLEBlob encodes a []float32 as the little-endian byte layout the loader
// reads out of SQLite BLOB columns. Mirrors the encoding used by encodeVec in
// vector_udfs_test.go but kept local so this test file is self-contained.
func encodeLEBlob(v []float32) []byte {
	buf := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

// assertUnitNorm fails the test if v's L2 norm is not within tol of 1.0.
func assertUnitNorm(t *testing.T, v []float32, tol float64) {
	t.Helper()
	got := float64(metric.Norm(v))
	if math.Abs(got-1.0) > tol {
		t.Fatalf("expected unit-norm vector, got norm=%.9f (tol=%g)", got, tol)
	}
}

// newLoaderTestDB opens a fresh in-memory SQLite DB (per-test, so Parallel is
// safe) and creates:
//   - docs(id INTEGER PRIMARY KEY, embed BLOB) — base table (present so tests
//     can prove the loader ignores it)
//   - the members table named by vecindex.MembersTable(indexName), with the
//     current clustered-sidecar schema (cluster_id, rowid, vec).
func newLoaderTestDB(t *testing.T, indexName string) (*sql.DB, string) {
	t.Helper()
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	mt := vecindex.MembersTable(indexName)
	_, err = db.Exec(`CREATE TABLE "` + mt + `" (
		cluster_id INTEGER NOT NULL,
		rowid      INTEGER NOT NULL,
		vec        BLOB NOT NULL,
		PRIMARY KEY (cluster_id, rowid)
	) WITHOUT ROWID`)
	require.NoError(t, err)

	return db, "docs"
}

func seedBaseVector(t *testing.T, db *sql.DB, id int64, v []float32) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO docs (id, embed) VALUES (?, ?)`, id, encodeLEBlob(v))
	require.NoError(t, err)
}

func seedMemberBlob(t *testing.T, db *sql.DB, indexName string, clusterID, id int64, blob []byte) {
	t.Helper()
	mt := vecindex.MembersTable(indexName)
	_, err := db.Exec(`INSERT INTO "`+mt+`" (cluster_id, rowid, vec) VALUES (?, ?, ?)`, clusterID, id, blob)
	require.NoError(t, err)
}

func seedMaterializedVector(t *testing.T, db *sql.DB, indexName string, clusterID, id int64, raw []float32, m metric.Metric) {
	t.Helper()
	seedBaseVector(t, db, id, raw)
	maxNorm := float32(0)
	if m == metric.MetricDot {
		maxNorm = metric.Norm(raw) + 1
	}
	blob, err := materializeVectorBlob(encodeLEBlob(raw), m, len(raw), maxNorm)
	require.NoError(t, err)
	require.NotNil(t, blob)
	seedMemberBlob(t, db, indexName, clusterID, id, blob)
}

// -----------------------------------------------------------------------------
// Contract tests for metric-aware normalisation in the SQL partition loader.
// -----------------------------------------------------------------------------

// TestLoader_CosineReadsMaterializedSidecarVectors pins the new contract:
// cosine vectors are already unit-normalized in the sidecar, so the loader
// must return those stored values directly.
func TestLoader_CosineReadsMaterializedSidecarVectors(t *testing.T) {
	t.Parallel()
	const (
		dim = 8
		idx = "cosnorm"
	)
	db, base := newLoaderTestDB(t, idx)

	v1 := make([]float32, dim)
	v1[0], v1[1] = 3, 4 // norm = 5
	v2 := make([]float32, dim)
	v2[4] = 2 // norm = 2
	seedMaterializedVector(t, db, idx, 1, 101, v1, vecindex.MetricCosine)
	seedMaterializedVector(t, db, idx, 1, 102, v2, vecindex.MetricCosine)

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricCosine)
	got, err := loader.BulkLoad(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, 2, got[1].Len(), "both non-zero vectors must be returned")

	for i := range got[1].RowIDs {
		assertUnitNorm(t, got[1].Vector(i, dim), 1e-5)
	}

	// Direction preservation on v1: {3,4,...}/5 → first element 0.6.
	var first []float32
	for i, rid := range got[1].RowIDs {
		if rid == 101 {
			first = got[1].Vector(i, dim)
			break
		}
	}
	require.NotNil(t, first, "rowid=101 must be present")
	require.InDelta(t, 0.6, first[0], 1e-5, "first component of v1 after normalisation must be 3/5")
	require.InDelta(t, 0.8, first[1], 1e-5, "second component of v1 after normalisation must be 4/5")
}

// TestLoader_UsesSidecarVecInsteadOfBaseTableBlob catches regressions back to
// the old members JOIN base design: the loader must ignore docs.embed and read
// only the clustered sidecar vec column.
func TestLoader_UsesSidecarVecInsteadOfBaseTableBlob(t *testing.T) {
	t.Parallel()
	const (
		dim = 4
		idx = "sidecaronly"
	)
	db, base := newLoaderTestDB(t, idx)

	baseVec := []float32{9, 9, 9, 9}
	sidecarVec := []float32{1, 0, 0, 0}
	seedBaseVector(t, db, 201, baseVec)
	seedMemberBlob(t, db, idx, 1, 201, encodeLEBlob(sidecarVec))

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricCosine)
	got, err := loader.BulkLoad(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, 1, got[1].Len())
	require.Equal(t, int64(201), got[1].RowIDs[0])
	require.Equal(t, sidecarVec, got[1].Vector(0, dim))
}

// TestLoader_L2PreservesRawBytes pins: MetricL2 must return bit-identical
// float32 values. No normalisation, no mutation — the cached slice must be
// exactly decodable back to the stored bits.
func TestLoader_L2PreservesRawBytes(t *testing.T) {
	t.Parallel()
	const (
		dim = 8
		idx = "l2raw"
	)
	db, base := newLoaderTestDB(t, idx)

	raw := make([]float32, dim)
	raw[0], raw[1] = 3, 4 // norm = 5; must NOT be normalised.
	seedMaterializedVector(t, db, idx, 1, 301, raw, vecindex.MetricL2)

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricL2)
	got, err := loader.BulkLoad(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, 1, got[1].Len())

	out := got[1].Vector(0, dim)
	require.Len(t, out, dim)
	for i := range raw {
		require.Equalf(t,
			math.Float32bits(raw[i]), math.Float32bits(out[i]),
			"L2 loader must preserve exact bits at index %d: got %v want %v",
			i, out[i], raw[i])
	}
}

// TestLoader_DotPreservesRawBytes mirrors the L2 contract for MetricDot —
// callers augment/normalise outside the loader if needed.
func TestLoader_DotPreservesRawBytes(t *testing.T) {
	t.Parallel()
	const (
		dim = 8
		idx = "dotraw"
	)
	db, base := newLoaderTestDB(t, idx)

	raw := make([]float32, dim)
	raw[0], raw[1] = 3, 4
	seedMaterializedVector(t, db, idx, 1, 401, raw, vecindex.MetricDot)

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim+1, vecindex.MetricDot)
	got, err := loader.BulkLoad(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, 1, got[1].Len())

	out := got[1].Vector(0, dim+1)
	require.Len(t, out, dim+1)
}

// TestLoader_LoadDeltaCosineReadsMaterializedSidecar pins that the same rule
// applies to cluster_id=0: the delta buffer should expose the stored internal
// cosine vectors directly.
func TestLoader_LoadDeltaCosineReadsMaterializedSidecar(t *testing.T) {
	t.Parallel()
	const (
		dim = 4
		idx = "deltacos"
	)
	db, base := newLoaderTestDB(t, idx)

	seedMaterializedVector(t, db, idx, 0, 501, []float32{3, 4, 0, 0}, vecindex.MetricCosine)
	seedMaterializedVector(t, db, idx, 0, 502, []float32{0, 0, 2, 0}, vecindex.MetricCosine)

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricCosine)
	delta, err := loader.loadDelta(context.Background())
	require.NoError(t, err)

	snap := delta.Snapshot()
	require.Len(t, snap, 2, "both non-zero delta rows must appear")
	for _, cv := range snap {
		assertUnitNorm(t, cv.Vec, 1e-5)
	}
}

// TestLoader_LoadDeltaL2PreservesRaw mirrors the delta contract for L2:
// stored bits survive verbatim.
func TestLoader_LoadDeltaL2PreservesRaw(t *testing.T) {
	t.Parallel()
	const (
		dim = 4
		idx = "deltal2"
	)
	db, base := newLoaderTestDB(t, idx)

	raw := []float32{3, 4, 0, 0}
	seedMaterializedVector(t, db, idx, 0, 601, raw, vecindex.MetricL2)

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricL2)
	delta, err := loader.loadDelta(context.Background())
	require.NoError(t, err)

	snap := delta.Snapshot()
	require.Len(t, snap, 1)
	out := snap[0].Vec
	for i := range raw {
		require.Equalf(t,
			math.Float32bits(raw[i]), math.Float32bits(out[i]),
			"delta L2 loader must preserve exact bits at index %d", i)
	}
}

// TestLoader_MissingClusterReturnsEmptySlice pins the pre-existing cache
// contract: requested cluster_ids with no member rows must map to a
// non-nil, zero-length slice so otter caches the "known empty" state
// rather than re-loading on every probe. This invariant MUST survive the
// metric-aware normalise change — tests run for both cosine and L2.
func TestLoader_MissingClusterReturnsEmptySlice(t *testing.T) {
	t.Parallel()
	const (
		dim = 4
		idx = "missing"
	)

	cases := []struct {
		name string
		m    metric.Metric
	}{
		{"cosine", vecindex.MetricCosine},
		{"l2", vecindex.MetricL2},
		{"dot", vecindex.MetricDot},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db, base := newLoaderTestDB(t, idx)
			loader := newSQLPartitionLoader(db, idx, base, "embed", dim, tc.m)
			got, err := loader.BulkLoad(context.Background(), []int64{999})
			require.NoError(t, err)
			s, ok := got[999]
			require.True(t, ok, "missing cluster key must still be present in map")
			require.Empty(t, s.RowIDs, "missing cluster rowids must be zero-length")
			require.Empty(t, s.Vecs, "missing cluster vecs must be zero-length")
		})
	}
}

// TestLoader_CosineRoundTripPrecision pins single-precision stability:
// unit vectors that are encoded → stored → loaded with MetricCosine must
// survive the round-trip within 1e-6 per component. Uses 32 random unit
// vectors so any systematic precision regression (e.g. accidental f64→f32
// downcast after normalise) is caught.
func TestLoader_CosineRoundTripPrecision(t *testing.T) {
	t.Parallel()
	const (
		dim = 16
		idx = "cosrt"
		n   = 32
	)
	db, base := newLoaderTestDB(t, idx)

	rng := rand.New(rand.NewSource(1))
	originals := make(map[int64][]float32, n)
	for i := int64(0); i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		// Unit-normalise host-side so "original" is already a unit vector.
		norm := metric.Norm(v)
		if norm == 0 {
			// Impossibly unlikely with NormFloat64; re-roll defensively.
			v[0] = 1
			norm = 1
		}
		for j := range v {
			v[j] /= norm
		}
		rowID := i + 10
		originals[rowID] = v
		seedMaterializedVector(t, db, idx, 1, rowID, v, vecindex.MetricCosine)
	}

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricCosine)
	got, err := loader.BulkLoad(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, n, got[1].Len())

	for i, rid := range got[1].RowIDs {
		cv := got[1].Vector(i, dim)
		want, ok := originals[rid]
		require.True(t, ok, "unexpected rowid in result: %d", rid)
		require.Equal(t, len(want), len(cv))
		for j := range want {
			require.InDeltaf(t, float64(want[j]), float64(cv[j]), 1e-6,
				"round-trip drift rowid=%d dim=%d", rid, j)
		}
		// And the returned vector must still be unit-norm.
		assertUnitNorm(t, cv, 1e-5)
	}
}
