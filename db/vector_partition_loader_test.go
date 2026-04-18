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

func TestLoader_DecodeBlob_CosineReadsMaterializedSidecarVectors(t *testing.T) {
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
	blob1, err := materializeVectorBlob(encodeLEBlob(v1), vecindex.MetricCosine, dim, 0)
	require.NoError(t, err)
	first := loader.decodeBlob(blob1)
	require.NotNil(t, first)
	require.InDelta(t, 0.6, first[0], 1e-5, "first component of v1 after normalisation must be 3/5")
	require.InDelta(t, 0.8, first[1], 1e-5, "second component of v1 after normalisation must be 4/5")
	assertUnitNorm(t, first, 1e-5)

	blob2, err := materializeVectorBlob(encodeLEBlob(v2), vecindex.MetricCosine, dim, 0)
	require.NoError(t, err)
	second := loader.decodeBlob(blob2)
	require.NotNil(t, second)
	assertUnitNorm(t, second, 1e-5)
}

// TestLoader_UsesSidecarVecInsteadOfBaseTableBlob catches regressions back to
// the old members JOIN base design: the loader must ignore docs.embed and read
// only the clustered sidecar vec column.
func TestLoader_LoadDelta_UsesSidecarVecInsteadOfBaseTableBlob(t *testing.T) {
	t.Parallel()
	const (
		dim = 4
		idx = "sidecaronly"
	)
	db, base := newLoaderTestDB(t, idx)

	baseVec := []float32{9, 9, 9, 9}
	sidecarVec := []float32{1, 0, 0, 0}
	seedBaseVector(t, db, 201, baseVec)
	seedMemberBlob(t, db, idx, 0, 201, encodeLEBlob(sidecarVec))

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricCosine)
	got, err := loader.loadDelta(context.Background())
	require.NoError(t, err)
	require.Len(t, got.Snapshot(), 1)
	require.Equal(t, int64(201), got.Snapshot()[0].RowID)
	require.Equal(t, sidecarVec, got.Snapshot()[0].Vec)
}

// TestLoader_L2PreservesRawBytes pins: MetricL2 must return bit-identical
// float32 values. No normalisation, no mutation — the cached slice must be
// exactly decodable back to the stored bits.
func TestLoader_DecodeBlob_L2PreservesRawBytes(t *testing.T) {
	t.Parallel()
	const (
		dim = 8
		idx = "l2raw"
	)
	db, base := newLoaderTestDB(t, idx)

	raw := make([]float32, dim)
	raw[0], raw[1] = 3, 4 // norm = 5; must NOT be normalised.
	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricL2)
	blob, err := materializeVectorBlob(encodeLEBlob(raw), vecindex.MetricL2, dim, 0)
	require.NoError(t, err)
	out := loader.decodeBlob(blob)
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
func TestLoader_DecodeBlob_DotPreservesRawBytes(t *testing.T) {
	t.Parallel()
	const (
		dim = 8
		idx = "dotraw"
	)
	db, base := newLoaderTestDB(t, idx)

	raw := make([]float32, dim)
	raw[0], raw[1] = 3, 4
	loader := newSQLPartitionLoader(db, idx, base, "embed", dim+1, vecindex.MetricDot)
	blob, err := materializeVectorBlob(encodeLEBlob(raw), vecindex.MetricDot, dim, metric.Norm(raw)+1)
	require.NoError(t, err)
	out := loader.decodeBlob(blob)
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
		seedMaterializedVector(t, db, idx, 0, rowID, v, vecindex.MetricCosine)
	}

	loader := newSQLPartitionLoader(db, idx, base, "embed", dim, vecindex.MetricCosine)
	got, err := loader.loadDelta(context.Background())
	require.NoError(t, err)
	snap := got.Snapshot()
	require.Len(t, snap, n)

	for _, entry := range snap {
		rid := entry.RowID
		cv := entry.Vec
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
