package db

import (
	"database/sql"
	"encoding/binary"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
)

// --- helpers ----------------------------------------------------------------

func encodeVec(t *testing.T, v []float32) []byte {
	t.Helper()
	buf := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

type stubProvider struct {
	assignCalls  atomic.Int64
	assignResult int64
	assignErr    error
	lastIndex    atomic.Value // string
}

func (s *stubProvider) AssignNearest(indexName string, vec []byte) (int64, error) {
	s.assignCalls.Add(1)
	s.lastIndex.Store(indexName)
	if s.assignErr != nil {
		return 0, s.assignErr
	}
	return s.assignResult, nil
}

func (s *stubProvider) TopNprobeClusters(_ string, _ []byte, _ int) ([]int64, error) {
	return nil, nil
}

func withProvider(t *testing.T, p vecindex.VectorUDFProvider) {
	t.Helper()
	SetVectorUDFProvider(p)
	t.Cleanup(func() { SetVectorUDFProvider(nil) })
}

func openVecDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// --- distance UDFs ----------------------------------------------------------

func TestVecDistanceL2(t *testing.T) {
	db := openVecDB(t)
	a := encodeVec(t, []float32{1, 2, 3})
	b := encodeVec(t, []float32{4, 6, 3})
	var got float64
	if err := db.QueryRow("SELECT vec_distance_l2(?, ?)", a, b).Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	// squared L2: (1-4)^2 + (2-6)^2 + (3-3)^2 = 9+16+0 = 25
	if math.Abs(got-25) > 1e-6 {
		t.Errorf("vec_distance_l2 = %v, want 25", got)
	}
}

func TestVecDistanceDot(t *testing.T) {
	db := openVecDB(t)
	a := encodeVec(t, []float32{1, 2, 3})
	b := encodeVec(t, []float32{4, 5, 6})
	var got float64
	if err := db.QueryRow("SELECT vec_distance_dot(?, ?)", a, b).Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	// negative dot: -(4+10+18) = -32
	if math.Abs(got+32) > 1e-5 {
		t.Errorf("vec_distance_dot = %v, want -32", got)
	}
}

func TestVecDistanceCosine(t *testing.T) {
	db := openVecDB(t)
	a := encodeVec(t, []float32{1, 0, 0})
	b := encodeVec(t, []float32{1, 0, 0})
	var got float64
	if err := db.QueryRow("SELECT vec_distance_cosine(?, ?)", a, b).Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if math.Abs(got) > 1e-6 {
		t.Errorf("vec_distance_cosine(identical) = %v, want 0", got)
	}

	c := encodeVec(t, []float32{0, 1, 0})
	if err := db.QueryRow("SELECT vec_distance_cosine(?, ?)", a, c).Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if math.Abs(got-1) > 1e-6 {
		t.Errorf("vec_distance_cosine(orthogonal) = %v, want 1", got)
	}
}

func TestVecDistanceDimensionMismatch(t *testing.T) {
	db := openVecDB(t)
	a := encodeVec(t, []float32{1, 2, 3})
	b := encodeVec(t, []float32{1, 2})
	var got float64
	err := db.QueryRow("SELECT vec_distance_l2(?, ?)", a, b).Scan(&got)
	if err == nil {
		t.Fatalf("expected dimension mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "MARMOT-VEC-014") {
		t.Errorf("error %q does not contain MARMOT-VEC-014", err.Error())
	}
}

func TestVecDistanceEmptyBlob(t *testing.T) {
	db := openVecDB(t)
	var got float64
	err := db.QueryRow("SELECT vec_distance_l2(?, ?)", []byte{}, []byte{}).Scan(&got)
	if err == nil {
		t.Fatalf("expected error on empty blob")
	}
	if !strings.Contains(err.Error(), "MARMOT-VEC-014") {
		t.Errorf("error %q lacks MARMOT-VEC-014", err.Error())
	}
}

func TestVecDistanceUnalignedBlob(t *testing.T) {
	db := openVecDB(t)
	var got float64
	err := db.QueryRow("SELECT vec_distance_l2(?, ?)", []byte{1, 2, 3}, []byte{1, 2, 3}).Scan(&got)
	if err == nil {
		t.Fatalf("expected error on non-multiple-of-4 blob")
	}
	if !strings.Contains(err.Error(), "MARMOT-VEC-014") {
		t.Errorf("error %q lacks MARMOT-VEC-014", err.Error())
	}
}

// --- side-effect UDFs -------------------------------------------------------

func TestVecAssignUsesProvider(t *testing.T) {
	stub := &stubProvider{assignResult: 17}
	withProvider(t, stub)
	db := openVecDB(t)
	var got int64
	if err := db.QueryRow("SELECT __marmot_vec_assign(?, ?)", "embeddings", encodeVec(t, []float32{1, 2, 3, 4})).Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got != 17 {
		t.Errorf("assign = %d, want 17", got)
	}
	if stub.assignCalls.Load() != 1 {
		t.Errorf("assignCalls = %d, want 1", stub.assignCalls.Load())
	}
	if name, _ := stub.lastIndex.Load().(string); name != "embeddings" {
		t.Errorf("lastIndex = %q, want embeddings", name)
	}
}

func TestVecAssignNoProvider(t *testing.T) {
	SetVectorUDFProvider(nil)
	db := openVecDB(t)
	var got int64
	err := db.QueryRow("SELECT __marmot_vec_assign(?, ?)", "x", encodeVec(t, []float32{1, 2, 3, 4})).Scan(&got)
	if err == nil || !strings.Contains(err.Error(), "MARMOT-VEC-013") {
		t.Fatalf("want MARMOT-VEC-013 error, got %v", err)
	}
}

func TestVecMatchSentinel(t *testing.T) {
	db := openVecDB(t)
	var got int64
	err := db.QueryRow("SELECT vec_match(?, ?, ?)", encodeVec(t, []float32{1, 2}), encodeVec(t, []float32{1, 2}), 5).Scan(&got)
	if err == nil {
		t.Fatalf("expected sentinel error")
	}
	if !strings.Contains(err.Error(), "MARMOT-VEC-010") {
		t.Errorf("error %q lacks MARMOT-VEC-010", err.Error())
	}
}

// TestVectorUDFsAcrossConnections confirms the ConnectHook registers the
// UDFs on every new connection, not just the first.
func TestVectorUDFsAcrossConnections(t *testing.T) {
	db := openVecDB(t)
	db.SetMaxOpenConns(4)
	a := encodeVec(t, []float32{1, 2, 3})
	b := encodeVec(t, []float32{1, 2, 3})
	for i := 0; i < 8; i++ {
		var got float64
		if err := db.QueryRow("SELECT vec_distance_l2(?, ?)", a, b).Scan(&got); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if got != 0 {
			t.Fatalf("iter %d: got %v want 0", i, got)
		}
	}
}
