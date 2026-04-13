package coordinator

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/protocol"
)

func TestContainsVecKnn(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT * FROM vec_knn('idx', ?, 10)", true},
		{"SELECT vec_knn('idx', ?, 5)", true},
		{"select VEC_KNN('idx', ?, 3)", true},
		{"SELECT * FROM regular_table", false},
		{"CREATE VECTOR INDEX idx ON t(c)", false},
		{"INSERT INTO vec_knn_log VALUES (1)", false}, // no ( immediately after vec_knn
	}

	for _, tc := range cases {
		got := protocol.ContainsVecKnn(tc.sql)
		if got != tc.want {
			t.Errorf("ContainsVecKnn(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

func TestParseVecKnnCall_Valid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql       string
		wantIndex string
		wantTopK  int
	}{
		{"SELECT * FROM vec_knn('my_index', ?, 10)", "my_index", 10},
		{"SELECT vec_knn('idx', ?, 5)", "idx", 5},
		{"SELECT VEC_KNN('UPPER_IDX', ?, 1)", "UPPER_IDX", 1},
		{"  vec_knn( 'spaced' , ? , 20 )  ", "spaced", 20},
	}

	for _, tc := range cases {
		call, err := protocol.ParseVecKnnCall(tc.sql)
		if err != nil {
			t.Errorf("ParseVecKnnCall(%q) unexpected error: %v", tc.sql, err)
			continue
		}
		if call.IndexName != tc.wantIndex {
			t.Errorf("ParseVecKnnCall(%q).IndexName = %q, want %q", tc.sql, call.IndexName, tc.wantIndex)
		}
		if call.TopK != tc.wantTopK {
			t.Errorf("ParseVecKnnCall(%q).TopK = %d, want %d", tc.sql, call.TopK, tc.wantTopK)
		}
	}
}

func TestParseVecKnnCall_InvalidSyntax(t *testing.T) {
	t.Parallel()

	cases := []string{
		"SELECT * FROM some_table",
		"SELECT vec_knn('idx', 10)",            // missing ? param
		"SELECT vec_knn(idx, ?, 10)",           // unquoted index name
		"SELECT vec_knn('idx', ?, notanumber)", // non-numeric K
	}

	for _, sql := range cases {
		_, err := protocol.ParseVecKnnCall(sql)
		if err == nil {
			t.Errorf("ParseVecKnnCall(%q) expected error, got nil", sql)
		}
	}
}

func TestDecodeFloat32Vector(t *testing.T) {
	t.Parallel()

	want := []float32{1.0, 2.0, 3.0}
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:], math.Float32bits(1.0))
	binary.LittleEndian.PutUint32(buf[4:], math.Float32bits(2.0))
	binary.LittleEndian.PutUint32(buf[8:], math.Float32bits(3.0))

	got := encoding.DecodeFloat32Slice(buf)
	if len(got) != len(want) {
		t.Fatalf("DecodeFloat32Slice: len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("DecodeFloat32Slice[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestDecodeFloat32Vector_Empty(t *testing.T) {
	t.Parallel()

	got := encoding.DecodeFloat32Slice([]byte{})
	if len(got) != 0 {
		t.Errorf("expected empty slice, got len=%d", len(got))
	}
}

func TestHandleVecKnn_NilDbManager(t *testing.T) {
	t.Parallel()

	h := newHandlerWithVecMgr(t, nil)
	h.dbManager = nil

	sql := "SELECT * FROM vec_knn('idx', ?, 5)"
	params := []interface{}{make([]byte, 8)}

	_, err := h.handleVecKnn(nil, sql, params)
	if err == nil {
		t.Fatal("expected error when dbManager is nil")
	}
}

func TestHandleVecKnn_NilVecMgr(t *testing.T) {
	t.Parallel()

	h := newHandlerWithVecMgr(t, nil)

	sql := "SELECT * FROM vec_knn('idx', ?, 5)"
	params := []interface{}{make([]byte, 8)}

	_, err := h.handleVecKnn(nil, sql, params)
	if err == nil {
		t.Fatal("expected error when VectorIndexManager is nil")
	}
}

func TestHandleVecKnn_InvalidSyntax(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{}
	h := newHandlerWithVecMgr(t, stub)

	_, err := h.handleVecKnn(nil, "SELECT * FROM foo", []interface{}{make([]byte, 8)})
	if err == nil {
		t.Fatal("expected error for invalid vec_knn syntax")
	}
}

func TestHandleVecKnn_MissingParam(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{}
	h := newHandlerWithVecMgr(t, stub)

	_, err := h.handleVecKnn(nil, "SELECT * FROM vec_knn('idx', ?, 5)", nil)
	if err == nil {
		t.Fatal("expected error when no params supplied")
	}
}

func TestHandleVecKnn_WrongParamType(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{}
	h := newHandlerWithVecMgr(t, stub)

	_, err := h.handleVecKnn(nil, "SELECT * FROM vec_knn('idx', ?, 5)", []interface{}{"not-bytes"})
	if err == nil {
		t.Fatal("expected error when param is not []byte")
	}
}

func TestHandleVecKnn_SearchError(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{searchErr: errors.New("index not found")}
	h := newHandlerWithVecMgr(t, stub)

	vec := encodeFloat32Vector([]float32{1.0, 2.0})
	_, err := h.handleVecKnn(nil, "SELECT * FROM vec_knn('idx', ?, 5)", []interface{}{vec})
	if err == nil {
		t.Fatal("expected error propagated from Search")
	}
}

func TestHandleVecKnn_Success(t *testing.T) {
	t.Parallel()

	rowid := int64(42)
	extID := make([]byte, 8)
	binary.BigEndian.PutUint64(extID, uint64(rowid))

	stub := &stubVectorIndexManager{
		searchResults: []common.VectorSearchHit{
			{ExternalID: extID, Distance: 0.25, Score: 0.75},
		},
	}
	h := newHandlerWithVecMgr(t, stub)

	vec := encodeFloat32Vector([]float32{1.0, 0.0})
	rs, err := h.handleVecKnn(nil, "SELECT * FROM vec_knn('my_index', ?, 3)", []interface{}{vec})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rs == nil {
		t.Fatal("expected non-nil result set")
	}
	if len(rs.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(rs.Columns))
	}
	if rs.Columns[0].Name != "rowid" || rs.Columns[1].Name != "distance" || rs.Columns[2].Name != "score" {
		t.Errorf("unexpected column names: %v", rs.Columns)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	if rs.Rows[0][0] != int64(42) {
		t.Errorf("rowid = %v, want 42", rs.Rows[0][0])
	}
	if rs.Rows[0][1] != float64(float32(0.25)) {
		t.Errorf("distance = %v, want 0.25", rs.Rows[0][1])
	}
	if rs.Rows[0][2] != float64(float32(0.75)) {
		t.Errorf("score = %v, want 0.75", rs.Rows[0][2])
	}
}

func TestHandleVecKnn_EmptyResults(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{searchResults: []common.VectorSearchHit{}}
	h := newHandlerWithVecMgr(t, stub)

	vec := encodeFloat32Vector([]float32{1.0})
	rs, err := h.handleVecKnn(nil, "SELECT * FROM vec_knn('idx', ?, 10)", []interface{}{vec})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rs.Rows))
	}
}

// TestHandleVecKnn_ViaHandleQuery verifies that HandleQuery intercepts vec_knn()
// before the Vitess parser and routes to vector search.
func TestHandleVecKnn_ViaHandleQuery(t *testing.T) {
	t.Parallel()

	rowid := int64(7)
	extID := make([]byte, 8)
	binary.BigEndian.PutUint64(extID, uint64(rowid))

	stub := &stubVectorIndexManager{
		searchResults: []common.VectorSearchHit{
			{ExternalID: extID, Distance: 0.1, Score: 0.9},
		},
	}
	h := newHandlerWithVecMgr(t, stub)

	session := &protocol.ConnectionSession{ConnID: 1, TranspilationEnabled: true}
	vec := encodeFloat32Vector([]float32{0.5, 0.5})
	rs, err := h.HandleQuery(session, "SELECT * FROM vec_knn('my_index', ?, 5)", []interface{}{vec})
	if err != nil {
		t.Fatalf("HandleQuery vec_knn: unexpected error: %v", err)
	}
	if rs == nil || len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %v", rs)
	}
	if rs.Rows[0][0] != int64(7) {
		t.Errorf("rowid = %v, want 7", rs.Rows[0][0])
	}
}

// encodeFloat32Vector encodes a []float32 as little-endian bytes (for test params).
func encodeFloat32Vector(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// stubSearchVectorIndexManager wraps stubVectorIndexManager and captures Search calls.
type stubSearchCapture struct {
	stubVectorIndexManager
	capturedIndex  string
	capturedVector []float32
	capturedTopK   int
}

func (s *stubSearchCapture) Search(_ context.Context, indexName string, vector []float32, topK int) ([]common.VectorSearchHit, error) {
	s.capturedIndex = indexName
	s.capturedVector = vector
	s.capturedTopK = topK
	return s.searchResults, s.searchErr
}

func TestHandleVecKnn_PassesCorrectArgs(t *testing.T) {
	t.Parallel()

	stub := &stubSearchCapture{}
	h := newHandlerWithVecMgr(t, stub)

	vec := encodeFloat32Vector([]float32{1.0, 2.0, 3.0})
	_, err := h.handleVecKnn(nil, "SELECT * FROM vec_knn('embed_idx', ?, 7)", []interface{}{vec})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stub.capturedIndex != "embed_idx" {
		t.Errorf("index = %q, want %q", stub.capturedIndex, "embed_idx")
	}
	if stub.capturedTopK != 7 {
		t.Errorf("topK = %d, want 7", stub.capturedTopK)
	}
	if len(stub.capturedVector) != 3 {
		t.Errorf("vector len = %d, want 3", len(stub.capturedVector))
	}
	if stub.capturedVector[0] != 1.0 || stub.capturedVector[1] != 2.0 || stub.capturedVector[2] != 3.0 {
		t.Errorf("vector = %v, want [1.0 2.0 3.0]", stub.capturedVector)
	}
}
