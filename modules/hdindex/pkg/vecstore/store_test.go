package vecstore

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func openTestStore(t *testing.T, dim int) *Store {
	t.Helper()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return Open(db, dim)
}

func TestPutGetVector(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 4)
	vec := []float32{1.0, 2.5, -3.14, 0.0}
	if err := s.PutVector(nil, 1, vec); err != nil {
		t.Fatalf("PutVector: %v", err)
	}
	got, err := s.GetVector(1)
	if err != nil {
		t.Fatalf("GetVector: %v", err)
	}
	if len(got) != len(vec) {
		t.Fatalf("len mismatch: got %d, want %d", len(got), len(vec))
	}
	for i := range vec {
		if got[i] != vec[i] {
			t.Errorf("vec[%d]: got %v, want %v", i, got[i], vec[i])
		}
	}
}

func TestGetVectors_Batch(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 3)
	stored := map[uint64][]float32{
		1: {1.0, 2.0, 3.0},
		2: {4.0, 5.0, 6.0},
		3: {7.0, 8.0, 9.0},
		4: {10.0, 11.0, 12.0},
		5: {13.0, 14.0, 15.0},
	}
	for id, v := range stored {
		if err := s.PutVector(nil, id, v); err != nil {
			t.Fatalf("PutVector(%d): %v", id, err)
		}
	}

	query := []uint64{1, 3, 5}
	result, err := s.GetVectors(query)
	if err != nil {
		t.Fatalf("GetVectors: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	for _, id := range query {
		got, ok := result[id]
		if !ok {
			t.Errorf("missing doc_id %d", id)
			continue
		}
		want := stored[id]
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("doc_id %d vec[%d]: got %v, want %v", id, i, got[i], want[i])
			}
		}
	}
}

func TestIDMapping_Roundtrip(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)
	extID := []byte("user:42")
	docID := uint64(7)

	if err := s.PutIDMapping(nil, extID, docID); err != nil {
		t.Fatalf("PutIDMapping: %v", err)
	}

	gotDoc, err := s.GetDocID(extID)
	if err != nil {
		t.Fatalf("GetDocID: %v", err)
	}
	if gotDoc != docID {
		t.Errorf("GetDocID: got %d, want %d", gotDoc, docID)
	}

	gotExt, err := s.GetExternalID(docID)
	if err != nil {
		t.Fatalf("GetExternalID: %v", err)
	}
	if string(gotExt) != string(extID) {
		t.Errorf("GetExternalID: got %q, want %q", gotExt, extID)
	}
}

func TestDeleteIDMapping(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)
	extID := []byte("user:99")
	docID := uint64(5)

	if err := s.PutIDMapping(nil, extID, docID); err != nil {
		t.Fatalf("PutIDMapping: %v", err)
	}
	if err := s.DeleteIDMapping(nil, extID, docID); err != nil {
		t.Fatalf("DeleteIDMapping: %v", err)
	}

	if _, err := s.GetDocID(extID); err != pebble.ErrNotFound {
		t.Errorf("GetDocID after delete: expected ErrNotFound, got %v", err)
	}
	if _, err := s.GetExternalID(docID); err != pebble.ErrNotFound {
		t.Errorf("GetExternalID after delete: expected ErrNotFound, got %v", err)
	}
}

func TestDeleteVector(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 3)
	vec := []float32{1.0, 2.0, 3.0}
	if err := s.PutVector(nil, 10, vec); err != nil {
		t.Fatalf("PutVector: %v", err)
	}
	if err := s.DeleteVector(nil, 10); err != nil {
		t.Fatalf("DeleteVector: %v", err)
	}
	if _, err := s.GetVector(10); err != pebble.ErrNotFound {
		t.Errorf("GetVector after delete: expected ErrNotFound, got %v", err)
	}
}

func TestReverseHilbert_Roundtrip(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)
	docID := uint64(42)
	hilbertKeys := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02}

	if err := s.PutReverseHilbert(nil, docID, hilbertKeys); err != nil {
		t.Fatalf("PutReverseHilbert: %v", err)
	}
	got, err := s.GetReverseHilbert(docID)
	if err != nil {
		t.Fatalf("GetReverseHilbert: %v", err)
	}
	if len(got) != len(hilbertKeys) {
		t.Fatalf("length mismatch: got %d, want %d", len(got), len(hilbertKeys))
	}
	for i := range hilbertKeys {
		if got[i] != hilbertKeys[i] {
			t.Errorf("hilbert[%d]: got 0x%02X, want 0x%02X", i, got[i], hilbertKeys[i])
		}
	}
}

func TestNextDocID_Increments(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)
	for want := uint64(1); want <= 5; want++ {
		got, err := s.NextDocID(nil)
		if err != nil {
			t.Fatalf("NextDocID: %v", err)
		}
		if got != want {
			t.Errorf("NextDocID call %d: got %d, want %d", want, got, want)
		}
	}
}

// TestNextDocID_UniqueAcrossBatch verifies that multiple NextDocID calls within
// a single uncommitted batch all return distinct, monotonically increasing IDs.
// This is the key invariant: bulk indexing allocates N vectors into one batch
// and each must get a unique docID even before the batch is committed.
func TestNextDocID_UniqueAcrossBatch(t *testing.T) {
	t.Parallel()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := Open(db, 2)

	batch := db.NewBatch()

	const n = 10
	ids := make([]uint64, n)
	for i := range n {
		id, err := s.NextDocID(batch)
		if err != nil {
			t.Fatalf("NextDocID call %d: %v", i, err)
		}
		ids[i] = id
	}

	// All IDs must be unique and contiguous 1..n.
	seen := make(map[uint64]bool, n)
	for i, id := range ids {
		if id < 1 || id > n {
			t.Errorf("ids[%d]=%d outside [1,%d]", i, id, n)
		}
		if seen[id] {
			t.Errorf("duplicate docID %d at index %d", id, i)
		}
		seen[id] = true
	}

	// Commit the batch.
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("batch.Commit: %v", err)
	}
	batch.Close()

	// After commit, the next allocation must not collide with any previously issued ID.
	nextID, err := s.NextDocID(nil)
	if err != nil {
		t.Fatalf("NextDocID after commit: %v", err)
	}
	if nextID != n+1 {
		t.Errorf("expected docID=%d after committing %d, got %d", n+1, n, nextID)
	}
}

func TestVectorCount(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)

	count, err := s.GetVectorCount()
	if err != nil {
		t.Fatalf("GetVectorCount (initial): %v", err)
	}
	if count != 0 {
		t.Errorf("initial count: got %d, want 0", count)
	}

	if err := s.SetVectorCount(nil, 10); err != nil {
		t.Fatalf("SetVectorCount: %v", err)
	}
	if err := s.IncrementVectorCount(nil, 5); err != nil {
		t.Fatalf("IncrementVectorCount +5: %v", err)
	}
	if err := s.IncrementVectorCount(nil, -3); err != nil {
		t.Fatalf("IncrementVectorCount -3: %v", err)
	}

	count, err = s.GetVectorCount()
	if err != nil {
		t.Fatalf("GetVectorCount: %v", err)
	}
	if count != 12 {
		t.Errorf("vector count: got %d, want 12", count)
	}
}

func TestWatermark(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)

	txnID, seqID, err := s.GetWatermark()
	if err != nil {
		t.Fatalf("GetWatermark (initial): %v", err)
	}
	if txnID != 0 || seqID != 0 {
		t.Errorf("initial watermark: got (%d, %d), want (0, 0)", txnID, seqID)
	}

	if err := s.SetWatermark(nil, 100, 42); err != nil {
		t.Fatalf("SetWatermark: %v", err)
	}
	gotTxn, gotSeq, err := s.GetWatermark()
	if err != nil {
		t.Fatalf("GetWatermark: %v", err)
	}
	if gotTxn != 100 || gotSeq != 42 {
		t.Errorf("watermark: got (%d, %d), want (100, 42)", gotTxn, gotSeq)
	}
}

func TestGetDocID_NotFound(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)
	_, err := s.GetDocID([]byte("nonexistent"))
	if err != pebble.ErrNotFound {
		t.Errorf("expected pebble.ErrNotFound, got %v", err)
	}
}

func TestGetVector_NotFound(t *testing.T) {
	t.Parallel()
	s := openTestStore(t, 2)
	_, err := s.GetVector(9999)
	if err != pebble.ErrNotFound {
		t.Errorf("expected pebble.ErrNotFound, got %v", err)
	}
}
