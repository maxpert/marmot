package rdb

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func openTestDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func makeRefDists(m int, val float32) []float32 {
	dists := make([]float32, m)
	for i := range m {
		dists[i] = val + float32(i)*0.1
	}
	return dists
}

func TestPutAndScanNearest(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 4)

	// Insert 100 entries in partition 0 with sequential Hilbert keys.
	for i := range 100 {
		hk := []byte{byte(i / 256), byte(i % 256)}
		if err := s.Put(nil, 0, hk, uint64(i), makeRefDists(4, float32(i))); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}

	entries, err := s.ScanNearest(0, []byte{0, 50}, 10)
	if err != nil {
		t.Fatalf("ScanNearest: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("expected 10 entries, got %d", len(entries))
	}
}

func TestScanNearest_Bidirectional(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 2)

	keys := []byte{1, 2, 3, 5, 6, 7, 10, 20, 30}
	for i, k := range keys {
		hk := []byte{0, k}
		if err := s.Put(nil, 0, hk, uint64(i+1), makeRefDists(2, float32(k))); err != nil {
			t.Fatalf("Put key=%d: %v", k, err)
		}
	}

	// Query from key=6, alpha=5. Expect the 5 closest keys to 6.
	// Keys sorted: 1,2,3,5,6,7,10,20,30
	// Distance from 6: |1-6|=5, |2-6|=4, |3-6|=3, |5-6|=1, |6-6|=0, |7-6|=1, |10-6|=4, |20-6|=14, |30-6|=24
	// 5 closest: 6(0), 5(1), 7(1), 3(3), 2(4) or 10(4)
	entries, err := s.ScanNearest(0, []byte{0, 6}, 5)
	if err != nil {
		t.Fatalf("ScanNearest: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}

	// Collect returned doc IDs (which correspond to index+1 of the keys slice).
	docIDs := make(map[uint64]bool, len(entries))
	for _, e := range entries {
		docIDs[e.DocID] = true
	}

	// The bidirectional scan from key=6 forward gets: 6,7,10,20,30 and backward gets: 5,3,2,1
	// Alternating: fwd=6, bwd=5, fwd=7, bwd=3, fwd=10 => docIDs: 5(key=6),4(key=5),6(key=7),3(key=3),7(key=10)
	expected := []uint64{5, 4, 6, 3, 7} // indices into keys[]+1
	for _, docID := range expected {
		if !docIDs[docID] {
			t.Errorf("expected docID %d in results, got docIDs: %v", docID, docIDs)
		}
	}
}

func TestScanNearest_EdgeStart(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 2)

	for i := range 10 {
		hk := []byte{0, byte(i + 5)} // keys 5..14
		if err := s.Put(nil, 0, hk, uint64(i), makeRefDists(2, float32(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Seek from key smaller than all entries: should return first alpha entries.
	entries, err := s.ScanNearest(0, []byte{0, 0}, 4)
	if err != nil {
		t.Fatalf("ScanNearest: %v", err)
	}
	if len(entries) != 4 {
		t.Errorf("expected 4 entries, got %d", len(entries))
	}
}

func TestScanNearest_EdgeEnd(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 2)

	for i := range 10 {
		hk := []byte{0, byte(i)} // keys 0..9
		if err := s.Put(nil, 0, hk, uint64(i), makeRefDists(2, float32(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Seek from key larger than all entries: should return the last alpha entries from backward scan.
	entries, err := s.ScanNearest(0, []byte{0, 255}, 4)
	if err != nil {
		t.Fatalf("ScanNearest: %v", err)
	}
	if len(entries) != 4 {
		t.Errorf("expected 4 entries, got %d", len(entries))
	}
}

func TestScanNearest_AlphaExceedsCount(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 2)

	for i := range 20 {
		hk := []byte{0, byte(i)}
		if err := s.Put(nil, 0, hk, uint64(i), makeRefDists(2, float32(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	entries, err := s.ScanNearest(0, []byte{0, 10}, 100)
	if err != nil {
		t.Fatalf("ScanNearest: %v", err)
	}
	if len(entries) != 20 {
		t.Errorf("expected 20 entries (all), got %d", len(entries))
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 2)

	type kv struct {
		hk    []byte
		docID uint64
	}
	items := []kv{
		{[]byte{0, 1}, 1},
		{[]byte{0, 2}, 2},
		{[]byte{0, 3}, 3},
		{[]byte{0, 4}, 4},
		{[]byte{0, 5}, 5},
	}
	for _, it := range items {
		if err := s.Put(nil, 0, it.hk, it.docID, makeRefDists(2, float32(it.docID))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Delete doc 3 (key={0,3}).
	if err := s.Delete(nil, 0, []byte{0, 3}, 3); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	entries, err := s.ScanNearest(0, []byte{0, 0}, 10)
	if err != nil {
		t.Fatalf("ScanNearest: %v", err)
	}
	if len(entries) != 4 {
		t.Errorf("expected 4 entries after delete, got %d", len(entries))
	}
	for _, e := range entries {
		if e.DocID == 3 {
			t.Error("deleted entry docID=3 still present")
		}
	}
}

func TestMultiplePartitions(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 2)

	// Insert 5 entries per partition for partitions 0, 1, 2.
	for p := range 3 {
		for i := range 5 {
			hk := []byte{0, byte(i)}
			docID := uint64(p*100 + i)
			if err := s.Put(nil, p, hk, docID, makeRefDists(2, float32(docID))); err != nil {
				t.Fatalf("Put p=%d i=%d: %v", p, i, err)
			}
		}
	}

	// Scan partition 1 only.
	entries, err := s.ScanNearest(1, []byte{0, 2}, 10)
	if err != nil {
		t.Fatalf("ScanNearest: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("expected 5 entries from partition 1, got %d", len(entries))
	}

	// All docIDs must be in range [100, 104].
	for _, e := range entries {
		if e.DocID < 100 || e.DocID > 104 {
			t.Errorf("cross-partition contamination: docID=%d not in partition 1 range", e.DocID)
		}
	}
}

func TestPut_BatchMode(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	s := Open(db, 3)

	batch := db.NewBatch()

	for i := range 10 {
		hk := []byte{0, byte(i)}
		if err := s.Put(batch, 0, hk, uint64(i), makeRefDists(3, float32(i))); err != nil {
			t.Fatalf("Put batch[%d]: %v", i, err)
		}
	}

	// Nothing committed yet — scan should return 0 results.
	entries, err := s.ScanNearest(0, []byte{0, 5}, 10)
	if err != nil {
		t.Fatalf("ScanNearest before commit: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries before batch commit, got %d", len(entries))
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("batch commit: %v", err)
	}
	batch.Close()

	entries, err = s.ScanNearest(0, []byte{0, 5}, 10)
	if err != nil {
		t.Fatalf("ScanNearest after commit: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("expected 10 entries after batch commit, got %d", len(entries))
	}
}

func BenchmarkScanNearest_10000entries_alpha4096(b *testing.B) {
	dir := b.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		b.Fatalf("open pebble: %v", err)
	}
	defer db.Close()

	s := Open(db, 8)

	// Insert 10000 entries using a batch for speed.
	batch := db.NewBatch()
	for i := range 10000 {
		hk := []byte{byte(i >> 8), byte(i & 0xff)}
		dists := makeRefDists(8, float32(i))
		if err := s.Put(batch, 0, hk, uint64(i), dists); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		b.Fatalf("batch commit: %v", err)
	}
	batch.Close()

	queryKey := []byte{0x13, 0x88} // key=5000

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entries, err := s.ScanNearest(0, queryKey, 4096)
		if err != nil {
			b.Fatalf("ScanNearest: %v", err)
		}
		_ = entries
	}
}
