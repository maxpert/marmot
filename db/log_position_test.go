//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestLogPositionCompareAndLess(t *testing.T) {
	cases := []struct {
		name string
		a, b LogPosition
		want int
	}{
		{"equal", LogPosition{1, 1}, LogPosition{1, 1}, 0},
		{"lower seq", LogPosition{1, 5}, LogPosition{2, 1}, -1},
		{"higher seq", LogPosition{3, 1}, LogPosition{2, 5}, 1},
		{"same seq lower txn", LogPosition{5, 1}, LogPosition{5, 2}, -1},
		{"same seq higher txn", LogPosition{5, 9}, LogPosition{5, 2}, 1},
		{"zero precedes real entry", LogPosition{}, LogPosition{1, 1}, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.a.Compare(c.b); got != c.want {
				t.Fatalf("Compare(%v,%v) = %d, want %d", c.a, c.b, got, c.want)
			}
			wantLess := c.want < 0
			if got := c.a.Less(c.b); got != wantLess {
				t.Fatalf("Less(%v,%v) = %v, want %v", c.a, c.b, got, wantLess)
			}
		})
	}
}

func TestLogPositionEncodeDecodeRoundTrip(t *testing.T) {
	p := LogPosition{Seq: 12345, TxnID: 67890}
	got := decodeLogPosition(encodeLogPosition(p))
	if got != p {
		t.Fatalf("round trip = %v, want %v", got, p)
	}
	if z := decodeLogPosition(nil); z != (LogPosition{}) {
		t.Fatalf("short buffer decode = %v, want zero value", z)
	}
}

func openTestPebbleDB(t *testing.T) *pebble.DB {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "logseq.pebble")
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestLogSeqAllocatorInitAboveOldLeasesAndMaxSeqNum enforces the migration
// rule: the very first allocation of the
// new store-wide sequence must be strictly above every pre-upgrade
// per-origin /seq/{nodeID} lease and above GetMaxSeqNum.
func TestLogSeqAllocatorInitAboveOldLeasesAndMaxSeqNum(t *testing.T) {
	db := openTestPebbleDB(t)

	// Simulate two pre-upgrade per-origin leases.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 500)
	if err := db.Set(pebbleSeqKey(1), buf, pebble.Sync); err != nil {
		t.Fatalf("seed lease 1: %v", err)
	}
	binary.BigEndian.PutUint64(buf, 900)
	if err := db.Set(pebbleSeqKey(2), buf, pebble.Sync); err != nil {
		t.Fatalf("seed lease 2: %v", err)
	}
	// Simulate an existing seq-index entry above both leases.
	if err := db.Set(pebbleTxnSeqKey(1200, 42), nil, pebble.Sync); err != nil {
		t.Fatalf("seed seq index: %v", err)
	}

	alloc, err := newLogSeqAllocator(db, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) {
		return computeInitialLogSeq(db)
	})
	if err != nil {
		t.Fatalf("newLogSeqAllocator: %v", err)
	}
	defer alloc.close()

	first := mustAllocate(t, alloc)
	if first <= 900 || first <= 1200 {
		t.Fatalf("first allocated seq %d must be strictly above the old leases (900) and GetMaxSeqNum (1200)", first)
	}
}

// TestLogSeqAllocatorResumesAtDurableEndAcrossReopen enforces the crash
// safety rule (Q3): a seq is never reissued across a restart, even though
// commit records themselves are written with NoSync.
func TestLogSeqAllocatorResumesAtDurableEndAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logseq.pebble")
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	alloc, err := newLogSeqAllocator(db, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) { return 1, nil })
	if err != nil {
		t.Fatalf("newLogSeqAllocator: %v", err)
	}
	seen := make(map[uint64]bool)
	for i := 0; i < 5; i++ {
		s := mustAllocate(t, alloc)
		alloc.markDone(s)
		seen[s] = true
	}
	alloc.close()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	alloc2, err := newLogSeqAllocator(db2, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) {
		t.Fatal("initialFn must not run once the key exists")
		return 0, nil
	})
	if err != nil {
		t.Fatalf("newLogSeqAllocator (reopen): %v", err)
	}
	defer alloc2.close()

	for i := 0; i < 20; i++ {
		s := mustAllocate(t, alloc2)
		alloc2.markDone(s)
		if seen[s] {
			t.Fatalf("seq %d was reissued after reopen", s)
		}
	}
}

// TestLogSeqAllocatorExtendsAsyncAndDoesNotReissue exercises many
// allocations across the bandwidth boundary, forcing at least one async
// lease extension, and checks every seq is unique and increasing.
func TestLogSeqAllocatorExtendsAsyncAndDoesNotReissue(t *testing.T) {
	db := openTestPebbleDB(t)
	alloc, err := newLogSeqAllocator(db, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) { return 1, nil })
	if err != nil {
		t.Fatalf("newLogSeqAllocator: %v", err)
	}
	defer alloc.close()

	const n = logSeqBandwidth * 3
	var last uint64
	first := true
	for i := 0; i < n; i++ {
		s := mustAllocate(t, alloc)
		alloc.markDone(s)
		if !first && s != last+1 {
			t.Fatalf("seq not contiguous: got %d after %d", s, last)
		}
		last = s
		first = false
	}
}

func TestLogSeqTrackerStableSeqExactness(t *testing.T) {
	var tr logSeqTracker
	tr.watermark.Store(0)

	// Out-of-order completion: 2 finishes before 1: stable must stay 0.
	tr.markDone(2)
	if got := tr.stable(); got != 0 {
		t.Fatalf("stable after marking 2 (not 1) = %d, want 0", got)
	}
	tr.markDone(1)
	if got := tr.stable(); got != 2 {
		t.Fatalf("stable after marking 1 and 2 = %d, want 2", got)
	}
	tr.markDone(3)
	if got := tr.stable(); got != 3 {
		t.Fatalf("stable after marking 1,2,3 = %d, want 3", got)
	}
}

// TestLogSeqTrackerStableSeqRaceStress is the -race stress test required by
// the design: concurrent writers allocate and mark done in overlapping
// order, while a reader repeatedly lists "committed" entries below a
// previously observed stable point and asserts that set never changes on a
// later re-list. This is the same guarantee ListCommittedLog relies on.
func TestLogSeqTrackerStableSeqRaceStress(t *testing.T) {
	var alloc logSeqAllocator
	alloc.next.Store(1)
	alloc.durableEnd.Store(1 << 30) // no lease extension needed for this test
	alloc.extendThreshold.Store(1 << 30)
	alloc.tracker.watermark.Store(0)

	const writers = 8
	const perWriter = 2000

	var committed sync.Map // seq -> struct{}: "durable" entries, written before markDone
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				seq, err := alloc.allocate()
				if err != nil {
					t.Error(err)
					return
				}
				committed.Store(seq, struct{}{})
				alloc.markDone(seq)
			}
		}()
	}

	var readerErr atomic.Value
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		var prevStable uint64
		var prevSet map[uint64]bool
		for {
			select {
			case <-stop:
				return
			default:
			}
			stable := alloc.StableSeq()
			if stable < prevStable {
				readerErr.Store("stable regressed")
				return
			}
			set := make(map[uint64]bool)
			committed.Range(func(k, _ any) bool {
				seq := k.(uint64)
				if seq <= stable {
					set[seq] = true
				}
				return true
			})
			// Every seq the previous, lower-or-equal snapshot listed at or
			// below prevStable must still be listed now: nothing observed
			// stable ever disappears.
			if prevSet != nil {
				for seq := range prevSet {
					if seq <= prevStable && !set[seq] {
						readerErr.Store("entry at or below a previously observed stable point vanished")
						return
					}
				}
			}
			prevStable = stable
			prevSet = set
		}
	}()

	wg.Wait()
	close(stop)
	readerWG.Wait()

	if v := readerErr.Load(); v != nil {
		t.Fatalf("reader observed violation: %v", v)
	}
	if got := alloc.StableSeq(); got != writers*perWriter {
		t.Fatalf("final StableSeq = %d, want %d (all writers done)", got, writers*perWriter)
	}
}

func BenchmarkLogSeqAllocate(b *testing.B) {
	dir, err := os.MkdirTemp("", "logseq_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	pdb, err := pebble.Open(filepath.Join(dir, "p"), &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer pdb.Close()

	alloc, err := newLogSeqAllocator(pdb, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) { return 1, nil })
	if err != nil {
		b.Fatal(err)
	}
	defer alloc.close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := mustAllocate(b, alloc)
			alloc.markDone(s)
		}
	})
}

// mustAllocate allocates the next seq or fails the test.
func mustAllocate(tb testing.TB, a *logSeqAllocator) uint64 {
	tb.Helper()
	seq, err := a.allocate()
	if err != nil {
		tb.Fatalf("allocate: %v", err)
	}
	return seq
}

// TestLogSeqAllocatorFailsInsteadOfSpinningWhenTheLeaseCannotBeExtended: a
// store whose lease cannot be written (here read-only; in production a full
// disk or an I/O error) fails the allocation with an error, and marks the
// seq done so the stable point is not held back by it. Before, allocate()
// spun forever, starting a new extension goroutine on every spin, and every
// commit of the store hung behind it.
//
// Mutation: drop the error return and spin until the durable end moves.
// The test times out.
func TestLogSeqAllocatorFailsInsteadOfSpinningWhenTheLeaseCannotBeExtended(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logseq.pebble")
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	if _, err := newLogSeqAllocator(db, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) { return 1, nil }); err != nil {
		t.Fatalf("newLogSeqAllocator: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ro, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("reopen read-only: %v", err)
	}
	defer ro.Close()
	alloc, err := newLogSeqAllocator(ro, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) {
		t.Fatal("initialFn must not run once the key exists")
		return 0, nil
	})
	if err != nil {
		t.Fatalf("newLogSeqAllocator (read-only): %v", err)
	}
	defer alloc.close()

	done := make(chan error, 1)
	go func() {
		_, err := alloc.allocate()
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("allocate succeeded past a lease that cannot be persisted")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("allocate hung on a lease that cannot be extended")
	}
	if got := alloc.StableSeq(); got != 1 {
		t.Fatalf("StableSeq = %d after the failed seq 1 was marked done, want 1", got)
	}
}

// TestLogSeqAllocatorReturnsAnErrorWhenTheLeaseCannotBeExtended: once the
// durable lease is used up and extending it fails (here a read-only store),
// allocate returns the error, promptly, instead of spinning on an extension
// that cannot succeed; the seq it took is marked done, so the stable point
// is not held back by it.
func TestLogSeqAllocatorReturnsAnErrorWhenTheLeaseCannotBeExtended(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logseq.pebble")
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	alloc, err := newLogSeqAllocator(db, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) { return 1, nil })
	if err != nil {
		t.Fatalf("newLogSeqAllocator: %v", err)
	}
	alloc.close()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ro, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("reopen read-only: %v", err)
	}
	alloc, err = newLogSeqAllocator(ro, []byte(pebblePrefixLogSeq), pebble.Sync, func() (uint64, error) {
		t.Fatal("initialFn must not run once the key exists")
		return 0, nil
	})
	if err != nil {
		t.Fatalf("newLogSeqAllocator (read-only): %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := alloc.allocate()
		done <- err
	}()
	select {
	case err := <-done:
		alloc.close()
		ro.Close()
		if err == nil {
			t.Fatal("allocate past a lease that cannot be extended returned no error")
		}
	case <-time.After(5 * time.Second):
		// The store stays open: the spinning allocation still uses it.
		t.Fatal("allocate spun on a lease extension that cannot succeed")
	}
	if got := alloc.StableSeq(); got != 1 {
		t.Fatalf("the failed seq held the stable point back: StableSeq=%d, want 1", got)
	}
}
