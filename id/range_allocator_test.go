package id

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Width ceilings of the narrow MySQL integer types, signed.
const (
	tinyMax   = 127
	smallMax  = 32767
	mediumMax = 8388607
	intMax    = 2147483647
)

// fakeCluster models the claim protocol's observable contract for one table:
// a committed base that only advances, a proposal below it is rejected with
// the base (the claimant retries there, re-sizing), and a granted claim of
// size s moves the base to newBase+s.
type fakeCluster struct {
	mu     sync.Mutex
	base   map[string]uint64
	claims []fakeClaim
	fail   error
}

type fakeClaim struct {
	table          string
	prevBase, size uint64
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{base: make(map[string]uint64)}
}

func (f *fakeCluster) ClaimRange(_ context.Context, database, table string, prevBase uint64, size RangeSizer) (uint64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return 0, 0, f.fail
	}
	key := database + "." + table
	if stored := f.base[key]; stored > prevBase {
		prevBase = stored // a rejection teaches the claimant the stored base
	}
	s, err := size(prevBase)
	if err != nil {
		return 0, 0, err
	}
	f.base[key] = prevBase + s
	f.claims = append(f.claims, fakeClaim{table: table, prevBase: prevBase, size: s})
	return prevBase, s, nil
}

func (f *fakeCluster) claimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.claims)
}

func (f *fakeCluster) claimSizes() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	sizes := make([]uint64, len(f.claims))
	for i, c := range f.claims {
		sizes[i] = c.size
	}
	return sizes
}

func mustAllocate(t *testing.T, a *RangeAllocator, table string, widthMax uint64, n int) uint64 {
	t.Helper()
	first, err := a.Allocate("db", table, widthMax, n)
	if err != nil {
		t.Fatalf("Allocate(%s, %d): %v", table, n, err)
	}
	return first
}

func TestRangeAllocator_ClaimsLazilyOnFirstUse(t *testing.T) {
	cluster := newFakeCluster()
	a := NewRangeAllocator(cluster, time.Second)
	if got := cluster.claimCount(); got != 0 {
		t.Fatalf("constructing an allocator made %d claims; claims must wait for the first insert", got)
	}
	if first := mustAllocate(t, a, "t", intMax, 1); first != 1 {
		t.Fatalf("first id = %d, want 1", first)
	}
	mustAllocate(t, a, "t", intMax, 1)
	if got := cluster.claimCount(); got != 1 {
		t.Fatalf("two inserts inside one range made %d claims, want 1", got)
	}
}

func TestRangeAllocator_RampClimbsToTheCap(t *testing.T) {
	cluster := newFakeCluster()
	a := NewRangeAllocator(cluster, time.Second)
	// Draining each range one id at a time forces one claim per range.
	for range 6 {
		first := mustAllocate(t, a, "t", intMax, 1)
		c := a.cursorFor("db", "t")
		for id := first + 1; id <= c.end; id++ {
			mustAllocate(t, a, "t", intMax, 1)
		}
	}
	want := []uint64{64, 512, 4096, 32768, 65536, 65536}
	got := cluster.claimSizes()
	if len(got) != len(want) {
		t.Fatalf("claim sizes %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("claim sizes %v, want %v: the ramp must climb x8 from 64 and stop at Rcap", got, want)
		}
	}
}

func TestRangeCapFor(t *testing.T) {
	cases := []struct {
		name           string
		base, widthMax uint64
		want           uint64
	}{
		{"tinyint at zero", 0, tinyMax, 1},
		{"smallint at zero", 0, smallMax, 31},
		{"mediumint at zero", 0, mediumMax, 8191},
		{"int at zero", 0, intMax, 65536},
		{"unsigned int at zero", 0, 4294967295, 65536},
		{"int mid-range", intMax / 2, intMax, 65536},
		{"int endgame taper", intMax - 64*100, intMax, 100},
		{"one short of the ceiling", intMax - 1, intMax, 1},
	}
	for _, tc := range cases {
		if got := rangeCapFor(tc.base, tc.widthMax); got != tc.want {
			t.Errorf("%s: Rcap = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestRangeAllocator_SingleNodeIsDenseAcrossRangeBoundaries(t *testing.T) {
	cluster := newFakeCluster()
	a := NewRangeAllocator(cluster, time.Second)
	for want := uint64(1); want <= 700; want++ {
		if got := mustAllocate(t, a, "t", intMax, 1); got != want {
			t.Fatalf("id %d issued where %d was expected: a single writer must mint 1, 2, 3 with no jump", got, want)
		}
	}
	if cluster.claimCount() < 2 {
		t.Fatalf("700 ids crossed no range boundary; the test proves nothing")
	}
}

func TestRangeAllocator_NAtOnceIsContiguousAndNeverStraddles(t *testing.T) {
	cluster := newFakeCluster()
	a := NewRangeAllocator(cluster, time.Second)
	mustAllocate(t, a, "t", intMax, 60) // ids 1..60 of the first range (64)

	// Four left in the range; a five-row statement must not take them.
	first := mustAllocate(t, a, "t", intMax, 5)
	if first != 65 {
		t.Fatalf("five-row statement started at %d, want 65: it must start a fresh range, not straddle", first)
	}
	sizes := cluster.claimSizes()
	if len(sizes) != 2 {
		t.Fatalf("claims %v, want two", sizes)
	}

	// A statement larger than the ramp claims at least its own size.
	first = mustAllocate(t, a, "t", intMax, 5000)
	c := a.cursorFor("db", "t")
	if c.end-first+1 < 5000 {
		t.Fatalf("5000-row statement got a range ending at %d from %d", c.end, first)
	}
	next := mustAllocate(t, a, "t", intMax, 1)
	if next <= first+4999 {
		t.Fatalf("id %d reissued inside the 5000-row statement's ids", next)
	}
}

func TestRangeAllocator_ExhaustionIsTerminal(t *testing.T) {
	cluster := newFakeCluster()
	a := NewRangeAllocator(cluster, time.Second)
	for want := uint64(1); want <= tinyMax; want++ {
		if got := mustAllocate(t, a, "tiny", tinyMax, 1); got != want {
			t.Fatalf("TINYINT id %d, want %d", got, want)
		}
	}
	_, err := a.Allocate("db", "tiny", tinyMax, 1)
	if !errors.Is(err, ErrRangeExhausted) {
		t.Fatalf("insert past 127 into TINYINT: err = %v, want ErrRangeExhausted", err)
	}

	// A multi-row statement that cannot fit in what is left is refused whole.
	b := NewRangeAllocator(newFakeCluster(), time.Second)
	mustAllocate(t, b, "s", smallMax, smallMax-3)
	if _, err := b.Allocate("db", "s", smallMax, 5); !errors.Is(err, ErrRangeExhausted) {
		t.Fatalf("five rows into a SMALLINT with three ids left: err = %v, want ErrRangeExhausted", err)
	}
}

func TestRangeAllocator_RestartClaimsFreshAndNeverReissues(t *testing.T) {
	cluster := newFakeCluster()
	first := NewRangeAllocator(cluster, time.Second)
	var highest uint64
	for range 10 {
		highest = mustAllocate(t, first, "t", intMax, 1)
	}

	// A new process over the same cluster: the old range's unissued tail
	// (11..64) is a gap, never a source of ids.
	restarted := NewRangeAllocator(cluster, time.Second)
	got := mustAllocate(t, restarted, "t", intMax, 1)
	if got <= highest || got <= 64 {
		t.Fatalf("restarted allocator issued %d; ids up to %d were issued and ids up to 64 were claimed before the restart", got, highest)
	}
}

func TestRangeAllocator_TwoNodesNeverShareAnID(t *testing.T) {
	cluster := newFakeCluster()
	nodes := []*RangeAllocator{
		NewRangeAllocator(cluster, time.Second),
		NewRangeAllocator(cluster, time.Second),
		NewRangeAllocator(cluster, time.Second),
	}
	var mu sync.Mutex
	seen := make(map[uint64]bool)
	var wg sync.WaitGroup
	for _, node := range nodes {
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 300 {
					n := 1 + i%3
					first, err := node.Allocate("db", "t", smallMax, n)
					if err != nil {
						t.Errorf("Allocate: %v", err)
						return
					}
					mu.Lock()
					for id := first; id < first+uint64(n); id++ {
						if id == 0 || id > smallMax {
							t.Errorf("id %d outside [1, %d]", id, smallMax)
						}
						if seen[id] {
							t.Errorf("id %d issued twice", id)
						}
						seen[id] = true
					}
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
}

// TestRangeAllocator_AdmitExplicitIDs pins the admission rule for ids that
// did not come from Allocate.
func TestRangeAllocator_AdmitExplicitIDs(t *testing.T) {
	t.Run("inside the local unissued range moves the cursor past it", func(t *testing.T) {
		cluster := newFakeCluster()
		a := NewRangeAllocator(cluster, time.Second)
		mustAllocate(t, a, "t", intMax, 1)
		if err := a.Admit("db", "t", intMax, 40); err != nil {
			t.Fatal(err)
		}
		if got := mustAllocate(t, a, "t", intMax, 1); got != 41 {
			t.Fatalf("next id after explicit 40 = %d, want 41", got)
		}
		if cluster.claimCount() != 1 {
			t.Fatalf("an id inside the local range made a claim")
		}
	})

	t.Run("an id this node already issued is admitted again", func(t *testing.T) {
		a := NewRangeAllocator(newFakeCluster(), time.Second)
		first := mustAllocate(t, a, "t", intMax, 3)
		for id := first; id < first+3; id++ {
			if err := a.Admit("db", "t", intMax, id); err != nil {
				t.Fatalf("generated id %d refused: %v", id, err)
			}
		}
		if got := mustAllocate(t, a, "t", intMax, 1); got != first+3 {
			t.Fatalf("admitting issued ids moved the cursor: next = %d, want %d", got, first+3)
		}
	})

	t.Run("above every known base raises the cluster base past it", func(t *testing.T) {
		cluster := newFakeCluster()
		a := NewRangeAllocator(cluster, time.Second)
		mustAllocate(t, a, "t", intMax, 1)
		if err := a.Admit("db", "t", intMax, 5000); err != nil {
			t.Fatal(err)
		}
		if base := cluster.base["db.t"]; base < 5000 {
			t.Fatalf("cluster base %d after an explicit id 5000; another node could still issue 5000", base)
		}
		if got := mustAllocate(t, a, "t", intMax, 1); got != 5001 {
			t.Fatalf("next id after explicit 5000 = %d, want 5001", got)
		}
		other := NewRangeAllocator(cluster, time.Second)
		if got := mustAllocate(t, other, "t", intMax, 1); got <= 5000 {
			t.Fatalf("another node issued %d after explicit 5000 raised the base", got)
		}
	})

	t.Run("inside another node's range is refused", func(t *testing.T) {
		// Mutation: admit any id at or below the base. "an id inside
		// another node's unissued range was admitted" fires.
		cluster := newFakeCluster()
		owner := NewRangeAllocator(cluster, time.Second)
		mustAllocate(t, owner, "t", intMax, 1) // owner holds 1..64
		other := NewRangeAllocator(cluster, time.Second)
		mustAllocate(t, other, "t", intMax, 1) // other learns base 64, holds 65..128

		err := other.Admit("db", "t", intMax, 30)
		var below *BelowBaseError
		if !errors.As(err, &below) {
			t.Fatalf("an id inside another node's unissued range was admitted: err = %v", err)
		}

		// A node whose knowledge is stale learns the base through the claim
		// protocol and refuses as well.
		stale := NewRangeAllocator(cluster, time.Second)
		if err := stale.Admit("db", "t", intMax, 100); !errors.As(err, &below) {
			t.Fatalf("a node with a stale base admitted id 100 inside another node's range: err = %v", err)
		}
	})

	t.Run("above the column maximum is refused", func(t *testing.T) {
		a := NewRangeAllocator(newFakeCluster(), time.Second)
		if err := a.Admit("db", "t", tinyMax, tinyMax+1); !errors.Is(err, ErrIDOutOfRange) {
			t.Fatalf("explicit id 128 for a TINYINT column: err = %v, want ErrIDOutOfRange", err)
		}
	})

	t.Run("a mysqldump restore admits every row", func(t *testing.T) {
		a := NewRangeAllocator(newFakeCluster(), time.Second)
		for id := uint64(1); id <= 1000; id++ {
			if err := a.Admit("db", "t", intMax, id); err != nil {
				t.Fatalf("dump row %d refused: %v", id, err)
			}
		}
		if got := mustAllocate(t, a, "t", intMax, 1); got <= 1000 {
			t.Fatalf("first generated id after the dump = %d, want above 1000", got)
		}
	})
}

// TestRangeAllocator_ForgetDiscardsTheRangeOfAReplacedName is the cross-name
// case the per-name monotone base does not cover on its own: n1 holds an
// unissued range of a dropped t, n2 fills a new t2 from t2's own grants, and
// t2 is renamed to t, inheriting t's base. n1's old range for t overlaps the
// ids the renamed table holds, so the rename makes n1 forget it and claim
// fresh above the inherited base.
//
// Mutation: make Forget a no-op. n1 issues 11, n2's id, and the first
// assertion fires.
func TestRangeAllocator_ForgetDiscardsTheRangeOfAReplacedName(t *testing.T) {
	cluster := newFakeCluster()
	n1 := NewRangeAllocator(cluster, time.Second)
	n2 := NewRangeAllocator(cluster, time.Second)

	for i := 0; i < 10; i++ {
		mustAllocate(t, n1, "t", intMax, 1)
	}
	for i := 0; i < 20; i++ {
		mustAllocate(t, n2, "t2", intMax, 1)
	}
	// RENAME t2 TO t: t inherits t2's base (db.AutoIncClaimStore.Inherit).
	cluster.mu.Lock()
	cluster.base["db.t"] = max(cluster.base["db.t"], cluster.base["db.t2"])
	cluster.mu.Unlock()
	n1.Forget("db", "t")

	if got := mustAllocate(t, n1, "t", intMax, 1); got <= 64 {
		t.Fatalf("after the rename n1 issued %d, inside the ids the renamed table holds (1..64)", got)
	}
	var below *BelowBaseError
	if err := n1.Admit("db", "t", intMax, 11); !errors.As(err, &below) {
		t.Fatalf("after the rename n1 admitted explicit id 11 from its forgotten range: %v", err)
	}
}

// TestRangeAllocator_ForgetDatabaseDiscardsEveryTableOfIt is reviewer A's DROP
// DATABASE + recreate: n1 holds an unissued range of d.t when d is dropped.
// The claim row survives, so the recreated d.t's grants start above it, and
// the drop makes n1 forget its range so n1 claims fresh too. Another
// database's cursor is kept.
//
// Mutation: make ForgetDatabase a no-op. n1 issues 4 from the dropped
// incarnation's range and the first assertion fires.
func TestRangeAllocator_ForgetDatabaseDiscardsEveryTableOfIt(t *testing.T) {
	cluster := newFakeCluster()
	n1 := NewRangeAllocator(cluster, time.Second)
	for i := 0; i < 3; i++ {
		mustAllocate(t, n1, "t", intMax, 1)
	}
	other, err := n1.Allocate("other", "t", intMax, 1)
	if err != nil {
		t.Fatal(err)
	}

	n1.ForgetDatabase("db")
	if got := mustAllocate(t, n1, "t", intMax, 1); got <= 64 {
		t.Fatalf("after DROP DATABASE n1 issued %d from the dropped incarnation's range (1..64)", got)
	}
	if got, err := n1.Allocate("other", "t", intMax, 1); err != nil || got != other+1 {
		t.Fatalf("DROP DATABASE db disturbed other.t's cursor: got %d, %v, want %d", got, err, other+1)
	}
}

func TestRangeAllocator_ClaimFailurePropagates(t *testing.T) {
	cluster := newFakeCluster()
	unavailable := errors.New("quorum not reached")
	cluster.fail = unavailable
	a := NewRangeAllocator(cluster, time.Second)
	_, err := a.Allocate("db", "t", intMax, 1)
	if !errors.Is(err, unavailable) {
		t.Fatalf("err = %v, want the claimer's error", err)
	}
	if errors.Is(err, ErrRangeExhausted) {
		t.Fatalf("an unavailable quorum was reported as exhaustion")
	}
}

func BenchmarkRangeAllocator_Allocate(b *testing.B) {
	a := NewRangeAllocator(newFakeCluster(), time.Second)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := a.Allocate("db", "t", 4294967295, 1); err != nil {
			b.Fatal(err)
		}
	}
}
