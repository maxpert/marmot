package id

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
)

// ErrRangeExhausted reports that a narrow AUTO_INCREMENT column has no ids
// left below its declared width. It is terminal: retrying cannot succeed, so
// it must never reach a client as the retryable lock-wait code.
var ErrRangeExhausted = errors.New("auto-increment column exhausted")

// ErrIDOutOfRange reports a client-supplied id its column cannot hold.
var ErrIDOutOfRange = errors.New("id above the column maximum")

// BelowBaseError refuses a client-supplied id at or below the cluster's
// allocation base that lies in no range this node claimed. Such an id may lie
// in another node's unissued range; when that node later issues it, the
// replicated row would replace the client's on every node, because replicated
// inserts apply as INSERT OR REPLACE.
type BelowBaseError struct {
	ID   uint64
	Base uint64
}

func (e *BelowBaseError) Error() string {
	return fmt.Sprintf("id %d is at or below the allocation base %d", e.ID, e.Base)
}

// RangeSizer returns how many ids to claim directly above base, the committed
// base a claim is about to propose. It is re-evaluated on every claim
// attempt, because a rejected attempt teaches the claimant a higher base, and
// both the endgame taper and the width ceiling depend on it. It returns
// ErrRangeExhausted when no claim above base can serve the request.
type RangeSizer func(base uint64) (uint64, error)

// Claimer obtains a cluster-wide range of ids for one table: on success
// (newBase, newBase+granted] belongs to the caller alone. It is
// coordinator.WriteCoordinator.ClaimRange.
type Claimer interface {
	ClaimRange(ctx context.Context, database, table string, prevBase uint64, size RangeSizer) (newBase, granted uint64, err error)
}

// Range sizing (FINAL.md §1.1). A claim is
//
//	R    = min(Rcap, ramp)
//	Rcap = max(1, min(rangeCap, widthMax/rangeWidthShare, (widthMax-base)/rangeTaperShare))
//
// rangeCap bounds cross-node ordering skew; widthMax/rangeWidthShare bounds
// what one abandoned range burns to under a thousandth of the column; the
// taper term shrinks the last claims so they fit instead of being rejected;
// the ramp bounds what a short-lived process abandons.
const (
	rangeCap        = 65536
	rangeWidthShare = 1024
	rangeTaperShare = 64
	rampInitial     = 64
	rampFactor      = 8
)

// RangeAllocator mints ids for narrow AUTO_INCREMENT columns from ranges
// claimed through the cluster-wide claim protocol.
//
// Its cursors live in memory only. A restarted process holds none, so its
// first insert into a table claims a fresh range; it never resumes a range
// across a process boundary (FINAL.md §1.6), and whatever the previous
// process had not issued is a permanent gap. Claims are lazy: a table no
// insert touches costs nothing.
type RangeAllocator struct {
	claimer Claimer
	timeout time.Duration
	cursors *xsync.MapOf[tableKey, *cursor]
}

type tableKey struct {
	database string
	table    string
}

// cursor is one table's local range: ids next..end are this node's alone.
// base is the highest committed base this node knows of - the end of its own
// last claim - and is what its next claim proposes. ramp is the size cap the
// next claim may ask for. owned lists every range this process claimed for
// the table, ascending and merged where adjacent: no other node can ever issue
// an id inside one.
type cursor struct {
	mu    sync.Mutex
	next  uint64
	end   uint64
	base  uint64
	ramp  uint64
	owned []idSpan
}

// idSpan is the closed interval [first, last].
type idSpan struct {
	first uint64
	last  uint64
}

// owns reports whether id lies in a range this process claimed.
func (c *cursor) owns(id uint64) bool {
	i := sort.Search(len(c.owned), func(i int) bool { return c.owned[i].last >= id })
	return i < len(c.owned) && c.owned[i].first <= id
}

// addOwned records a newly claimed range. Claims only ever move upward, so it
// either extends the last span or follows it.
func (c *cursor) addOwned(first, last uint64) {
	if n := len(c.owned); n > 0 && c.owned[n-1].last+1 == first {
		c.owned[n-1].last = last
		return
	}
	c.owned = append(c.owned, idSpan{first: first, last: last})
}

// NewRangeAllocator returns an allocator that claims through claimer, giving
// each claim at most timeout.
func NewRangeAllocator(claimer Claimer, timeout time.Duration) *RangeAllocator {
	return &RangeAllocator{
		claimer: claimer,
		timeout: timeout,
		cursors: xsync.NewMapOf[tableKey, *cursor](),
	}
}

func (a *RangeAllocator) cursorFor(database, table string) *cursor {
	c, _ := a.cursors.LoadOrCompute(tableKey{database: database, table: table}, func() *cursor {
		return &cursor{ramp: rampInitial}
	})
	return c
}

// Allocate returns first, where first..first+n-1 are n contiguous ids no
// other node will ever issue for the table. A statement never straddles two
// ranges: when the current range cannot hold all n, its remainder is
// abandoned and one claim of at least n is made.
func (a *RangeAllocator) Allocate(database, table string, widthMax uint64, n int) (uint64, error) {
	if n < 1 {
		return 0, fmt.Errorf("allocate %d ids for %s.%s: count must be positive", n, database, table)
	}
	want := uint64(n)
	c := a.cursorFor(database, table)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.next == 0 || c.next > c.end || c.end-c.next+1 < want {
		if err := a.claim(c, database, table, widthMax, want, 0); err != nil {
			return 0, err
		}
	}
	first := c.next
	c.next += want
	return first, nil
}

// Forget discards the table's cursor, abandoning whatever of its range is
// unissued. A cursor is valid only for the table incarnation it claimed for,
// and Forget is called when a DDL statement ends that incarnation: the ranges
// claimed for it are disjoint from every later grant under the name, but not
// from the grants under another name a renamed table's rows came from.
func (a *RangeAllocator) Forget(database, table string) {
	a.cursors.Delete(tableKey{database: database, table: table})
}

// ForgetDatabase discards the cursor of every table in database (Forget).
func (a *RangeAllocator) ForgetDatabase(database string) {
	a.cursors.Range(func(key tableKey, _ *cursor) bool {
		if key.database == database {
			a.cursors.Delete(key)
		}
		return true
	})
}

// Admit decides whether a table may hold explicit, an id that did not come
// from Allocate: a client-supplied one, or one SQLite or a query produced.
//
//   - Above widthMax it is refused with ErrIDOutOfRange.
//   - Inside a range this node claimed it is admitted: no other node can
//     issue it. If this node has not issued it yet, its cursor moves past it.
//   - At or below the base this node knows, it is refused with a
//     *BelowBaseError: it may lie in another node's unissued range.
//   - Above that base, one claim covering it is made. The claim learns the
//     cluster's real base through the protocol, so an id that turns out to be
//     at or below it is refused as well; otherwise it now lies in this node's
//     own range and is admitted.
//
// Admitting the same id twice is harmless, so every place that learns of an
// id may call it.
func (a *RangeAllocator) Admit(database, table string, widthMax, explicit uint64) error {
	if explicit > widthMax {
		return fmt.Errorf("%w: id %d, maximum %d", ErrIDOutOfRange, explicit, widthMax)
	}
	if explicit == 0 {
		return nil
	}
	c := a.cursorFor(database, table)
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.owns(explicit) {
		if explicit <= c.base {
			return &BelowBaseError{ID: explicit, Base: c.base}
		}
		if err := a.claim(c, database, table, widthMax, 1, explicit); err != nil {
			return err
		}
		if !c.owns(explicit) {
			return &BelowBaseError{ID: explicit, Base: c.base}
		}
	}
	if c.next != 0 && explicit >= c.next && explicit <= c.end {
		c.next = explicit + 1
	}
	return nil
}

// claim replaces c's range with a freshly claimed one of at least want ids,
// extended to cover cover when cover lies above the base. The caller holds
// c.mu.
func (a *RangeAllocator) claim(c *cursor, database, table string, widthMax, want, cover uint64) error {
	ramp := c.ramp
	sizer := func(base uint64) (uint64, error) {
		return claimSize(base, widthMax, want, cover, ramp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	newBase, granted, err := a.claimer.ClaimRange(ctx, database, table, c.base, sizer)
	if err != nil {
		return err
	}

	c.next = newBase + 1
	c.end = newBase + granted
	c.base = c.end
	c.addOwned(c.next, c.end)
	if c.ramp < rangeCap {
		c.ramp *= rampFactor
	}
	return nil
}

// claimSize sizes a claim above base per FINAL.md §1.1, for a request of want
// contiguous ids that must, when cover is above base, also reach cover and
// leave the same room above it. cover never exceeds widthMax.
func claimSize(base, widthMax, want, cover, ramp uint64) (uint64, error) {
	if base >= widthMax || widthMax-base < want {
		return 0, fmt.Errorf("%w: base %d leaves fewer than %d ids below %d", ErrRangeExhausted, base, want, widthMax)
	}
	size := max(min(rangeCapFor(base, widthMax), ramp), want)
	if cover > base {
		size += cover - base
	}
	return min(size, widthMax-base), nil
}

// rangeCapFor is Rcap: max(1, min(rangeCap, widthMax/rangeWidthShare,
// (widthMax-base)/rangeTaperShare)). base is below widthMax.
func rangeCapFor(base, widthMax uint64) uint64 {
	return max(1, min(rangeCap, widthMax/rangeWidthShare, (widthMax-base)/rangeTaperShare))
}
