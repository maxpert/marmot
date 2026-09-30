package db

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// LogPosition identifies an entry in a database's local commit log: the
// store-wide local commit sequence paired with the transaction that
// produced it. Ordering is lexicographic (Seq, TxnID), which is the
// ordering of the existing seq-index key pebbleTxnSeqKey(seq, txnID). The
// zero value precedes every real entry, since the sequence never hands out
// 0 (see newLogSeqAllocator).
type LogPosition struct {
	Seq   uint64
	TxnID uint64
}

// Compare returns -1, 0 or 1 as p sorts before, equal to, or after o.
func (p LogPosition) Compare(o LogPosition) int {
	switch {
	case p.Seq < o.Seq:
		return -1
	case p.Seq > o.Seq:
		return 1
	case p.TxnID < o.TxnID:
		return -1
	case p.TxnID > o.TxnID:
		return 1
	default:
		return 0
	}
}

// Less reports whether p sorts strictly before o.
func (p LogPosition) Less(o LogPosition) bool {
	return p.Compare(o) < 0
}

// encodeLogPosition serializes p as the 16-byte big-endian (Seq, TxnID)
// pair used both by the seq index key suffix and by every persisted
// LogPosition value (cursors, consumed positions, truncation point).
func encodeLogPosition(p LogPosition) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf, p.Seq)
	binary.BigEndian.PutUint64(buf[8:], p.TxnID)
	return buf
}

// decodeLogPosition is the inverse of encodeLogPosition. A short buffer
// (missing key, corrupt value) decodes as the zero position.
func decodeLogPosition(buf []byte) LogPosition {
	if len(buf) < 16 {
		return LogPosition{}
	}
	return LogPosition{
		Seq:   binary.BigEndian.Uint64(buf),
		TxnID: binary.BigEndian.Uint64(buf[8:]),
	}
}

const (
	// logSeqBandwidth is the number of sequence numbers durably reserved by
	// each lease extension of the store-wide local commit sequence.
	logSeqBandwidth = 10000

	// logSeqRingSize bounds how many sequence numbers a process may have
	// allocated but not yet marked done at once. It must be a power of two;
	// allocation spins (no I/O) until the watermark frees a slot, which
	// only happens under an implausibly deep backlog of concurrent writers.
	logSeqRingSize = 1 << 16
)

// logSeqTracker is a lock-free, exact record of which sequence numbers a
// process has finished allocating. It replaces a time-based "eps" staleness
// assumption: a position is never silently
// skipped, because nothing is ever reported stable until every seq up to it
// is known, exactly, to be done.
//
// Proof that StableSeq() is safe for a reader to snapshot before it opens
// its pebble iterator: a reader takes stable = StableSeq() before opening
// the iterator. Pebble makes every completed batch.Commit visible to any
// iterator opened after that Commit call returns. For every seq <= stable,
// markDone(seq) has already run; markDone is invoked by a defer that covers
// every exit path of CommitTransaction and StoreReplayedTransaction, so by
// the time it runs, either the commit-record batch has already committed
// (in which case it is visible to the reader's iterator, opened afterwards)
// or the writer gave up without committing (in which case that seq's
// record was never written, and never will be, since a seq is allocated
// exactly once). So every seq <= stable is either visible now or will never
// exist. Nothing becomes visible at or below stable afterwards, because
// stable only ever increases and a seq is never reused. Entries written
// before this process started are, by construction, below this process's
// first allocated seq, hence already covered by the initial watermark (see
// newLogSeqAllocator). No wall-clock reading appears anywhere in this
// argument.
type logSeqTracker struct {
	// slots[i] holds the seq value most recently marked done at index
	// i = seq % logSeqRingSize, or 0 if none has been yet. Storing the
	// actual seq (not a done bit) lets advance() tell a stale value left
	// behind by an earlier generation apart from the one it is looking for.
	slots [logSeqRingSize]atomic.Uint64

	// watermark is StableSeq(): the highest seq s such that every seq <= s
	// allocated by this process has been marked done.
	watermark atomic.Uint64
}

// markDone records that seq has finished (successfully or not) and
// advances the watermark over any newly-stable contiguous prefix.
func (t *logSeqTracker) markDone(seq uint64) {
	t.slots[seq%logSeqRingSize].Store(seq)
	for {
		w := t.watermark.Load()
		next := w + 1
		if t.slots[next%logSeqRingSize].Load() != next {
			return
		}
		if !t.watermark.CompareAndSwap(w, next) {
			continue // another marker moved the watermark; retry from its value
		}
	}
}

// stable returns the current watermark (StableSeq).
func (t *logSeqTracker) stable() uint64 {
	return t.watermark.Load()
}

// waitForSlot spins, without doing any I/O, until seq's ring slot has been
// vacated by the watermark advancing past seq-ringSize. This is what bounds
// the number of seqs a process may have in flight at once; by the time seq
// is allocated, the watermark must already have passed seq-ringSize, so the
// slot's previous occupant is guaranteed done.
func (t *logSeqTracker) waitForSlot(seq uint64) {
	for seq-t.watermark.Load() >= logSeqRingSize {
		runtime.Gosched()
	}
}

// logSeqAllocator mints the store-wide local commit sequence (LogPosition.Seq)
// for one PebbleMetaStore. It replaces the per-origin AtomicSequence leases
// keyed by /seq/{nodeID}: every committed transaction in a database's meta
// store, whatever its origin, now draws from this single sequence, so the
// seq index totally orders the store's local log.
//
// The lease end is persisted with the store's durable write option
// (pebble.Sync, or pebble.NoSync for a store opened with DisableWAL, tests
// only, which has no WAL to lose), so a seq is never reissued even across a
// crash that lost NoSync commit records: allocate() never returns a seq at
// or above the last durably persisted end without a new, durably persisted
// end past it existing first. The lease is extended asynchronously, in the
// background, once half of the current lease has been consumed, so its
// fsync never sits on the allocation path; allocate() only extends
// synchronously in the rare case where consumption outruns that extension
// and reaches the durable end before it completes.
type logSeqAllocator struct {
	db        *pebble.DB
	key       []byte
	syncWrite *pebble.WriteOptions // pebble.Sync, or pebble.NoSync for a WAL-disabled store (tests only)

	next atomic.Uint64 // next seq to hand out

	durableEnd      atomic.Uint64 // durable lease end; allocate() blocks here
	extendThreshold atomic.Uint64 // seq at which the next extension is triggered
	extending       atomic.Bool   // guards a single in-flight async extension
	extendWG        sync.WaitGroup
	extendMu        sync.Mutex // serialises lease extensions (extendPast)

	tracker logSeqTracker
}

// newLogSeqAllocator opens the store-wide sequence, resuming from its
// persisted durable end if the key already exists, or establishing one at
// the value initialFn computes (evaluated only on this first-ever open, to
// avoid rescanning the store on every restart) otherwise. initialFn must
// return a value strictly above every seq this store, or its per-origin
// predecessor, has ever handed out.
//
// syncWrite is the store's durable write option: pebble.Sync normally, or
// pebble.NoSync for a store opened with DisableWAL (tests only), which has
// no durable write to offer and refuses a synced one.
func newLogSeqAllocator(db *pebble.DB, key []byte, syncWrite *pebble.WriteOptions, initialFn func() (uint64, error)) (*logSeqAllocator, error) {
	a := &logSeqAllocator{db: db, key: key, syncWrite: syncWrite}

	var end uint64
	val, closer, err := db.Get(key)
	switch {
	case err == nil:
		if len(val) >= 8 {
			end = binary.BigEndian.Uint64(val)
		}
		closer.Close()
	case err == pebble.ErrNotFound:
		end, err = initialFn()
		if err != nil {
			return nil, fmt.Errorf("failed to derive initial log sequence: %w", err)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, end)
		if err := db.Set(key, buf, syncWrite); err != nil {
			return nil, fmt.Errorf("failed to persist initial log sequence: %w", err)
		}
	default:
		return nil, fmt.Errorf("failed to read log sequence: %w", err)
	}

	a.next.Store(end)
	a.durableEnd.Store(end)
	a.extendThreshold.Store(end) // first allocate() triggers the first extension
	a.tracker.watermark.Store(end - 1)
	return a, nil
}

// allocate reserves and returns the next seq. It performs no I/O while the
// durable lease covers the seq; an extension is started in the background
// once half the lease is consumed. Only a seq at or past the durable end -
// consumption outran the background extension - extends the lease
// synchronously, and a failure there is returned: the seq is marked done
// (never written) and the caller's commit fails instead of waiting on a
// write that cannot succeed.
func (a *logSeqAllocator) allocate() (uint64, error) {
	seq := a.next.Add(1) - 1

	a.tracker.waitForSlot(seq)

	if seq >= a.extendThreshold.Load() && a.extending.CompareAndSwap(false, true) {
		a.extendWG.Add(1)
		go func() {
			defer a.extendWG.Done()
			defer a.extending.Store(false)
			if err := a.extendPast(a.durableEnd.Load()); err != nil {
				log.Error().Err(err).Msg("failed to extend durable log sequence lease")
			}
		}()
	}

	if seq < a.durableEnd.Load() {
		return seq, nil
	}
	if err := a.extendPast(seq); err != nil {
		a.tracker.markDone(seq)
		return 0, fmt.Errorf("extend durable log sequence lease: %w", err)
	}
	return seq, nil
}

// extendPast durably extends the lease, one bandwidth at a time, until it
// covers seq. Extensions are serialised by extendMu, which no allocation
// holds except while it waits for exactly this.
func (a *logSeqAllocator) extendPast(seq uint64) error {
	a.extendMu.Lock()
	defer a.extendMu.Unlock()
	for seq >= a.durableEnd.Load() {
		newEnd := a.durableEnd.Load() + logSeqBandwidth
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, newEnd)
		if err := a.db.Set(a.key, buf, a.syncWrite); err != nil {
			return err
		}
		a.durableEnd.Store(newEnd)
		a.extendThreshold.Store(newEnd - logSeqBandwidth/2)
	}
	return nil
}

// markDone records that seq finished its commit-record write, successfully
// or not. It must be called exactly once per value allocate() returned, on
// every exit path (see logSeqTracker's doc comment for why).
func (a *logSeqAllocator) markDone(seq uint64) {
	a.tracker.markDone(seq)
}

// StableSeq returns the highest seq s such that every seq <= s this process
// has allocated has finished (logSeqTracker's doc comment has the proof).
func (a *logSeqAllocator) StableSeq() uint64 {
	return a.tracker.stable()
}

// close waits for any in-flight async extension to finish before the
// underlying pebble.DB is closed.
func (a *logSeqAllocator) close() {
	a.extendWG.Wait()
}

// maxSeqNumFromIndex scans the seq index for the highest seq recorded,
// shared by PebbleMetaStore.GetMaxSeqNum and the store-wide sequence's
// migration path (computeInitialLogSeq).
func maxSeqNumFromIndex(db *pebble.DB) (uint64, error) {
	var maxSeq uint64
	prefix := []byte(pebblePrefixTxnSeq)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if iter.Last() {
		key := iter.Key()
		if len(key) >= len(pebblePrefixTxnSeq)+8 {
			maxSeq = binary.BigEndian.Uint64(key[len(pebblePrefixTxnSeq):])
		}
	}
	return maxSeq, iter.Error()
}

// computeInitialLogSeq derives the first value the new store-wide log
// sequence may hand out, on the first run after the per-origin-sequence
// upgrade: strictly above every existing per-origin /seq/{nodeID} lease end
// and above the highest seq already recorded in the seq index, so the new
// sequence can never reissue a seq a pre-upgrade node already used.
func computeInitialLogSeq(db *pebble.DB) (uint64, error) {
	max := uint64(0)

	prefix := []byte(pebblePrefixSeq)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, err
	}
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			_ = iter.Close()
			return 0, err
		}
		if len(val) >= 8 {
			if v := binary.BigEndian.Uint64(val); v > max {
				max = v
			}
		}
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}

	maxIndexed, err := maxSeqNumFromIndex(db)
	if err != nil {
		return 0, err
	}
	if maxIndexed > max {
		max = maxIndexed
	}

	return max + 1, nil
}

// CommittedLogEntry is one committed log entry: its position, and the commit
// timestamp its transaction committed at (NodeID is its origin). The
// timestamp is stored with the position itself, so a listed entry carries
// it however its transaction's other records fare. It is zero for an entry
// written before positions carried one.
type CommittedLogEntry struct {
	LogPosition
	CommitTS hlc.Timestamp
}

// logEntryStamp is the seq-index value: the entry's commit timestamp.
type logEntryStamp struct {
	Wall    int64  `msgpack:"w"`
	Logical int32  `msgpack:"l"`
	Node    uint64 `msgpack:"n"`
}

func encodeLogEntryStamp(commitTS hlc.Timestamp) ([]byte, error) {
	return encoding.Marshal(logEntryStamp{Wall: commitTS.WallTime, Logical: commitTS.Logical, Node: commitTS.NodeID})
}

// decodeLogEntryStamp decodes a seq-index value; an empty one, from before
// positions carried a timestamp, is the zero timestamp.
func decodeLogEntryStamp(val []byte) (hlc.Timestamp, error) {
	if len(val) == 0 {
		return hlc.Timestamp{}, nil
	}
	var stamp logEntryStamp
	if err := encoding.Unmarshal(val, &stamp); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("decode log entry commit timestamp: %w", err)
	}
	return hlc.Timestamp{WallTime: stamp.Wall, Logical: stamp.Logical, NodeID: stamp.Node}, nil
}
