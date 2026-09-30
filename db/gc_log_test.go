//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// commitTestTxn begins and commits a simple transaction and returns its
// LogPosition.
func commitTestTxn(t *testing.T, store *PebbleMetaStore, txnID, nodeID uint64) LogPosition {
	t.Helper()
	clock := hlc.NewClock(nodeID)
	ts := clock.Now()
	if err := store.BeginTransaction(txnID, nodeID, ts); err != nil {
		t.Fatalf("BeginTransaction(%d): %v", txnID, err)
	}
	if err := store.CommitTransaction(txnID, ts, nil, "testdb", "", 0, 0); err != nil {
		t.Fatalf("CommitTransaction(%d): %v", txnID, err)
	}
	rec, err := store.GetTransaction(txnID)
	if err != nil || rec == nil {
		t.Fatalf("GetTransaction(%d): rec=%v err=%v", txnID, rec, err)
	}
	return LogPosition{Seq: rec.SeqNum, TxnID: txnID}
}

func TestListCommittedLogOrderingLimitMoreStable(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	var positions []LogPosition
	for i := uint64(1); i <= 5; i++ {
		positions = append(positions, commitTestTxn(t, store, 1000+i, 1))
	}

	// First page: limit 2, from the zero position.
	entries, stable, more, err := store.ListCommittedLog(LogPosition{}, 2)
	if err != nil {
		t.Fatalf("ListCommittedLog: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("page 1 len = %d, want 2", len(entries))
	}
	if entries[0].LogPosition != positions[0] || entries[1].LogPosition != positions[1] {
		t.Fatalf("page 1 = %v, want %v", entries, positions[:2])
	}
	if !more {
		t.Fatal("expected more=true with 3 entries left")
	}
	if stable < positions[len(positions)-1].Seq {
		t.Fatalf("stable=%d should cover every committed entry (last seq %d)", stable, positions[len(positions)-1].Seq)
	}

	// Second page: continue after the last entry of page 1.
	entries2, _, more2, err := store.ListCommittedLog(entries[len(entries)-1].LogPosition, 10)
	if err != nil {
		t.Fatalf("ListCommittedLog page 2: %v", err)
	}
	if len(entries2) != 3 {
		t.Fatalf("page 2 len = %d, want 3", len(entries2))
	}
	for i, want := range positions[2:] {
		if entries2[i].LogPosition != want {
			t.Fatalf("page 2[%d] = %v, want %v", i, entries2[i], want)
		}
	}
	if more2 {
		t.Fatal("expected more=false once every entry has been listed")
	}

	// Listing strictly after the last entry returns nothing.
	entries3, _, more3, err := store.ListCommittedLog(positions[len(positions)-1], 10)
	if err != nil {
		t.Fatalf("ListCommittedLog page 3: %v", err)
	}
	if len(entries3) != 0 || more3 {
		t.Fatalf("page 3 = %v more=%v, want empty/false", entries3, more3)
	}
}

func TestListCommittedLogExcludesUncommitted(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	committed := commitTestTxn(t, store, 2001, 1)

	// A PENDING transaction has no seq-index entry, so it can't appear.
	clock := hlc.NewClock(1)
	if err := store.BeginTransaction(2002, 1, clock.Now()); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}

	entries, _, _, err := store.ListCommittedLog(LogPosition{}, 10)
	if err != nil {
		t.Fatalf("ListCommittedLog: %v", err)
	}
	if len(entries) != 1 || entries[0].LogPosition != committed {
		t.Fatalf("entries = %v, want only %v", entries, committed)
	}
}

func TestPullCursorAndConsumedPositionAreSeparateAndAsIs(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	peer := uint64(42)

	zero, err := store.GetPullCursor(peer)
	if err != nil || zero != (LogPosition{}) {
		t.Fatalf("GetPullCursor before any Set = %v, %v; want zero, nil", zero, err)
	}

	high := LogPosition{Seq: 100, TxnID: 5}
	if err := store.SetPullCursor(peer, high); err != nil {
		t.Fatalf("SetPullCursor: %v", err)
	}
	got, err := store.GetPullCursor(peer)
	if err != nil || got != high {
		t.Fatalf("GetPullCursor = %v, %v; want %v", got, err, high)
	}

	// Consumed positions are stored under a distinct namespace: setting one
	// for the same node id must not change the pull cursor, and vice versa.
	if err := store.SetConsumedPosition(peer, LogPosition{Seq: 7, TxnID: 1}); err != nil {
		t.Fatalf("SetConsumedPosition: %v", err)
	}
	stillHigh, err := store.GetPullCursor(peer)
	if err != nil || stillHigh != high {
		t.Fatalf("pull cursor changed by SetConsumedPosition: got %v, want %v", stillHigh, high)
	}

	// SetConsumedPosition stores the value as is, not as a max: a lower
	// position must overwrite a higher one (needed after a restore).
	low := LogPosition{Seq: 3, TxnID: 1}
	if err := store.SetConsumedPosition(peer, low); err != nil {
		t.Fatalf("SetConsumedPosition (regress): %v", err)
	}
	all, err := store.ConsumedPositions()
	if err != nil {
		t.Fatalf("ConsumedPositions: %v", err)
	}
	if all[peer] != low {
		t.Fatalf("ConsumedPositions[%d] = %v, want %v (as-is, not max)", peer, all[peer], low)
	}

	if err := store.DeleteConsumedPosition(peer); err != nil {
		t.Fatalf("DeleteConsumedPosition: %v", err)
	}
	all2, err := store.ConsumedPositions()
	if err != nil {
		t.Fatalf("ConsumedPositions after delete: %v", err)
	}
	if _, ok := all2[peer]; ok {
		t.Fatalf("consumed position for %d should be gone after delete", peer)
	}
}

func TestGCDeletesCommittedAtOrBelowSafeOlderThanMinRetention(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	pos := commitTestTxn(t, store, 3001, 1)
	time.Sleep(2 * time.Millisecond)

	// minRetention=0 => already "older than min retention"; maxRetention
	// large => the unconditional branch doesn't apply. safe covers pos.
	n, err := store.CleanupOldTransactionRecords(0, time.Hour, pos)
	if err != nil {
		t.Fatalf("CleanupOldTransactionRecords: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted = %d, want 1", n)
	}

	rec, err := store.GetTransaction(3001)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected the record to be gone, got %+v", rec)
	}

	through, err := store.TruncatedThrough()
	if err != nil {
		t.Fatalf("TruncatedThrough: %v", err)
	}
	if through != pos {
		t.Fatalf("TruncatedThrough = %v, want %v", through, pos)
	}
}

func TestGCNeverDeletesAnEntryNoMemberHasConsumed(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	commitTestTxn(t, store, 3101, 1)
	time.Sleep(2 * time.Millisecond)

	// safe is the zero position: nothing has been reported consumed, so
	// the min-retention branch must not fire regardless of age.
	n, err := store.CleanupOldTransactionRecords(0, time.Hour, LogPosition{})
	if err != nil {
		t.Fatalf("CleanupOldTransactionRecords: %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted = %d, want 0 (nothing consumed yet)", n)
	}

	rec, err := store.GetTransaction(3101)
	if err != nil || rec == nil {
		t.Fatalf("record should survive: rec=%v err=%v", rec, err)
	}
}

func TestGCDeletesOlderThanMaxRetentionRegardlessOfSafe(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	commitTestTxn(t, store, 3201, 1)
	time.Sleep(2 * time.Millisecond)

	// maxRetention=1ms (the entry is 2ms old) forces unconditional deletion;
	// minRetention huge so that branch alone would not fire; safe is zero
	// (would refuse via the min-retention branch, but must not block the
	// max-retention branch).
	n, err := store.CleanupOldTransactionRecords(time.Hour, time.Millisecond, LogPosition{})
	if err != nil {
		t.Fatalf("CleanupOldTransactionRecords: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted = %d, want 1 (max retention is unconditional)", n)
	}
}

// TestGCZeroMaxRetentionMeansNoForcedDeletion pins that maxRetention 0 is
// "no maximum": nothing is deleted past the safe position, however old, and
// consumed entries are still deleted once older than min retention.
func TestGCZeroMaxRetentionMeansNoForcedDeletion(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	posA := commitTestTxn(t, store, 3251, 1)
	commitTestTxn(t, store, 3252, 1)
	posC := commitTestTxn(t, store, 3253, 1)
	time.Sleep(2 * time.Millisecond)

	if n, err := store.CleanupOldTransactionRecords(0, 0, LogPosition{}); err != nil || n != 0 {
		t.Fatalf("nothing consumed: deleted = %d, err = %v; want 0", n, err)
	}
	if n, err := store.CleanupOldTransactionRecords(time.Hour, 0, posC); err != nil || n != 0 {
		t.Fatalf("all consumed but younger than min retention: deleted = %d, err = %v; want 0", n, err)
	}
	if n, err := store.CleanupOldTransactionRecords(0, 0, posA); err != nil || n != 1 {
		t.Fatalf("safe covers A only: deleted = %d, err = %v; want 1", n, err)
	}
	for _, id := range []uint64{3252, 3253} {
		if rec, _ := store.GetTransaction(id); rec == nil {
			t.Fatalf("txn %d is above safe and must survive", id)
		}
	}
	if through, err := store.TruncatedThrough(); err != nil || through != posA {
		t.Fatalf("TruncatedThrough = %v, err = %v; want %v", through, err, posA)
	}

	if n, err := store.CleanupOldTransactionRecords(0, 0, posC); err != nil || n != 2 {
		t.Fatalf("safe covers C: deleted = %d, err = %v; want 2", n, err)
	}
}

// TestGCStopsAtFirstNonDeletableEntryPrefixOnly enforces the no-holes
// invariant: GC only ever deletes a prefix of the log in position order, so
// TruncatedThrough always means "everything at or below this is gone."
func TestGCStopsAtFirstNonDeletableEntryPrefixOnly(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	posA := commitTestTxn(t, store, 3301, 1)
	posB := commitTestTxn(t, store, 3302, 1)
	posC := commitTestTxn(t, store, 3303, 1)
	time.Sleep(2 * time.Millisecond)

	// safe covers only A; B (and, behind it, C) must stop the walk even
	// though C individually would also be old enough.
	n, err := store.CleanupOldTransactionRecords(0, time.Hour, posA)
	if err != nil {
		t.Fatalf("CleanupOldTransactionRecords: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted = %d, want 1 (only A)", n)
	}

	if rec, _ := store.GetTransaction(3301); rec != nil {
		t.Fatal("A should be deleted")
	}
	if rec, _ := store.GetTransaction(3302); rec == nil {
		t.Fatal("B must survive: it is not covered by safe")
	}
	if rec, _ := store.GetTransaction(3303); rec == nil {
		t.Fatal("C must survive: the walk must stop at B, before reaching C")
	}

	through, err := store.TruncatedThrough()
	if err != nil {
		t.Fatalf("TruncatedThrough: %v", err)
	}
	if through != posA {
		t.Fatalf("TruncatedThrough = %v, want %v", through, posA)
	}
	_ = posB
	_ = posC
}

func TestTruncatedThroughNeverDecreases(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	posA := commitTestTxn(t, store, 3401, 1)
	posB := commitTestTxn(t, store, 3402, 1)
	time.Sleep(2 * time.Millisecond)

	if _, err := store.CleanupOldTransactionRecords(0, time.Hour, posB); err != nil {
		t.Fatalf("first GC: %v", err)
	}
	first, err := store.TruncatedThrough()
	if err != nil {
		t.Fatalf("TruncatedThrough: %v", err)
	}
	if first != posB {
		t.Fatalf("TruncatedThrough = %v, want %v", first, posB)
	}
	_ = posA

	// A second pass with nothing left to delete must not regress T.
	if _, err := store.CleanupOldTransactionRecords(0, time.Hour, LogPosition{}); err != nil {
		t.Fatalf("second GC: %v", err)
	}
	second, err := store.TruncatedThrough()
	if err != nil {
		t.Fatalf("TruncatedThrough (2): %v", err)
	}
	if second != first {
		t.Fatalf("TruncatedThrough regressed: %v -> %v", first, second)
	}
}

// TestGCSkipsANonCommittedSeqEntryUntilMaxRetention pins that a seq-index
// entry whose status is no longer COMMITTED (a record a late PREPARE's begin
// overwrote before BeginTransactionWithID refused that) never stops GC
// forever: the entries after it are still deleted, and it is deleted itself
// once past max retention.
func TestGCSkipsANonCommittedSeqEntryUntilMaxRetention(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	commitTestTxn(t, store, 3401, 1)
	commitTestTxn(t, store, 3402, 1)
	posC := commitTestTxn(t, store, 3403, 1)
	if err := store.db.Set(pebbleTxnStatusKey(3402), []byte{byte(TxnStatusPending)}, nil); err != nil {
		t.Fatalf("overwrite status: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	n, err := store.CleanupOldTransactionRecords(0, time.Hour, posC)
	if err != nil {
		t.Fatalf("CleanupOldTransactionRecords: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted = %d, want 2 (the committed entries around the stray one)", n)
	}
	if rec, _ := store.GetTransaction(3403); rec != nil {
		t.Fatal("the committed entry after the stray one must be deleted")
	}
	if rec, _ := store.GetTransaction(3402); rec == nil {
		t.Fatal("the stray entry must survive until max retention")
	}
	through, err := store.TruncatedThrough()
	if err != nil {
		t.Fatalf("TruncatedThrough: %v", err)
	}
	if through != posC {
		t.Fatalf("TruncatedThrough = %v, want %v", through, posC)
	}

	// With no maximum, nothing ever forces the stray entry out.
	if n, err := store.CleanupOldTransactionRecords(0, 0, posC); err != nil || n != 0 {
		t.Fatalf("max retention 0: deleted = %d, err = %v; want 0", n, err)
	}

	n, err = store.CleanupOldTransactionRecords(0, time.Millisecond, posC)
	if err != nil {
		t.Fatalf("CleanupOldTransactionRecords: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted = %d, want 1 (the stray entry, past max retention)", n)
	}
	if rec, _ := store.GetTransaction(3402); rec != nil {
		t.Fatal("the stray entry must be deleted past max retention")
	}
}
