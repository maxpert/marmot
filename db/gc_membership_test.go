package db

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// TestGCSafePositionNeverDeletesAnEntryAMemberHasNotConsumed drives the real
// TransactionManager GC (cleanupOldTransactionRecords, via the
// GCSafePositionFunc DatabaseManager.wireGCCoordination wires in) through a
// live DatabaseManager, not a bare MetaStore call: member 2 is current but
// has never reported a consumed position, which must pin GC regardless of
// the entry's age: no member may lose an entry it has not pulled.
func TestGCSafePositionNeverDeletesAnEntryAMemberHasNotConsumed(t *testing.T) {
	dbMgr, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()

	// Short retentions so the test does not wait out real hours; the min
	// retention branch is what this test exercises.
	tm.mu.Lock()
	tm.gcMinRetention = time.Millisecond
	tm.gcMaxRetention = time.Hour
	tm.mu.Unlock()

	// Membership: self (1) and members 2 and 3, which have not called
	// SetConsumedPosition yet.
	dbMgr.SetGCMembershipFunc(func() []uint64 { return []uint64{1, 2, 3} })

	txn := &ReplayTxn{TxnID: 9001, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: time.Now().UnixNano(), NodeID: 1}, Rows: []*EncodedCapturedRow{insertRow(1, "a")}}
	applied, err := mdb.ApplyReplayedTxn(t.Context(), txn, true)
	require.NoError(t, err)
	require.True(t, applied)

	time.Sleep(5 * time.Millisecond) // exceed gcMinRetention

	n, err := tm.cleanupOldTransactionRecords()
	require.NoError(t, err)
	require.Equal(t, 0, n, "member 2 has never reported a consumed position; GC must not delete")
	require.True(t, rowExists(t, mdb, 1))

	// Member 2 now reports it has consumed through the txn's position.
	rec, err := mdb.GetMetaStore().GetTransaction(9001)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.NoError(t, mdb.GetMetaStore().SetConsumedPosition(2, LogPosition{Seq: rec.SeqNum, TxnID: 9001}))

	n, err = tm.cleanupOldTransactionRecords()
	require.NoError(t, err)
	require.Equal(t, 0, n, "member 3 has not consumed the entry; GC must not delete it (the safe position is a min)")

	require.NoError(t, mdb.GetMetaStore().SetConsumedPosition(3, LogPosition{Seq: rec.SeqNum, TxnID: 9001}))
	n, err = tm.cleanupOldTransactionRecords()
	require.NoError(t, err)
	require.Equal(t, 1, n, "every member has now consumed through the entry; GC must delete it")
}

// TestGCPermanentlyDownMemberPinsUntilMaxRetentionThenDeletes drives the
// real TransactionManager GC through a live DatabaseManager: a member that
// never reports a consumed position pins deletion only until gcMaxRetention
// elapses, after which GC deletes unconditionally (the documented bound: a
// member down longer than gc_max_retention_hours restores by snapshot).
func TestGCPermanentlyDownMemberPinsUntilMaxRetentionThenDeletes(t *testing.T) {
	dbMgr, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()

	tm.mu.Lock()
	tm.gcMinRetention = time.Hour // keep the min-retention branch out of the way
	tm.gcMaxRetention = 30 * time.Millisecond
	tm.mu.Unlock()

	// Member 2 is a current member that never reports a consumed position -
	// permanently down.
	dbMgr.SetGCMembershipFunc(func() []uint64 { return []uint64{1, 2} })

	txn := &ReplayTxn{TxnID: 9101, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: time.Now().UnixNano(), NodeID: 1}, Rows: []*EncodedCapturedRow{insertRow(2, "b")}}
	applied, err := mdb.ApplyReplayedTxn(t.Context(), txn, true)
	require.NoError(t, err)
	require.True(t, applied)

	n, err := tm.cleanupOldTransactionRecords()
	require.NoError(t, err)
	require.Equal(t, 0, n, "before max retention elapses, a down member's missing position must still pin GC")

	time.Sleep(60 * time.Millisecond) // exceed gcMaxRetention

	n, err = tm.cleanupOldTransactionRecords()
	require.NoError(t, err)
	require.Equal(t, 1, n, "past max retention, GC must delete unconditionally even though member 2 never reported")

	through, err := mdb.GetMetaStore().TruncatedThrough()
	require.NoError(t, err)
	require.NotEqual(t, LogPosition{}, through, "TruncatedThrough must advance past the deleted entry")
}

// TestGCSafePositionExcludesDepartedMembersConsumedPosition drives the real
// wiring's cleanup of a departed member's consumed position: once a member
// is no longer current (SetGCMembershipFunc stops listing it), its stale
// R[self,m,d] must no longer pin GC, and wireGCCoordination's closure
// deletes it via MetaStore.DeleteConsumedPosition.
func TestGCSafePositionExcludesDepartedMembersConsumedPosition(t *testing.T) {
	dbMgr, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()

	tm.mu.Lock()
	tm.gcMinRetention = time.Millisecond
	tm.gcMaxRetention = time.Hour
	tm.mu.Unlock()

	// Three-member cluster: member 3 stays current and catches all the way
	// up; member 2 is about to depart while stuck at the zero position, so
	// the test can tell "still pinning while current" apart from "no longer
	// pins once departed" without also depending on the single-member
	// (no-other-member) fallback case, which GCSafePositionFunc documents
	// separately as "unknown - defer to max retention".
	dbMgr.SetGCMembershipFunc(func() []uint64 { return []uint64{1, 2, 3} })

	txn := &ReplayTxn{TxnID: 9201, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: time.Now().UnixNano(), NodeID: 1}, Rows: []*EncodedCapturedRow{insertRow(3, "c")}}
	applied, err := mdb.ApplyReplayedTxn(t.Context(), txn, true)
	require.NoError(t, err)
	require.True(t, applied)

	rec, err := mdb.GetMetaStore().GetTransaction(9201)
	require.NoError(t, err)
	require.NotNil(t, rec)
	entryPos := LogPosition{Seq: rec.SeqNum, TxnID: 9201}

	// Member 3 has fully caught up; member 2 is stuck at the zero position,
	// which would pin GC if it were still a member.
	require.NoError(t, mdb.GetMetaStore().SetConsumedPosition(2, LogPosition{}))
	require.NoError(t, mdb.GetMetaStore().SetConsumedPosition(3, entryPos))

	time.Sleep(5 * time.Millisecond)

	n, err := tm.cleanupOldTransactionRecords()
	require.NoError(t, err)
	require.Equal(t, 0, n, "member 2's stale (zero) consumed position must still pin GC while it is current")

	// Member 2 leaves membership entirely.
	dbMgr.SetGCMembershipFunc(func() []uint64 { return []uint64{1, 3} })

	n, err = tm.cleanupOldTransactionRecords()
	require.NoError(t, err)
	require.Equal(t, 1, n, "a departed member's consumed position must no longer pin GC")

	positions, err := mdb.GetMetaStore().ConsumedPositions()
	require.NoError(t, err)
	require.NotContains(t, positions, uint64(2), "a departed member's consumed position must be deleted, not merely ignored")
}
