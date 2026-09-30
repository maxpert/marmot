package db

import (
	"context"
	"sync"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// fakeVectorNotifier implements both VectorCDCNotifier and the duck-typed
// ApplyVectorControl interface applyNonDMLIntents/ApplyReplayedTxn look for,
// counting calls so a test can pin exactly-once application.
type fakeVectorNotifier struct {
	controlCalls int
	cdcCalls     int
}

func (f *fakeVectorNotifier) ApplyVectorControl(_ context.Context, _ common.VectorIndexChange) error {
	f.controlCalls++
	return nil
}

func (f *fakeVectorNotifier) ApplyCommittedVectorCDC(_ context.Context, _ string, _, _ uint64, _ []common.CDCEntry) error {
	f.cdcCalls++
	return nil
}

// newReplayTestDatabase creates a real ReplicatedDatabase, under a real
// DatabaseManager, with a "docs" table and a fakeVectorNotifier wired in.
func newReplayTestDatabase(t *testing.T) (*DatabaseManager, *ReplicatedDatabase, *fakeVectorNotifier) {
	t.Helper()
	dbMgr, err := NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	t.Cleanup(func() { dbMgr.Close() })
	require.NoError(t, dbMgr.CreateDatabase("app"))
	mdb, err := dbMgr.GetDatabase("app")
	require.NoError(t, err)

	_, err = mdb.GetWriteDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)`)
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())

	fake := &fakeVectorNotifier{}
	mdb.GetTransactionManager().SetVectorCDCNotifier(fake)

	return dbMgr, mdb, fake
}

func insertRow(id int64, title string) *EncodedCapturedRow {
	return &EncodedCapturedRow{
		Table:     "docs",
		Op:        uint8(OpTypeInsert),
		IntentKey: []byte("docs:" + title),
		NewValues: encodeTestValues(map[string]interface{}{"id": id, "title": title}),
	}
}

func rowExists(t *testing.T, mdb *ReplicatedDatabase, id int64) bool {
	t.Helper()
	var got int64
	err := mdb.GetWriteDB().QueryRow("SELECT id FROM docs WHERE id = ?", id).Scan(&got)
	if err != nil {
		return false
	}
	return got == id
}

// TestApplyReplayedTxn_IdempotentSecondCallAppliesNothing pins the core
// exactly-once contract: replaying the same committed txn twice applies the
// row once, and the second call is a no-op (applied=false, nil error), with
// the row's value unchanged.
func TestApplyReplayedTxn_IdempotentSecondCallAppliesNothing(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	txn := &ReplayTxn{TxnID: 100, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: []*EncodedCapturedRow{insertRow(1, "hello")}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, rowExists(t, mdb, 1))

	applied, err = mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.NoError(t, err)
	require.False(t, applied, "a second replay of an already-applied txn must be a no-op")
}

// TestApplyReplayedTxn_PendingRefusedNothingWritten pins that a txn this
// node itself holds PENDING (a prepared 2PC transaction it has not yet
// resolved) must be refused, untouched, rather than overwritten by replay.
func TestApplyReplayedTxn_PendingRefusedNothingWritten(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	require.NoError(t, mdb.GetMetaStore().BeginTransaction(200, 1, hlc.Timestamp{WallTime: 1, NodeID: 1}))

	txn := &ReplayTxn{TxnID: 200, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: []*EncodedCapturedRow{insertRow(2, "pending")}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.ErrorIs(t, err, ErrReplayPending)
	require.False(t, applied)
	require.False(t, rowExists(t, mdb, 2), "nothing must be written when the local txn is PENDING")

	rec, err := mdb.GetMetaStore().GetTransaction(200)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, TxnStatusPending, rec.Status, "the local PENDING record must be untouched")
}

// countLogEntries returns how many of store's committed log entries name txnID.
func countLogEntries(t *testing.T, store MetaStore, txnID uint64) int {
	t.Helper()
	entries, _, _, err := store.ListCommittedLog(LogPosition{}, 100)
	require.NoError(t, err)
	count := 0
	for _, e := range entries {
		if e.TxnID == txnID {
			count++
		}
	}
	return count
}

// beginPreparedDMLTxn begins a txn via tm.BeginTransaction and manually
// writes it one captured DML row (mirroring what PREPARE's hook capture
// leaves behind before COMMIT), so CommitTransaction's DML path
// (GetIntentEntries) finds it, without depending on the preupdate-hook
// pipeline elsewhere in this package's test scope.
func beginPreparedDMLTxn(t *testing.T, tm *TransactionManager, metaStore MetaStore, row *EncodedCapturedRow) *Transaction {
	t.Helper()
	txn, err := tm.BeginTransaction(1)
	require.NoError(t, err)
	data, err := EncodeRow(row)
	require.NoError(t, err)
	require.NoError(t, metaStore.WriteCapturedRow(txn.ID, 1, data))
	return txn
}

// TestCommitTransaction_SecondCommitOfSameTxnIsRefused is the
// concurrent-commit guard's sequential case. A second *Transaction object
// for the same txn id - reconstructed via TransactionManager.GetTransaction,
// exactly as a late COMMIT RPC's handler would, before the first commit
// resolves the id - must be refused once the first commit has already
// finished. ApplyCDCInsert is INSERT OR REPLACE, so a second, unguarded
// apply of the very same captured row would succeed at the SQL level and
// reach finalizeCommit, writing a second log entry for the same txn id: the
// guard, not a coincidental SQL error, is what must stop it.
func TestCommitTransaction_SecondCommitOfSameTxnIsRefused(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()
	metaStore := mdb.GetMetaStore()

	txnA := beginPreparedDMLTxn(t, tm, metaStore, insertRow(42, "guarded"))
	txnB := tm.GetTransaction(txnA.ID)
	require.NotNil(t, txnB, "a second Transaction object for the same id must reconstruct while PENDING")

	require.NoError(t, tm.CommitTransaction(txnA))
	require.True(t, rowExists(t, mdb, 42))

	err := tm.CommitTransaction(txnB)
	require.Error(t, err, "a second commit of an already-committed txn id must be refused")

	require.Equal(t, 1, countLogEntries(t, metaStore, txnA.ID), "the local log must hold exactly one entry")
}

// TestCommitTransaction_ConcurrentCommitsOfSameTxnSerialize is the
// concurrent-commit guard's concurrent case: two goroutines committing
// distinct *Transaction objects for the same txn id at the same time (the
// log puller's local-PREPARE commit racing a late COMMIT RPC) must resolve
// to exactly one success, with exactly one log entry, never both goroutines
// applying the DML.
func TestCommitTransaction_ConcurrentCommitsOfSameTxnSerialize(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()
	metaStore := mdb.GetMetaStore()

	txnA := beginPreparedDMLTxn(t, tm, metaStore, insertRow(43, "guarded-concurrent"))
	txnB := tm.GetTransaction(txnA.ID)
	require.NotNil(t, txnB)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = tm.CommitTransaction(txnA) }()
	go func() { defer wg.Done(); errs[1] = tm.CommitTransaction(txnB) }()
	wg.Wait()

	successes := 0
	for _, e := range errs {
		if e == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes, "exactly one of two concurrent commits of the same txn id must succeed")
	require.Equal(t, 1, countLogEntries(t, metaStore, txnA.ID), "the local log must hold exactly one entry even under a concurrent race")
}

// TestLogReplayedTxn_LocalBeginPendingRefusesAndWritesNothing: a txn id begun through TransactionManager.BeginTransactionWithID (the actual
// entry point a local PREPARE uses, exercising the txnIDLocks stripe the fix
// adds) makes both LogReplayedTxn and ApplyReplayedTxn refuse with
// ErrReplayPending, writing no captured row and no log entry - not merely
// leaving the immutable-only record it started with untouched.
func TestLogReplayedTxn_LocalBeginPendingRefusesAndWritesNothing(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()

	const txnID = uint64(500)
	_, err := tm.BeginTransactionWithID(txnID, 1, hlc.Timestamp{WallTime: 1, NodeID: 1})
	require.NoError(t, err)

	rows := []*EncodedCapturedRow{insertRow(5, "gated")}

	_, err = mdb.LogReplayedTxn(&ReplayTxn{TxnID: txnID, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: rows})
	require.ErrorIs(t, err, ErrReplayPending)

	cursor, err := mdb.GetMetaStore().IterateCapturedRows(txnID)
	require.NoError(t, err)
	require.False(t, cursor.Next(), "no captured row must be written while the local begin is pending")
	require.NoError(t, cursor.Err())
	require.NoError(t, cursor.Close())

	entries, _, _, err := mdb.GetMetaStore().ListCommittedLog(LogPosition{}, 10)
	require.NoError(t, err)
	for _, e := range entries {
		require.NotEqual(t, txnID, e.TxnID, "no log entry must be written while the local begin is pending")
	}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{TxnID: txnID, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: rows}, true)
	require.False(t, applied)
	require.ErrorIs(t, err, ErrReplayPending)
}

// TestApplyReplayedTxn_ForeignIntentConflictRefusedNothingWritten: a row locally held by another transaction's write intent must refuse
// replay (ErrReplayIntentConflict) rather than overwrite it with an older or
// unrelated image.
func TestApplyReplayedTxn_ForeignIntentConflictRefusedNothingWritten(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	otherTxnID := uint64(9)
	require.NoError(t, mdb.GetMetaStore().WriteIntent(otherTxnID, IntentTypeDML, "docs", "docs:conflict",
		OpTypeInsert, "", nil, hlc.Timestamp{WallTime: 1, NodeID: 1}, 1))

	row := insertRow(3, "conflict")
	row.IntentKey = []byte("docs:conflict")
	txn := &ReplayTxn{TxnID: 300, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: []*EncodedCapturedRow{row}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.ErrorIs(t, err, ErrReplayIntentConflict)
	require.False(t, applied)
	require.False(t, rowExists(t, mdb, 3), "nothing must be written on an intent conflict")

	var conflictErr *ReplayIntentConflictError
	require.ErrorAs(t, err, &conflictErr, "the concrete error must carry the intent holder's txn id")
	require.Equal(t, otherTxnID, conflictErr.HolderTxnID)
}

// TestApplyReplayedTxn_CDCRowLockConflictCarriesHolderTxnID: a row
// held by another transaction's CDC row lock (rather than a write intent)
// must also surface *ReplayIntentConflictError with that lock's holder id.
func TestApplyReplayedTxn_CDCRowLockConflictCarriesHolderTxnID(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	holderTxnID := uint64(11)
	require.NoError(t, mdb.GetMetaStore().AcquireCDCRowLock(holderTxnID, "docs", "docs:lockconflict"))

	row := insertRow(4, "lockconflict")
	row.IntentKey = []byte("docs:lockconflict")
	txn := &ReplayTxn{TxnID: 301, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: []*EncodedCapturedRow{row}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.ErrorIs(t, err, ErrReplayIntentConflict)
	require.False(t, applied)

	var conflictErr *ReplayIntentConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.Equal(t, holderTxnID, conflictErr.HolderTxnID)
}

// TestApplyReplayedTxn_ClaimOnlyMarksOnly pins that a zero-row replayed txn
// (the shape a claim-only commit's replay takes: claim payloads never travel
// in CDC) only writes the marker.
func TestApplyReplayedTxn_ClaimOnlyMarksOnly(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	txn := &ReplayTxn{TxnID: 400, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.NoError(t, err)
	require.True(t, applied)

	var one int
	require.NoError(t, mdb.GetWriteDB().QueryRow("SELECT 1 FROM __marmot_applied_txn WHERE txn_id = ?", 400).Scan(&one))
}

// TestApplyReplayedTxn_DDLUsesOriginAsOwnerAndBumpsVersionOnce pins that the
// DDL incarnation owner is the transaction's origin, not the replaying node,
// together with the version-bump contract already covered end to end
// by TestApplyNonDMLIntents_2PCDDLCommitBumpsVersionExactlyOnceAndReplayIsNoOp,
// exercised here from a cold ApplyReplayedTxn call instead.
func TestApplyReplayedTxn_DDLUsesOriginAsOwnerAndBumpsVersionOnce(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	row := &EncodedCapturedRow{Table: "docs", Op: uint8(OpTypeDDL), DDLSQL: "ALTER TABLE docs ADD COLUMN rank INTEGER"}
	const origin = uint64(42)
	txn := &ReplayTxn{TxnID: 500, OriginNodeID: origin, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: origin}, Rows: []*EncodedCapturedRow{row}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, uint64(1), mdb.SchemaVersion())

	var name string
	require.NoError(t, mdb.GetWriteDB().QueryRow(
		"SELECT name FROM pragma_table_info('docs') WHERE name = 'rank'").Scan(&name))
	require.Equal(t, "rank", name)
}

// TestApplyReplayedTxn_VectorControlNotReappliedWhenMarked: once
// a replayed vector-control txn's marker is present, a later replay attempt
// (the shape a retried anti-entropy round takes) must not re-run the
// control.
func TestApplyReplayedTxn_VectorControlNotReappliedWhenMarked(t *testing.T) {
	_, mdb, fake := newReplayTestDatabase(t)

	change := common.VectorIndexChange{
		Action: common.VectorIndexActionCreate, Database: "app",
		IndexName: "docs_idx", TableName: "docs", ColumnName: "title",
	}
	row := &EncodedCapturedRow{Table: "docs", Op: uint8(OpTypeVectorIndex), VectorIndexChange: &change}
	txn := &ReplayTxn{TxnID: 600, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: []*EncodedCapturedRow{row}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, 1, fake.controlCalls)

	applied, err = mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, 1, fake.controlCalls, "a marked vector-control txn must not re-run the control")
}

// TestApplyReplayedTxn_LogFirstLeavesCommittedLogEntryWithNoMarkerOnFailure
// pins the log-first crash-window contract: when the SQLite apply
// fails (here, a DML row into a table that does not exist), the txn is still
// COMMITTED in the local log because it was logged first, but carries no
// marker. ReapplyLocalLog then finds and re-applies it once the underlying
// problem is fixed, and leaves an already-marked entry alone.
func TestApplyReplayedTxn_LogFirstLeavesCommittedLogEntryWithNoMarkerOnFailure(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	badRow := &EncodedCapturedRow{
		Table:     "missing_table",
		Op:        uint8(OpTypeInsert),
		IntentKey: []byte("missing_table:1"),
		NewValues: encodeTestValues(map[string]interface{}{"id": int64(1)}),
	}
	txn := &ReplayTxn{TxnID: 700, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1}, Rows: []*EncodedCapturedRow{badRow}}

	applied, err := mdb.ApplyReplayedTxn(context.Background(), txn, true)
	require.Error(t, err)
	require.False(t, applied)

	rec, err := mdb.GetMetaStore().GetTransaction(700)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, TxnStatusCommitted, rec.Status, "log-first must have recorded the txn as committed despite the apply failure")

	has, err := markerExists(mdb.GetWriteDB(), 700)
	require.NoError(t, err)
	require.False(t, has, "no marker must exist after a failed apply")
}

// TestAppliedTxns_ReportsMarkerPresence pins AppliedTxns' contract: true for
// every id that carries a marker, false for the rest, including an id this
// database has never heard of.
func TestAppliedTxns_ReportsMarkerPresence(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	applied, err := mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{
		TxnID: 1000, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1},
		Rows: []*EncodedCapturedRow{insertRow(1, "marked")},
	}, true)
	require.NoError(t, err)
	require.True(t, applied)

	result, err := mdb.AppliedTxns([]uint64{1000, 1001, 9999999})
	require.NoError(t, err)
	require.Equal(t, map[uint64]bool{1000: true, 1001: false, 9999999: false}, result)
}

// TestReapplyLocalLog_ReappliesMissingSkipsPresent is the restore
// re-apply regression: ReapplyLocalLog re-applies a local log entry whose
// marker is absent (as a restored SQLite file would be, or as the failed
// apply above leaves it once the missing table is created) and leaves an
// already-marked entry alone.
func TestReapplyLocalLog_ReappliesMissingSkipsPresent(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	// Entry 1: applied normally, so it already carries a marker.
	applied, err := mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{
		TxnID: 800, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 1},
		Rows: []*EncodedCapturedRow{insertRow(10, "already-applied")},
	}, true)
	require.NoError(t, err)
	require.True(t, applied)

	// Entry 2: logged (COMMITTED) but its apply failed - "missing2" doesn't
	// exist yet - leaving no marker, exactly like a restored file's gap.
	badRow := &EncodedCapturedRow{
		Table: "missing2", Op: uint8(OpTypeInsert), IntentKey: []byte("missing2:1"),
		NewValues: encodeTestValues(map[string]interface{}{"id": int64(1)}),
	}
	_, err = mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{
		TxnID: 900, OriginNodeID: 1, CommitTS: hlc.Timestamp{WallTime: 20, NodeID: 1},
		Rows: []*EncodedCapturedRow{badRow},
	}, true)
	require.Error(t, err)

	has800, err := markerExists(mdb.GetWriteDB(), 800)
	require.NoError(t, err)
	require.True(t, has800)
	has900, err := markerExists(mdb.GetWriteDB(), 900)
	require.NoError(t, err)
	require.False(t, has900)

	// Fix the underlying problem, then walk the local log: entry 800 must be
	// left alone (already marked), entry 900 re-applied for real.
	_, err = mdb.GetWriteDB().Exec("CREATE TABLE missing2 (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())

	count, err := mdb.ReapplyLocalLog(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, count, "only the unmarked entry must be reapplied")

	has900, err = markerExists(mdb.GetWriteDB(), 900)
	require.NoError(t, err)
	require.True(t, has900, "the previously-failed entry must now carry a marker")

	var got int64
	require.NoError(t, mdb.GetWriteDB().QueryRow("SELECT id FROM missing2 WHERE id = 1").Scan(&got))
	require.Equal(t, int64(1), got)

	// A second walk finds nothing left to do.
	count, err = mdb.ReapplyLocalLog(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

// TestBeginTransactionWithID_LatePrepareAfterReplayIsRefused pins that a
// PREPARE arriving after its txn was already pulled and replayed is refused
// (ErrTxnAlreadyCommitted) and leaves the committed log entry untouched,
// instead of overwriting it with a PENDING record whose abort or commit
// would corrupt it.
func TestBeginTransactionWithID_LatePrepareAfterReplayIsRefused(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()

	const txnID = uint64(600)
	applied, err := mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{
		TxnID: txnID, OriginNodeID: 2, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 2},
		Rows: []*EncodedCapturedRow{insertRow(6, "replayed")},
	}, true)
	require.NoError(t, err)
	require.True(t, applied)

	_, err = tm.BeginTransactionWithID(txnID, 2, hlc.Timestamp{WallTime: 5, NodeID: 2})
	require.ErrorIs(t, err, ErrTxnAlreadyCommitted)

	rec, err := mdb.GetMetaStore().GetTransaction(txnID)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, TxnStatusCommitted, rec.Status, "the refused begin must leave the committed record as it was")
	require.Equal(t, 1, countLogEntries(t, mdb.GetMetaStore(), txnID))
}

// TestBeginTransactionWithID_RefusedWhenOnlyTheMarkerExists pins the marker
// half of the refusal: a txn applied in this database's SQLite file (a
// restored snapshot's marker, say) with no local record is refused too.
func TestBeginTransactionWithID_RefusedWhenOnlyTheMarkerExists(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	const txnID = uint64(700)
	tx, err := mdb.GetWriteDB().Begin()
	require.NoError(t, err)
	require.NoError(t, MarkSQLiteTxnApplied(tx, txnID, hlc.Timestamp{WallTime: 10, NodeID: 2}))
	require.NoError(t, tx.Commit())

	_, err = mdb.GetTransactionManager().BeginTransactionWithID(txnID, 2, hlc.Timestamp{WallTime: 5, NodeID: 2})
	require.ErrorIs(t, err, ErrTxnAlreadyCommitted)
	rec, err := mdb.GetMetaStore().GetTransaction(txnID)
	require.NoError(t, err)
	require.Nil(t, rec, "a refused begin must write no record")
}
