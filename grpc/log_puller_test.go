package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestLogPuller_MissedTxnBelowLocalMaxIsDelivered: resuming a pull from
// GetMaxTxnID would skip any missed txn whose id is below the local max.
// PullPair instead pulls a persisted position cursor, so a txn below the
// local max is delivered like any other.
func TestLogPuller_MissedTxnBelowLocalMaxIsDelivered(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "coordinated-by-2")
	peer.seed(t, "app", 2000, 3, 2000, 2, "coordinated-by-3")
	peer.seed(t, "app", 3000, 2, 3000, 3, "coordinated-by-2")
	local.seed(t, "app", 2000, 3, 2000, 2, "coordinated-by-3")

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: peer.addr}, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Equal(t, 2, res.Applied, "only the two locally-missing txns are newly applied")

	require.Equal(t, map[int64]string{1: "coordinated-by-2", 2: "coordinated-by-3", 3: "coordinated-by-2"}, local.rows(t, "app"))

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	cursor, err := mdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	require.Equal(t, uint64(3000), cursor.TxnID, "the cursor must reach the peer's last entry")
}

// TestLogPuller_TwoCoordinatorsInterleavedIdsDeliveredInOnePull pins that
// entries from multiple origins, missing in an order that does not track
// txn id, all arrive from one PullPair call.
func TestLogPuller_TwoCoordinatorsInterleavedIdsDeliveredInOnePull(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	// Interleaved origins 2 and 3; local starts with none of them.
	peer.seed(t, "app", 100, 2, 100, 1, "o2-a")
	peer.seed(t, "app", 150, 3, 150, 2, "o3-a")
	peer.seed(t, "app", 200, 2, 200, 3, "o2-b")
	peer.seed(t, "app", 250, 3, 250, 4, "o3-b")
	peer.seed(t, "app", 300, 2, 300, 5, "o2-c")

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: peer.addr}, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Equal(t, 5, res.Applied)

	require.Equal(t, map[int64]string{1: "o2-a", 2: "o3-a", 3: "o2-b", 4: "o3-b", 5: "o2-c"}, local.rows(t, "app"))
}

// TestLogPuller_InterruptedThenResumedNoDoubleApply pins that an
// interrupted pull persists its cursor mid-log (SetPullCursor after every
// page) and a later PullPair call resumes and finishes, applying nothing
// twice.
func TestLogPuller_InterruptedThenResumedNoDoubleApply(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")
	peer.seed(t, "app", 3000, 2, 3000, 3, "c")

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm, PageSize: 1})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}
	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = lp.PullPair(ctx, peerRef, "app")
	}()
	require.Eventually(t, func() bool {
		pos, err := mdb.GetMetaStore().GetPullCursor(2)
		return err == nil && pos.TxnID == 1000
	}, 5*time.Second, time.Millisecond, "the first page's cursor was never persisted")
	cancel()
	<-done

	res, err := lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp, "a resumed pull must finish the peer's log")

	require.Equal(t, map[int64]string{1: "a", 2: "b", 3: "c"}, local.rows(t, "app"))
	applied, err := mdb.AppliedTxns([]uint64{1000, 2000, 3000})
	require.NoError(t, err)
	require.True(t, applied[1000] && applied[2000] && applied[3000], "every txn must end up applied exactly once")
}

// TestLogPuller_LiveLocalBeginSkippedLaterEntriesApply pins that a txn
// this node holds begun by a PREPARE still executing (live, not yet durably
// prepared) is left for that PREPARE to finish - neither committed nor
// replayed over - while every entry after it still applies in the same
// call; the cursor stays before it until it resolves.
func TestLogPuller_LiveLocalBeginSkippedLaterEntriesApply(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")
	peer.seed(t, "app", 3000, 2, 3000, 3, "c")

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	require.NoError(t, mdb.GetMetaStore().BeginTransaction(2000, 1, hlc.Timestamp{WallTime: 1, NodeID: 1}))

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}

	res, err := lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.False(t, res.CaughtUp)
	require.Equal(t, 2, res.Applied, "the entries before and after the live begin must apply")
	require.Equal(t, map[int64]string{1: "a", 3: "c"}, local.rows(t, "app"))

	cursor, err := mdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), cursor.TxnID, "the cursor must stop before the live begin")

	require.NoError(t, mdb.GetMetaStore().AbortTransaction(2000))

	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Equal(t, map[int64]string{1: "a", 2: "b", 3: "c"}, local.rows(t, "app"), "once resolved, the entry must apply")
}

// TestLogPuller_PreparedLocalTxnCommittedLocallyInSamePull pins that a txn
// this node holds durably prepared, which the peer's log proves COMMITTED,
// is committed through the local commit path in the same PullPair call -
// not left PENDING until the stale-transaction GC - and the entries after it
// apply in that call too.
func TestLogPuller_PreparedLocalTxnCommittedLocallyInSamePull(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")
	peer.seed(t, "app", 3000, 2, 3000, 3, "c")

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	rh := NewReplicationHandler(1, local.dm, hlc.NewClock(1), db.NewSchemaVersionManager(local.dm))
	resp, err := rh.HandleReplicateTransaction(context.Background(), &TransactionRequest{
		TxnId: 2000, SourceNodeId: 2, Database: "app", Phase: TransactionPhase_PREPARE,
		Timestamp: &HLC{WallTime: 2000, NodeId: 2}, RequiredSchemaVersion: mdb.SchemaVersion(),
		Statements: []*Statement{{Type: pb.StatementType_INSERT, TableName: "t", Database: "app",
			Payload: &Statement_RowChange{RowChange: testInsertRowChange("t", []byte("t:2"), map[string][]byte{
				"id": mustMarshalMsgpack(t, int64(2)), "v": mustMarshalMsgpack(t, "b"),
			})}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.ErrorMessage)

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: peer.addr}, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp, "one call must resolve the prepared txn and reach the peer's end")
	require.Equal(t, 3, res.Applied)
	require.Equal(t, map[int64]string{1: "a", 2: "b", 3: "c"}, local.rows(t, "app"))

	rec, err := mdb.GetMetaStore().GetTransaction(2000)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, db.TxnStatusCommitted, rec.Status, "the prepared txn must be committed locally")
	require.Equal(t, int64(2000), rec.CommitTSWall, "the local commit must take the commit timestamp the peer's log records")

	cursor, err := mdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	require.Equal(t, uint64(3000), cursor.TxnID)

	// The coordinator's COMMIT arriving afterwards finds the txn already
	// committed here and is acknowledged, not refused.
	resp, err = rh.HandleReplicateTransaction(context.Background(), &TransactionRequest{
		TxnId: 2000, SourceNodeId: 2, Database: "app", Phase: TransactionPhase_COMMIT,
		Timestamp: &HLC{WallTime: 2000, NodeId: 2},
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.ErrorMessage)
	require.Equal(t, 1, countLocalLogEntries(t, mdb, 2000), "the late COMMIT must not log the txn twice")
}

// countLocalLogEntries counts mdb's local log entries for txnID.
func countLocalLogEntries(t *testing.T, mdb *db.ReplicatedDatabase, txnID uint64) int {
	t.Helper()
	entries, _, _, err := mdb.GetMetaStore().ListCommittedLog(db.LogPosition{}, 4096)
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if e.TxnID == txnID {
			n++
		}
	}
	return n
}

// TestLogPuller_AbandonedBeginAfterRestartResolvedInOnePull pins that a txn
// this node had only begun (never durably prepared) when its process died
// does not read PENDING forever after the restart: the first PullPair call
// delivers that txn from the peer, and every entry after it.
func TestLogPuller_AbandonedBeginAfterRestartResolvedInOnePull(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")
	peer.seed(t, "app", 3000, 2, 3000, 3, "c")

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	require.NoError(t, mdb.GetMetaStore().BeginTransaction(2000, 1, hlc.Timestamp{WallTime: 1, NodeID: 1}))
	local.restart(t)

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: peer.addr}, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Equal(t, 3, res.Applied)
	require.Equal(t, map[int64]string{1: "a", 2: "b", 3: "c"}, local.rows(t, "app"))
}

// TestLogPuller_LocalIntentConflictSkipsEntryContinuesAndResolvesLater
// pins that a row locally held by a foreign write intent refuses that
// entry (ErrReplayIntentConflict), not counted as a failure, while later
// entries in the same page still apply; the cursor stops before the
// conflicting entry until the intent is released.
func TestLogPuller_LocalIntentConflictSkipsEntryContinuesAndResolvesLater(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b") // row id=2 conflicts locally
	peer.seed(t, "app", 3000, 2, 3000, 3, "c")

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	const foreignTxnID = uint64(9999)
	require.NoError(t, mdb.GetMetaStore().WriteIntent(foreignTxnID, db.IntentTypeDML, "t", "t:2",
		db.OpTypeInsert, "", nil, hlc.Timestamp{WallTime: 1, NodeID: 1}, 1))

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}

	res, err := lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.False(t, res.CaughtUp)
	require.Zero(t, res.Stuck, "an intent conflict must not be counted as a failed attempt")
	require.Equal(t, 2, res.Applied, "the entry before and after the conflict must both apply")
	require.Equal(t, map[int64]string{1: "a", 3: "c"}, local.rows(t, "app"))

	cursor, err := mdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), cursor.TxnID, "the cursor must stop before the conflicting entry")

	require.NoError(t, mdb.GetMetaStore().DeleteIntentsByTxn(foreignTxnID))

	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Equal(t, map[int64]string{1: "a", 2: "b", 3: "c"}, local.rows(t, "app"))
}

// TestLogPuller_PoisonTxnStuckAfterMaxAttempts pins that a
// deterministically-failing entry is reported STUCK exactly once after
// MaxReplayAttempts, exposed via StuckTxns and the LogPullStuckTxns metric,
// while other entries (before and after it) still apply and the cursor
// never passes it. Once STUCK, it is retried only every
// StuckRetryEveryRounds calls of PullPair for that pair.
func TestLogPuller_PoisonTxnStuckAfterMaxAttempts(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peerMdb, err := peer.dm.GetDatabase("app")
	require.NoError(t, err)

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")

	// txn 2000 targets a table that exists nowhere, so every replay attempt
	// fails deterministically - including on the peer itself, which still
	// logs it COMMITTED (log-first) before the SQLite apply fails.
	poisonRow := &db.EncodedCapturedRow{
		Table:     "missing_table",
		Op:        uint8(db.OpTypeInsert),
		IntentKey: []byte("missing_table:1"),
		NewValues: encodeSeedValues(t, map[string]interface{}{"id": int64(1)}),
	}
	_, err = peerMdb.ApplyReplayedTxn(context.Background(), &db.ReplayTxn{
		TxnID: 2000, OriginNodeID: 2, CommitTS: hlc.Timestamp{WallTime: 2000, NodeID: 2},
		Rows: []*db.EncodedCapturedRow{poisonRow},
	}, true)
	require.Error(t, err, "the poison row must fail to apply on the peer too")

	peer.seed(t, "app", 3000, 2, 3000, 3, "c")

	lp := NewLogPuller(LogPullerConfig{
		NodeID: 1, Client: NewClient(1), DBManager: local.dm,
		MaxReplayAttempts: 3, StuckRetryEveryRounds: 2,
	})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}

	var res PairResult
	for i := 0; i < 3; i++ {
		res, err = lp.PullPair(context.Background(), peerRef, "app")
		require.NoError(t, err)
	}

	require.Equal(t, map[int64]string{1: "a", 3: "c"}, local.rows(t, "app"), "entries before and after the poison txn must still apply")

	stuck := lp.StuckTxns()
	require.Len(t, stuck, 1)
	require.Equal(t, "app", stuck[0].Database)
	require.Equal(t, uint64(2), stuck[0].PeerNodeID)
	require.Equal(t, uint64(2000), stuck[0].TxnID)
	require.Equal(t, 3, stuck[0].Attempts)
	require.NotEmpty(t, stuck[0].LastError)
	require.Equal(t, 1, res.Stuck)

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	cursor, err := mdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), cursor.TxnID, "the cursor must never pass a STUCK txn")

	// Round 4 (4 % StuckRetryEveryRounds(2) == 0): retried, fails again.
	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.Equal(t, 1, res.Stuck)
	stuck = lp.StuckTxns()
	require.Equal(t, 4, stuck[0].Attempts, "a retry round must re-attempt a STUCK txn")

	// Round 5 (5 % 2 != 0): skipped, attempts unchanged.
	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.Equal(t, 1, res.Stuck)
	stuck = lp.StuckTxns()
	require.Equal(t, 4, stuck[0].Attempts, "a non-retry round must not re-attempt a STUCK txn")
}

// TestLogPuller_NeedsSnapshotWhenPeerTruncatedPastCursor pins that a
// peer whose GC has truncated past this pair's cursor is reported
// NeedsSnapshot with nothing applied, keeps reporting it until the database
// is restored, and after MarkRestored the next pull moves the cursor up to
// the peer's truncation point instead.
func TestLogPuller_NeedsSnapshotWhenPeerTruncatedPastCursor(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")

	peerMdb, err := peer.dm.GetDatabase("app")
	require.NoError(t, err)
	deleted, err := peerMdb.GetMetaStore().CleanupOldTransactionRecords(0, 0, db.LogPosition{Seq: ^uint64(0), TxnID: ^uint64(0)})
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}

	res, err := lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.NeedsSnapshot)
	require.Zero(t, res.Applied)
	require.Empty(t, local.rows(t, "app"))

	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.NeedsSnapshot, "without a restore the truncated gap is never skipped")

	lp.MarkRestored("app")
	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.False(t, res.NeedsSnapshot, "after a restore the pull resumes from the truncation point")
	require.True(t, res.CaughtUp)

	localMdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	cursor, err := localMdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	truncated, err := peerMdb.GetMetaStore().TruncatedThrough()
	require.NoError(t, err)
	require.Equal(t, truncated, cursor, "MarkRestored must move the cursor to the peer's truncation point")
}

// TestLogPuller_SecondRestoreLowersCursorToRecoverAnEntryTheFirstCovered
// pins the two-restore loss: a first restore brings txn X's marker without X
// entering this node's log, so the pull covers X by marker and its cursor
// passes X; a second restore, from a source lacking X, removes X again. The
// pull after that restore must move its cursor down to the peer's
// truncation point - not keep a cursor already past it - so X is listed and
// applied again.
func TestLogPuller_SecondRestoreLowersCursorToRecoverAnEntryTheFirstCovered(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "x")
	peer.seed(t, "app", 2000, 2, 2000, 2, "y")

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	// Restore 1: X's row and marker arrive in the file, not in the log.
	applied, err := mdb.ApplyReplayedTxn(context.Background(), &db.ReplayTxn{
		TxnID: 1000, OriginNodeID: 2, CommitTS: hlc.Timestamp{WallTime: 1000, NodeID: 2},
		Rows: []*db.EncodedCapturedRow{{Table: "t", Op: uint8(db.OpTypeInsert), IntentKey: []byte("t:1"),
			NewValues: encodeSeedValues(t, map[string]interface{}{"id": int64(1), "v": "x"})}},
	}, false)
	require.NoError(t, err)
	require.True(t, applied)

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}
	lp.MarkRestored("app")
	res, err := lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	cursor, err := mdb.GetMetaStore().GetPullCursor(2)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), cursor.TxnID, "the cursor passes X, covered by its marker")

	// Restore 2, from a source that never had X.
	_, err = mdb.GetWriteDB().Exec("DELETE FROM t WHERE id = 1")
	require.NoError(t, err)
	_, err = mdb.GetWriteDB().Exec("DELETE FROM __marmot_applied_txn WHERE txn_id = 1000")
	require.NoError(t, err)
	lp.MarkRestored("app")

	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Equal(t, map[int64]string{1: "x", 2: "y"}, local.rows(t, "app"), "X must be pulled again after the second restore")
}

// failingFetchServer serves FetchTransactions like the real server, except
// that it fails the whole call, as the real one does for an entry it cannot
// serve, when it reaches badTxnID.
type failingFetchServer struct {
	*Server
	badTxnID uint64
}

func (f failingFetchServer) FetchTransactions(req *FetchTransactionsRequest, stream MarmotService_FetchTransactionsServer) error {
	for _, id := range req.TxnIds {
		if id == f.badTxnID {
			return status.Errorf(codes.FailedPrecondition, "transaction %d: captured rows unreadable", id)
		}
		one := &FetchTransactionsRequest{Database: req.Database, RequestingNodeId: req.RequestingNodeId, TxnIds: []uint64{id}}
		if err := f.Server.FetchTransactions(one, stream); err != nil {
			return err
		}
	}
	return nil
}

// TestLogPuller_OneUnservableTxnDoesNotBlockLaterIdsInItsFetch pins that a
// fetch the peer fails at one transaction still delivers the healthy ids
// after it in the same call, and that only the failing id counts a failed
// attempt.
func TestLogPuller_OneUnservableTxnDoesNotBlockLaterIdsInItsFetch(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, func(s *Server) MarmotServiceServer { return failingFetchServer{Server: s, badTxnID: 2000} })
	local := newPullTestNode(t, 1, "app")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")
	peer.seed(t, "app", 3000, 2, 3000, 3, "c")
	peer.seed(t, "app", 4000, 2, 4000, 4, "d")

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm, MaxReplayAttempts: 1})
	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: peer.addr}, "app")
	require.NoError(t, err)
	require.False(t, res.CaughtUp)
	require.Equal(t, 3, res.Applied, "every healthy id must apply in the same call")
	require.Equal(t, map[int64]string{1: "a", 3: "c", 4: "d"}, local.rows(t, "app"))

	stuck := lp.StuckTxns()
	require.Len(t, stuck, 1, "only the unservable id may count a failed attempt")
	require.Equal(t, uint64(2000), stuck[0].TxnID)
	require.Equal(t, 1, res.Stuck)
}

// TestLogPuller_UnimplementedPeerNoError pins the rolling-upgrade path:
// a peer that does not implement ListCommittedLog answers Unimplemented,
// and PullPair reports that without an error.
func TestLogPuller_UnimplementedPeerNoError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := grpc.NewServer()
	RegisterMarmotServiceServer(gs, UnimplementedMarmotServiceServer{})
	go func() { _ = gs.Serve(listener) }()
	t.Cleanup(gs.Stop)

	local := newPullTestNode(t, 1, "app")
	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})

	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: listener.Addr().String()}, "app")
	require.NoError(t, err)
	require.True(t, res.Unimplemented)
	require.Zero(t, res.Applied)
}

// TestLogPuller_DDLReplayAdvancesSchemaVersionForPrepareGate: a DDL applied
// through PullPair advances the local schema
// version (bumped inside the same tx as the marker, on the puller's own
// node), and a subsequent PREPARE stamped with that version is accepted.
func TestLogPuller_DDLReplayAdvancesSchemaVersionForPrepareGate(t *testing.T) {
	peer := newPullTestNode(t, 2, "app")
	peer.serve(t, nil)
	local := newPullTestNode(t, 1, "app")

	peer.seedDDL(t, "app", 1000, 2, 1000, "ALTER TABLE t ADD COLUMN extra TEXT")

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: peer.addr}, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)

	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	require.Equal(t, uint64(1), mdb.SchemaVersion(), "the replayed DDL must advance the local schema version")

	svm := db.NewSchemaVersionManager(local.dm)
	rh := NewReplicationHandler(1, local.dm, hlc.NewClock(1), svm)

	resp, err := rh.HandleReplicateTransaction(context.Background(), &TransactionRequest{
		TxnId: 2000, SourceNodeId: 2, Database: "app", Phase: TransactionPhase_PREPARE,
		Timestamp: &HLC{WallTime: 2000, NodeId: 2}, RequiredSchemaVersion: mdb.SchemaVersion(),
		Statements: []*Statement{{Type: pb.StatementType_INSERT, TableName: "t", Database: "app",
			Payload: &Statement_RowChange{RowChange: testInsertRowChange("t", []byte("t:1"), map[string][]byte{
				"id": mustMarshalMsgpack(t, int64(1)), "v": mustMarshalMsgpack(t, "prepared"),
			})}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.ErrorMessage)
}
