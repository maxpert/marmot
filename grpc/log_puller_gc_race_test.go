package grpc

import (
	"context"
	"sync"
	"testing"

	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// TestLogPuller_PreparedPayloadGoneNeverPassesTheCursor: the stale-transaction GC
// deletes a prepared transaction's intents and captured rows, and the log
// puller then reaches that transaction in a peer's log. Its local commit must
// be refused - committing it would record it COMMITTED with no rows, and the
// cursor would pass it for good - so the puller discards the local prepare
// and replays the peer's committed copy in the same call.
//
// Mutation: make TransactionManager.verifyPreparedPayload return nil. The
// pull commits txn 2000 locally with no rows, passes the cursor over it, and
// row 2 is never applied.
func TestLogPuller_PreparedPayloadGoneNeverPassesTheCursor(t *testing.T) {
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

	ms := mdb.GetMetaStore()
	require.NoError(t, ms.DeleteIntentsByTxn(2000))
	require.NoError(t, ms.DeleteIntentEntries(2000))

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}
	res, err := lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Zero(t, res.Stuck)
	require.Equal(t, map[int64]string{1: "a", 2: "b", 3: "c"}, local.rows(t, "app"), "row 2 must come from the peer's committed copy")
	applied, err := mdb.AppliedTxns([]uint64{2000})
	require.NoError(t, err)
	require.True(t, applied[2000])
}

// fetchHookServer serves FetchTransactions like the real server, running
// beforeFetch on the request before anything is streamed and beforeSend on
// each event before it is sent.
type fetchHookServer struct {
	*Server
	beforeFetch func(req *FetchTransactionsRequest)
	beforeSend  func(ev *ChangeEvent)
}

type hookedFetchStream struct {
	MarmotService_FetchTransactionsServer
	beforeSend func(ev *ChangeEvent)
}

func (s hookedFetchStream) Send(ev *ChangeEvent) error {
	if s.beforeSend != nil {
		s.beforeSend(ev)
	}
	return s.MarmotService_FetchTransactionsServer.Send(ev)
}

func (f fetchHookServer) FetchTransactions(req *FetchTransactionsRequest, stream MarmotService_FetchTransactionsServer) error {
	if f.beforeFetch != nil {
		f.beforeFetch(req)
	}
	return f.Server.FetchTransactions(req, hookedFetchStream{MarmotService_FetchTransactionsServer: stream, beforeSend: f.beforeSend})
}

// TestLogPuller_EventForAnotherDatabaseIsAFailedAttempt pins that a fetched
// event naming a database other than the pair's is never applied (it would
// cover the pair's entry with another database's transaction) and counts as
// a failed attempt for that entry.
//
// Mutation: drop the ev.Database check in applyFetched. The event applies,
// the entry is covered and nothing is reported STUCK.
func TestLogPuller_EventForAnotherDatabaseIsAFailedAttempt(t *testing.T) {
	peer := newPullTestNode(t, 2, "app", "other")
	peer.serve(t, func(s *Server) MarmotServiceServer {
		return fetchHookServer{Server: s, beforeSend: func(ev *ChangeEvent) {
			if ev.TxnId == 2000 {
				ev.Database = "other"
			}
		}}
	})
	local := newPullTestNode(t, 1, "app", "other")

	peer.seed(t, "app", 1000, 2, 1000, 1, "a")
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm, MaxReplayAttempts: 1})
	res, err := lp.PullPair(context.Background(), PeerRef{NodeID: 2, Address: peer.addr}, "app")
	require.NoError(t, err)
	require.False(t, res.CaughtUp)
	require.Equal(t, 1, res.Stuck)
	require.Equal(t, map[int64]string{1: "a"}, local.rows(t, "app"))
	require.Empty(t, local.rows(t, "other"), "the mislabelled event must not be applied anywhere")
	stuck := lp.StuckTxns()
	require.Len(t, stuck, 1)
	require.Equal(t, uint64(2000), stuck[0].TxnID)
}

// TestLogPuller_LateLocalBeginDuringFetchIsRetriedNotFailed pins that a
// local PREPARE's begin landing for a txn after the walk decided to fetch it
// (so its replay finds the txn PENDING, db.ErrReplayPending) is retried on a
// later call like an intent conflict, never counted as a failed attempt.
//
// Mutation: drop ErrReplayPending from applyFetched's retry case. The entry
// is reported STUCK after one attempt.
func TestLogPuller_LateLocalBeginDuringFetchIsRetriedNotFailed(t *testing.T) {
	local := newPullTestNode(t, 1, "app")
	mdb, err := local.dm.GetDatabase("app")
	require.NoError(t, err)
	peer := newPullTestNode(t, 2, "app")
	var once sync.Once
	begun := make(chan error, 1)
	peer.serve(t, func(s *Server) MarmotServiceServer {
		return fetchHookServer{Server: s, beforeFetch: func(*FetchTransactionsRequest) {
			once.Do(func() {
				begun <- mdb.GetMetaStore().BeginTransaction(2000, 2, hlc.Timestamp{WallTime: 1, NodeID: 2})
			})
		}}
	})
	peer.seed(t, "app", 2000, 2, 2000, 2, "b")

	lp := NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local.dm, MaxReplayAttempts: 1})
	peerRef := PeerRef{NodeID: 2, Address: peer.addr}
	res, err := lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.NoError(t, <-begun, "the late local begin must land during the fetch")
	require.False(t, res.CaughtUp)
	require.Zero(t, res.Stuck, "a late local begin must not count as a failed attempt")
	require.Empty(t, lp.StuckTxns())

	require.NoError(t, mdb.GetMetaStore().AbortTransaction(2000))
	res, err = lp.PullPair(context.Background(), peerRef, "app")
	require.NoError(t, err)
	require.True(t, res.CaughtUp)
	require.Equal(t, map[int64]string{2: "b"}, local.rows(t, "app"))
}
