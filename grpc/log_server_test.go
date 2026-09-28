package grpc

import (
	"context"
	"io"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// dialLogPullClient connects to addr with no cluster-secret interceptor
// (servePeer's grpc.Server registers none); auth coverage for these RPCs is
// TestLogPullRPCsRequireClusterSecret in auth_test.go.
func dialLogPullClient(t *testing.T, addr string) MarmotServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return NewMarmotServiceClient(conn)
}

// TestListCommittedLog_PagingStableTruncatedAndConsumedPosition drives
// ListCommittedLog against a real in-process server (servePeer, the pattern
// catch_up_detach_test.go uses) over 5 committed DDL transactions: it must
// page correctly, report every entry as stable (the log-seq tracker marks a
// commit done synchronously, before CommitTransaction returns, so nothing
// pulled from the same goroutine's commits is ever missing), and it must
// record the requester's consumed position as-is - not as a max -
// before it answers.
func TestListCommittedLog_PagingStableTruncatedAndConsumedPosition(t *testing.T) {
	dm, err := db.NewDatabaseManager(t.TempDir(), 2, hlc.NewClock(2))
	require.NoError(t, err)
	t.Cleanup(func() { dm.Close() })
	require.NoError(t, dm.CreateDatabase("app"))
	bumpSchemaVersionForTest(t, dm, "app", 5)

	addr := servePeer(t, dm, 2, nil)
	client := dialLogPullClient(t, addr)

	mdb, err := dm.GetDatabase("app")
	require.NoError(t, err)
	metaStore := mdb.GetMetaStore()

	// database_absent for a database this node does not have.
	absentResp, err := client.ListCommittedLog(context.Background(), &LogListRequest{Database: "nope", RequestingNodeId: 42})
	require.NoError(t, err)
	require.True(t, absentResp.DatabaseAbsent)

	// Page through the whole log with a small limit and collect every entry.
	var all []*LogEntry
	after := &LogListRequest{Database: "app", RequestingNodeId: 42, Limit: 2}
	for {
		resp, err := client.ListCommittedLog(context.Background(), after)
		require.NoError(t, err)
		require.False(t, resp.DatabaseAbsent)
		all = append(all, resp.Entries...)
		if !resp.More {
			require.LessOrEqual(t, len(resp.Entries), 2)
			break
		}
		require.NotEmpty(t, resp.Entries)
		last := resp.Entries[len(resp.Entries)-1]
		after = &LogListRequest{Database: "app", RequestingNodeId: 42, AfterSeq: last.Seq, AfterTxnId: last.TxnId, Limit: 2}
	}
	require.Len(t, all, 5, "expected all 5 committed DDL transactions to be listed")
	for i := 1; i < len(all); i++ {
		require.True(t,
			all[i-1].Seq < all[i].Seq || (all[i-1].Seq == all[i].Seq && all[i-1].TxnId < all[i].TxnId),
			"entries must be strictly increasing in position order: %+v then %+v", all[i-1], all[i])
	}

	stable := metaStore.StableSeq()
	require.GreaterOrEqual(t, stable, all[len(all)-1].Seq, "every entry returned must be at or below the store's own stable point")

	lastResp, err := client.ListCommittedLog(context.Background(), &LogListRequest{Database: "app", RequestingNodeId: 42})
	require.NoError(t, err)
	require.False(t, lastResp.More)
	require.Equal(t, uint64(0), lastResp.TruncatedSeq, "GC has not run: nothing is truncated yet")
	require.Equal(t, uint64(0), lastResp.TruncatedTxnId)
	require.Equal(t, mdb.SchemaVersion(), lastResp.SchemaVersion)

	// The consumed position is the request's consumed field, recorded
	// as-is, not as a max, and never the list position: a requester lists
	// past an entry it has not applied yet.
	highPos := db.LogPosition{Seq: all[len(all)-1].Seq, TxnID: all[len(all)-1].TxnId}
	lowPos := db.LogPosition{Seq: all[0].Seq, TxnID: all[0].TxnId}
	_, err = client.ListCommittedLog(context.Background(), &LogListRequest{
		Database: "app", RequestingNodeId: 99, AfterSeq: highPos.Seq, AfterTxnId: highPos.TxnID,
		ConsumedSeq: highPos.Seq, ConsumedTxnId: highPos.TxnID,
	})
	require.NoError(t, err)
	positions, err := metaStore.ConsumedPositions()
	require.NoError(t, err)
	require.Equal(t, highPos, positions[99])

	_, err = client.ListCommittedLog(context.Background(), &LogListRequest{
		Database: "app", RequestingNodeId: 99, AfterSeq: highPos.Seq, AfterTxnId: highPos.TxnID,
		ConsumedSeq: lowPos.Seq, ConsumedTxnId: lowPos.TxnID,
	})
	require.NoError(t, err)
	positions, err = metaStore.ConsumedPositions()
	require.NoError(t, err)
	require.Equal(t, lowPos, positions[99], "the consumed field is stored as-is (not a max) and not replaced by the list position")
}

// TestFetchTransactions_EOFOriginAndRowCount streams every committed
// transaction of a database to EOF and checks origin_node_id and row_count
// match the commit record exactly.
func TestFetchTransactions_EOFOriginAndRowCount(t *testing.T) {
	dm, err := db.NewDatabaseManager(t.TempDir(), 3, hlc.NewClock(3))
	require.NoError(t, err)
	t.Cleanup(func() { dm.Close() })
	require.NoError(t, dm.CreateDatabase("app"))
	bumpSchemaVersionForTest(t, dm, "app", 3)

	mdb, err := dm.GetDatabase("app")
	require.NoError(t, err)
	metaStore := mdb.GetMetaStore()
	entries, _, more, err := metaStore.ListCommittedLog(db.LogPosition{}, 100)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, entries, 3)

	addr := servePeer(t, dm, 3, nil)
	client := dialLogPullClient(t, addr)

	ids := make([]uint64, len(entries))
	for i, e := range entries {
		ids[i] = e.TxnID
	}
	stream, err := client.FetchTransactions(context.Background(), &FetchTransactionsRequest{
		Database: "app", RequestingNodeId: 42, TxnIds: ids,
	})
	require.NoError(t, err)

	var got []*ChangeEvent
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, ev)
	}
	require.Len(t, got, 3)
	for _, ev := range got {
		rec, err := metaStore.GetTransaction(ev.TxnId)
		require.NoError(t, err)
		require.NotNil(t, rec)
		require.Equal(t, rec.NodeID, ev.OriginNodeId, "origin_node_id must be the commit record's immutable NodeID")
		require.Equal(t, rec.NodeID, ev.Timestamp.NodeId, "timestamp NodeId must also carry the origin")
		require.Equal(t, rec.RowCount, ev.RowCount)
		require.Len(t, ev.Statements, int(rec.RowCount))
	}
}

// TestFetchTransactions_NotFoundForMissingTxn fails the whole call with
// codes.NotFound for a requested id this node's log does not hold (missing,
// or GC'd), never serving a partial set.
func TestFetchTransactions_NotFoundForMissingTxn(t *testing.T) {
	dm, err := db.NewDatabaseManager(t.TempDir(), 4, hlc.NewClock(4))
	require.NoError(t, err)
	t.Cleanup(func() { dm.Close() })
	require.NoError(t, dm.CreateDatabase("app"))

	addr := servePeer(t, dm, 4, nil)
	client := dialLogPullClient(t, addr)

	stream, err := client.FetchTransactions(context.Background(), &FetchTransactionsRequest{
		Database: "app", RequestingNodeId: 42, TxnIds: []uint64{999999},
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

// TestFetchTransactions_FailedPreconditionOnCorruptCapturedRow writes a
// captured row directly through MetaStore that db.DecodeRow cannot parse,
// then checks FetchTransactions fails the whole call rather than serve a
// partial or garbage transaction.
func TestFetchTransactions_FailedPreconditionOnCorruptCapturedRow(t *testing.T) {
	dm, err := db.NewDatabaseManager(t.TempDir(), 5, hlc.NewClock(5))
	require.NoError(t, err)
	t.Cleanup(func() { dm.Close() })
	require.NoError(t, dm.CreateDatabase("app"))

	mdb, err := dm.GetDatabase("app")
	require.NoError(t, err)
	metaStore := mdb.GetMetaStore()

	const txnID = 12345
	require.NoError(t, metaStore.BeginTransaction(txnID, 5, hlc.Timestamp{WallTime: 1}))
	require.NoError(t, metaStore.WriteCapturedRow(txnID, 1, []byte("not a valid msgpack captured row")))
	require.NoError(t, metaStore.SealCapturedRows(txnID))
	require.NoError(t, metaStore.CommitTransaction(txnID, hlc.Timestamp{WallTime: 2}, nil, "app", "", 0, 1))

	addr := servePeer(t, dm, 5, nil)
	client := dialLogPullClient(t, addr)

	stream, err := client.FetchTransactions(context.Background(), &FetchTransactionsRequest{
		Database: "app", RequestingNodeId: 42, TxnIds: []uint64{txnID},
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

// TestListDatabaseRegistry_IncludesTombstones lists both a live and a
// dropped database, so a peer's anti-entropy round can reconcile a drop it
// missed.
func TestListDatabaseRegistry_IncludesTombstones(t *testing.T) {
	dm, err := db.NewDatabaseManager(t.TempDir(), 6, hlc.NewClock(6))
	require.NoError(t, err)
	t.Cleanup(func() { dm.Close() })
	require.NoError(t, dm.CreateDatabase("keepme"))
	require.NoError(t, dm.CreateDatabase("dropme"))
	require.NoError(t, dm.DropDatabase("dropme"))

	addr := servePeer(t, dm, 6, nil)
	client := dialLogPullClient(t, addr)

	resp, err := client.ListDatabaseRegistry(context.Background(), &DatabaseRegistryRequest{RequestingNodeId: 42})
	require.NoError(t, err)

	byName := make(map[string]*DatabaseRegistryEntry, len(resp.Entries))
	for _, e := range resp.Entries {
		byName[e.Name] = e
	}
	require.NotNil(t, byName["keepme"])
	require.False(t, byName["keepme"].Dropped)
	require.GreaterOrEqual(t, byName["keepme"].Generation, uint64(1))

	require.NotNil(t, byName["dropme"])
	require.True(t, byName["dropme"].Dropped, "a dropped database must be listed as a tombstone, not omitted")
}
