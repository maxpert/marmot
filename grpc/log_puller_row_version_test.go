package grpc

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/stretchr/testify/require"
)

// tRow builds a captured change of table t's row id, keyed exactly as the
// CDC hook keys it.
func tRow(t *testing.T, op db.OpType, id int64, oldV, newV string) *db.EncodedCapturedRow {
	row := &db.EncodedCapturedRow{
		Table:     "t",
		Op:        uint8(op),
		IntentKey: filter.EncodeIntentKey("t", []filter.TypedPKValue{{Type: filter.PKTypeInt64, Value: filter.EncodeInt64(id)}}),
	}
	if oldV != "" {
		row.OldValues = encodeSeedValues(t, map[string]interface{}{"id": id, "v": oldV})
	}
	if newV != "" {
		row.NewValues = encodeSeedValues(t, map[string]interface{}{"id": id, "v": newV})
	}
	return row
}

// seedRows commits rows as txnID, coordinated by origin at wall, into
// database d, logging it in n's own commit log in call order.
func (n *pullTestNode) seedRows(t *testing.T, d string, txnID, origin uint64, wall int64, rows ...*db.EncodedCapturedRow) {
	t.Helper()
	mdb, err := n.dm.GetDatabase(d)
	require.NoError(t, err)
	_, err = mdb.ApplyReplayedTxn(context.Background(), &db.ReplayTxn{
		TxnID: txnID, OriginNodeID: origin, CommitTS: hlc.Timestamp{WallTime: wall, NodeID: origin}, Rows: rows,
	}, true)
	require.NoError(t, err)
}

// pullBoth pulls database d from peers a then b into local.
func pullBoth(t *testing.T, local, a, b *pullTestNode, d string) {
	t.Helper()
	lp := NewLogPuller(LogPullerConfig{NodeID: local.id, Client: NewClient(local.id), DBManager: local.dm})
	for _, p := range []*pullTestNode{a, b} {
		res, err := lp.PullPair(context.Background(), PeerRef{NodeID: p.id, Address: p.addr}, d)
		require.NoError(t, err)
		require.True(t, res.CaughtUp)
	}
}

// Peer 2 logged T2 (row 1 = v2) before T1 (row 1 = v1), as a peer that
// missed T1's COMMIT and pulled it later does; peer 3 logged them in commit
// order. Whichever order the puller meets them in, row 1 ends as v2.
func TestLogPuller_PeersListingSameTxnsInDifferentOrdersConvergeOnNewestImage(t *testing.T) {
	late, inOrder := newPullTestNode(t, 2, "app"), newPullTestNode(t, 3, "app")
	late.serve(t, nil)
	inOrder.serve(t, nil)
	const t0, t1, t2 = 100, 200, 300
	for _, p := range []*pullTestNode{late, inOrder} {
		p.seedRows(t, "app", t0, 3, 10, tRow(t, db.OpTypeInsert, 1, "", "v0"))
	}
	late.seedRows(t, "app", t2, 3, 30, tRow(t, db.OpTypeUpdate, 1, "v1", "v2"))
	late.seedRows(t, "app", t1, 2, 20, tRow(t, db.OpTypeUpdate, 1, "v0", "v1"))
	inOrder.seedRows(t, "app", t1, 2, 20, tRow(t, db.OpTypeUpdate, 1, "v0", "v1"))
	inOrder.seedRows(t, "app", t2, 3, 30, tRow(t, db.OpTypeUpdate, 1, "v1", "v2"))

	for _, order := range [][2]*pullTestNode{{late, inOrder}, {inOrder, late}} {
		local := newPullTestNode(t, 1, "app")
		pullBoth(t, local, order[0], order[1], "app")
		require.Equal(t, map[int64]string{1: "v2"}, local.rows(t, "app"), "pulled from node %d first", order[0].id)
	}
}

// A peer that logged a DELETE before the older INSERT it follows must not
// make a puller resurrect the row.
func TestLogPuller_OlderInsertListedAfterDeleteDoesNotResurrectRow(t *testing.T) {
	late, inOrder := newPullTestNode(t, 2, "app"), newPullTestNode(t, 3, "app")
	late.serve(t, nil)
	inOrder.serve(t, nil)
	late.seedRows(t, "app", 200, 3, 30, tRow(t, db.OpTypeDelete, 1, "v1", ""))
	late.seedRows(t, "app", 100, 2, 20, tRow(t, db.OpTypeInsert, 1, "", "v1"))
	inOrder.seedRows(t, "app", 100, 2, 20, tRow(t, db.OpTypeInsert, 1, "", "v1"))
	inOrder.seedRows(t, "app", 200, 3, 30, tRow(t, db.OpTypeDelete, 1, "v1", ""))

	for _, order := range [][2]*pullTestNode{{late, inOrder}, {inOrder, late}} {
		local := newPullTestNode(t, 1, "app")
		pullBoth(t, local, order[0], order[1], "app")
		require.Empty(t, local.rows(t, "app"), "pulled from node %d first", order[0].id)
	}
}

// A COMMIT's decided timestamp and a PREPARE answer's clock survive the
// wire in both directions; a request without one sends none.
func TestCommitTimestampAndAppliedAtCrossTheWire(t *testing.T) {
	commitTS := hlc.Timestamp{WallTime: 42, Logical: 3, NodeID: 7}
	req, err := transactionRequestToProto(&coordinator.ReplicationRequest{TxnID: 1, Phase: coordinator.PhaseCommit, CommitTS: commitTS})
	require.NoError(t, err)
	require.Equal(t, commitTS, HLCToTimestamp(req.CommitTimestamp))

	req, err = transactionRequestToProto(&coordinator.ReplicationRequest{TxnID: 1, Phase: coordinator.PhasePrep})
	require.NoError(t, err)
	require.Nil(t, req.CommitTimestamp)

	resp := convertTransactionResponse(&TransactionResponse{Success: true, AppliedAt: &HLC{WallTime: 9, Logical: 1, NodeId: 2}})
	require.Equal(t, hlc.Timestamp{WallTime: 9, Logical: 1, NodeID: 2}, resp.AppliedAt)
}

// prepareOneRow PREPAREs txnID on rh as coordinator 2's insert of t row id.
func prepareOneRow(t *testing.T, rh *ReplicationHandler, mdb *db.ReplicatedDatabase, txnID uint64, id int64) {
	t.Helper()
	resp, err := rh.HandleReplicateTransaction(context.Background(), &TransactionRequest{
		TxnId: txnID, SourceNodeId: 2, Database: "app", Phase: TransactionPhase_PREPARE,
		Timestamp: &HLC{WallTime: 1, NodeId: 2}, RequiredSchemaVersion: mdb.SchemaVersion(),
		Statements: []*Statement{{Type: pb.StatementType_INSERT, TableName: "t", Database: "app",
			Payload: &Statement_RowChange{RowChange: testInsertRowChange("t", tRow(t, db.OpTypeInsert, id, "", "x").IntentKey, map[string][]byte{
				"id": mustMarshalMsgpack(t, id), "v": mustMarshalMsgpack(t, "x"),
			})}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.ErrorMessage)
}

// A participant commits with the coordinator's commit timestamp, so its log
// serves the same one every other node's does.
func TestCommitRPCCommitsWithTheCoordinatorsTimestamp(t *testing.T) {
	node := newPullTestNode(t, 1, "app")
	mdb, err := node.dm.GetDatabase("app")
	require.NoError(t, err)
	rh := NewReplicationHandler(1, node.dm, hlc.NewClock(1), db.NewSchemaVersionManager(node.dm))
	prepareOneRow(t, rh, mdb, 4000, 1)

	decided := &HLC{WallTime: 777, Logical: 2, NodeId: 2}
	resp, err := rh.HandleReplicateTransaction(context.Background(), &TransactionRequest{
		TxnId: 4000, SourceNodeId: 2, Database: "app", Phase: TransactionPhase_COMMIT,
		Timestamp: &HLC{WallTime: 1, NodeId: 2}, CommitTimestamp: decided,
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.ErrorMessage)

	rec, err := mdb.GetMetaStore().GetTransaction(4000)
	require.NoError(t, err)
	require.Equal(t, decided.WallTime, rec.CommitTSWall)
	require.Equal(t, decided.Logical, rec.CommitTSLogical)
}
