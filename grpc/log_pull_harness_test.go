package grpc

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// pullTestNode is one in-process node for LogPuller regression tests: a
// real db.DatabaseManager, optionally served over a real gRPC
// MarmotServiceServer, so LogPuller exercises the exact
// ListCommittedLog/FetchTransactions wire path a production pull round
// uses.
type pullTestNode struct {
	id   uint64
	dir  string
	dm   *db.DatabaseManager
	addr string
}

// newPullTestNode creates a node with the given databases, each holding a
// table t(id INTEGER PRIMARY KEY, v TEXT) ready for seeding through
// ReplicatedDatabase.ApplyReplayedTxn (the same path production replay
// uses), so a test can build a peer's local commit log directly, without a
// second node or the 2PC wire path.
func newPullTestNode(t *testing.T, id uint64, databases ...string) *pullTestNode {
	t.Helper()
	dir := t.TempDir()
	dm, err := db.NewDatabaseManager(dir, id, hlc.NewClock(id))
	require.NoError(t, err)
	n := &pullTestNode{id: id, dir: dir, dm: dm}
	t.Cleanup(func() { _ = n.dm.Close() })

	for _, name := range databases {
		require.NoError(t, dm.CreateDatabase(name))
		mdb, err := dm.GetDatabase(name)
		require.NoError(t, err)
		_, err = mdb.GetWriteDB().Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
		require.NoError(t, err)
		require.NoError(t, mdb.ReloadSchema())
	}
	return n
}

// restart closes n's DatabaseManager and reopens it over the same data
// directory, as a process restart would. n must not be serving.
func (n *pullTestNode) restart(t *testing.T) {
	t.Helper()
	require.NoError(t, n.dm.Close())
	dm, err := db.NewDatabaseManager(n.dir, n.id, hlc.NewClock(n.id))
	require.NoError(t, err)
	n.dm = dm
}

// serve starts n over real gRPC, optionally through wrap (for example, a
// server that deliberately omits ListCommittedLog to simulate a
// rolling-upgrade peer), and records the listen address.
func (n *pullTestNode) serve(t *testing.T, wrap func(*Server) MarmotServiceServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &Server{nodeID: n.id, registry: NewNodeRegistry(n.id, listener.Addr().String())}
	server.SetDatabaseManager(n.dm)

	var service MarmotServiceServer = server
	if wrap != nil {
		service = wrap(server)
	}
	gs := grpc.NewServer()
	RegisterMarmotServiceServer(gs, service)
	go func() { _ = gs.Serve(listener) }()
	t.Cleanup(gs.Stop)
	n.addr = listener.Addr().String()
}

// encodeSeedValues msgpack-encodes each value, as EncodedCapturedRow's
// OldValues/NewValues require (CDC never uses JSON - CLAUDE.md).
func encodeSeedValues(t *testing.T, values map[string]interface{}) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte, len(values))
	for k, v := range values {
		b, err := encoding.Marshal(v)
		require.NoError(t, err)
		out[k] = b
	}
	return out
}

// seed commits txnID (coordinated by origin, committed at wall) into
// database d through ReplicatedDatabase.ApplyReplayedTxn, inserting one row
// (rowID, v) into table t.
func (n *pullTestNode) seed(t *testing.T, d string, txnID, origin uint64, wall int64, rowID int64, v string) {
	t.Helper()
	mdb, err := n.dm.GetDatabase(d)
	require.NoError(t, err)

	row := &db.EncodedCapturedRow{
		Table:     "t",
		Op:        uint8(db.OpTypeInsert),
		IntentKey: []byte(fmt.Sprintf("t:%d", rowID)),
		NewValues: encodeSeedValues(t, map[string]interface{}{"id": rowID, "v": v}),
	}
	applied, err := mdb.ApplyReplayedTxn(context.Background(), &db.ReplayTxn{
		TxnID:        txnID,
		OriginNodeID: origin,
		CommitTS:     hlc.Timestamp{WallTime: wall, NodeID: origin},
		Rows:         []*db.EncodedCapturedRow{row},
	}, true)
	require.NoError(t, err)
	require.True(t, applied)
}

// seedDDL commits txnID (coordinated by origin) into database d as a DDL
// transaction, through the same ApplyReplayedTxn path, bumping the schema
// version exactly as a 2PC DDL commit or a real replay would.
func (n *pullTestNode) seedDDL(t *testing.T, d string, txnID, origin uint64, wall int64, sql string) {
	t.Helper()
	mdb, err := n.dm.GetDatabase(d)
	require.NoError(t, err)

	row := &db.EncodedCapturedRow{Table: "t", Op: uint8(db.OpTypeDDL), DDLSQL: sql}
	applied, err := mdb.ApplyReplayedTxn(context.Background(), &db.ReplayTxn{
		TxnID:        txnID,
		OriginNodeID: origin,
		CommitTS:     hlc.Timestamp{WallTime: wall, NodeID: origin},
		Rows:         []*db.EncodedCapturedRow{row},
	}, true)
	require.NoError(t, err)
	require.True(t, applied)
}

// rows reads table t of database d.
func (n *pullTestNode) rows(t *testing.T, d string) map[int64]string {
	t.Helper()
	mdb, err := n.dm.GetDatabase(d)
	require.NoError(t, err)
	rs, err := mdb.GetReadDB().Query("SELECT id, v FROM t")
	require.NoError(t, err)
	defer rs.Close()

	got := map[int64]string{}
	for rs.Next() {
		var id int64
		var v string
		require.NoError(t, rs.Scan(&id, &v))
		got[id] = v
	}
	require.NoError(t, rs.Err())
	return got
}
