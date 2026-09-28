package grpc

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestCatchUpStrategyFor: a starting node with data always catches up by a
// log pull, however far apart the two sides' max txn ids are. Those ids are
// start timestamps, so their difference is no count of missing txns: on
// 7a66596 a restarted node that missed a few seconds of writes computed a
// "delta" of ~7.5e18 and took a whole-node snapshot, which under write load
// failed its checksum and killed the node.
//
// Mutation: choose FULL_SNAPSHOT when the seed's id is far above the local
// one. "a node with data was sent to a full snapshot" fires.
func TestCatchUpStrategyFor(t *testing.T) {
	cases := []struct {
		name        string
		local, peer map[string]uint64
		want        CatchUpStrategy
	}{
		{"seed without data", map[string]uint64{"marmot": 5}, map[string]uint64{"marmot": 0}, NO_CATCHUP},
		{"new node", map[string]uint64{}, map[string]uint64{"marmot": 7509703466722328577}, FULL_SNAPSHOT},
		{"restarted far behind by id", map[string]uint64{"marmot": 1}, map[string]uint64{"marmot": 7509703466722328577}, DELTA_SYNC},
		{"restarted ahead by id", map[string]uint64{"marmot": 900}, map[string]uint64{"marmot": 100}, DELTA_SYNC},
		{"database missing locally", map[string]uint64{"marmot": 100}, map[string]uint64{"marmot": 100, "other": 5}, DELTA_SYNC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := catchUpStrategyFor(tc.local, tc.peer)
			if tc.want == DELTA_SYNC {
				require.Equal(t, DELTA_SYNC, got, "a node with data was sent to a full snapshot or skipped")
				return
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// TestCatchUpDecision_IncludesPeerNodeID verifies that CatchUpDecision
// includes the peer node ID, not just the address
func TestCatchUpDecision_IncludesPeerNodeID(t *testing.T) {
	decision := &CatchUpDecision{
		Strategy:   DELTA_SYNC,
		PeerNodeID: 12345,
		PeerAddr:   "localhost:5002",
	}

	require.NotZero(t, decision.PeerNodeID,
		"CatchUpDecision should include peer node ID")
	assert.Equal(t, uint64(12345), decision.PeerNodeID,
		"Peer node ID should match expected value")
}

// TestSnapshotFilesToRestore_RunningNodeInstallsOnlyItsDatabase: anti-entropy
// on a running node restores one database. Every other file in the peer's
// snapshot, the system database included, is open on this node; swapping it
// would leave its connections writing to an unlinked file and hand the peer's
// AUTO_INCREMENT claim bases to the next restart.
//
// Mutation: ignore the database argument. "a running node's restore would
// install" fires.
func TestSnapshotFilesToRestore_RunningNodeInstallsOnlyItsDatabase(t *testing.T) {
	dbs := []*DatabaseFileInfo{
		{Name: db.SystemDatabaseName, Filename: db.SystemDatabaseName + ".db"},
		{Name: "app", Filename: "databases/app.db"},
		{Name: "other", Filename: "databases/other.db"},
	}

	files, err := snapshotFilesToRestore(dbs, "app")
	require.NoError(t, err)
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name)
	}
	require.Equal(t, []string{"app"}, names, "a running node's restore would install %v", names)

	all, err := snapshotFilesToRestore(dbs, "")
	require.NoError(t, err)
	require.Len(t, all, 3, "startup catch-up installs every file")

	_, err = snapshotFilesToRestore(dbs, "missing")
	require.Error(t, err)
}

// gatedSnapshotServer is a real peer Server whose StreamSnapshot waits for the
// test, so the test can act while the requesting node's catch-up is between
// its detach and the peer taking its snapshot.
type gatedSnapshotServer struct {
	*Server
	requested chan struct{}
	release   chan struct{}
}

func (g *gatedSnapshotServer) StreamSnapshot(req *SnapshotRequest, stream MarmotService_StreamSnapshotServer) error {
	close(g.requested)
	<-g.release
	return g.Server.StreamSnapshot(req, stream)
}

func newCatchUpTestDB(t *testing.T, dir string, nodeID uint64, rows map[int]string) *db.DatabaseManager {
	t.Helper()
	dm, err := db.NewDatabaseManager(dir, nodeID, hlc.NewClock(nodeID))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dm.Close() })
	require.NoError(t, dm.CreateDatabase("app"))
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	_, err = app.GetWriteDB().Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	require.NoError(t, err)
	for id, v := range rows {
		_, err = app.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (?, ?)", id, v)
		require.NoError(t, err)
	}
	return dm
}

// TestCatchUpFromPeer_RunningNodeDetachesTheDatabaseForTheRestore:
// anti-entropy replaces one database's file on a running node while clients
// and 2PC keep using it. From before the peer takes its snapshot until the
// file is replaced, the database is detached: a caller that looks it up gets
// ErrDatabaseDetached, a caller still holding it gets "sql: database is
// closed", no pool is ever nil, and so no write is ACKed into the file the
// restore discards. Afterwards the node serves exactly the peer's rows, and
// the system database and other databases were never touched.
//
// Mutation: make DetachDatabase leave the database in service (no map
// removal, no close). "a write was ACKed during the restore window" fires.
// Mutation: close the detached pools the way CloseSQLiteConnections does,
// setting them nil. "a caller holding the database observed a nil pool" fires,
// and under -race so does the race detector.
func TestCatchUpFromPeer_RunningNodeDetachesTheDatabaseForTheRestore(t *testing.T) {
	peer := newCatchUpTestDB(t, t.TempDir(), 2, map[int]string{1: "peer"})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peerServer := &Server{}
	peerServer.SetDatabaseManager(peer)
	gated := &gatedSnapshotServer{Server: peerServer, requested: make(chan struct{}), release: make(chan struct{})}
	grpcServer := grpc.NewServer()
	RegisterMarmotServiceServer(grpcServer, gated)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	localDir := t.TempDir()
	local := newCatchUpTestDB(t, localDir, 1, map[int]string{1: "local", 2: "local-only"})
	require.NoError(t, local.CreateDatabase("other"))
	held, err := local.GetDatabase("app")
	require.NoError(t, err)

	client := NewCatchUpClient(1, localDir, NewNodeRegistry(1, "localhost:5001"), nil)
	client.SetDatabaseManager(local)

	// A caller that holds the database across the whole catch-up, detach and
	// reattach included, must only ever see a pool or an error.
	var nilPools atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			writeDB, readDB := held.GetWriteDB(), held.GetReadDB()
			if writeDB == nil || readDB == nil {
				nilPools.Add(1)
				continue
			}
			var n int
			_ = readDB.QueryRow("SELECT COUNT(*) FROM t").Scan(&n)
		}
	}()

	done := make(chan error, 1)
	go func() { done <- client.CatchUpFromPeer(context.Background(), 2, listener.Addr().String(), "app") }()
	<-gated.requested

	// The window: the peer has not taken its snapshot yet.
	acked := 0
	if _, err := local.GetDatabase("app"); err == nil {
		acked++
	} else {
		require.ErrorIs(t, err, db.ErrDatabaseDetached)
	}
	heldWrite, heldRead := held.GetWriteDB(), held.GetReadDB()
	require.NotNil(t, heldWrite, "a caller holding the database observed a nil pool")
	require.NotNil(t, heldRead, "a caller holding the database observed a nil pool")
	if _, err := heldWrite.Exec("INSERT INTO t (id, v) VALUES (100, 'window')"); err == nil {
		acked++
	}
	require.Zero(t, acked, "a write was ACKed during the restore window")
	var n int
	require.Error(t, heldRead.QueryRow("SELECT COUNT(*) FROM t").Scan(&n), "a read was served from the file being replaced")
	other, err := local.GetDatabase("other")
	require.NoError(t, err, "a database not being restored left service")
	_, err = other.GetWriteDB().Exec("CREATE TABLE IF NOT EXISTS o (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	_, err = local.GetSystemDatabase().GetReadDB().Exec("SELECT 1")
	require.NoError(t, err, "the system database left service")

	close(gated.release)
	require.NoError(t, <-done)
	close(stop)
	wg.Wait()
	require.Zero(t, nilPools.Load(), "a caller holding the database observed a nil pool")

	app, err := local.GetDatabase("app")
	require.NoError(t, err)
	rows, err := app.GetReadDB().Query("SELECT id, v FROM t ORDER BY id")
	require.NoError(t, err)
	got := map[int]string{}
	for rows.Next() {
		var id int
		var v string
		require.NoError(t, rows.Scan(&id, &v))
		got[id] = v
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, map[int]string{1: "peer"}, got, "the restored database mixes rows from both files")
	_, err = app.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (3, 'after')")
	require.NoError(t, err, "the reattached database does not accept writes")
}
