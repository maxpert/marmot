package grpc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// servePeer serves dm over real gRPC as node nodeID, through wrap when it is
// not nil, and returns the address.
func servePeer(t *testing.T, dm *db.DatabaseManager, nodeID uint64, wrap func(*Server) MarmotServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &Server{
		nodeID:   nodeID,
		registry: NewNodeRegistry(nodeID, listener.Addr().String()),
	}
	server.SetDatabaseManager(dm)
	var service MarmotServiceServer = server
	if wrap != nil {
		service = wrap(server)
	}
	grpcServer := grpc.NewServer()
	RegisterMarmotServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	return listener.Addr().String()
}

// appRows reads table t of dm's database "app".
func appRows(t *testing.T, dm *db.DatabaseManager) map[int64]string {
	t.Helper()
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	rows, err := app.GetReadDB().Query("SELECT id, v FROM t")
	require.NoError(t, err)
	defer rows.Close()
	got := map[int64]string{}
	for rows.Next() {
		var id int64
		var v string
		require.NoError(t, rows.Scan(&id, &v))
		got[id] = v
	}
	require.NoError(t, rows.Err())
	return got
}

// TestCatchUpFromPeer_WriteInFlightAcrossTheDetachIsRefusedOrRestored: a
// running-node restore under concurrent load - writers and readers that look
// the database up per call, a writer and a reader that hold it throughout -
// with one write transaction begun before the catch-up and committed after
// its detach has begun. That transaction's commit is refused or its row
// survives the restore; no write that began after the detach was observed is
// ACKed and then lost; no caller sees a nil pool, a panic, or rows from both
// files.
//
// Mutation: drop the commit hook's refusal (writeGate.commitHook returns 0).
// The detach waits for the transaction, which then commits into the file the
// restore replaces, and "an in-flight transaction was ACKed and lost to the
// restore" fires.
func TestCatchUpFromPeer_WriteInFlightAcrossTheDetachIsRefusedOrRestored(t *testing.T) {
	peerRows := map[int]string{}
	for i := 1; i <= 20; i++ {
		peerRows[i] = "peer"
	}
	peer := newCatchUpTestDB(t, t.TempDir(), 2, peerRows)
	var gated *gatedSnapshotServer
	addr := servePeer(t, peer, 2, func(s *Server) MarmotServiceServer {
		gated = &gatedSnapshotServer{Server: s, requested: make(chan struct{}), release: make(chan struct{})}
		return gated
	})

	localDir := t.TempDir()
	local := newCatchUpTestDB(t, localDir, 1, map[int]string{1: "local", 2: "local-only"})
	held, err := local.GetDatabase("app")
	require.NoError(t, err)
	client := NewCatchUpClient(1, localDir, NewNodeRegistry(1, "localhost:5001"), nil)
	client.SetDatabaseManager(local)

	inflight, err := held.GetWriteDB().Begin()
	require.NoError(t, err)
	_, err = inflight.Exec("INSERT INTO t (id, v) VALUES (500, 'in flight')")
	require.NoError(t, err)

	type ack struct {
		id               int64
		beganAfterDetach bool
	}
	var (
		nextID                  atomic.Int64
		detached                atomic.Bool
		panics, nilPools, mixed atomic.Int64
		acksMu                  sync.Mutex
		acks                    []ack
		stop                    = make(chan struct{})
		wg                      sync.WaitGroup
	)
	nextID.Store(1000)
	run := func(step func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				func() {
					defer func() {
						if recover() != nil {
							panics.Add(1)
						}
					}()
					step()
				}()
			}
		}()
	}
	write := func(mdb *db.ReplicatedDatabase) {
		beganAfterDetach := detached.Load()
		w := mdb.GetWriteDB()
		if w == nil {
			nilPools.Add(1)
			return
		}
		id := nextID.Add(1)
		if _, err := w.Exec("INSERT INTO t (id, v) VALUES (?, 'w')", id); err != nil {
			return
		}
		acksMu.Lock()
		acks = append(acks, ack{id: id, beganAfterDetach: beganAfterDetach})
		acksMu.Unlock()
	}
	read := func(mdb *db.ReplicatedDatabase) {
		r := mdb.GetReadDB()
		if r == nil {
			nilPools.Add(1)
			return
		}
		rows, err := r.Query("SELECT v FROM t")
		if err != nil {
			return
		}
		defer rows.Close()
		seen := map[string]bool{}
		for rows.Next() {
			var v string
			if rows.Scan(&v) == nil {
				seen[v] = true
			}
		}
		if seen["peer"] && (seen["local"] || seen["local-only"]) {
			mixed.Add(1)
		}
	}
	lookup := func() *db.ReplicatedDatabase {
		mdb, err := local.GetDatabase("app")
		if err != nil {
			detached.Store(true)
			return nil
		}
		return mdb
	}
	for i := 0; i < 3; i++ {
		run(func() {
			if mdb := lookup(); mdb != nil {
				write(mdb)
			}
		})
		run(func() {
			if mdb := lookup(); mdb != nil {
				read(mdb)
			}
		})
	}
	run(func() { write(held) })
	run(func() { read(held) })

	done := make(chan error, 1)
	go func() { done <- client.CatchUpFromPeer(context.Background(), 2, addr, "app") }()
	require.Eventually(t, func() bool {
		_, err := local.GetDatabase("app")
		return errors.Is(err, db.ErrDatabaseDetached)
	}, 10*time.Second, time.Millisecond, "the catch-up never detached the database")
	detached.Store(true)
	inflightErr := inflight.Commit()
	<-gated.requested
	close(gated.release)
	require.NoError(t, <-done)
	time.Sleep(100 * time.Millisecond) // writes after the reattach
	close(stop)
	wg.Wait()

	present := appRows(t, local)
	require.Equal(t, "peer", present[1], "the peer snapshot was not installed")
	require.Equal(t, "peer", present[20], "the peer snapshot was not installed")
	require.Equal(t, "peer", present[2], "the local file survived the restore")
	require.Zero(t, panics.Load(), "a caller panicked")
	require.Zero(t, nilPools.Load(), "a caller observed a nil pool")
	require.Zero(t, mixed.Load(), "a read saw rows from both files")
	for _, a := range acks {
		if a.beganAfterDetach {
			require.Contains(t, present, a.id, "a write begun after the detach was ACKed and lost")
		}
	}
	_, inflightPresent := present[500]
	t.Logf("in-flight commit: err=%v, row present after restore=%v; %d writes ACKed", inflightErr, inflightPresent, len(acks))
	require.False(t, inflightErr == nil && !inflightPresent, "an in-flight transaction was ACKed and lost to the restore")
}

// unavailableOnceServer records the first GetSnapshotInfo it answers with
// codes.Unavailable.
type unavailableOnceServer struct {
	*Server
	once        sync.Once
	unavailable chan struct{}
}

func (u *unavailableOnceServer) GetSnapshotInfo(ctx context.Context, req *SnapshotInfoRequest) (*SnapshotInfoResponse, error) {
	resp, err := u.Server.GetSnapshotInfo(ctx, req)
	if status.Code(err) == codes.Unavailable {
		u.once.Do(func() { close(u.unavailable) })
	}
	return resp, err
}

// TestCatchUpRetriesASeedWhoseSnapshotIsUnavailable: a seed with one of its
// databases detached for its own restore cannot ship a complete snapshot, so
// it answers codes.Unavailable instead of silently leaving that database's
// file out. A joining node's startup catch-up retries, and completes with
// every database once the seed has reattached it.
//
// Mutation: return the producer's error as is from GetSnapshotInfo (no
// snapshotError). "a startup catch-up gave up on a seed whose snapshot was
// only temporarily unavailable" fires. Mutation: drop CatchUp's retry loop.
// The same assertion fires.
func TestCatchUpRetriesASeedWhoseSnapshotIsUnavailable(t *testing.T) {
	seed := newCatchUpTestDB(t, t.TempDir(), 2, map[int]string{1: "seed"})
	require.NoError(t, seed.DetachDatabase(context.Background(), "app"))
	var wrapped *unavailableOnceServer
	addr := servePeer(t, seed, 2, func(s *Server) MarmotServiceServer {
		wrapped = &unavailableOnceServer{Server: s, unavailable: make(chan struct{})}
		return wrapped
	})

	_, err := wrapped.Server.GetSnapshotInfo(context.Background(), &SnapshotInfoRequest{RequestingNodeId: 1})
	require.Equal(t, codes.Unavailable, status.Code(err), "a seed with a detached database did not report its snapshot unavailable: %v", err)

	joinDir := t.TempDir()
	client := NewCatchUpClient(1, joinDir, NewNodeRegistry(1, "localhost:5001"), []string{addr})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.CatchUp(ctx)
		done <- err
	}()
	select {
	case <-wrapped.unavailable:
	case err := <-done:
		t.Fatalf("a startup catch-up gave up on a seed whose snapshot was only temporarily unavailable: %v", err)
	}
	require.NoError(t, seed.AttachDatabase("app"))
	require.NoError(t, <-done, "a startup catch-up gave up on a seed whose snapshot was only temporarily unavailable")
	_, err = os.Stat(filepath.Join(joinDir, "databases", "app.db"))
	require.NoError(t, err, "the startup catch-up left out the database that was detached on the seed")
}

// TestAntiEntropyRestoresADatabaseWhoseReattachFailed: a restore whose
// reattach cannot open the installed file leaves the database out of service
// and out of ListDatabases. Anti-entropy's next round restores it from the
// first alive peer that can serve it, and it is back in service with the
// peer's rows.
//
// Mutation: make DetachDatabase refuse a database whose reattach failed. The
// restore fails and "a database whose reattach failed was never restored"
// fires. Mutation: stop restoreAwaitingDatabases after the first peer. The
// same assertion fires.
func TestAntiEntropyRestoresADatabaseWhoseReattachFailed(t *testing.T) {
	peer := newCatchUpTestDB(t, t.TempDir(), 3, map[int]string{1: "peer"})
	addr := servePeer(t, peer, 3, nil)

	localDir := t.TempDir()
	local := newCatchUpTestDB(t, localDir, 1, map[int]string{1: "local"})
	path, err := local.GetDatabasePath("app")
	require.NoError(t, err)
	require.NoError(t, local.DetachDatabase(context.Background(), "app"))
	for _, suffix := range []string{"-wal", "-shm"} {
		require.NoError(t, os.RemoveAll(path+suffix))
	}
	require.NoError(t, os.WriteFile(path, []byte("not a SQLite database, not at all"), 0o644))
	require.Error(t, local.AttachDatabase("app"))
	require.Equal(t, []string{"app"}, local.DatabasesAwaitingRestore())
	require.NotContains(t, local.ListDatabases(), "app")

	client := NewCatchUpClient(1, localDir, NewNodeRegistry(1, "localhost:5001"), nil)
	client.SetDatabaseManager(local)
	var tried []uint64
	ae := &AntiEntropyService{
		dbManager: local,
		interval:  30 * time.Second,
		logPuller: NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: local}),
		snapshotFunc: func(ctx context.Context, peerNodeID uint64, peerAddr string, database string) error {
			tried = append(tried, peerNodeID)
			return client.CatchUpFromPeer(ctx, peerNodeID, peerAddr, database)
		},
	}
	// Peer 2 is alive in the registry but its address refuses connections.
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachable := closed.Addr().String()
	require.NoError(t, closed.Close())
	nodes := []*NodeState{{NodeId: 2, Address: unreachable}, {NodeId: 3, Address: addr}}
	ae.restoreAwaitingDatabases(nodes)

	require.Empty(t, local.DatabasesAwaitingRestore(), "a database whose reattach failed was never restored")
	require.Equal(t, []uint64{2, 3}, tried)
	require.Equal(t, map[int64]string{1: "peer"}, appRows(t, local))
}

// strandAwaitingRestore leaves dm's database name out of service awaiting a
// restore: its file is replaced by garbage during a detach, so the reattach
// fails.
func strandAwaitingRestore(t *testing.T, dm *db.DatabaseManager, name string) {
	t.Helper()
	path, err := dm.GetDatabasePath(name)
	require.NoError(t, err)
	require.NoError(t, dm.DetachDatabase(context.Background(), name))
	for _, suffix := range []string{"-wal", "-shm"} {
		require.NoError(t, os.RemoveAll(path+suffix))
	}
	require.NoError(t, os.WriteFile(path, []byte("not a SQLite database, not at all"), 0o644))
	require.Error(t, dm.AttachDatabase(name))
	require.Equal(t, []string{name}, dm.DatabasesAwaitingRestore())
}

// TestTwoNodesAwaitingRestoresRecoverFromEachOther: nodes x and y are each
// other's only peer, and each has a different database awaiting a restore.
// A per-database snapshot is refused only while that database is out of
// service on the peer, so one anti-entropy round on each node restores both.
// A whole-node snapshot is still refused while any database is detached.
//
// Mutation: CatchUpFromPeer asks for every database's snapshot info (no
// SnapshotInfoRequest.Database). Each peer refuses because of its own
// awaiting database, and "a database awaiting a restore was never restored
// by a peer awaiting another" fires. Mutation: TakeSnapshotForDatabase refuses
// while any database is detached (errIfDetachedLocked). The same assertion
// fires. Mutation: return TakeSnapshotForDatabase's error as is from
// GetSnapshotInfo (no snapshotError). "a per-database snapshot of a detached
// database was not reported unavailable" fires.
func TestTwoNodesAwaitingRestoresRecoverFromEachOther(t *testing.T) {
	xDir, yDir := t.TempDir(), t.TempDir()
	x := newCatchUpTestDB(t, xDir, 1, map[int]string{1: "x"})
	y := newCatchUpTestDB(t, yDir, 2, map[int]string{1: "y"})
	require.NoError(t, x.CreateDatabase("b"))
	require.NoError(t, y.CreateDatabase("b"))
	var yServer *Server
	xAddr := servePeer(t, x, 1, nil)
	yAddr := servePeer(t, y, 2, func(s *Server) MarmotServiceServer {
		yServer = s
		return s
	})
	strandAwaitingRestore(t, x, "app")
	strandAwaitingRestore(t, y, "b")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := yServer.GetSnapshotInfo(ctx, &SnapshotInfoRequest{RequestingNodeId: 1})
	require.Equal(t, codes.Unavailable, status.Code(err), "a whole-node snapshot was served while a database was detached: %v", err)
	_, err = yServer.GetSnapshotInfo(ctx, &SnapshotInfoRequest{RequestingNodeId: 1, Database: "b"})
	require.Equal(t, codes.Unavailable, status.Code(err), "a per-database snapshot of a detached database was not reported unavailable: %v", err)

	antiEntropy := func(dm *db.DatabaseManager, nodeID uint64, dir string) *AntiEntropyService {
		client := NewCatchUpClient(nodeID, dir, NewNodeRegistry(nodeID, "localhost:5001"), nil)
		client.SetDatabaseManager(dm)
		return &AntiEntropyService{
			dbManager:    dm,
			interval:     30 * time.Second,
			logPuller:    NewLogPuller(LogPullerConfig{NodeID: nodeID, Client: NewClient(nodeID), DBManager: dm}),
			snapshotFunc: client.CatchUpFromPeer,
		}
	}
	yNode := []*NodeState{{NodeId: 2, Address: yAddr}}
	xNode := []*NodeState{{NodeId: 1, Address: xAddr}}
	antiEntropy(x, 1, xDir).restoreAwaitingDatabases(yNode)
	antiEntropy(y, 2, yDir).restoreAwaitingDatabases(xNode)

	require.Empty(t, x.DatabasesAwaitingRestore(), "a database awaiting a restore was never restored by a peer awaiting another")
	require.Empty(t, y.DatabasesAwaitingRestore(), "a database awaiting a restore was never restored by a peer awaiting another")
	require.Equal(t, map[int64]string{1: "y"}, appRows(t, x))
	_, err = y.GetDatabase("b")
	require.NoError(t, err)
}
