package grpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// collectingSnapshotStream records every chunk StreamSnapshot sends and calls
// onSend, when set, after each one.
type collectingSnapshotStream struct {
	grpc.ServerStream
	data   bytes.Buffer
	chunks int
	onSend func(chunks int)
}

func (s *collectingSnapshotStream) Send(c *SnapshotChunk) error {
	s.chunks++
	s.data.Write(c.Data)
	if s.onSend != nil {
		s.onSend(s.chunks)
	}
	return nil
}

func (s *collectingSnapshotStream) SetTrailer(metadata.MD) {}

func (s *collectingSnapshotStream) Context() context.Context { return context.Background() }

func setSnapshotCacheTTL(t *testing.T, seconds int) {
	t.Helper()
	prev := cfg.Config.Replica.SnapshotCacheTTLSec
	cfg.Config.Replica.SnapshotCacheTTLSec = seconds
	t.Cleanup(func() { cfg.Config.Replica.SnapshotCacheTTLSec = prev })
}

func fillApp(t *testing.T, dm *db.DatabaseManager, from, n int) {
	t.Helper()
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	for id := from; id < from+n; id++ {
		_, err = app.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (?, ?)", id, strings.Repeat("p", 200))
		require.NoError(t, err)
	}
}

func snapshotExportDirs(t *testing.T, dm *db.DatabaseManager) []string {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(dm.GetDataDir(), snapshotExportPrefix+"*"))
	require.NoError(t, err)
	return dirs
}

// TestOverlappingCatchUpsLeaveNoSnapshotExport: two nodes catch up the same
// database from one peer at once, ten rounds in a row, as two anti-entropy
// loops do. Every catch-up succeeds and installs the peer's rows. With the
// cluster default TTL of 0 no export directory is left on the peer; with a
// TTL above 0 only the cached export is left, and it goes once it expires.
//
// Mutation: cache the export whatever the TTL. The TTL 0 case finds a
// directory left behind. Mutation: drop StreamSnapshot's release of its
// export. The TTL 0 case finds a directory per catch-up.
func TestOverlappingCatchUpsLeaveNoSnapshotExport(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ttlSeconds int
		cachedDirs int
	}{
		{name: "ttl 0", ttlSeconds: 0, cachedDirs: 0},
		{name: "ttl 30", ttlSeconds: 30, cachedDirs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setSnapshotCacheTTL(t, tc.ttlSeconds)
			peer := newCatchUpTestDB(t, t.TempDir(), 2, map[int]string{1: "peer"})
			fillApp(t, peer, 10, 5000)
			var peerServer *Server
			addr := servePeer(t, peer, 2, func(s *Server) MarmotServiceServer {
				peerServer = s
				return s
			})
			want := appRows(t, peer)

			nodes := make([]*db.DatabaseManager, 2)
			clients := make([]*CatchUpClient, 2)
			for i := range nodes {
				dir := t.TempDir()
				id := uint64(10 + i)
				nodes[i] = newCatchUpTestDB(t, dir, id, map[int]string{1: "local"})
				clients[i] = NewCatchUpClient(id, dir, NewNodeRegistry(id, "localhost:5001"), nil)
				clients[i].SetDatabaseManager(nodes[i])
			}

			for round := 0; round < 10; round++ {
				errs := make([]error, len(clients))
				var wg sync.WaitGroup
				for i, client := range clients {
					wg.Add(1)
					go func() {
						defer wg.Done()
						errs[i] = client.CatchUpFromPeer(context.Background(), 2, addr, "app")
					}()
				}
				wg.Wait()
				for i, err := range errs {
					require.NoError(t, err, "round %d node %d", round, i)
				}
			}
			for _, node := range nodes {
				require.Equal(t, want, appRows(t, node))
			}

			require.Len(t, snapshotExportDirs(t, peer), tc.cachedDirs, "export directories left on the peer")
			peerServer.snapshotExports.evictExpired(time.Now().Add(time.Hour))
			require.Empty(t, snapshotExportDirs(t, peer), "export directories left after the cache expired")
		})
	}
}

// TestSnapshotExportOutlivesEvictionUntilItsReaderFinishes: while a stream
// reads a cached export, the export is evicted and replaced by a newer one.
// Its directory stays until the stream finishes, the stream delivers the
// whole file, and then the directory is removed. Both stream kinds (one
// database and every database) remove their directory when the cache does not
// hold it.
//
// Mutation: make release remove the directory whatever the reference count.
// "the directory was removed while a stream read it" fires.
func TestSnapshotExportOutlivesEvictionUntilItsReaderFinishes(t *testing.T) {
	setSnapshotCacheTTL(t, 30)
	prevChunk := cfg.Config.Replication.StreamChunkSizeKB
	cfg.Config.Replication.StreamChunkSizeKB = 1
	t.Cleanup(func() { cfg.Config.Replication.StreamChunkSizeKB = prevChunk })

	peer := newCatchUpTestDB(t, t.TempDir(), 2, map[int]string{1: "peer"})
	fillApp(t, peer, 10, 200)
	server := &Server{}
	server.SetDatabaseManager(peer)
	req := &SnapshotRequest{RequestingNodeId: 9, Database: "app"}

	var readDir string
	stream := &collectingSnapshotStream{}
	stream.onSend = func(chunks int) {
		if chunks != 1 {
			return
		}
		dirs := snapshotExportDirs(t, peer)
		require.Len(t, dirs, 1)
		readDir = dirs[0]
		server.snapshotExports.evictExpired(time.Now().Add(time.Hour))
		require.NoError(t, server.StreamSnapshot(req, &collectingSnapshotStream{}))
		_, err := os.Stat(readDir)
		require.NoError(t, err, "the directory was removed while a stream read it")
	}
	require.NoError(t, server.StreamSnapshot(req, stream))
	require.Greater(t, stream.chunks, 1)

	info, _, err := peer.TakeDatabaseSnapshotInfo("app")
	require.NoError(t, err)
	sum := sha256.Sum256(stream.data.Bytes())
	require.Equal(t, info.Size, int64(stream.data.Len()), "the stream was truncated")
	require.Equal(t, info.SHA256, hex.EncodeToString(sum[:]))

	_, err = os.Stat(readDir)
	require.True(t, os.IsNotExist(err), "the evicted directory outlived its last reader")
	require.Len(t, snapshotExportDirs(t, peer), 1, "only the replacing cached export may remain")

	server.snapshotExports.evictExpired(time.Now().Add(time.Hour))
	require.Empty(t, snapshotExportDirs(t, peer))
	require.NoError(t, server.StreamSnapshot(&SnapshotRequest{RequestingNodeId: 9}, &collectingSnapshotStream{}))
	require.Empty(t, snapshotExportDirs(t, peer), "a whole-node export outlived its stream")
}

// TestSnapshotExportCacheServesOneExportWithinTheTTL: within the TTL a second
// stream of the same database reads the cached export instead of exporting
// again; an expired entry is never served.
func TestSnapshotExportCacheServesOneExportWithinTheTTL(t *testing.T) {
	setSnapshotCacheTTL(t, 30)
	peer := newCatchUpTestDB(t, t.TempDir(), 2, map[int]string{1: "peer"})
	server := &Server{}
	server.SetDatabaseManager(peer)
	req := &SnapshotRequest{RequestingNodeId: 9, Database: "app"}

	require.NoError(t, server.StreamSnapshot(req, &collectingSnapshotStream{}))
	first := snapshotExportDirs(t, peer)
	require.Len(t, first, 1)
	require.NoError(t, server.StreamSnapshot(req, &collectingSnapshotStream{}))
	require.Equal(t, first, snapshotExportDirs(t, peer), "a hit exported again")

	require.Nil(t, server.snapshotExports.acquire("app", time.Now().Add(time.Hour)), "an expired export was served")
}

// TestSnapshotExportCachePublishReleasesTheReplacedExport: when two misses of
// one database both publish, the export that loses is removed once its own
// stream releases it, and the winner stays until it is evicted.
//
// Mutation: let publish drop the replaced export without releasing it.
// "the replaced export outlived its last reader" fires.
func TestSnapshotExportCachePublishReleasesTheReplacedExport(t *testing.T) {
	dataDir := t.TempDir()
	var cache snapshotExportCache
	expiresAt := time.Now().Add(time.Hour)
	exports := make([]*snapshotExport, 2)
	for i := range exports {
		e, err := newSnapshotExport(dataDir)
		require.NoError(t, err)
		e.expiresAt = expiresAt
		cache.publish("app", e)
		exports[i] = e
	}
	for _, e := range exports {
		e.release()
	}

	_, err := os.Stat(exports[0].dir)
	require.True(t, os.IsNotExist(err), "the replaced export outlived its last reader")
	_, err = os.Stat(exports[1].dir)
	require.NoError(t, err, "the cached export was removed while mapped")

	require.Equal(t, 1, cache.evictExpired(expiresAt))
	_, err = os.Stat(exports[1].dir)
	require.True(t, os.IsNotExist(err), "the evicted export was not removed")
}

// TestSetDatabaseManagerRemovesOrphanedSnapshotExports: export directories a
// crash left in the data directory are removed when the server is first wired
// to its database manager; nothing else in the data directory is touched.
//
// Mutation: drop the sweep from SetDatabaseManager. "an orphaned export
// survived startup" fires.
func TestSetDatabaseManagerRemovesOrphanedSnapshotExports(t *testing.T) {
	dir := t.TempDir()
	dm := newCatchUpTestDB(t, dir, 2, map[int]string{1: "peer"})
	orphan := filepath.Join(dir, snapshotExportPrefix+"123", "databases")
	require.NoError(t, os.MkdirAll(orphan, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(orphan, "app.db"), []byte("partial copy"), 0o644))

	server := &Server{}
	server.SetDatabaseManager(dm)

	require.Empty(t, snapshotExportDirs(t, dm), "an orphaned export survived startup")
	require.Equal(t, map[int64]string{1: "peer"}, appRows(t, dm))
}
