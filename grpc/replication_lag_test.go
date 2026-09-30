package grpc

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/marmot/telemetry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordingGaugeVec is a telemetry.GaugeVec that keeps each label's last
// Set value, so a test asserts on its own instance, never the global one.
type recordingGaugeVec struct {
	mu     sync.Mutex
	values map[string]float64
}

func newRecordingGaugeVec() *recordingGaugeVec {
	return &recordingGaugeVec{values: make(map[string]float64)}
}

func (g *recordingGaugeVec) With(labels ...string) telemetry.Gauge {
	return recordingGauge{vec: g, label: labels[0]}
}

func (g *recordingGaugeVec) Delete(labels ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.values, labels[0])
}

// value returns peer's series value and whether the series exists.
func (g *recordingGaugeVec) value(peer uint64) (float64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.values[strconv.FormatUint(peer, 10)]
	return v, ok
}

type recordingGauge struct {
	telemetry.NoopStat
	vec   *recordingGaugeVec
	label string
}

func (r recordingGauge) Set(v float64) {
	r.vec.mu.Lock()
	defer r.vec.mu.Unlock()
	r.vec.values[r.label] = v
}

// switchableFetchServer fails every FetchTransactions call with a transport
// error while fail is set, so listed entries stay unapplied.
type switchableFetchServer struct {
	*Server
	fail *atomic.Bool
}

func (s switchableFetchServer) FetchTransactions(req *FetchTransactionsRequest, stream MarmotService_FetchTransactionsServer) error {
	if s.fail.Load() {
		return status.Error(codes.Unavailable, "fetch disabled by test")
	}
	return s.Server.FetchTransactions(req, stream)
}

// legacyListServer answers ListCommittedLog as a peer that predates
// remaining_committed does: never setting it.
type legacyListServer struct {
	*Server
}

func (s legacyListServer) ListCommittedLog(ctx context.Context, req *LogListRequest) (*LogListResponse, error) {
	resp, err := s.Server.ListCommittedLog(ctx, req)
	if resp != nil {
		resp.RemainingCommitted = nil
	}
	return resp, err
}

// cancelledCountServer serves ListCommittedLog under an already-cancelled
// context, so any remaining-entries count it runs is cut short.
type cancelledCountServer struct {
	*Server
}

func (s cancelledCountServer) ListCommittedLog(ctx context.Context, req *LogListRequest) (*LogListResponse, error) {
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	return s.Server.ListCommittedLog(cancelled, req)
}

// seedRange seeds n txns (ids first..first+n-1) into d on node.
func seedRange(t *testing.T, node *aeRegNode, d string, first uint64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		id := first + uint64(i)
		node.seed(t, d, id, node.id, int64(id), int64(id), "v")
	}
}

// TestReplicationLag_SumsUnappliedOverDatabasesThenZero: the gauge for a
// peer is the number of its committed entries this node has not applied,
// summed over databases, and 0 once a round applies them all.
func TestReplicationLag_SumsUnappliedOverDatabasesThenZero(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	peer := newAERegNode(t, 2, "db1", "db2")
	seedRange(t, peer, "db1", 100, 3)
	seedRange(t, peer, "db2", 200, 2)
	peer.serveWrapped(t, func(s *Server) MarmotServiceServer { return switchableFetchServer{Server: s, fail: &fail} })

	local := newAERegNode(t, 1, "db1", "db2")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)
	ae := local.antiEntropyFor()
	gauge := newRecordingGaugeVec()
	ae.lagGauge = gauge

	ae.performAntiEntropy()
	v, ok := gauge.value(2)
	require.True(t, ok, "the lag of a peer whose whole log was listed was not published")
	require.Equal(t, float64(5), v, "the lag is not the sum of unapplied entries over databases")

	fail.Store(false)
	ae.performAntiEntropy()
	v, ok = gauge.value(2)
	require.True(t, ok)
	require.Zero(t, v, "the lag is not 0 after a round applied everything")
}

// TestReplicationLag_CountsThePeersRemainderPastThePageCap: a round that
// stops at maxPagesPerPull still publishes the exact lag, from the peer's
// count of the entries past the last page.
func TestReplicationLag_CountsThePeersRemainderPastThePageCap(t *testing.T) {
	peer := newAERegNode(t, 2, "app")
	seedRange(t, peer, "app", 1000, maxPagesPerPull+3)
	peer.serve(t)

	local := newAERegNode(t, 1, "app")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)
	ae := local.antiEntropyFor()
	ae.logPuller = NewLogPuller(LogPullerConfig{NodeID: 1, Client: local.client, DBManager: local.dm, PageSize: 1})
	gauge := newRecordingGaugeVec()
	ae.lagGauge = gauge

	ae.performAntiEntropy()
	v, ok := gauge.value(2)
	require.True(t, ok)
	require.Equal(t, float64(3), v, "the entries past the page cap were not counted")

	ae.performAntiEntropy()
	v, _ = gauge.value(2)
	require.Zero(t, v)
}

// TestReplicationLag_OlderPeerAtThePageCapLeavesTheGaugeUnset: a peer that
// cannot report the entries past the page cap has an unknown lag, which is
// never published as a number.
func TestReplicationLag_OlderPeerAtThePageCapLeavesTheGaugeUnset(t *testing.T) {
	peer := newAERegNode(t, 2, "app")
	seedRange(t, peer, "app", 1000, maxPagesPerPull+3)
	peer.serveWrapped(t, func(s *Server) MarmotServiceServer { return legacyListServer{Server: s} })

	local := newAERegNode(t, 1, "app")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)
	ae := local.antiEntropyFor()
	ae.logPuller = NewLogPuller(LogPullerConfig{NodeID: 1, Client: local.client, DBManager: local.dm, PageSize: 1})
	gauge := newRecordingGaugeVec()
	ae.lagGauge = gauge

	ae.performAntiEntropy()
	_, ok := gauge.value(2)
	require.False(t, ok, "an unknown lag was published")
}

// TestReplicationLag_AbandonedCountNeverFailsTheListing: a remaining-entries
// count cut short still serves the page, with remaining_committed unset,
// and the round applies the page but leaves the lag unpublished.
func TestReplicationLag_AbandonedCountNeverFailsTheListing(t *testing.T) {
	peer := newAERegNode(t, 2, "app")
	seedRange(t, peer, "app", 1000, maxPagesPerPull+3)
	peer.serveWrapped(t, func(s *Server) MarmotServiceServer { return cancelledCountServer{Server: s} })

	server := &Server{nodeID: 2, registry: NewNodeRegistry(2, "127.0.0.1:0")}
	server.SetDatabaseManager(peer.dm)
	resp, err := cancelledCountServer{Server: server}.ListCommittedLog(context.Background(),
		&LogListRequest{Database: "app", Limit: 1, CountRemaining: true})
	require.NoError(t, err, "an abandoned count failed the listing")
	require.Len(t, resp.Entries, 1)
	require.True(t, resp.More)
	require.Nil(t, resp.RemainingCommitted, "an abandoned count was reported")

	local := newAERegNode(t, 1, "app")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)
	ae := local.antiEntropyFor()
	ae.logPuller = NewLogPuller(LogPullerConfig{NodeID: 1, Client: local.client, DBManager: local.dm, PageSize: 1})
	gauge := newRecordingGaugeVec()
	ae.lagGauge = gauge

	ae.performAntiEntropy()
	require.Len(t, local.rows(t, "app"), maxPagesPerPull, "the round did not apply every page it listed")
	_, ok := gauge.value(2)
	require.False(t, ok, "an unknown lag was published")
}

// TestReplicationLag_RemovedPeerSeriesDeleted: a peer that leaves
// membership stops being exported.
func TestReplicationLag_RemovedPeerSeriesDeleted(t *testing.T) {
	peer := newAERegNode(t, 2, "app")
	seedRange(t, peer, "app", 1000, 2)
	peer.serve(t)

	local := newAERegNode(t, 1, "app")
	addPeer(local.registry, 2, peer.addr, NodeStatus_ALIVE)
	ae := local.antiEntropyFor()
	gauge := newRecordingGaugeVec()
	ae.lagGauge = gauge

	ae.performAntiEntropy()
	_, ok := gauge.value(2)
	require.True(t, ok)

	require.NoError(t, local.registry.MarkRemoved(2))
	ae.performAntiEntropy()
	_, ok = gauge.value(2)
	require.False(t, ok, "a removed peer's lag series lingers")
}

// TestReplicationLag_CorrectAfterSnapshotRestore: the round that falls back
// to a snapshot cannot count the lag and leaves it unset; the next round,
// from the cursor the restore reset, publishes the true value.
func TestReplicationLag_CorrectAfterSnapshotRestore(t *testing.T) {
	source := newAERegNode(t, 2, "app")
	source.seed(t, "app", 1, 2, 1000, 99, "throwaway")
	source.truncateThrough(t, "app")
	seedRange(t, source, "app", 100, 2)
	source.serve(t)

	local := newAERegNode(t, 1, "app")
	addPeer(local.registry, 2, source.addr, NodeStatus_ALIVE)
	ae := local.antiEntropyFor()
	gauge := newRecordingGaugeVec()
	ae.lagGauge = gauge

	ae.performAntiEntropy()
	_, ok := gauge.value(2)
	require.False(t, ok, "the snapshot-fallback round published a lag it could not count")

	ae.performAntiEntropy()
	v, ok := gauge.value(2)
	require.True(t, ok)
	require.Zero(t, v, "the lag after a restore is not the true value")
}
