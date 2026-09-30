package grpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// fakeBaseStore records the merge a held node performs and the raises the
// membership backstop performs.
type fakeBaseStore struct {
	mu     sync.Mutex
	held   bool
	merged []db.AutoIncBase
	merges int
	raised []db.AutoIncBase
	raises int
}

func (s *fakeBaseStore) RaiseAutoIncBases(bases []db.AutoIncBase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raised = bases
	s.raises++
	return nil
}

func (s *fakeBaseStore) raiseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.raises
}

func (s *fakeBaseStore) AutoIncVotesHeld() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held, nil
}

func (s *fakeBaseStore) MergeAutoIncBasesAndReleaseVotes(bases []db.AutoIncBase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.merged = bases
	s.merges++
	s.held = false
	return nil
}

func peerAnswers(answers map[uint64]*AutoIncBasesResponse) fetchAutoIncBases {
	return func(_ context.Context, peer uint64) (*AutoIncBasesResponse, error) {
		resp, ok := answers[peer]
		if !ok {
			return nil, errors.New("unreachable")
		}
		return resp, nil
	}
}

func basesByTable(bases []db.AutoIncBase) map[string]uint64 {
	out := make(map[string]uint64, len(bases))
	for _, b := range bases {
		out[b.Database+"."+b.Table] = b.Base
	}
	return out
}

// TestAutoIncMergeSafe pins the release condition: unanswered + min(held,
// max(N-Q, 1)) < Q, with this node counted among the held.
//
// Mutation: drop the held nodes from the bound (min(held, f') -> 0). Among
// others, the N=5 row with two held and one unanswered releases, and "B's
// H1" is reported.
func TestAutoIncMergeSafe(t *testing.T) {
	cases := []struct {
		name                     string
		membership, unheld, held int
		standalone, want         bool
	}{
		{"membership of one, not standalone (B d)", 1, 0, 1, false, false},
		{"membership of one, standalone", 1, 0, 1, true, true},
		{"two nodes, the peer answers unheld", 2, 1, 1, false, true},
		{"two nodes, the peer does not answer", 2, 0, 1, false, false},
		{"three nodes, both peers unheld", 3, 2, 1, false, true},
		{"three nodes, one peer silent: ceil(3/2) not met", 3, 1, 1, false, false},
		{"three nodes, all held (new cluster)", 3, 0, 3, false, true},
		{"three nodes, one released, one held", 3, 1, 2, false, true},
		{"four nodes, two of three peers unheld", 4, 2, 1, false, true},
		{"five nodes, three unheld: ceil(5/2)", 5, 3, 1, false, true},
		{"five nodes, two unheld: not ceil(5/2)", 5, 2, 1, false, false},
		{"five nodes, B's H1: two held, one silent ACKer", 5, 2, 2, false, false},
		{"five nodes, two held, all answer", 5, 3, 2, false, true},
		{"five nodes, all held", 5, 0, 5, false, true},
	}
	for _, tc := range cases {
		if got := autoIncMergeSafe(tc.membership, tc.unheld, tc.held, tc.standalone); got != tc.want {
			t.Errorf("%s: autoIncMergeSafe = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestMergeAutoIncBasesOnceIntersectsEveryCommittedClaim is reviewer B's N=5
// counterexample: claim C committed on {n1,n2,n3}; n2 and n3 were rebuilt and
// are held; n1 is slow. n2 (this node) hears held n3 and unheld n4 and n5,
// which both missed C. It must stay held. Once n1 answers it releases with
// C's base.
//
// Mutation: count a peer that answered held as unheld (gatherAutoIncBases).
// The first round releases on n3, n4 and n5 and "released without hearing
// from any ACKer of C" fires.
func TestMergeAutoIncBasesOnceIntersectsEveryCommittedClaim(t *testing.T) {
	ctx := context.Background()
	answers := map[uint64]*AutoIncBasesResponse{
		3: {VotesHeld: true},
		4: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 10}}},
		5: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 10}, {Database: "d", Table: "u", Base: 5}}},
	}
	store := &fakeBaseStore{held: true}
	released, err := mergeAutoIncBasesOnce(ctx, store, []uint64{1, 3, 4, 5}, 5, false, peerAnswers(answers))
	require.NoError(t, err)
	require.False(t, released, "released without hearing from any ACKer of C")
	require.Equal(t, 0, store.merges)

	answers[1] = &AutoIncBasesResponse{Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 64}}}
	released, err = mergeAutoIncBasesOnce(ctx, store, []uint64{1, 3, 4, 5}, 5, false, peerAnswers(answers))
	require.NoError(t, err)
	require.True(t, released, "every member answered")
	require.Equal(t, map[string]uint64{"d.t": 64, "d.u": 5}, basesByTable(store.merged),
		"the merge must take every table's maximum across the answers")
}

// TestMergeAutoIncBasesOnceReleasesANewCluster is the all-held case: every
// member of a new cluster is held, and once all of them answer each one
// releases, including one that asks after another already released.
//
// Mutation: do not count this node among the held (answers.heldPeers instead
// of answers.heldPeers+1). It then counts as a member that did not answer,
// and "a cluster whose every member is held never releases" fires.
func TestMergeAutoIncBasesOnceReleasesANewCluster(t *testing.T) {
	ctx := context.Background()
	store := &fakeBaseStore{held: true}
	allHeld := map[uint64]*AutoIncBasesResponse{2: {VotesHeld: true}, 3: {VotesHeld: true}}
	released, err := mergeAutoIncBasesOnce(ctx, store, []uint64{2, 3}, 3, false, peerAnswers(allHeld))
	require.NoError(t, err)
	require.True(t, released, "a cluster whose every member is held never releases")

	store = &fakeBaseStore{held: true}
	oneReleased := map[uint64]*AutoIncBasesResponse{2: {}, 3: {VotesHeld: true}}
	released, err = mergeAutoIncBasesOnce(ctx, store, []uint64{2, 3}, 3, false, peerAnswers(oneReleased))
	require.NoError(t, err)
	require.True(t, released, "a node asking after a peer released stayed held")

	store = &fakeBaseStore{held: true}
	released, err = mergeAutoIncBasesOnce(ctx, store, nil, 1, false, peerAnswers(nil))
	require.NoError(t, err)
	require.False(t, released, "a node that knows only itself released without being standalone")
}

// TestMergeAutoIncBasesOnceLeavesAnUnheldNodeAlone pins that a node whose
// votes are not held never merges.
func TestMergeAutoIncBasesOnceLeavesAnUnheldNodeAlone(t *testing.T) {
	store := &fakeBaseStore{}
	released, err := mergeAutoIncBasesOnce(context.Background(), store, []uint64{1}, 3, false,
		func(context.Context, uint64) (*AutoIncBasesResponse, error) {
			t.Fatal("an unheld node asked a peer for its bases")
			return nil, nil
		})
	require.NoError(t, err)
	require.True(t, released)
	require.Equal(t, 0, store.merges)
}

// TestGetAutoIncBasesServesBasesAndHold pins the RPC a held peer merges from:
// it returns every base this node holds and whether its own votes are held.
//
// Mutation: drop VotesHeld from the response. A held node's bases would then
// count toward a peer's majority, and "the responder's hold did not reach
// the wire" fires.
func TestGetAutoIncBasesServesBasesAndHold(t *testing.T) {
	dm, err := db.NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dm.Close()
	require.NoError(t, db.NewAutoIncClaimStore(dm.GetSystemDatabase()).Seed("d", "t", 77, 1))

	s := &Server{}
	s.SetDatabaseManager(dm)
	resp, err := s.GetAutoIncBases(context.Background(), &AutoIncBasesRequest{RequestingNodeId: 2})
	require.NoError(t, err)
	require.True(t, resp.GetVotesHeld(), "the responder's hold did not reach the wire")
	require.Len(t, resp.GetBases(), 1)
	require.Equal(t, "d", resp.GetBases()[0].GetDatabase())
	require.Equal(t, "t", resp.GetBases()[0].GetTable())
	require.Equal(t, uint64(77), resp.GetBases()[0].GetBase())
}

// TestForceReleaseAutoIncVotesReleasesWithoutASafeMerge pins the operator's
// way out: it merges whatever the answering peers report and releases even
// where autoIncMergeSafe does not hold, and it leaves an unheld node alone.
//
// Mutation: gate forceReleaseAutoIncVotes on autoIncMergeSafe. With one of
// four peers answering the node stays held and "the operator release did not
// release" fires.
func TestForceReleaseAutoIncVotesReleasesWithoutASafeMerge(t *testing.T) {
	store := &fakeBaseStore{held: true}
	answers := map[uint64]*AutoIncBasesResponse{2: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 30}}}}
	answered, wasHeld, err := forceReleaseAutoIncVotes(context.Background(), store, []uint64{2, 3, 4, 5}, peerAnswers(answers))
	require.NoError(t, err)
	require.False(t, store.held, "the operator release did not release")
	require.True(t, wasHeld)
	require.Equal(t, 1, answered)
	require.Equal(t, map[string]uint64{"d.t": 30}, basesByTable(store.merged))

	answered, wasHeld, err = forceReleaseAutoIncVotes(context.Background(), &fakeBaseStore{}, []uint64{2}, peerAnswers(answers))
	require.NoError(t, err)
	require.False(t, wasHeld)
	require.Zero(t, answered)
}

// fakeMembership is a membership whose every member is alive unless listed
// in down.
type fakeMembership struct {
	members []uint64
	down    map[uint64]bool
	// alive signals a peer turning ALIVE; nil never does.
	alive *common.Broadcast
}

func (m fakeMembership) Count() int          { return len(m.members) }
func (m fakeMembership) MemberIDs() []uint64 { return m.members }
func (m fakeMembership) AliveChanged() <-chan struct{} {
	if m.alive == nil {
		return nil
	}
	return m.alive.Next()
}
func (m fakeMembership) GetAlive() []*NodeState {
	var alive []*NodeState
	for _, id := range m.members {
		if !m.down[id] {
			alive = append(alive, &NodeState{NodeId: id, Status: NodeStatus_ALIVE})
		}
	}
	return alive
}

// TestSyncAutoIncBasesRaisesFromEveryAlivePeer pins the membership backstop
// (R3c-8a) on fix1-B C1's shape: after n4 joined, claim C = (0,64] is known
// only to n1, and a quorum of the grown membership (n2, n3, n4) can avoid it.
// n4's sync asks every alive peer, not a quorum, so it raises to C's end as
// soon as n1 answers, and reports that it reached every member.
//
// Mutation: gatherAutoIncBases stops after a quorum of answers. n1 is asked
// last and "the backstop missed a claim only one member knows" fires.
func TestSyncAutoIncBasesRaisesFromEveryAlivePeer(t *testing.T) {
	ctx := context.Background()
	answers := map[uint64]*AutoIncBasesResponse{
		2: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 10}}},
		3: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 10}}, VotesHeld: true},
		1: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 64}, {Database: "d", Table: "u", Base: 7}}},
	}
	members := fakeMembership{members: []uint64{2, 3, 4, 1}}
	store := &fakeBaseStore{}

	report, err := syncAutoIncBasesOnce(ctx, 4, store, members, peerAnswers(answers))
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"d.t": 64, "d.u": 7}, basesByTable(store.raised),
		"the backstop missed a claim only one member knows")
	require.Equal(t, 0, store.merges, "the backstop must never touch the vote hold")
	require.Equal(t, []uint64{1, 2, 3, 4}, report.Reached)
	require.True(t, report.ReachedEveryMember())
	require.False(t, report.VotesHeld)

	// n1 down: the sync still raises from the rest, and says it did not
	// reach every member.
	members.down = map[uint64]bool{1: true}
	report, err = syncAutoIncBasesOnce(ctx, 4, store, members, peerAnswers(answers))
	require.NoError(t, err)
	require.False(t, report.ReachedEveryMember(), "a sync that missed a member reported reaching every one")
	require.Equal(t, []uint64{2, 3, 4}, report.Reached)

	// An alive member that does not answer is not reached either.
	members.down = nil
	delete(answers, 1)
	report, err = syncAutoIncBasesOnce(ctx, 4, store, members, peerAnswers(answers))
	require.NoError(t, err)
	require.False(t, report.ReachedEveryMember())
}

// testMergeConfig is the production default merge configuration.
var testMergeConfig = AutoIncMergeConfig{
	MergeInterval:    cfg.DefaultAutoIncMergeIntervalMS * time.Millisecond,
	BaseSyncInterval: cfg.DefaultAutoIncBaseSyncIntervalMS * time.Millisecond,
}

// TestReleasedNodeSyncsAtOnce pins that a node whose votes are released runs
// the backstop immediately, not an interval later: a node that just merged
// is exactly one whose membership just changed.
//
// Mutation: wait for the first ticker tick before the first sync in
// runAutoIncBaseMerge. No raise happens within the deadline, and "a released
// node did not sync at once" fires.
func TestReleasedNodeSyncsAtOnce(t *testing.T) {
	answers := map[uint64]*AutoIncBasesResponse{
		2: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 30}}},
		3: {Bases: []*AutoIncBase{{Database: "d", Table: "t", Base: 50}}},
	}
	store := &fakeBaseStore{held: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runAutoIncBaseMerge(ctx, 1, store, fakeMembership{members: []uint64{1, 2, 3}}, testMergeConfig, peerAnswers(answers))
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	deadline := time.Now().Add(testMergeConfig.BaseSyncInterval / 2)
	for store.raiseCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a released node did not sync at once")
		}
		time.Sleep(10 * time.Millisecond)
	}
	held, err := store.AutoIncVotesHeld()
	require.NoError(t, err)
	require.False(t, held)
}

// TestHeldNodeMergesWhenPeerTurnsAlive pins that a held node retries its
// merge the moment a peer turns ALIVE, not a merge interval later: narrow
// inserts on a held node wait for the release.
//
// Mutation: drop the AliveChanged case from holdUntilMerged's wait. The node
// stays held until the hour-long interval, and "not released on ALIVE" fires.
func TestHeldNodeMergesWhenPeerTurnsAlive(t *testing.T) {
	var reachable atomic.Bool
	answers := peerAnswers(map[uint64]*AutoIncBasesResponse{2: {VotesHeld: true}, 3: {VotesHeld: true}})
	fetch := func(ctx context.Context, peer uint64) (*AutoIncBasesResponse, error) {
		if !reachable.Load() {
			return nil, errors.New("unreachable")
		}
		return answers(ctx, peer)
	}
	store := &fakeBaseStore{held: true}
	alive := &common.Broadcast{}
	conf := AutoIncMergeConfig{MergeInterval: time.Hour, BaseSyncInterval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runAutoIncBaseMerge(ctx, 1, store, fakeMembership{members: []uint64{1, 2, 3}, alive: alive}, conf, fetch)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	// The first round finds no peer; only an ALIVE can start the next.
	require.Never(t, func() bool {
		held, _ := store.AutoIncVotesHeld()
		return !held
	}, 100*time.Millisecond, 10*time.Millisecond, "released with no peer answering")
	reachable.Store(true)
	alive.Notify()
	require.Eventually(t, func() bool {
		held, _ := store.AutoIncVotesHeld()
		return !held
	}, time.Second, 10*time.Millisecond, "not released on ALIVE")
}

// TestStandaloneNeverReleasesAloneAfterSeeingPeers pins R3c-11: a node
// configured standalone releases alone only while it has never seen another
// member. One that did is a member of a cluster, whatever its configuration
// says, and releasing alone could lose a claim that member holds.
//
// Mutation: allowed returns r.configured. "a standalone node that saw peers
// released alone" fires.
func TestStandaloneNeverReleasesAloneAfterSeeingPeers(t *testing.T) {
	release := standaloneRelease{configured: true}
	require.True(t, release.allowed(1), "a standalone node that never saw a peer must release alone")
	require.False(t, release.allowed(3), "a standalone node that sees peers released alone")
	require.False(t, release.allowed(1), "a standalone node that saw peers released alone")

	notStandalone := standaloneRelease{}
	require.False(t, notStandalone.allowed(1))
}

// TestSyncAutoIncBasesEverywhereReportsEveryMember pins the operator's sync
// command (R3c-8b): it syncs locally and asks every member, alive or not,
// and an outcome is Complete only for a member that answered, is unheld and
// reached every member.
//
// Mutation: skip members that are not alive in syncAutoIncBasesEverywhere.
// The outcome for the down member 3 is missing and "every member must be
// reported" fires.
func TestSyncAutoIncBasesEverywhereReportsEveryMember(t *testing.T) {
	members := fakeMembership{members: []uint64{1, 2, 3}, down: map[uint64]bool{3: true}}
	fetch := peerAnswers(map[uint64]*AutoIncBasesResponse{2: {}})
	ask := func(_ context.Context, member uint64) (*AutoIncSyncResponse, error) {
		if member == 2 {
			return &AutoIncSyncResponse{NodeId: 2, Members: []uint64{1, 2, 3}, Reached: []uint64{1, 2, 3}}, nil
		}
		return nil, errors.New("unreachable")
	}

	outcomes := syncAutoIncBasesEverywhere(context.Background(), 1, &fakeBaseStore{}, members, fetch, ask)
	require.Len(t, outcomes, 3, "every member must be reported")
	byNode := map[uint64]AutoIncSyncOutcome{}
	for _, o := range outcomes {
		byNode[o.NodeID] = o
	}
	require.False(t, byNode[1].Complete(), "node 1 did not reach the down member 3")
	require.True(t, byNode[2].Complete())
	require.Error(t, byNode[3].Err)
	require.False(t, byNode[3].Complete())

	held := AutoIncSyncOutcome{NodeID: 2, Report: AutoIncSyncReport{VotesHeld: true, Members: []uint64{2}, Reached: []uint64{2}}}
	require.False(t, held.Complete(), "a held member is not ready for the next membership change")
}
