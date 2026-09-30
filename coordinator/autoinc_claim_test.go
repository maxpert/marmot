package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// claimStepReplicator is a minimal fake Replicator used only by the
// ClaimRange tests below. It answers PREPARE from a per-node queue of canned
// responses, consumed one per call, so a test can script a participant
// rejecting on an early attempt and accepting once ClaimRange retries above
// the base it reported. Once a node's queue is empty, PREPARE succeeds.
// COMMIT and ABORT always succeed: ClaimRange's retry logic only inspects
// PREPARE-phase errors.
type claimStepReplicator struct {
	mu           sync.Mutex
	prepareQueue map[uint64][]*ReplicationResponse
	prepareCalls map[uint64][]*ReplicationRequest
	fail         map[uint64]bool // simulate a transport failure (missing ACK), not a rejection
}

func newClaimStepReplicator() *claimStepReplicator {
	return &claimStepReplicator{
		prepareQueue: make(map[uint64][]*ReplicationResponse),
		prepareCalls: make(map[uint64][]*ReplicationRequest),
		fail:         make(map[uint64]bool),
	}
}

func (r *claimStepReplicator) push(nodeID uint64, resp *ReplicationResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prepareQueue[nodeID] = append(r.prepareQueue[nodeID], resp)
}

func (r *claimStepReplicator) setTransportFailure(nodeID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail[nodeID] = true
}

func (r *claimStepReplicator) prepareCallCount(nodeID uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.prepareCalls[nodeID])
}

func (r *claimStepReplicator) prepareCall(nodeID uint64, i int) *ReplicationRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prepareCalls[nodeID][i]
}

func (r *claimStepReplicator) ReplicateTransaction(_ context.Context, nodeID uint64, req *ReplicationRequest) (*ReplicationResponse, error) {
	if req.Phase != PhasePrep {
		return &ReplicationResponse{Success: true}, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.prepareCalls[nodeID] = append(r.prepareCalls[nodeID], req)

	if r.fail[nodeID] {
		return nil, errors.New("simulated transport failure")
	}

	q := r.prepareQueue[nodeID]
	if len(q) == 0 {
		return &ReplicationResponse{Success: true}, nil
	}
	resp := q[0]
	r.prepareQueue[nodeID] = q[1:]
	return resp, nil
}

func rejectionWithBase(base uint64) *ReplicationResponse {
	return &ReplicationResponse{
		Success:          false,
		Rejected:         true,
		Error:            "auto-increment claim is stale",
		AutoIDStoredBase: base,
	}
}

// decodePreparedClaim pulls the AutoIncClaim payload off the single claim
// statement a PREPARE request must carry.
func decodePreparedClaim(t *testing.T, req *ReplicationRequest) protocol.AutoIncClaim {
	t.Helper()
	if len(req.Statements) != 1 {
		t.Fatalf("prepare request carries %d statements, want 1", len(req.Statements))
	}
	stmt := req.Statements[0]
	if !stmt.AutoIDClaim {
		t.Fatal("statement does not carry the claim flag")
	}
	if len(stmt.EncodedRow) != 0 {
		t.Fatal("claim statement carries an EncodedRow; the design requires it carry none")
	}
	if len(stmt.IntentKey) == 0 {
		t.Fatal("claim statement carries no IntentKey")
	}
	claim, err := protocol.DecodeAutoIncClaim(stmt.AutoIDClaimPayload)
	if err != nil {
		t.Fatalf("DecodeAutoIncClaim: %v", err)
	}
	return claim
}

// TestClaimRange_SuccessfulClaimSingleRound pins the shape of an
// uncontested claim: one PREPARE round, a statement carrying the flag, a
// non-empty intent key, a decodable payload with NewBase == PrevBase, and no
// EncodedRow.
func TestClaimRange_SuccessfulClaimSingleRound(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	nodeProvider := newMockNodeProvider([]uint64{1})
	wc := NewWriteCoordinator(1, nodeProvider, fake, fake, 200*time.Millisecond, hlc.NewClock(1))

	newBase, granted, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50))
	if err != nil {
		t.Fatalf("ClaimRange: %v", err)
	}
	if newBase != 100 {
		t.Errorf("newBase = %d, want 100", newBase)
	}
	if granted != 50 {
		t.Errorf("granted = %d, want 50", granted)
	}

	if got := fake.prepareCallCount(1); got != 1 {
		t.Fatalf("PREPARE calls = %d, want exactly 1 (single round)", got)
	}
	req := fake.prepareCall(1, 0)
	claim := decodePreparedClaim(t, req)
	if claim.Table != "orders" {
		t.Errorf("claim table = %q, want orders", claim.Table)
	}
	if claim.PrevBase != 100 || claim.NewBase != 100 {
		t.Errorf("claim base = {prev:%d new:%d}, want both 100", claim.PrevBase, claim.NewBase)
	}
	if claim.Size != 50 {
		t.Errorf("claim size = %d, want 50", claim.Size)
	}
}

// TestClaimRange_ConsistencyIsAlwaysQuorum pins that ClaimRange pins
// ConsistencyQuorum unconditionally: quorums of 1 (ConsistencyOne,
// ConsistencyLocalOne) do not intersect, so a claim under either would not
// have the majority-intersection soundness argument prepareAutoIncClaim
// relies on.
//
// Proven behaviourally, not by inspecting a field: with a 3-node cluster,
// only the coordinator's own PREPARE succeeds (1 ack) while the other two
// nodes never answer. A quorum of 1 (ConsistencyOne/LocalOne) would let this
// succeed; requiring the majority of 3 (ConsistencyQuorum = 2) must fail
// quorum instead.
//
// Mutation: make ClaimRange honour a caller-supplied or config-default
// consistency level instead of pinning quorum.
func TestClaimRange_ConsistencyIsAlwaysQuorum(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	fake.setTransportFailure(2)
	fake.setTransportFailure(3)
	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	wc := NewWriteCoordinator(1, nodeProvider, fake, fake, 100*time.Millisecond, hlc.NewClock(1))

	_, _, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50))
	if err == nil {
		t.Fatal("expected ClaimRange to fail with only 1 of 3 nodes acking - it would succeed under a quorum of 1, proving consistency was not pinned to QUORUM")
	}

	var mysqlErr *protocol.MySQLError
	if !errors.As(err, &mysqlErr) {
		t.Fatalf("expected a *protocol.MySQLError, got %T: %v", err, err)
	}
	if mysqlErr.Code != protocol.ErrCodeLockTimeout {
		t.Errorf("MySQL code = %d, want %d (lock wait timeout / retry)", mysqlErr.Code, protocol.ErrCodeLockTimeout)
	}
}

// TestClaimRange_RetriesAboveRejectedBase pins the single-participant retry
// path: a PREPARE rejection reporting a higher base causes exactly one retry
// whose payload carries that base, and the retry succeeds.
func TestClaimRange_RetriesAboveRejectedBase(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	fake.push(1, rejectionWithBase(150))
	nodeProvider := newMockNodeProvider([]uint64{1})
	wc := NewWriteCoordinator(1, nodeProvider, fake, fake, 200*time.Millisecond, hlc.NewClock(1))

	newBase, granted, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50))
	if err != nil {
		t.Fatalf("ClaimRange: %v", err)
	}
	if newBase != 150 {
		t.Errorf("newBase = %d, want 150 (the rejected base)", newBase)
	}
	if granted != 50 {
		t.Errorf("granted = %d, want 50", granted)
	}

	if got := fake.prepareCallCount(1); got != 2 {
		t.Fatalf("PREPARE calls = %d, want exactly 2 (one rejection, one retry)", got)
	}
	retryClaim := decodePreparedClaim(t, fake.prepareCall(1, 1))
	if retryClaim.PrevBase != 150 || retryClaim.NewBase != 150 {
		t.Errorf("retry claim base = {prev:%d new:%d}, want both 150", retryClaim.PrevBase, retryClaim.NewBase)
	}
}

// TestClaimRange_RetriesAboveMaximumRejectedBase pins the multi-participant
// case: two participants reject in the same round with different bases, and
// the retry uses the MAXIMUM, not merely the first rejection the coordinator
// happened to record.
//
// Mutation: retry using RemotePrepareRejectedError's own base (the first
// remote rejection) instead of executePreparePhase's tracked round maximum.
func TestClaimRange_RetriesAboveMaximumRejectedBase(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	fake.push(2, rejectionWithBase(50))
	fake.push(3, rejectionWithBase(80))
	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	wc := NewWriteCoordinator(1, nodeProvider, fake, fake, 200*time.Millisecond, hlc.NewClock(1))

	newBase, granted, err := wc.ClaimRange(context.Background(), "testdb", "orders", 40, fixedClaimSize(10))
	if err != nil {
		t.Fatalf("ClaimRange: %v", err)
	}
	if newBase != 80 {
		t.Errorf("newBase = %d, want 80 (the MAXIMUM of the two rejected bases, not 50)", newBase)
	}
	if granted != 10 {
		t.Errorf("granted = %d, want 10", granted)
	}

	if got := fake.prepareCallCount(2); got != 2 {
		t.Fatalf("node 2 PREPARE calls = %d, want 2", got)
	}
	if got := fake.prepareCallCount(3); got != 2 {
		t.Fatalf("node 3 PREPARE calls = %d, want 2", got)
	}
	retryClaim := decodePreparedClaim(t, fake.prepareCall(2, 1))
	if retryClaim.PrevBase != 80 || retryClaim.NewBase != 80 {
		t.Errorf("retry claim base = {prev:%d new:%d}, want both 80 (the max)", retryClaim.PrevBase, retryClaim.NewBase)
	}
}

// TestClaimRange_EightConsecutiveRejectionsReturn1205 pins the retry bound:
// after 8 attempts all rejected (each carrying a usable, strictly increasing
// base so the loop always has a reason to retry), ClaimRange gives up and
// returns the retryable MySQL 1205 rather than looping forever.
func TestClaimRange_EightConsecutiveRejectionsReturn1205(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	base := uint64(100)
	for i := 0; i < 8; i++ {
		base += 10
		fake.push(1, rejectionWithBase(base))
	}
	nodeProvider := newMockNodeProvider([]uint64{1})
	wc := NewWriteCoordinator(1, nodeProvider, fake, fake, 200*time.Millisecond, hlc.NewClock(1))

	_, _, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50))
	if err == nil {
		t.Fatal("expected ClaimRange to give up after 8 rejections, got nil error")
	}
	var mysqlErr *protocol.MySQLError
	if !errors.As(err, &mysqlErr) {
		t.Fatalf("expected a *protocol.MySQLError, got %T: %v", err, err)
	}
	if mysqlErr.Code != protocol.ErrCodeLockTimeout {
		t.Errorf("MySQL code = %d, want %d (lock wait timeout / retry)", mysqlErr.Code, protocol.ErrCodeLockTimeout)
	}

	if got := fake.prepareCallCount(1); got != 8 {
		t.Fatalf("PREPARE calls = %d, want exactly 8", got)
	}
}

// claimKeyHolder is a fake participant set that declines every PREPARE,
// without a verdict, until a deadline: another claim holds the claim key on
// each of them until its COMMIT has synced, the way a participant answers a
// lost race for the key.
type claimKeyHolder struct {
	until time.Time
	mu    sync.Mutex
	calls int
}

func (h *claimKeyHolder) ReplicateTransaction(_ context.Context, _ uint64, req *ReplicationRequest) (*ReplicationResponse, error) {
	if req.Phase != PhasePrep {
		return &ReplicationResponse{Success: true}, nil
	}
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	if time.Now().Before(h.until) {
		return &ReplicationResponse{Success: false, Error: "write-write conflict: claim key locked"}, nil
	}
	return &ReplicationResponse{Success: true}, nil
}

// TestClaimRange_BacksOffWhileAnotherClaimHoldsTheKey: every participant
// declines while another claim holds the key for 60ms. ClaimRange must wait
// that out within one call: its declined-round waits are at least 2, 4, 8,
// 16 and 32ms, so by the sixth round more than 60ms have passed. Retrying at
// once spends all maxClaimAttempts rounds inside the hold and returns 1205,
// which is what contending claimants did once a claim COMMIT synced with
// F_FULLFSYNC.
//
// Mutation: drop the backoff sleep (retry at once). "a claim gave up while
// another claim held the key" fires. Mutation: stop retrying declined rounds.
// The same assertion fires after one round.
func TestClaimRange_BacksOffWhileAnotherClaimHoldsTheKey(t *testing.T) {
	InitTestTelemetry()

	const hold = 60 * time.Millisecond
	holder := &claimKeyHolder{until: time.Now().Add(hold)}
	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	wc := NewWriteCoordinator(1, nodeProvider, holder, holder, 200*time.Millisecond, hlc.NewClock(1))

	start := time.Now()
	newBase, granted, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50))
	if err != nil {
		t.Fatalf("a claim gave up while another claim held the key: %v", err)
	}
	if newBase != 100 || granted != 50 {
		t.Fatalf("claim = (%d, %d), want (100, 50)", newBase, granted)
	}
	elapsed := time.Since(start)
	if elapsed < hold {
		t.Fatalf("claim granted after %v, inside the %v hold", elapsed, hold)
	}
	holder.mu.Lock()
	calls := holder.calls
	holder.mu.Unlock()
	if rounds := calls / 3; rounds > maxClaimAttempts {
		t.Fatalf("claim used %d PREPARE rounds, more than maxClaimAttempts", rounds)
	}
	t.Logf("granted after %v and %d PREPARE calls", elapsed.Round(time.Millisecond), calls)
}

// TestClaimRange_QuorumNotAchievedReturns1205WithoutRetrying pins the other
// terminal case: a failure that carries no usable base (plain quorum
// failure, no participant explicitly rejecting) must not spend all 8
// attempts retrying something that cannot change - it returns 1205
// immediately, after a single round.
func TestClaimRange_QuorumNotAchievedReturns1205WithoutRetrying(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	fake.setTransportFailure(2)
	fake.setTransportFailure(3)
	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3})
	wc := NewWriteCoordinator(1, nodeProvider, fake, fake, 100*time.Millisecond, hlc.NewClock(1))

	_, _, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50))
	if err == nil {
		t.Fatal("expected an error when quorum cannot be achieved")
	}
	var mysqlErr *protocol.MySQLError
	if !errors.As(err, &mysqlErr) {
		t.Fatalf("expected a *protocol.MySQLError, got %T: %v", err, err)
	}
	if mysqlErr.Code != protocol.ErrCodeLockTimeout {
		t.Errorf("MySQL code = %d, want %d (lock wait timeout / retry)", mysqlErr.Code, protocol.ErrCodeLockTimeout)
	}

	if got := fake.prepareCallCount(1); got != 1 {
		t.Fatalf("PREPARE calls to the coordinator's own node = %d, want exactly 1 (no retry spin on an unusable failure)", got)
	}
}

// countingVoteHold is a VoteHold that is always released and counts waits.
type countingVoteHold struct {
	mu    sync.Mutex
	waits int
}

func (h *countingVoteHold) WaitAutoIncVotesReleased(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.waits++
	return nil
}

// TestClaimRange_WaitsOutLocalVotesHeld pins the startup shape of a new
// cluster: every node declines while its votes are held, so the round fails
// quorum with this node's decline marked VotesHeld. ClaimRange waits for the
// release after each such round and claims once the node votes, and a held
// round is not an attempt: more held rounds than maxClaimAttempts still end
// in a grant.
//
// Mutations: breaking out on the decline (HEAD's behaviour) returns 1205;
// counting held rounds as attempts returns 1205 after maxClaimAttempts.
func TestClaimRange_WaitsOutLocalVotesHeld(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	heldRounds := maxClaimAttempts + 2
	for range heldRounds {
		fake.push(1, &ReplicationResponse{Error: "votes held", VotesHeld: true})
		fake.push(2, &ReplicationResponse{Error: "votes held"})
		fake.push(3, &ReplicationResponse{Error: "votes held"})
	}
	hold := &countingVoteHold{}
	wc := NewWriteCoordinator(1, newMockNodeProvider([]uint64{1, 2, 3}), fake, fake, 200*time.Millisecond, hlc.NewClock(1))
	wc.SetVoteHold(hold)

	newBase, granted, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50))
	if err != nil {
		t.Fatalf("ClaimRange: %v", err)
	}
	if newBase != 100 || granted != 50 {
		t.Errorf("claim = (%d, +%d), want (100, +50)", newBase, granted)
	}
	if hold.waits != heldRounds {
		t.Errorf("waits for the release = %d, want %d (one per held round)", hold.waits, heldRounds)
	}
}

// TestClaimRange_RemoteVotesHeldIsNotALocalWait pins that only this node's
// own held votes make ClaimRange wait: a peer's held decline carries no
// VotesHeld flag across the wire and stays an ordinary decline.
//
// Mutation: classify any declined round as held. The claim waits on the
// hold instead of backing off, and waits becomes non-zero.
func TestClaimRange_RemoteVotesHeldIsNotALocalWait(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	fake.push(2, &ReplicationResponse{Error: "votes held"})
	fake.push(3, &ReplicationResponse{Error: "votes held"})
	hold := &countingVoteHold{}
	wc := NewWriteCoordinator(1, newMockNodeProvider([]uint64{1, 2, 3}), fake, fake, 200*time.Millisecond, hlc.NewClock(1))
	wc.SetVoteHold(hold)

	if _, _, err := wc.ClaimRange(context.Background(), "testdb", "orders", 100, fixedClaimSize(50)); err != nil {
		t.Fatalf("ClaimRange: %v", err)
	}
	if hold.waits != 0 {
		t.Errorf("waits for the release = %d, want 0: only this node's own hold is waited on", hold.waits)
	}
}

// fixedClaimSize is a RangeSizer asking for the same size above any base:
// these tests pin the claim protocol, not the allocator's sizing policy.
func fixedClaimSize(size uint64) id.RangeSizer {
	return func(uint64) (uint64, error) { return size, nil }
}

// TestClaimRange_SizerExhaustionIsTerminal pins the claimant-side half of
// exhaustion: when the sizer finds no room above the base a rejection taught
// it, ClaimRange stops without another round and reports
// id.ErrRangeExhausted - never the retryable 1205.
//
// Mutation: treat a sizer error as a retryable failure. The error is then
// the lock-wait timeout and "exhaustion reached the client as retryable"
// fires.
func TestClaimRange_SizerExhaustionIsTerminal(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	fake.push(1, rejectionWithBase(127))
	wc := NewWriteCoordinator(1, newMockNodeProvider([]uint64{1}), fake, fake, 200*time.Millisecond, hlc.NewClock(1))

	sizer := func(base uint64) (uint64, error) {
		if base >= 127 {
			return 0, id.ErrRangeExhausted
		}
		return 1, nil
	}
	_, _, err := wc.ClaimRange(context.Background(), "testdb", "tiny", 120, sizer)
	if !errors.Is(err, id.ErrRangeExhausted) {
		t.Fatalf("err = %v, want id.ErrRangeExhausted", err)
	}
	var mysqlErr *protocol.MySQLError
	if errors.As(err, &mysqlErr) {
		t.Fatalf("exhaustion reached the client as retryable %d", mysqlErr.Code)
	}
	if got := fake.prepareCallCount(1); got != 1 {
		t.Fatalf("PREPARE calls = %d, want 1: no round may be sent once the sizer finds no room", got)
	}
}

// TestClaimRange_ParticipantExhaustionIsTerminal pins the participant-side
// half: a rejection carrying ER_DUP_ENTRY - the one prepareAutoIncClaim gives
// a range past its own column ceiling - ends the claim as exhausted, while a
// rejection with no code and no usable base stays the retryable 1205.
//
// Mutation: drop the exhaustedByParticipant check. The participant's verdict
// then reaches the client as 1205 and "a full column was reported as
// retryable" fires.
func TestClaimRange_ParticipantExhaustionIsTerminal(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	fake.push(1, &ReplicationResponse{Rejected: true, Error: "auto-increment claim for tiny exhausts the column",
		AutoIDStoredBase: 120, ErrorCode: mysqlcode.ErrCodeDupEntry})
	wc := NewWriteCoordinator(1, newMockNodeProvider([]uint64{1}), fake, fake, 200*time.Millisecond, hlc.NewClock(1))
	_, _, err := wc.ClaimRange(context.Background(), "testdb", "tiny", 120, fixedClaimSize(64))
	if !errors.Is(err, id.ErrRangeExhausted) {
		t.Fatalf("a full column was reported as retryable: %v", err)
	}

	unavailable := newClaimStepReplicator()
	unavailable.push(1, &ReplicationResponse{Rejected: true, Error: "refused", AutoIDStoredBase: 120})
	wc = NewWriteCoordinator(1, newMockNodeProvider([]uint64{1}), unavailable, unavailable, 200*time.Millisecond, hlc.NewClock(1))
	_, _, err = wc.ClaimRange(context.Background(), "testdb", "tiny", 120, fixedClaimSize(64))
	if errors.Is(err, id.ErrRangeExhausted) {
		t.Fatalf("a rejection without the exhaustion code was reported as exhaustion: %v", err)
	}
	var mysqlErr *protocol.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Code != protocol.ErrCodeLockTimeout {
		t.Fatalf("err = %v, want the retryable 1205", err)
	}
}

// TestClaimRange_CarriesTheClaimantsMembership pins that every attempt
// carries the membership this node's quorum is computed over, so a
// participant that counts the cluster differently can refuse it.
//
// Mutation: leave Membership unset. "claim carries membership 0" fires.
func TestClaimRange_CarriesTheClaimantsMembership(t *testing.T) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	wc := NewWriteCoordinator(1, newMockNodeProvider([]uint64{1, 2, 3}), fake, fake, 200*time.Millisecond, hlc.NewClock(1))
	_, _, err := wc.ClaimRange(context.Background(), "testdb", "orders", 0, fixedClaimSize(8))
	if err != nil {
		t.Fatalf("ClaimRange: %v", err)
	}
	claim := decodePreparedClaim(t, fake.prepareCall(2, 0))
	if claim.Membership != 3 {
		t.Fatalf("claim carries membership %d, want 3", claim.Membership)
	}
}
