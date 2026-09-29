package grpc

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// autoIncMergePeerTimeout bounds each peer's answer within one merge round.
const autoIncMergePeerTimeout = 5 * time.Second

// AutoIncMergeConfig is how a node runs its claim base merge and sync
// (RunAutoIncBaseMerge).
type AutoIncMergeConfig struct {
	// Standalone is the node's cluster.standalone setting: only such a node
	// releases while it is its own whole membership, and only while it has
	// never seen another member (standaloneRelease).
	Standalone bool
	// MergeInterval is how often a node whose claim votes are held retries
	// the merge that releases them.
	MergeInterval time.Duration
	// BaseSyncInterval is how often an unheld node raises its claim bases to
	// the maximum every alive peer reports (the membership backstop). Claim
	// safety is proved for a stable membership; after a membership change a
	// claim committed under the old membership may be known only to members
	// a new quorum can avoid. Every node pulling every peer's bases bounds
	// that window to one interval.
	BaseSyncInterval time.Duration
}

// GetAutoIncBases serves this node's AUTO_INCREMENT claim bases
// (db.AutoIncClaimStore.Bases) to a peer's vote-hold merge or base sync.
func (s *Server) GetAutoIncBases(ctx context.Context, req *AutoIncBasesRequest) (*AutoIncBasesResponse, error) {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()
	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	bases, held, err := dbManager.AutoIncBases()
	if err != nil {
		return nil, err
	}
	resp := &AutoIncBasesResponse{VotesHeld: held, Bases: make([]*AutoIncBase, len(bases))}
	for i, b := range bases {
		resp.Bases[i] = &AutoIncBase{Database: b.Database, Table: b.Table, Base: b.Base}
	}
	return resp, nil
}

// SyncAutoIncBases runs one claim base sync on this node now
// (syncAutoIncBasesOnce) and reports which members it reached.
func (s *Server) SyncAutoIncBases(ctx context.Context, req *AutoIncSyncRequest) (*AutoIncSyncResponse, error) {
	s.mu.RLock()
	dbManager, registry, gossip := s.dbManager, s.registry, s.gossip
	s.mu.RUnlock()
	if dbManager == nil || registry == nil || gossip == nil || gossip.GetClient() == nil {
		return nil, fmt.Errorf("cluster not initialized")
	}
	report, err := syncAutoIncBasesOnce(ctx, s.nodeID, dbManager, registry, peerAutoIncBases(s.nodeID, gossip.GetClient()))
	if err != nil {
		return nil, err
	}
	return report.toProto(), nil
}

// autoIncBaseStore is the part of db.DatabaseManager the merge needs.
type autoIncBaseStore interface {
	AutoIncVotesHeld() (bool, error)
	MergeAutoIncBasesAndReleaseVotes(bases []db.AutoIncBase) error
	RaiseAutoIncBases(bases []db.AutoIncBase) error
}

// fetchAutoIncBases asks one peer for its bases.
type fetchAutoIncBases func(ctx context.Context, nodeID uint64) (*AutoIncBasesResponse, error)

// autoIncMergeSafe reports whether a held node may merge and release its
// votes, in a membership of membership nodes, after unheld peers answered
// with their votes not held and held nodes - this node included - answered
// held. Every other member did not answer.
//
// Failure model: at most f' = max(N-Q, 1) members have lost claim state they
// voted with and not yet merged, where N is the membership and Q its quorum
// (the 1 because this node itself is held and so may be one of them).
//
// Proof. Take any committed claim C and its ACK set A, |A| >= Q. A node is
// held only after losing its claim state, and a held node never votes, so an
// ACKer now held lost state after ACKing: |A n held| <= min(held, f'). Every
// ACKer that did not answer is among the unanswered. So when
// unanswered + min(held, f') < Q, some unheld peer that answered ACKed C.
// That peer's base is at or above C's end - an unheld node either kept the
// base it ACKed with, or became unheld through a merge that met this same
// condition - so the maximum this node merges covers C.
//
// With only this node held the condition is the classic one: answers from
// ceil(N/2) unheld peers of the full membership. Held peers never count as
// unheld. When every member answers, it always holds: with no unanswered
// member the sum is at most f' < Q, which is also why a cluster whose every
// node is held - a new cluster - releases on its own: under the failure model
// fewer than Q of its nodes ever voted, so no claim ever committed. A node
// whose membership is 1 never releases this way (0 + 1 < 1 fails): its view
// may be one only because it has not learned its cluster yet. Only a node
// configured as standalone releases alone.
func autoIncMergeSafe(membership, unheld, held int, standalone bool) bool {
	if membership <= 1 {
		return standalone
	}
	quorum := coordinator.QuorumSize(protocol.ConsistencyQuorum, membership)
	lost := max(membership-quorum, 1)
	unanswered := max(membership-unheld-held, 0)
	return unanswered+min(held, lost) < quorum
}

// autoIncAnswers is what one round of asking every peer for its bases
// learned.
type autoIncAnswers struct {
	bases       []db.AutoIncBase
	unheldPeers int
	heldPeers   int
	answered    []uint64
}

// gatherAutoIncBases asks every peer for its bases and returns the maximum
// reported for each table - held peers included, since a higher base only
// ever raises the merge - with how many unheld and held peers answered.
func gatherAutoIncBases(ctx context.Context, peers []uint64, fetch fetchAutoIncBases) autoIncAnswers {
	var answers autoIncAnswers
	merged := make(map[[2]string]uint64)
	for _, peer := range peers {
		peerCtx, cancel := context.WithTimeout(ctx, autoIncMergePeerTimeout)
		resp, err := fetch(peerCtx, peer)
		cancel()
		if err != nil {
			continue
		}
		answers.answered = append(answers.answered, peer)
		if resp.GetVotesHeld() {
			answers.heldPeers++
		} else {
			answers.unheldPeers++
		}
		for _, b := range resp.GetBases() {
			key := [2]string{b.GetDatabase(), b.GetTable()}
			merged[key] = max(merged[key], b.GetBase())
		}
	}
	answers.bases = make([]db.AutoIncBase, 0, len(merged))
	for key, base := range merged {
		answers.bases = append(answers.bases, db.AutoIncBase{Database: key[0], Table: key[1], Base: base})
	}
	return answers
}

// mergeAutoIncBasesOnce runs one merge round: it asks every peer and, once
// autoIncMergeSafe holds for the answers, raises this node's bases to the
// maximum any answer reported and releases its votes. It reports whether the
// votes are now released.
func mergeAutoIncBasesOnce(ctx context.Context, store autoIncBaseStore, peers []uint64, membership int, standalone bool, fetch fetchAutoIncBases) (bool, error) {
	held, err := store.AutoIncVotesHeld()
	if err != nil || !held {
		return !held, err
	}
	answers := gatherAutoIncBases(ctx, peers, fetch)
	// This node is held, so it counts among the held.
	if !autoIncMergeSafe(membership, answers.unheldPeers, answers.heldPeers+1, standalone) {
		return false, nil
	}
	if err := store.MergeAutoIncBasesAndReleaseVotes(answers.bases); err != nil {
		return false, err
	}
	return true, nil
}

// alivePeers lists every alive member other than nodeID.
func alivePeers(registry autoIncMembership, nodeID uint64) []uint64 {
	var peers []uint64
	for _, node := range registry.GetAlive() {
		if node.NodeId != nodeID {
			peers = append(peers, node.NodeId)
		}
	}
	return peers
}

// peerAutoIncBases asks peers for their bases through client.
func peerAutoIncBases(nodeID uint64, client *Client) fetchAutoIncBases {
	return func(ctx context.Context, peer uint64) (*AutoIncBasesResponse, error) {
		c, err := client.GetClient(peer)
		if err != nil {
			return nil, err
		}
		return c.GetAutoIncBases(ctx, &AutoIncBasesRequest{RequestingNodeId: nodeID})
	}
}

// RunAutoIncBaseMerge releases this node's held claim votes once it has merged
// claim bases from enough of its membership (autoIncMergeSafe), retrying
// every conf.MergeInterval until then. From then on - at once on a node
// whose votes were never held - it runs the membership backstop: one sync
// immediately, then one every conf.BaseSyncInterval, until ctx ends.
func RunAutoIncBaseMerge(ctx context.Context, nodeID uint64, store autoIncBaseStore, registry *NodeRegistry, client *Client, conf AutoIncMergeConfig) {
	runAutoIncBaseMerge(ctx, nodeID, store, registry, conf, peerAutoIncBases(nodeID, client))
}

// runAutoIncBaseMerge is RunAutoIncBaseMerge over an explicit membership and
// peer fetch.
func runAutoIncBaseMerge(ctx context.Context, nodeID uint64, store autoIncBaseStore, registry autoIncMembership, conf AutoIncMergeConfig, fetch fetchAutoIncBases) {
	if !holdUntilMerged(ctx, nodeID, store, registry, conf, fetch) {
		return
	}
	ticker := time.NewTicker(conf.BaseSyncInterval)
	defer ticker.Stop()
	for {
		if _, err := syncAutoIncBasesOnce(ctx, nodeID, store, registry, fetch); err != nil {
			log.Warn().Err(err).Msg("AUTO_INCREMENT claim base sync failed; retrying")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// holdUntilMerged runs merge rounds every conf.MergeInterval until this
// node's votes are released. It reports false when ctx ended first.
func holdUntilMerged(ctx context.Context, nodeID uint64, store autoIncBaseStore, registry autoIncMembership, conf AutoIncMergeConfig, fetch fetchAutoIncBases) bool {
	if held, err := store.AutoIncVotesHeld(); err == nil && !held {
		return true
	}
	ticker := time.NewTicker(conf.MergeInterval)
	defer ticker.Stop()
	release := standaloneRelease{configured: conf.Standalone}
	warned := false
	for {
		membership := registry.Count()
		released, err := mergeAutoIncBasesOnce(ctx, store, alivePeers(registry, nodeID), membership, release.allowed(membership), fetch)
		if err != nil {
			log.Warn().Err(err).Msg("AUTO_INCREMENT claim base merge failed; retrying")
		}
		if released {
			log.Info().Msg("AUTO_INCREMENT claim votes released after merging claim bases")
			return true
		}
		if !warned && err == nil {
			warned = true
			log.Warn().Int("membership", membership).Bool("standalone", conf.Standalone).
				Msg("AUTO_INCREMENT claim votes held until claim bases are merged from enough cluster members; " +
					"narrow AUTO_INCREMENT inserts may return 1205 until then. A single-node deployment sets " +
					"cluster.standalone = true; an operator can force a release with " +
					"POST /admin/cluster/autoinc/release-votes?accept_risk=" + ForceReleaseRiskToken)
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// standaloneRelease decides whether a node configured as standalone may
// release its votes alone. A standalone node joins no cluster, so a node that
// has ever seen another member is not one, whatever its configuration says:
// releasing alone could lose a claim that member holds.
type standaloneRelease struct {
	configured bool
	sawPeers   bool
	logged     bool
}

// allowed reports whether the node may release while its membership is
// membership, remembering whether it has ever seen another member.
func (r *standaloneRelease) allowed(membership int) bool {
	if membership > 1 {
		r.sawPeers = true
	}
	if r.configured && r.sawPeers && !r.logged {
		r.logged = true
		log.Warn().Int("membership", membership).
			Msg("cluster.standalone is set but this node has seen other cluster members; it will not release " +
				"its AUTO_INCREMENT claim votes alone and merges claim bases from its cluster instead")
	}
	return r.configured && !r.sawPeers
}

// AutoIncSyncReport is what one claim base sync on a node learned.
type AutoIncSyncReport struct {
	NodeID    uint64
	VotesHeld bool
	// Members is the node's membership, itself included.
	Members []uint64
	// Reached is the members whose bases the sync merged, itself included.
	Reached []uint64
}

// ReachedEveryMember reports whether the sync merged the bases of every
// member of the node's membership.
func (r AutoIncSyncReport) ReachedEveryMember() bool {
	reached := make(map[uint64]bool, len(r.Reached))
	for _, id := range r.Reached {
		reached[id] = true
	}
	for _, id := range r.Members {
		if !reached[id] {
			return false
		}
	}
	return true
}

func (r AutoIncSyncReport) toProto() *AutoIncSyncResponse {
	return &AutoIncSyncResponse{NodeId: r.NodeID, VotesHeld: r.VotesHeld, Members: r.Members, Reached: r.Reached}
}

// autoIncMembership is the part of NodeRegistry the merge and the sync need.
type autoIncMembership interface {
	Count() int
	MemberIDs() []uint64
	GetAlive() []*NodeState
}

// syncAutoIncBasesOnce asks every alive peer - not a quorum, every one, held
// peers included - for its bases and raises this node's bases to the maximum
// reported, in one transaction. Raising is always safe (db.RaiseBases), so no
// count of answers gates it.
func syncAutoIncBasesOnce(ctx context.Context, nodeID uint64, store autoIncBaseStore, registry autoIncMembership, fetch fetchAutoIncBases) (AutoIncSyncReport, error) {
	members := registry.MemberIDs()
	answers := gatherAutoIncBases(ctx, alivePeers(registry, nodeID), fetch)
	if err := store.RaiseAutoIncBases(answers.bases); err != nil {
		return AutoIncSyncReport{}, err
	}
	held, err := store.AutoIncVotesHeld()
	if err != nil {
		return AutoIncSyncReport{}, err
	}
	reached := append([]uint64{nodeID}, answers.answered...)
	slices.Sort(reached)
	return AutoIncSyncReport{NodeID: nodeID, VotesHeld: held, Members: members, Reached: reached}, nil
}

// syncAutoIncBasesEverywhere runs a claim base sync on this node and on every
// other member of its membership - alive or not - and returns every node's
// report, a member that could not be asked included with its error.
func syncAutoIncBasesEverywhere(ctx context.Context, nodeID uint64, store autoIncBaseStore, registry autoIncMembership, fetch fetchAutoIncBases, ask askAutoIncSync) []AutoIncSyncOutcome {
	local, err := syncAutoIncBasesOnce(ctx, nodeID, store, registry, fetch)
	outcomes := []AutoIncSyncOutcome{{NodeID: nodeID, Report: local, Err: err}}
	for _, member := range registry.MemberIDs() {
		if member == nodeID {
			continue
		}
		memberCtx, cancel := context.WithTimeout(ctx, autoIncSyncMemberTimeout)
		resp, err := ask(memberCtx, member)
		cancel()
		outcome := AutoIncSyncOutcome{NodeID: member, Err: err}
		if err == nil {
			outcome.Report = AutoIncSyncReport{NodeID: resp.GetNodeId(), VotesHeld: resp.GetVotesHeld(), Members: resp.GetMembers(), Reached: resp.GetReached()}
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

// autoIncSyncMemberTimeout bounds one member's sync when an operator runs
// it everywhere: the member asks each of its own peers within
// autoIncMergePeerTimeout.
const autoIncSyncMemberTimeout = 3 * autoIncMergePeerTimeout

// askAutoIncSync asks one member to run a sync now.
type askAutoIncSync func(ctx context.Context, nodeID uint64) (*AutoIncSyncResponse, error)

// AutoIncSyncOutcome is one member's answer to a sync run everywhere.
type AutoIncSyncOutcome struct {
	NodeID uint64
	Report AutoIncSyncReport
	Err    error
}

// Complete reports whether the member answered, is unheld and reached every
// member of its membership: the state the membership-change procedure waits
// for before the next change.
func (o AutoIncSyncOutcome) Complete() bool {
	return o.Err == nil && !o.Report.VotesHeld && o.Report.ReachedEveryMember()
}

// SyncAutoIncBasesEverywhere runs the membership backstop's sync now on this
// node and on every member, for the operator's membership-change procedure:
// change one member, wait until it is ALIVE and unheld, sync - re-run until
// the report is complete, since a sync gathers only from peers a node sees
// ALIVE - confirm every outcome is Complete, then make the next change.
func SyncAutoIncBasesEverywhere(ctx context.Context, nodeID uint64, store autoIncBaseStore, registry *NodeRegistry, client *Client) []AutoIncSyncOutcome {
	ask := func(ctx context.Context, member uint64) (*AutoIncSyncResponse, error) {
		c, err := client.GetClient(member)
		if err != nil {
			return nil, err
		}
		return c.SyncAutoIncBases(ctx, &AutoIncSyncRequest{RequestingNodeId: nodeID})
	}
	return syncAutoIncBasesEverywhere(ctx, nodeID, store, registry, peerAutoIncBases(nodeID, client), ask)
}

// ForceReleaseRiskToken is the value an operator must pass to force a vote
// release, naming the risk they accept.
const ForceReleaseRiskToken = "duplicate-ids"

// ForceReleaseAutoIncVotes merges the bases of every peer that answers and
// releases this node's votes whether or not autoIncMergeSafe holds. It is the
// operator's way out of a cluster held beyond the failure model - for
// example, more than N-Q members rebuilt at once, or a membership that cannot
// be reached. It is unsafe exactly when a member that holds a newer claim
// than any answer reports did not answer: this node may then grant a range
// that overlaps it, and the same ids can be issued twice. It returns how many
// peers answered and whether this node's votes were held at all.
func ForceReleaseAutoIncVotes(ctx context.Context, nodeID uint64, store autoIncBaseStore, registry *NodeRegistry, client *Client) (answered int, wasHeld bool, err error) {
	return forceReleaseAutoIncVotes(ctx, store, alivePeers(registry, nodeID), peerAutoIncBases(nodeID, client))
}

// forceReleaseAutoIncVotes is ForceReleaseAutoIncVotes over explicit peers.
func forceReleaseAutoIncVotes(ctx context.Context, store autoIncBaseStore, peers []uint64, fetch fetchAutoIncBases) (answered int, wasHeld bool, err error) {
	held, err := store.AutoIncVotesHeld()
	if err != nil || !held {
		return 0, false, err
	}
	answers := gatherAutoIncBases(ctx, peers, fetch)
	if err := store.MergeAutoIncBasesAndReleaseVotes(answers.bases); err != nil {
		return 0, true, err
	}
	log.Warn().Int("peers_answered", answers.unheldPeers+answers.heldPeers).
		Msg("AUTO_INCREMENT claim votes released by operator without a safe merge; ids may be issued twice " +
			"if a member holding a newer claim did not answer")
	return answers.unheldPeers + answers.heldPeers, true, nil
}
