package grpc

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

// LogPullProtocolVersion is the log-pull anti-entropy protocol generation this
// binary serves (ListCommittedLog / FetchTransactions / ListDatabaseRegistry,
// and a per-database DDL history counter comparable across the cluster). A
// release before this protocol never sets NodeState.LogProtocolVersion, so it
// is always observed as 0 for such a node, and DDL is refused cluster-wide
// while any member reports less than this (LegacyLogProtocolMembers).
const LogPullProtocolVersion uint32 = 1

// copySchemaVersionMap creates a deep copy of a schema version map
func copySchemaVersionMap(m map[string]uint64) map[string]uint64 {
	if m == nil {
		return nil
	}
	result := make(map[string]uint64, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// copyNodeState creates a deep copy of a NodeState
func copyNodeState(node *NodeState) *NodeState {
	return &NodeState{
		NodeId:                 node.NodeId,
		Address:                node.Address,
		Status:                 node.Status,
		Incarnation:            node.Incarnation,
		DatabaseSchemaVersions: copySchemaVersionMap(node.DatabaseSchemaVersions),
		MinAppliedSeq:          node.MinAppliedSeq,
		LogProtocolVersion:     node.LogProtocolVersion,
	}
}

// NodeRegistry tracks cluster membership using SWIM protocol
type NodeRegistry struct {
	localNodeID       uint64
	nodes             map[uint64]*NodeState
	lastSeen          map[uint64]time.Time
	mu                sync.RWMutex
	onNodeAliveFunc   func(*NodeState) // Callback when node transitions to ALIVE, or is discovered ALIVE
	onNodeDeadFunc    func(*NodeState) // Callback when node transitions to DEAD
	onNodeLeavingFunc func()           // Callback when local node marked LEAVING via remote decommission
	callbackMu        sync.RWMutex
	// aliveChanged is notified, after onNodeAliveFunc, whenever a node turns
	// or is discovered ALIVE, and when an ALIVE node announces ALIVE at a
	// higher incarnation (AliveChanged).
	aliveChanged common.Broadcast

	// store persists membership so a restarted node does not compute a quorum
	// from a membership of one. nil disables persistence.
	store *membershipStore
	// persistedFingerprint is the membership the snapshot on disk describes.
	// It lets the persist hook skip the write when a mutator changed something
	// the snapshot does not record, so an fsync only happens on a real change.
	persistedFingerprint uint64
}

// NewNodeRegistry creates a new node registry with no durable membership.
// Used by tests and by any embedding that has no data directory; production
// goes through NewNodeRegistryWithDataDir so a restart can re-learn what the
// cluster looked like.
func NewNodeRegistry(localNodeID uint64, advertiseAddress string) *NodeRegistry {
	return NewNodeRegistryWithDataDir(localNodeID, advertiseAddress, "")
}

// NewNodeRegistryWithDataDir creates a registry that persists membership under
// dataDir and restores it before returning.
//
// Restoring matters because quorum is a majority of TOTAL membership. A registry
// that starts with self only makes a lone restarted node its own majority, so it
// can commit a write - or, once narrow id ranges exist, grant itself a range -
// that overlaps a live peer's. The invariant this establishes: a node that has
// ever known a multi-node membership does not compute a quorum from fewer
// members than it last knew, until gossip re-learns membership from a live peer.
func NewNodeRegistryWithDataDir(localNodeID uint64, advertiseAddress string, dataDir string) *NodeRegistry {
	log.Debug().
		Uint64("node_id", localNodeID).
		Str("advertise_address", advertiseAddress).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("BOOT: Creating node registry")

	nr := &NodeRegistry{
		localNodeID: localNodeID,
		nodes:       make(map[uint64]*NodeState),
		lastSeen:    make(map[uint64]time.Time),
		store:       newMembershipStore(dataDir),
	}

	// Add self to registry as ALIVE
	now := time.Now()
	nr.nodes[localNodeID] = &NodeState{
		NodeId:             localNodeID,
		Address:            advertiseAddress,
		Status:             NodeStatus_ALIVE,
		Incarnation:        0,
		LogProtocolVersion: LogPullProtocolVersion,
	}
	nr.lastSeen[localNodeID] = now

	nr.mu.Lock()
	restored := nr.restoreMembershipLocked()
	// Initialize cluster metrics with the membership we start from.
	nr.updateClusterMetricsLocked()
	nr.mu.Unlock()

	if restored > 0 {
		log.Info().
			Uint64("node_id", localNodeID).
			Int("restored_peers", restored).
			Int("membership", nr.Count()).
			Msg("BOOT: Restored persisted cluster membership; peers start SUSPECT until gossip confirms them")
	}

	log.Debug().
		Uint64("node_id", localNodeID).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("BOOT: Node registry created - self added as ALIVE")

	return nr
}

// Add adds a node to the registry
func (nr *NodeRegistry) Add(node *NodeState) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	nr.nodes[node.NodeId] = node
	nr.lastSeen[node.NodeId] = time.Now()
	nr.updateClusterMetricsLocked()
}

// Update updates a node's state using SWIM protocol rules
func (nr *NodeRegistry) Update(node *NodeState) {
	nr.mu.Lock()

	// Rule 1: SWIM refutation - local node rejects non-ALIVE status for itself
	if node.NodeId == nr.localNodeID {
		nr.handleSelfUpdateLocked(node)
		nr.mu.Unlock()
		return
	}

	// Rule 2: Discover new nodes. A node discovered ALIVE gets the same
	// callback as one that becomes ALIVE: it may never change status again
	// (a restarted peer keeps its incarnation), and the callback is what
	// opens this node's connection to it.
	existing, exists := nr.nodes[node.NodeId]
	if !exists {
		log.Debug().
			Uint64("local_node", nr.localNodeID).
			Uint64("new_node", node.NodeId).
			Str("status", node.Status.String()).
			Uint64("incarnation", node.Incarnation).
			Msg("REGISTRY: Adding NEW node")
		nr.nodes[node.NodeId] = node
		nr.lastSeen[node.NodeId] = time.Now()
		nr.updateClusterMetricsLocked()
		nr.mu.Unlock()
		if node.Status == NodeStatus_ALIVE {
			nr.fireOnNodeAlive(node)
		}
		return
	}

	// Rule 3: Always update lastSeen (even if state unchanged)
	nr.lastSeen[node.NodeId] = time.Now()

	// Track if node transitions to ALIVE for callback
	becameAlive := false
	// reannouncedAlive is an ALIVE node at a higher incarnation: no status
	// change, so no callback, but AliveChanged waiters may now get an answer.
	reannouncedAlive := false

	// Rule 4: Apply SWIM state update rules
	stateChanged := false
	// recordReplaced is separate from stateChanged because the higher-incarnation
	// branch swaps the whole record: address and incarnation can change with the
	// status staying put. Persistence must follow the record, not just the
	// status - a peer that restarted on a new address, or refuted a suspicion,
	// otherwise stayed at its old address and incarnation on disk, and a restart
	// restored the stale one. The stale incarnation is the dangerous half: SWIM
	// compares incarnations to decide who wins, so an older rumour could then
	// overwrite the record.
	recordReplaced := false
	if node.Incarnation > existing.Incarnation {
		// Higher incarnation always wins, EXCEPT:
		// - REMOVED status is sticky (can only be cleared via admin API AllowRejoin)
		// - If existing is REMOVED, only accept another REMOVED or DEAD (from AllowRejoin)
		if existing.Status == NodeStatus_REMOVED && node.Status != NodeStatus_REMOVED && node.Status != NodeStatus_DEAD {
			log.Debug().
				Uint64("local_node", nr.localNodeID).
				Uint64("update_node", node.NodeId).
				Str("incoming_status", node.Status.String()).
				Msg("REGISTRY: Ignoring update for REMOVED node (sticky state)")
			nr.mu.Unlock()
			return
		}

		oldStatus := existing.Status
		log.Debug().
			Uint64("local_node", nr.localNodeID).
			Uint64("update_node", node.NodeId).
			Str("old_status", oldStatus.String()).
			Str("new_status", node.Status.String()).
			Uint64("old_inc", existing.Incarnation).
			Uint64("new_inc", node.Incarnation).
			Msg("REGISTRY: Updating node (higher incarnation)")
		nr.nodes[node.NodeId] = node
		recordReplaced = true

		// Record state transition if status changed
		if oldStatus != node.Status {
			telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), node.Status.String()).Inc()
			stateChanged = true
		}

		// Check if node became ALIVE, or announced ALIVE again: a joiner's
		// promotion reads ALIVE -> ALIVE here when its JOINING, set without a
		// bump (MarkJoining), never reached this node.
		if node.Status == NodeStatus_ALIVE {
			if oldStatus != NodeStatus_ALIVE {
				becameAlive = true
			} else {
				reannouncedAlive = true
			}
		}
	} else if node.Incarnation == existing.Incarnation && nr.shouldEscalate(existing.Status, node.Status) {
		// Same incarnation: only allow escalation (ALIVE -> SUSPECT -> DEAD)
		oldStatus := existing.Status
		log.Debug().
			Uint64("local_node", nr.localNodeID).
			Uint64("update_node", node.NodeId).
			Str("old_status", oldStatus.String()).
			Str("new_status", node.Status.String()).
			Uint64("incarnation", node.Incarnation).
			Msg("REGISTRY: Escalating node status (same incarnation)")
		existing.Status = node.Status
		if node.LogProtocolVersion > existing.LogProtocolVersion {
			existing.LogProtocolVersion = node.LogProtocolVersion
		}

		// Record state transition
		telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), node.Status.String()).Inc()
		stateChanged = true
	} else if node.Incarnation == existing.Incarnation && node.LogProtocolVersion > existing.LogProtocolVersion {
		// Same incarnation, no status escalation: a relayed gossip copy of a
		// node's own state can lack the version an earlier copy carried (for
		// example a seed's Join response, which builds a bare NodeState for
		// the joining node - see grpc/server.go Join). Never downgrade a
		// known version; take the max for the same incarnation.
		existing.LogProtocolVersion = node.LogProtocolVersion
		stateChanged = true
	} else {
		log.Debug().
			Uint64("local_node", nr.localNodeID).
			Uint64("update_node", node.NodeId).
			Str("existing_status", existing.Status.String()).
			Str("incoming_status", node.Status.String()).
			Uint64("existing_inc", existing.Incarnation).
			Uint64("incoming_inc", node.Incarnation).
			Msg("REGISTRY: Ignoring update (stale or invalid)")
	}
	// Ignore updates with same/older incarnation that don't escalate and
	// don't raise the known log protocol version

	// Refresh metrics and persist when the record changed at all. The persist
	// hook fingerprints exactly the fields the snapshot stores, so a call here
	// that changed nothing it records performs no write.
	if stateChanged || recordReplaced {
		nr.updateClusterMetricsLocked()
	}

	nr.mu.Unlock()

	// Call callback outside lock to avoid deadlock
	if becameAlive {
		nr.fireOnNodeAlive(node)
	} else if reannouncedAlive {
		nr.aliveChanged.Notify()
	}
}

// fireOnNodeAlive runs the ALIVE callback for node. The caller must not hold
// nr.mu.
func (nr *NodeRegistry) fireOnNodeAlive(node *NodeState) {
	nr.callbackMu.RLock()
	callback := nr.onNodeAliveFunc
	nr.callbackMu.RUnlock()

	if callback != nil {
		callback(node)
	}
	nr.aliveChanged.Notify()
}

// AliveChanged returns a channel closed the next time a node turns or is
// discovered ALIVE, once the ALIVE callback has run for it, or an ALIVE node
// announces ALIVE at a higher incarnation (a promotion this node saw no
// JOINING for, or a restart).
func (nr *NodeRegistry) AliveChanged() <-chan struct{} {
	return nr.aliveChanged.Next()
}

// handleSelfUpdateLocked implements SWIM refutation for local node
// Caller must hold nr.mu lock
func (nr *NodeRegistry) handleSelfUpdateLocked(node *NodeState) {
	self := nr.nodes[nr.localNodeID]

	// If we are LEAVING, we do not refute SUSPECT/DEAD — we are intentionally departing.
	// However, stale ALIVE gossip must not override our intentional LEAVING status.
	if self.Status == NodeStatus_LEAVING {
		if node.Status == NodeStatus_ALIVE && node.Incarnation >= self.Incarnation {
			// Stale ALIVE gossip: refute to keep LEAVING status propagating
			self.Incarnation = node.Incarnation + 1
			log.Debug().
				Uint64("refuted_incarnation", node.Incarnation).
				Uint64("new_incarnation", self.Incarnation).
				Msg("SWIM refutation: rejecting stale ALIVE claim while LEAVING")
		}
		return
	}

	// REMOVED is terminal: every peer keeps it sticky, so refuting it would only
	// climb our incarnation. Only the admin allow endpoint clears it.
	if node.Status == NodeStatus_REMOVED {
		return
	}

	// Accept LEAVING from higher incarnation — this is a remote decommission
	// via admin API, not a failure detection. Don't refute it. The departure
	// a previous run of this node gossiped never reaches here: a restart
	// starts above the incarnation it was persisted at (restoreMembershipLocked).
	if node.Status == NodeStatus_LEAVING && node.Incarnation > self.Incarnation {
		oldStatus := self.Status
		oldIncarnation := self.Incarnation
		self.Status = NodeStatus_LEAVING
		self.Incarnation = node.Incarnation
		log.Info().
			Uint64("old_incarnation", oldIncarnation).
			Uint64("new_incarnation", self.Incarnation).
			Msg("Accepting remote decommission — transitioning to LEAVING")
		if oldStatus != NodeStatus_LEAVING {
			telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), NodeStatus_LEAVING.String()).Inc()
			nr.updateClusterMetricsLocked()
		}
		// Release mu before firing callback to avoid lock inversion with callbackMu.
		// State is fully committed above; the immediate return after reacquire means
		// no code runs under the reacquired lock.
		nr.mu.Unlock()
		nr.fireOnNodeLeaving()
		nr.mu.Lock()
		return
	}

	// If someone claims we're not ALIVE, refute by incrementing incarnation.
	//
	// A view of us with an older log protocol version (the one we had before
	// a restart onto a newer binary, which does not bump our incarnation) is
	// refuted only when it carries a HIGHER incarnation than ours: SWIM never
	// lets a lower incarnation replace it, so without the refutation it would
	// stay stale forever and keep DDL refused as if we had not been upgraded
	// (LegacyLogProtocolMembers). At our own incarnation it is left alone:
	// every current peer keeps the max version per incarnation (Update), so
	// our own gossip corrects it, and a peer on an older release relays our
	// state with the version stripped - refuting every such relay would climb our incarnation for as
	// long as that peer runs.
	stale := node.Status != NodeStatus_ALIVE && node.Incarnation >= self.Incarnation
	staleVersion := node.LogProtocolVersion < self.LogProtocolVersion && node.Incarnation > self.Incarnation
	if stale || staleVersion {
		self.Incarnation = node.Incarnation + 1
		// Only set to ALIVE if we're not JOINING
		// JOINING nodes stay JOINING until explicitly promoted
		if self.Status != NodeStatus_JOINING {
			self.Status = NodeStatus_ALIVE
		}
		// Persist the bump, so a restart starts above what this run announced.
		nr.updateClusterMetricsLocked()
		log.Debug().
			Uint64("refuted_incarnation", node.Incarnation).
			Uint64("new_incarnation", self.Incarnation).
			Str("current_status", self.Status.String()).
			Msg("SWIM refutation: rejecting a stale view of this node")
	}
}

// shouldEscalate returns true if oldStatus -> newStatus is a valid escalation
func (nr *NodeRegistry) shouldEscalate(oldStatus, newStatus NodeStatus) bool {
	// ALIVE -> SUSPECT -> DEAD is the normal escalation path
	if oldStatus == NodeStatus_ALIVE && (newStatus == NodeStatus_SUSPECT || newStatus == NodeStatus_DEAD) {
		return true
	}
	if oldStatus == NodeStatus_SUSPECT && newStatus == NodeStatus_DEAD {
		return true
	}
	// Any state can escalate to REMOVED (admin action)
	if newStatus == NodeStatus_REMOVED && oldStatus != NodeStatus_REMOVED {
		return true
	}
	// ALIVE -> LEAVING (graceful shutdown)
	if oldStatus == NodeStatus_ALIVE && newStatus == NodeStatus_LEAVING {
		return true
	}
	// LEAVING -> SUSPECT/DEAD (node crashes mid-leaving)
	if oldStatus == NodeStatus_LEAVING && (newStatus == NodeStatus_SUSPECT || newStatus == NodeStatus_DEAD) {
		return true
	}
	return false
}

// Get retrieves a node's state (returns a copy to avoid race conditions)
func (nr *NodeRegistry) Get(nodeID uint64) (*NodeState, bool) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return nil, false
	}

	return copyNodeState(node), true
}

// GetLocalNodeID returns the local node's ID
func (nr *NodeRegistry) GetLocalNodeID() uint64 {
	return nr.localNodeID
}

// GetAll returns all nodes (returns copies to avoid race conditions)
func (nr *NodeRegistry) GetAll() []*NodeState {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	nodes := make([]*NodeState, 0, len(nr.nodes))
	for _, node := range nr.nodes {
		nodes = append(nodes, copyNodeState(node))
	}

	return nodes
}

// KnownMemberCount returns how many members this node has ever known and
// still records, in any status, REMOVED included: every member whose log
// could still hold an entry. The registry restores the persisted
// membership at boot, so the count survives a restart. A node configured
// with seeds (seeded) that knows only itself has not learned its
// membership yet and returns 0: unknown.
func (nr *NodeRegistry) KnownMemberCount(seeded bool) int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	if seeded && len(nr.nodes) < 2 {
		return 0
	}
	return len(nr.nodes)
}

// GetAlive returns all alive nodes (returns copies to avoid race conditions)
func (nr *NodeRegistry) GetAlive() []*NodeState {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	nodes := make([]*NodeState, 0)
	for _, node := range nr.nodes {
		if node.Status == NodeStatus_ALIVE {
			nodes = append(nodes, copyNodeState(node))
		}
	}

	return nodes
}

// transitionState transitions a node to a new state with SWIM protocol compliance
func (nr *NodeRegistry) transitionState(nodeID uint64, newStatus NodeStatus, reason string) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return
	}

	// Validate transition follows SWIM escalation rules
	if !nr.shouldEscalate(node.Status, newStatus) {
		log.Debug().
			Uint64("node_id", nodeID).
			Str("current", node.Status.String()).
			Str("new", newStatus.String()).
			Msg("Invalid state transition, skipping")
		return
	}

	oldStatus := node.Status
	node.Status = newStatus
	node.Incarnation++ // Increment to propagate via gossip

	// Record state transition metric
	telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), newStatus.String()).Inc()

	log.Warn().
		Uint64("node_id", nodeID).
		Str("old_status", oldStatus.String()).
		Str("new_status", newStatus.String()).
		Uint64("incarnation", node.Incarnation).
		Str("reason", reason).
		Msg("Node state transition")

	// Update cluster node gauges after transition
	nr.updateClusterMetricsLocked()
}

// MarkSuspect marks a node as suspect
// Increments incarnation per SWIM protocol to propagate suspicion via gossip
func (nr *NodeRegistry) MarkSuspect(nodeID uint64) {
	nr.transitionState(nodeID, NodeStatus_SUSPECT, "gossip timeout")
}

// MarkDead marks a node as dead
func (nr *NodeRegistry) MarkDead(nodeID uint64) {
	nr.transitionState(nodeID, NodeStatus_DEAD, "failure timeout")
}

// Remove removes a node from the registry
func (nr *NodeRegistry) Remove(nodeID uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	delete(nr.nodes, nodeID)
	delete(nr.lastSeen, nodeID)
	nr.updateClusterMetricsLocked()

	log.Info().Uint64("node_id", nodeID).Msg("Node removed from registry")
}

// CheckTimeouts marks nodes as SUSPECT or DEAD based on timeout rules
func (nr *NodeRegistry) CheckTimeouts(suspectTimeout, deadTimeout time.Duration) {
	nr.mu.Lock()

	now := time.Now()
	var deadNodes []*NodeState // Track nodes that became DEAD for cleanup callback
	stateChanged := false

	for nodeID, node := range nr.nodes {
		if nodeID == nr.localNodeID {
			continue
		}

		elapsed := now.Sub(nr.lastSeen[nodeID])

		switch node.Status {
		case NodeStatus_ALIVE:
			if elapsed > suspectTimeout {
				telemetry.NodeStateTransitionsTotal.With(NodeStatus_ALIVE.String(), NodeStatus_SUSPECT.String()).Inc()
				node.Status = NodeStatus_SUSPECT
				stateChanged = true
				log.Warn().Uint64("node_id", nodeID).Msg("Node marked SUSPECT")
			}
		case NodeStatus_SUSPECT:
			if elapsed > deadTimeout {
				telemetry.NodeStateTransitionsTotal.With(NodeStatus_SUSPECT.String(), NodeStatus_DEAD.String()).Inc()
				node.Status = NodeStatus_DEAD
				stateChanged = true
				log.Error().Uint64("node_id", nodeID).Msg("Node marked DEAD")
				deadNodes = append(deadNodes, copyNodeState(node))
			}
		}
	}

	// Update cluster metrics if state changed
	if stateChanged {
		nr.updateClusterMetricsLocked()
	}

	nr.mu.Unlock()

	// Call cleanup callback for DEAD nodes outside lock
	if len(deadNodes) > 0 {
		nr.callbackMu.RLock()
		callback := nr.onNodeDeadFunc
		nr.callbackMu.RUnlock()

		if callback != nil {
			for _, node := range deadNodes {
				callback(node)
			}
		}
	}
}

// MemberIDs returns the IDs of the nodes Count counts, sorted.
func (nr *NodeRegistry) MemberIDs() []uint64 {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	ids := make([]uint64, 0, len(nr.nodes))
	for id, node := range nr.nodes {
		if node.Status != NodeStatus_REMOVED && node.Status != NodeStatus_LEAVING {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// LegacyLogProtocolMembers returns the ids of every current member (any
// status except REMOVED, self included) whose LogProtocolVersion is below
// LogPullProtocolVersion, sorted ascending. A non-empty result means at least
// one member runs an older release: DDL and CREATE/DROP DATABASE must be
// refused cluster-wide until every member reports LogPullProtocolVersion
// (coordinator.LegacyMembersDDLRefusal).
func (nr *NodeRegistry) LegacyLogProtocolMembers() []uint64 {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	ids := make([]uint64, 0)
	for id, node := range nr.nodes {
		if node.Status == NodeStatus_REMOVED {
			continue
		}
		if node.LogProtocolVersion < LogPullProtocolVersion {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// Count returns the number of nodes in membership (excludes REMOVED and LEAVING nodes)
// Used for quorum calculation to prevent split-brain. LEAVING nodes are excluded
// because they are intentionally departing and should not count toward quorum.
func (nr *NodeRegistry) Count() int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	count := 0
	for _, node := range nr.nodes {
		if node.Status != NodeStatus_REMOVED && node.Status != NodeStatus_LEAVING {
			count++
		}
	}
	return count
}

// CountAlive returns the number of alive nodes
func (nr *NodeRegistry) CountAlive() int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	count := 0
	for _, node := range nr.nodes {
		if node.Status == NodeStatus_ALIVE {
			count++
		}
	}

	return count
}

// GetReplicationEligible returns nodes eligible for DML replication (ALIVE only).
// Excludes JOINING (catching up), LEAVING (gracefully departing), and all other non-ALIVE states.
func (nr *NodeRegistry) GetReplicationEligible() []*NodeState {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	nodes := make([]*NodeState, 0)
	for _, node := range nr.nodes {
		// Only ALIVE nodes participate in DML replication
		// JOINING nodes are syncing and shouldn't receive DML writes
		if node.Status == NodeStatus_ALIVE {
			nodes = append(nodes, copyNodeState(node))
		}
	}

	return nodes
}

// MarkJoining marks a node as joining (catching up)
func (nr *NodeRegistry) MarkJoining(nodeID uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	if node, exists := nr.nodes[nodeID]; exists {
		oldStatus := node.Status
		node.Status = NodeStatus_JOINING

		// Record state transition if status changed
		if oldStatus != NodeStatus_JOINING {
			telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), NodeStatus_JOINING.String()).Inc()
			nr.updateClusterMetricsLocked()
		}

		log.Info().Uint64("node_id", nodeID).Msg("Node marked as joining (catching up)")
	}
}

// MarkAlive marks a node as alive (fully synced)
func (nr *NodeRegistry) MarkAlive(nodeID uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	if node, exists := nr.nodes[nodeID]; exists {
		oldStatus := node.Status
		node.Status = NodeStatus_ALIVE
		node.Incarnation++ // Increment to propagate state change via gossip
		nr.lastSeen[nodeID] = time.Now()

		// Record state transition if status changed
		if oldStatus != NodeStatus_ALIVE {
			telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), NodeStatus_ALIVE.String()).Inc()
		}
		// The incarnation changed even when the status did not: persist it.
		nr.updateClusterMetricsLocked()
	}
}

// TouchLastSeen updates the lastSeen timestamp for a node without changing state.
// Call this when receiving any message from a peer to treat it as implicit heartbeat.
// This reduces heartbeat traffic when nodes are actively communicating.
func (nr *NodeRegistry) TouchLastSeen(nodeID uint64) {
	nr.mu.Lock()
	if _, exists := nr.nodes[nodeID]; exists {
		nr.lastSeen[nodeID] = time.Now()
	}
	nr.mu.Unlock()
}

// UpdateSchemaVersions updates the schema versions for the local node
// This is called when DDL operations complete
func (nr *NodeRegistry) UpdateSchemaVersions(versions map[string]uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	node, exists := nr.nodes[nr.localNodeID]
	if !exists {
		log.Error().
			Uint64("node_id", nr.localNodeID).
			Msg("BUG: Local node not found in registry during schema version update")
		return
	}

	node.DatabaseSchemaVersions = copySchemaVersionMap(versions)
	log.Debug().
		Uint64("node_id", nr.localNodeID).
		Interface("versions", versions).
		Msg("Updated schema versions")
}

// GetLocalSchemaVersions returns the local node's schema versions
func (nr *NodeRegistry) GetLocalSchemaVersions() map[string]uint64 {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	if node, exists := nr.nodes[nr.localNodeID]; exists {
		return copySchemaVersionMap(node.DatabaseSchemaVersions)
	}
	return nil
}

// SetOnNodeAlive sets the callback for when a node becomes ALIVE
func (nr *NodeRegistry) SetOnNodeAlive(callback func(*NodeState)) {
	nr.callbackMu.Lock()
	defer nr.callbackMu.Unlock()
	nr.onNodeAliveFunc = callback
}

// SetOnNodeDead sets the callback for when a node becomes DEAD
func (nr *NodeRegistry) SetOnNodeDead(callback func(*NodeState)) {
	nr.callbackMu.Lock()
	defer nr.callbackMu.Unlock()
	nr.onNodeDeadFunc = callback
}

// SetOnNodeLeaving sets the callback for when the local node is marked LEAVING
// via remote decommission (gossip from admin API on another node).
func (nr *NodeRegistry) SetOnNodeLeaving(callback func()) {
	nr.callbackMu.Lock()
	defer nr.callbackMu.Unlock()
	nr.onNodeLeavingFunc = callback
}

// fireOnNodeLeaving calls the LEAVING callback outside the main lock.
func (nr *NodeRegistry) fireOnNodeLeaving() {
	nr.callbackMu.RLock()
	callback := nr.onNodeLeavingFunc
	nr.callbackMu.RUnlock()

	if callback != nil {
		callback()
	}
}

// DetectSchemaDrift logs warnings for nodes with different schema versions
func (nr *NodeRegistry) DetectSchemaDrift() {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	localNode, exists := nr.nodes[nr.localNodeID]
	if !exists {
		return
	}

	for nodeID, node := range nr.nodes {
		if nodeID == nr.localNodeID || node.Status != NodeStatus_ALIVE {
			continue
		}

		// Check each database
		for dbName, localVersion := range localNode.DatabaseSchemaVersions {
			peerVersion, hasPeerVersion := node.DatabaseSchemaVersions[dbName]

			if !hasPeerVersion {
				log.Warn().
					Uint64("peer_node", nodeID).
					Str("database", dbName).
					Uint64("local_version", localVersion).
					Msg("Peer missing schema version for database")
				continue
			}

			if peerVersion != localVersion {
				log.Warn().
					Uint64("peer_node", nodeID).
					Str("database", dbName).
					Uint64("local_version", localVersion).
					Uint64("peer_version", peerVersion).
					Msg("Schema version drift detected")
			}
		}
	}
}

// =======================
// MEMBERSHIP MANAGEMENT
// =======================

// MarkRemoved marks a node as REMOVED from the cluster
// This is called by admin API to permanently remove a node
// The REMOVED state will be gossiped to all other nodes
func (nr *NodeRegistry) MarkRemoved(nodeID uint64) error {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	// Cannot remove self
	if nodeID == nr.localNodeID {
		return fmt.Errorf("cannot remove self from cluster")
	}

	node, exists := nr.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %d not found in registry", nodeID)
	}

	// Already removed
	if node.Status == NodeStatus_REMOVED {
		return nil
	}

	oldStatus := node.Status
	node.Status = NodeStatus_REMOVED
	node.Incarnation++ // Increment to propagate via gossip

	// Record state transition and update metrics
	telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), NodeStatus_REMOVED.String()).Inc()
	nr.updateClusterMetricsLocked()

	log.Info().
		Uint64("node_id", nodeID).
		Str("old_status", oldStatus.String()).
		Msg("Node marked as REMOVED from cluster")

	return nil
}

// AllowRejoin clears the REMOVED status for a node, allowing it to rejoin
// The node must re-join via normal gossip after this
func (nr *NodeRegistry) AllowRejoin(nodeID uint64) error {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %d not found in registry", nodeID)
	}

	if node.Status != NodeStatus_REMOVED {
		return fmt.Errorf("node %d is not in REMOVED state (current: %s)", nodeID, node.Status.String())
	}

	// Set to DEAD so the node can rejoin via normal Join flow
	// When the node rejoins, it will transition through JOINING -> ALIVE
	node.Status = NodeStatus_DEAD
	node.Incarnation++ // Increment to propagate via gossip

	// Record state transition and update metrics
	telemetry.NodeStateTransitionsTotal.With(NodeStatus_REMOVED.String(), NodeStatus_DEAD.String()).Inc()
	nr.updateClusterMetricsLocked()

	log.Info().
		Uint64("node_id", nodeID).
		Msg("Node allowed to rejoin cluster")

	return nil
}

// IsRemoved checks if a node is in REMOVED state
func (nr *NodeRegistry) IsRemoved(nodeID uint64) bool {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return false
	}
	return node.Status == NodeStatus_REMOVED
}

// MarkLeaving marks a node as LEAVING — gracefully departing the cluster.
// Unlike MarkRemoved, this can be called on self and is reversible.
// Returns an error if the node is not found or is already LEAVING/REMOVED.
func (nr *NodeRegistry) MarkLeaving(nodeID uint64) error {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %d not found in registry", nodeID)
	}

	if node.Status == NodeStatus_LEAVING {
		return fmt.Errorf("node %d is already LEAVING", nodeID)
	}
	if node.Status == NodeStatus_REMOVED {
		return fmt.Errorf("node %d is REMOVED; use AllowRejoin first", nodeID)
	}

	oldStatus := node.Status
	node.Status = NodeStatus_LEAVING
	node.Incarnation++ // Increment to propagate via gossip

	telemetry.NodeStateTransitionsTotal.With(oldStatus.String(), NodeStatus_LEAVING.String()).Inc()
	nr.updateClusterMetricsLocked()

	log.Info().
		Uint64("node_id", nodeID).
		Str("old_status", oldStatus.String()).
		Uint64("incarnation", node.Incarnation).
		Msg("Node marked as LEAVING cluster")

	return nil
}

// MarkSelfLeaving marks the local node as LEAVING.
// Increments incarnation so the LEAVING status propagates via gossip.
func (nr *NodeRegistry) MarkSelfLeaving() error {
	return nr.MarkLeaving(nr.localNodeID)
}

// RevertLeaving transitions a LEAVING node back to ALIVE, cancelling decommission.
// Returns an error if the node is not currently in LEAVING state.
func (nr *NodeRegistry) RevertLeaving(nodeID uint64) error {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %d not found in registry", nodeID)
	}

	if node.Status != NodeStatus_LEAVING {
		return fmt.Errorf("node %d is not LEAVING (current: %s)", nodeID, node.Status.String())
	}

	node.Status = NodeStatus_ALIVE
	node.Incarnation++ // Increment to propagate via gossip

	telemetry.NodeStateTransitionsTotal.With(NodeStatus_LEAVING.String(), NodeStatus_ALIVE.String()).Inc()
	nr.updateClusterMetricsLocked()

	log.Info().
		Uint64("node_id", nodeID).
		Uint64("incarnation", node.Incarnation).
		Msg("Node reverted from LEAVING back to ALIVE")

	return nil
}

// IsLeaving checks if a node is in LEAVING state
func (nr *NodeRegistry) IsLeaving(nodeID uint64) bool {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return false
	}
	return node.Status == NodeStatus_LEAVING
}

// MemberInfo represents membership information for admin API
type MemberInfo struct {
	NodeID      uint64
	Address     string
	Status      string
	Incarnation uint64
}

// GetMembershipInfo returns membership information for all nodes
func (nr *NodeRegistry) GetMembershipInfo() []MemberInfo {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	members := make([]MemberInfo, 0, len(nr.nodes))
	for _, node := range nr.nodes {
		members = append(members, MemberInfo{
			NodeID:      node.NodeId,
			Address:     node.Address,
			Status:      node.Status.String(),
			Incarnation: node.Incarnation,
		})
	}
	return members
}

// updateClusterMetricsLocked updates cluster node gauges
// Must be called with mu held
func (nr *NodeRegistry) updateClusterMetricsLocked() {
	counts := make(map[NodeStatus]int)
	for _, node := range nr.nodes {
		counts[node.Status]++
	}

	telemetry.ClusterNodes.With("ALIVE").Set(float64(counts[NodeStatus_ALIVE]))
	telemetry.ClusterNodes.With("SUSPECT").Set(float64(counts[NodeStatus_SUSPECT]))
	telemetry.ClusterNodes.With("DEAD").Set(float64(counts[NodeStatus_DEAD]))
	telemetry.ClusterNodes.With("JOINING").Set(float64(counts[NodeStatus_JOINING]))
	telemetry.ClusterNodes.With("REMOVED").Set(float64(counts[NodeStatus_REMOVED]))
	telemetry.ClusterNodes.With("LEAVING").Set(float64(counts[NodeStatus_LEAVING]))

	// Update quorum available — exclude REMOVED and LEAVING from membership denominator
	aliveCount := counts[NodeStatus_ALIVE]
	totalMembership := len(nr.nodes) - counts[NodeStatus_REMOVED] - counts[NodeStatus_LEAVING]
	quorumSize := (totalMembership / 2) + 1
	if aliveCount >= quorumSize {
		telemetry.ClusterQuorumAvailable.Set(1)
	} else {
		telemetry.ClusterQuorumAvailable.Set(0)
	}

	nr.persistMembershipLocked()
}

// membershipFingerprintLocked summarises exactly the state the snapshot records,
// so the persist hook can tell a real membership change from a mutator that
// touched something the snapshot does not store.
func (nr *NodeRegistry) membershipFingerprintLocked() uint64 {
	ids := make([]uint64, 0, len(nr.nodes))
	for id := range nr.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	h := fnv.New64a()
	var buf [8]byte
	write := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		_, _ = h.Write(buf[:])
	}
	for _, id := range ids {
		node := nr.nodes[id]
		write(id)
		write(uint64(node.Status))
		write(node.Incarnation)
		_, _ = h.Write([]byte(node.Address))
	}
	return h.Sum64()
}

// persistMembershipLocked writes the snapshot when the membership actually
// changed. It runs under the registry's write lock, which is acceptable because
// it is reached only on a real membership change - never from TouchLastSeen,
// the per-heartbeat path - and a synchronous write has no window in which a
// crash loses the very change that would have widened the quorum denominator.
func (nr *NodeRegistry) persistMembershipLocked() {
	if nr.store == nil {
		return
	}

	fingerprint := nr.membershipFingerprintLocked()
	if fingerprint == nr.persistedFingerprint {
		return
	}

	snapshot := membershipSnapshot{Nodes: make([]membershipSnapshotNode, 0, len(nr.nodes))}
	for id, node := range nr.nodes {
		snapshot.Nodes = append(snapshot.Nodes, membershipSnapshotNode{
			NodeID:      id,
			Address:     node.Address,
			Incarnation: node.Incarnation,
			Status:      int32(node.Status),
		})
	}

	if err := nr.store.save(snapshot); err != nil {
		// Not fatal: the running node's in-memory membership is still correct.
		// The cost is that a restart falls back to self-only membership, which
		// the seed-node belt in coordinator.GetClusterState still refuses to
		// treat as a quorum.
		log.Error().Err(err).Msg("Failed to persist cluster membership snapshot")
		return
	}
	nr.persistedFingerprint = fingerprint
}

// restoreMembershipLocked loads the persisted membership and returns how many
// peers it restored. Caller holds the write lock and has already added self.
//
// Restored peers enter as SUSPECT rather than at their persisted status: this
// node has heard from none of them since booting, so their liveness is unknown.
// SUSPECT counts toward TOTAL membership (Count) and is excluded from
// GetAliveNodes, which is exactly the fail-closed shape wanted - the quorum
// denominator is restored immediately while nothing is treated as reachable
// until gossip says so.
//
// REMOVED stays REMOVED, so a decommissioned peer does not come back as a
// member. Incarnation is restored because SWIM refutation compares incarnations:
// Update() rejects an update whose incarnation is not newer (see the SWIM rules
// in Update), so a peer that has moved on refutes our stale record and wins,
// while a restored record is not silently overwritten by an older rumour.
func (nr *NodeRegistry) restoreMembershipLocked() int {
	snapshot, ok := nr.store.load()
	if !ok {
		return 0
	}

	restored := 0
	var selfIncarnation uint64
	selfRecorded := false
	for _, record := range snapshot.Nodes {
		if record.NodeID == nr.localNodeID {
			// Self is ALIVE by construction and was added by the caller.
			selfIncarnation, selfRecorded = record.Incarnation, true
			continue
		}

		status := NodeStatus_SUSPECT
		if NodeStatus(record.Status) == NodeStatus_REMOVED {
			status = NodeStatus_REMOVED
		}

		nr.nodes[record.NodeID] = &NodeState{
			NodeId:      record.NodeID,
			Address:     record.Address,
			Status:      status,
			Incarnation: record.Incarnation,
		}
		// Deliberately NOT time.Now(): a restored peer has not been seen, and
		// dating it now would delay the failure detector by a full timeout.
		nr.lastSeen[record.NodeID] = time.Time{}
		restored++
	}

	// The snapshot we just read is what is on disk; record it so an unchanged
	// membership does not rewrite the same bytes on the first mutation.
	nr.persistedFingerprint = nr.membershipFingerprintLocked()

	// Start above every incarnation the previous run announced: every change
	// of our own incarnation is persisted (MarkLeaving, MarkAlive and SWIM
	// refutation all reach persistMembershipLocked). Peers keep that run's
	// last record of us - LEAVING after a graceful stop - and a restart at or
	// below it would lose to it. Set after the fingerprint so the next
	// membership change persists the new value. A failed persist is logged
	// and leaves the older incarnation on disk.
	if selfRecorded {
		nr.nodes[nr.localNodeID].Incarnation = selfIncarnation + 1
	}
	return restored
}

// QuorumInfo returns quorum calculation information
func (nr *NodeRegistry) QuorumInfo() (totalMembership int, aliveCount int, quorumSize int) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	for _, node := range nr.nodes {
		if node.Status != NodeStatus_REMOVED && node.Status != NodeStatus_LEAVING {
			totalMembership++
			if node.Status == NodeStatus_ALIVE {
				aliveCount++
			}
		}
	}

	// Quorum = majority of total membership
	quorumSize = (totalMembership / 2) + 1
	return
}
