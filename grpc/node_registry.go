package grpc

import (
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

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
	}
}

// NodeRegistry tracks cluster membership using SWIM protocol
type NodeRegistry struct {
	localNodeID     uint64
	nodes           map[uint64]*NodeState
	lastSeen        map[uint64]time.Time
	mu              sync.RWMutex
	onNodeAliveFunc func(*NodeState) // Callback when node transitions to ALIVE
	onNodeDeadFunc  func(*NodeState) // Callback when node transitions to DEAD
	callbackMu      sync.RWMutex
}

// NewNodeRegistry creates a new node registry
func NewNodeRegistry(localNodeID uint64, advertiseAddress string) *NodeRegistry {
	log.Debug().
		Uint64("node_id", localNodeID).
		Str("advertise_address", advertiseAddress).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("BOOT: Creating node registry")

	nr := &NodeRegistry{
		localNodeID: localNodeID,
		nodes:       make(map[uint64]*NodeState),
		lastSeen:    make(map[uint64]time.Time),
	}

	// Add self to registry as ALIVE
	now := time.Now()
	nr.nodes[localNodeID] = &NodeState{
		NodeId:      localNodeID,
		Address:     advertiseAddress,
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	}
	nr.lastSeen[localNodeID] = now

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

	// Rule 2: Discover new nodes
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
		nr.mu.Unlock()
		return
	}

	// Rule 3: Always update lastSeen (even if state unchanged)
	nr.lastSeen[node.NodeId] = time.Now()

	// Track if node transitions to ALIVE for callback
	becameAlive := false

	// Rule 4: Apply SWIM state update rules
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

		// Check if node became ALIVE
		if oldStatus != NodeStatus_ALIVE && node.Status == NodeStatus_ALIVE {
			becameAlive = true
		}
	} else if node.Incarnation == existing.Incarnation && nr.shouldEscalate(existing.Status, node.Status) {
		// Same incarnation: only allow escalation (ALIVE -> SUSPECT -> DEAD)
		log.Debug().
			Uint64("local_node", nr.localNodeID).
			Uint64("update_node", node.NodeId).
			Str("old_status", existing.Status.String()).
			Str("new_status", node.Status.String()).
			Uint64("incarnation", node.Incarnation).
			Msg("REGISTRY: Escalating node status (same incarnation)")
		existing.Status = node.Status
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
	// Ignore updates with same/older incarnation that don't escalate

	nr.mu.Unlock()

	// Call callback outside lock to avoid deadlock
	if becameAlive {
		nr.callbackMu.RLock()
		callback := nr.onNodeAliveFunc
		nr.callbackMu.RUnlock()

		if callback != nil {
			callback(node)
		}
	}
}

// handleSelfUpdateLocked implements SWIM refutation for local node
// Caller must hold nr.mu lock
func (nr *NodeRegistry) handleSelfUpdateLocked(node *NodeState) {
	self := nr.nodes[nr.localNodeID]

	// If someone claims we're SUSPECT/DEAD, refute by incrementing incarnation
	if node.Status != NodeStatus_ALIVE && node.Incarnation >= self.Incarnation {
		self.Incarnation = node.Incarnation + 1
		// Only set to ALIVE if we're not JOINING
		// JOINING nodes stay JOINING until explicitly promoted
		if self.Status != NodeStatus_JOINING {
			self.Status = NodeStatus_ALIVE
		}
		log.Debug().
			Uint64("refuted_incarnation", node.Incarnation).
			Uint64("new_incarnation", self.Incarnation).
			Str("current_status", self.Status.String()).
			Msg("SWIM refutation: rejecting SUSPECT/DEAD claim")
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

	log.Info().Uint64("node_id", nodeID).Msg("Node removed from registry")
}

// CheckTimeouts marks nodes as SUSPECT or DEAD based on timeout rules
func (nr *NodeRegistry) CheckTimeouts(suspectTimeout, deadTimeout time.Duration) {
	nr.mu.Lock()

	now := time.Now()
	var deadNodes []*NodeState // Track nodes that became DEAD for cleanup callback

	for nodeID, node := range nr.nodes {
		if nodeID == nr.localNodeID {
			continue
		}

		elapsed := now.Sub(nr.lastSeen[nodeID])

		switch node.Status {
		case NodeStatus_ALIVE:
			if elapsed > suspectTimeout {
				node.Status = NodeStatus_SUSPECT
				log.Warn().Uint64("node_id", nodeID).Msg("Node marked SUSPECT")
			}
		case NodeStatus_SUSPECT:
			if elapsed > deadTimeout {
				node.Status = NodeStatus_DEAD
				log.Error().Uint64("node_id", nodeID).Msg("Node marked DEAD")
				deadNodes = append(deadNodes, copyNodeState(node))
			}
		}
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

// Count returns the number of nodes in membership (excludes REMOVED nodes)
// Used for quorum calculation to prevent split-brain
func (nr *NodeRegistry) Count() int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	count := 0
	for _, node := range nr.nodes {
		if node.Status != NodeStatus_REMOVED {
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

// GetReplicationEligible returns nodes eligible for DML replication (ALIVE only, excludes JOINING)
// JOINING nodes are catching up and should not receive DML replication yet
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
		node.Status = NodeStatus_JOINING
		log.Info().Uint64("node_id", nodeID).Msg("Node marked as joining (catching up)")
	}
}

// MarkAlive marks a node as alive (fully synced)
func (nr *NodeRegistry) MarkAlive(nodeID uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	if node, exists := nr.nodes[nodeID]; exists {
		node.Status = NodeStatus_ALIVE
		node.Incarnation++ // Increment to propagate state change via gossip
		nr.lastSeen[nodeID] = time.Now()
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

	// Update quorum available
	aliveCount := counts[NodeStatus_ALIVE]
	totalMembership := len(nr.nodes) - counts[NodeStatus_REMOVED]
	quorumSize := (totalMembership / 2) + 1
	if aliveCount >= quorumSize {
		telemetry.ClusterQuorumAvailable.Set(1)
	} else {
		telemetry.ClusterQuorumAvailable.Set(0)
	}
}

// QuorumInfo returns quorum calculation information
func (nr *NodeRegistry) QuorumInfo() (totalMembership int, aliveCount int, quorumSize int) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	for _, node := range nr.nodes {
		if node.Status != NodeStatus_REMOVED {
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

// =======================
// WATERMARK PROTOCOL
// =======================

// UpdateLocalWatermark updates the minimum applied sequence number for the local node
// This watermark is gossiped to all peers and used for GC coordination
func (nr *NodeRegistry) UpdateLocalWatermark(minSeq uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	node, exists := nr.nodes[nr.localNodeID]
	if !exists {
		log.Error().
			Uint64("node_id", nr.localNodeID).
			Msg("BUG: Local node not found in registry during watermark update")
		return
	}

	// Only update if the new watermark is higher (watermarks only advance)
	if minSeq > node.MinAppliedSeq {
		node.MinAppliedSeq = minSeq
		log.Debug().
			Uint64("node_id", nr.localNodeID).
			Uint64("watermark", minSeq).
			Msg("Updated local watermark")
	}
}

// GetLocalWatermark returns the local node's watermark
func (nr *NodeRegistry) GetLocalWatermark() uint64 {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	if node, exists := nr.nodes[nr.localNodeID]; exists {
		return node.MinAppliedSeq
	}
	return 0
}

// GetClusterMinWatermark returns the minimum watermark across all ALIVE nodes
// This is the safe point for garbage collection - transactions with seq_num below
// this value have been applied by all nodes and can be safely cleaned up
func (nr *NodeRegistry) GetClusterMinWatermark() uint64 {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	var minWatermark uint64 = ^uint64(0) // Max uint64
	hasAliveNodes := false

	for _, node := range nr.nodes {
		if node.Status == NodeStatus_ALIVE {
			hasAliveNodes = true
			if node.MinAppliedSeq < minWatermark {
				minWatermark = node.MinAppliedSeq
			}
		}
	}

	// If no alive nodes (shouldn't happen, we're always alive), return 0
	if !hasAliveNodes {
		return 0
	}

	return minWatermark
}
