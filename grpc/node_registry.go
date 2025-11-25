package grpc

import (
	"sync"
	"time"

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
		// Higher incarnation always wins
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
	// ALIVE -> SUSPECT -> DEAD is the only valid escalation path
	if oldStatus == NodeStatus_ALIVE && (newStatus == NodeStatus_SUSPECT || newStatus == NodeStatus_DEAD) {
		return true
	}
	if oldStatus == NodeStatus_SUSPECT && newStatus == NodeStatus_DEAD {
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

	log.Warn().
		Uint64("node_id", nodeID).
		Str("old_status", oldStatus.String()).
		Str("new_status", newStatus.String()).
		Uint64("incarnation", node.Incarnation).
		Str("reason", reason).
		Msg("Node state transition")
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

// Count returns the number of nodes
func (nr *NodeRegistry) Count() int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	return len(nr.nodes)
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

// GetReplicationEligible returns nodes eligible for replication (ALIVE only, excludes JOINING)
// JOINING nodes are catching up and should not receive write replication yet
func (nr *NodeRegistry) GetReplicationEligible() []*NodeState {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	nodes := make([]*NodeState, 0)
	for _, node := range nr.nodes {
		// Only ALIVE nodes participate in replication
		// JOINING nodes are syncing and shouldn't receive writes
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
