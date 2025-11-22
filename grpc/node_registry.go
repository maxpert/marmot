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
	localNodeID uint64
	nodes       map[uint64]*NodeState
	lastSeen    map[uint64]time.Time
	mu          sync.RWMutex
}

// NewNodeRegistry creates a new node registry
func NewNodeRegistry(localNodeID uint64, advertiseAddress string) *NodeRegistry {
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
	defer nr.mu.Unlock()

	// Rule 1: SWIM refutation - local node rejects non-ALIVE status for itself
	if node.NodeId == nr.localNodeID {
		nr.handleSelfUpdate(node)
		return
	}

	// Rule 2: Discover new nodes
	existing, exists := nr.nodes[node.NodeId]
	if !exists {
		nr.nodes[node.NodeId] = node
		nr.lastSeen[node.NodeId] = time.Now()
		return
	}

	// Rule 3: Always update lastSeen (even if state unchanged)
	nr.lastSeen[node.NodeId] = time.Now()

	// Rule 4: Apply SWIM state update rules
	if node.Incarnation > existing.Incarnation {
		// Higher incarnation always wins
		nr.nodes[node.NodeId] = node
	} else if node.Incarnation == existing.Incarnation && nr.shouldEscalate(existing.Status, node.Status) {
		// Same incarnation: only allow escalation (ALIVE -> SUSPECT -> DEAD)
		existing.Status = node.Status
	}
	// Ignore updates with same/older incarnation that don't escalate
}

// handleSelfUpdate implements SWIM refutation for local node
func (nr *NodeRegistry) handleSelfUpdate(node *NodeState) {
	self := nr.nodes[nr.localNodeID]

	// If someone claims we're SUSPECT/DEAD, refute by incrementing incarnation
	if node.Status != NodeStatus_ALIVE && node.Incarnation >= self.Incarnation {
		self.Incarnation = node.Incarnation + 1
		self.Status = NodeStatus_ALIVE
		log.Warn().
			Uint64("refuted_incarnation", node.Incarnation).
			Uint64("new_incarnation", self.Incarnation).
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

// MarkSuspect marks a node as suspect
func (nr *NodeRegistry) MarkSuspect(nodeID uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	if node, exists := nr.nodes[nodeID]; exists {
		node.Status = NodeStatus_SUSPECT
		log.Warn().Uint64("node_id", nodeID).Msg("Node marked as suspect")
	}
}

// MarkDead marks a node as dead
func (nr *NodeRegistry) MarkDead(nodeID uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	if node, exists := nr.nodes[nodeID]; exists {
		node.Status = NodeStatus_DEAD
		log.Warn().Uint64("node_id", nodeID).Msg("Node marked as dead")
	}
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
	defer nr.mu.Unlock()

	now := time.Now()
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

// DetectSchemaD rift logs warnings for nodes with different schema versions
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
