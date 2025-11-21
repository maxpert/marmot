package grpc

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// NodeRegistry tracks cluster membership
type NodeRegistry struct {
	localNodeID uint64
	nodes       map[uint64]*NodeState
	lastSeen    map[uint64]time.Time
	mu          sync.RWMutex
}

// NewNodeRegistry creates a new node registry
func NewNodeRegistry(localNodeID uint64) *NodeRegistry {
	nr := &NodeRegistry{
		localNodeID: localNodeID,
		nodes:       make(map[uint64]*NodeState),
		lastSeen:    make(map[uint64]time.Time),
	}

	// Add self to registry
	nr.nodes[localNodeID] = &NodeState{
		NodeId:      localNodeID,
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	}
	nr.lastSeen[localNodeID] = time.Now()

	return nr
}

// Add adds a node to the registry
func (nr *NodeRegistry) Add(node *NodeState) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	nr.nodes[node.NodeId] = node
	nr.lastSeen[node.NodeId] = time.Now()

	log.Debug().
		Uint64("node_id", node.NodeId).
		Str("address", node.Address).
		Str("status", node.Status.String()).
		Msg("Node added to registry")
}

// Update updates a node's state
func (nr *NodeRegistry) Update(node *NodeState) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	existing, exists := nr.nodes[node.NodeId]
	if !exists {
		nr.nodes[node.NodeId] = node
		nr.lastSeen[node.NodeId] = time.Now()
		return
	}

	// Always update lastSeen when we hear about a node (even if state doesn't change)
	nr.lastSeen[node.NodeId] = time.Now()

	// Update state only if newer incarnation
	// SWIM protocol: updates with same/older incarnation should be ignored
	if node.Incarnation > existing.Incarnation {
		nr.nodes[node.NodeId] = node

		log.Debug().
			Uint64("node_id", node.NodeId).
			Str("old_status", existing.Status.String()).
			Str("new_status", node.Status.String()).
			Msg("Node state updated")
	} else if node.Incarnation == existing.Incarnation {
		// With same incarnation, only allow status escalation (ALIVE -> SUSPECT -> DEAD)
		shouldUpdate := false
		if existing.Status == NodeStatus_ALIVE && node.Status != NodeStatus_ALIVE {
			shouldUpdate = true // Escalate ALIVE -> SUSPECT or DEAD
		} else if existing.Status == NodeStatus_SUSPECT && node.Status == NodeStatus_DEAD {
			shouldUpdate = true // Escalate SUSPECT -> DEAD
		}

		if shouldUpdate {
			nr.nodes[node.NodeId] = node
			log.Debug().
				Uint64("node_id", node.NodeId).
				Str("old_status", existing.Status.String()).
				Str("new_status", node.Status.String()).
				Msg("Node state updated")
		}
	}
}

// Get retrieves a node's state (returns a copy to avoid race conditions)
func (nr *NodeRegistry) Get(nodeID uint64) (*NodeState, bool) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid races when caller accesses fields
	nodeCopy := &NodeState{
		NodeId:      node.NodeId,
		Address:     node.Address,
		Status:      node.Status,
		Incarnation: node.Incarnation,
	}
	return nodeCopy, true
}

// GetAll returns all nodes (returns copies to avoid race conditions)
func (nr *NodeRegistry) GetAll() []*NodeState {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	nodes := make([]*NodeState, 0, len(nr.nodes))
	for _, node := range nr.nodes {
		// Return a copy to avoid races during serialization
		nodeCopy := &NodeState{
			NodeId:      node.NodeId,
			Address:     node.Address,
			Status:      node.Status,
			Incarnation: node.Incarnation,
		}
		nodes = append(nodes, nodeCopy)
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
			// Return a copy to avoid races during serialization
			nodeCopy := &NodeState{
				NodeId:      node.NodeId,
				Address:     node.Address,
				Status:      node.Status,
				Incarnation: node.Incarnation,
			}
			nodes = append(nodes, nodeCopy)
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

// CheckTimeouts checks for nodes that haven't been seen recently
func (nr *NodeRegistry) CheckTimeouts(suspectTimeout, deadTimeout time.Duration) {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	now := time.Now()

	for nodeID, node := range nr.nodes {
		if nodeID == nr.localNodeID {
			continue // Skip self
		}

		lastSeen := nr.lastSeen[nodeID]
		elapsed := now.Sub(lastSeen)

		switch node.Status {
		case NodeStatus_ALIVE:
			if elapsed > suspectTimeout {
				node.Status = NodeStatus_SUSPECT
				log.Warn().
					Uint64("node_id", nodeID).
					Dur("elapsed", elapsed).
					Msg("Node became suspect (timeout)")
			}

		case NodeStatus_SUSPECT:
			if elapsed > deadTimeout {
				node.Status = NodeStatus_DEAD
				log.Error().
					Uint64("node_id", nodeID).
					Dur("elapsed", elapsed).
					Msg("Node declared dead (timeout)")
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
			nodeCopy := &NodeState{
				NodeId:      node.NodeId,
				Address:     node.Address,
				Status:      node.Status,
				Incarnation: node.Incarnation,
			}
			nodes = append(nodes, nodeCopy)
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
		log.Info().Uint64("node_id", nodeID).Msg("Node marked as alive")
	}
}
