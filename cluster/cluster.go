package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// MemberInfo represents cluster member information
type MemberInfo struct {
	NodeID   uint64 `json:"node_id"`
	Address  string `json:"address"`
	Status   string `json:"status"`
	LastSeen int64  `json:"last_seen"`
}

// NodeRegistry interface for cluster membership operations
type NodeRegistry interface {
	GetMembershipInfo() []MemberInfo
	QuorumInfo() (int, int, int)
	MarkRemoved(nodeID uint64) error
	AllowRejoin(nodeID uint64) error
}

// GossipProtocol interface for gossip operations
type GossipProtocol interface {
	BroadcastImmediate()
}

// ClusterManager handles cluster membership operations
type ClusterManager struct {
	registry NodeRegistry
	gossip   GossipProtocol
	nodeID   uint64
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(registry NodeRegistry, gossip GossipProtocol, nodeID uint64) *ClusterManager {
	return &ClusterManager{
		registry: registry,
		gossip:   gossip,
		nodeID:   nodeID,
	}
}

// HandleMembers handles GET /admin/cluster/members
func (cm *ClusterManager) HandleMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	members := cm.registry.GetMembershipInfo()
	totalMembership, aliveCount, quorumSize := cm.registry.QuorumInfo()

	// Convert MemberInfo to map for JSON response
	memberMaps := make([]map[string]interface{}, len(members))
	for i, member := range members {
		memberMaps[i] = map[string]interface{}{
			"node_id":   member.NodeID,
			"address":   member.Address,
			"status":    member.Status,
			"last_seen": member.LastSeen,
		}
	}

	response := map[string]interface{}{
		"members":          memberMaps,
		"total_membership": totalMembership,
		"alive_count":      aliveCount,
		"quorum_size":      quorumSize,
		"local_node_id":    cm.nodeID,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Failed to encode cluster members response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

// HandleRemove handles POST /admin/cluster/remove/{node_id}
func (cm *ClusterManager) HandleRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse node_id from URL path
	path := strings.TrimPrefix(r.URL.Path, "/admin/cluster/remove/")
	nodeID, err := strconv.ParseUint(path, 10, 64)
	if err != nil {
		http.Error(w, "invalid node_id: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Mark node as removed
	if err := cm.registry.MarkRemoved(nodeID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Trigger immediate gossip to propagate removal
	if cm.gossip != nil {
		cm.gossip.BroadcastImmediate()
	}

	// Return updated membership
	totalMembership, aliveCount, quorumSize := cm.registry.QuorumInfo()

	response := map[string]interface{}{
		"success":          true,
		"message":          fmt.Sprintf("node %d marked as REMOVED", nodeID),
		"total_membership": totalMembership,
		"alive_count":      aliveCount,
		"quorum_size":      quorumSize,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Failed to encode cluster remove response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

// HandleAllow handles POST /admin/cluster/allow/{node_id}
func (cm *ClusterManager) HandleAllow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse node_id from URL path
	path := strings.TrimPrefix(r.URL.Path, "/admin/cluster/allow/")
	nodeID, err := strconv.ParseUint(path, 10, 64)
	if err != nil {
		http.Error(w, "invalid node_id: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Allow node to rejoin
	if err := cm.registry.AllowRejoin(nodeID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Trigger immediate gossip to propagate change
	if cm.gossip != nil {
		cm.gossip.BroadcastImmediate()
	}

	response := map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("node %d allowed to rejoin cluster", nodeID),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Failed to encode cluster allow response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
