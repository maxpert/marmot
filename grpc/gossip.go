package grpc

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// GossipProtocol implements SWIM-style gossip protocol over gRPC
type GossipProtocol struct {
	nodeID      uint64
	incarnation uint64
	registry    *NodeRegistry
	client      *Client
	interval    time.Duration
	fanout      int // Number of nodes to gossip to each round
	running     bool
	stopChan    chan struct{}
	mu          sync.RWMutex
}

// GossipConfig holds gossip protocol configuration
type GossipConfig struct {
	Interval       time.Duration
	Fanout         int
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
}

// DefaultGossipConfig returns default gossip configuration
func DefaultGossipConfig() GossipConfig {
	return GossipConfig{
		Interval:       1 * time.Second,
		Fanout:         3,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    10 * time.Second,
	}
}

// NewGossipProtocol creates a new gossip protocol instance
func NewGossipProtocol(nodeID uint64, registry *NodeRegistry) *GossipProtocol {
	return &GossipProtocol{
		nodeID:      nodeID,
		incarnation: 0,
		registry:    registry,
		client:      NewClient(nodeID),
		interval:    1 * time.Second,
		fanout:      3,
		running:     false,
		stopChan:    make(chan struct{}),
	}
}

// SetClient sets the gRPC client
func (gp *GossipProtocol) SetClient(client *Client) {
	gp.client = client
}

// Start starts the gossip protocol
func (gp *GossipProtocol) Start(config GossipConfig) {
	gp.mu.Lock()
	if gp.running {
		gp.mu.Unlock()
		return
	}
	gp.running = true
	gp.interval = config.Interval
	gp.fanout = config.Fanout
	gp.mu.Unlock()

	log.Info().
		Uint64("node_id", gp.nodeID).
		Dur("interval", config.Interval).
		Int("fanout", config.Fanout).
		Msg("Starting gossip protocol")

	// Start gossip rounds
	go gp.gossipLoop(config)

	// Start timeout checker
	go gp.timeoutLoop(config.SuspectTimeout, config.DeadTimeout)
}

// Stop stops the gossip protocol
func (gp *GossipProtocol) Stop() {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	if !gp.running {
		return
	}

	log.Info().Msg("Stopping gossip protocol")
	close(gp.stopChan)
	gp.running = false
}

// gossipLoop runs periodic gossip rounds
func (gp *GossipProtocol) gossipLoop(config GossipConfig) {
	ticker := time.NewTicker(gp.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gp.doGossipRound()

		case <-gp.stopChan:
			return
		}
	}
}

// doGossipRound performs one round of gossip
func (gp *GossipProtocol) doGossipRound() {
	// Select random peers to gossip to
	peers := gp.selectRandomPeers(gp.fanout)
	if len(peers) == 0 {
		return
	}

	// Prepare gossip message with our view of the cluster
	req := &GossipRequest{
		SourceNodeId: gp.nodeID,
		Nodes:        gp.registry.GetAll(),
		Incarnation:  gp.incarnation,
	}

	// Send to selected peers
	for _, peer := range peers {
		go gp.sendGossip(peer, req)
	}
}

// sendGossip sends a gossip message to a peer
func (gp *GossipProtocol) sendGossip(peer *NodeState, req *GossipRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := gp.client.SendGossip(ctx, peer.NodeId, req)
	if err != nil {
		log.Debug().
			Err(err).
			Uint64("peer_id", peer.NodeId).
			Msg("Failed to send gossip")

		// Mark peer as suspect on failure
		gp.registry.MarkSuspect(peer.NodeId)
		return
	}

	// Merge received node states
	for _, nodeState := range resp.Nodes {
		gp.registry.Update(nodeState)
	}
}

// selectRandomPeers selects random peers for gossiping
func (gp *GossipProtocol) selectRandomPeers(n int) []*NodeState {
	aliveNodes := gp.registry.GetAlive()

	// Filter out self
	peers := make([]*NodeState, 0)
	for _, node := range aliveNodes {
		if node.NodeId != gp.nodeID {
			peers = append(peers, node)
		}
	}

	if len(peers) == 0 {
		return nil
	}

	// Randomly select up to n peers
	if len(peers) <= n {
		return peers
	}

	// Shuffle and take first n
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	return peers[:n]
}

// timeoutLoop checks for node timeouts
func (gp *GossipProtocol) timeoutLoop(suspectTimeout, deadTimeout time.Duration) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gp.registry.CheckTimeouts(suspectTimeout, deadTimeout)

		case <-gp.stopChan:
			return
		}
	}
}

// JoinCluster sends join requests to seed nodes with advertise address
func (gp *GossipProtocol) JoinCluster(seedAddresses []string, advertiseAddress string) error {
	log.Info().
		Strs("seeds", seedAddresses).
		Str("advertise_address", advertiseAddress).
		Msg("Joining cluster via seed nodes")

	for i, addr := range seedAddresses {
		// Use temporary node ID for seeds (will be replaced with actual)
		seedNodeID := uint64(1000 + i)

		// Connect to seed
		if err := gp.client.Connect(seedNodeID, addr); err != nil {
			log.Warn().
				Err(err).
				Str("address", addr).
				Msg("Failed to connect to seed node")
			continue
		}

		// Send join request with our advertise address
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req := &JoinRequest{
			NodeId:  gp.nodeID,
			Address: advertiseAddress,
		}

		resp, err := gp.client.SendJoin(ctx, seedNodeID, req)
		cancel()

		if err != nil {
			log.Warn().
				Err(err).
				Str("address", addr).
				Msg("Failed to send join request")
			continue
		}

		if !resp.Success {
			log.Warn().
				Str("address", addr).
				Msg("Join request rejected")
			continue
		}

		// Add received cluster nodes to registry
		for _, node := range resp.ClusterNodes {
			gp.registry.Add(node)

			// Connect to new nodes
			if node.NodeId != gp.nodeID && node.Address != "" {
				if err := gp.client.Connect(node.NodeId, node.Address); err != nil {
					log.Warn().
						Err(err).
						Uint64("node_id", node.NodeId).
						Msg("Failed to connect to cluster node")
				}
			}
		}

		log.Info().
			Int("cluster_size", len(resp.ClusterNodes)).
			Msg("Successfully joined cluster")

		return nil
	}

	return nil // Continue even if all seeds fail (single-node cluster)
}

// GetIncarnation returns the current incarnation number
func (gp *GossipProtocol) GetIncarnation() uint64 {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	return gp.incarnation
}

// IncrementIncarnation increments the incarnation number
func (gp *GossipProtocol) IncrementIncarnation() {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	gp.incarnation++
}

// OnNodeJoin is called when a new node joins the cluster
func (gp *GossipProtocol) OnNodeJoin(node *NodeState) {
	// Connect to the new node if we have its address
	if node.NodeId != gp.nodeID && node.Address != "" {
		if err := gp.client.Connect(node.NodeId, node.Address); err != nil {
			log.Warn().
				Err(err).
				Uint64("node_id", node.NodeId).
				Str("address", node.Address).
				Msg("Failed to connect to joining node")
		} else {
			log.Info().
				Uint64("node_id", node.NodeId).
				Str("address", node.Address).
				Msg("Connected to joining node")
		}
	}
}

// GetNodeRegistry returns the node registry for accessing cluster membership
func (gp *GossipProtocol) GetNodeRegistry() *NodeRegistry {
	return gp.registry
}
