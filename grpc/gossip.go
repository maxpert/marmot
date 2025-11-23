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
		SuspectTimeout: 15 * time.Second, // Increased from 5s to allow gossip propagation
		DeadTimeout:    30 * time.Second, // Increased proportionally
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
	peers := gp.selectRandomPeers(gp.fanout)
	if len(peers) == 0 {
		return
	}

	// Prepare gossip message with our view of the cluster
	allNodes := gp.registry.GetAll()
	req := &GossipRequest{
		SourceNodeId: gp.nodeID,
		Nodes:        allNodes,
		Incarnation:  gp.incarnation,
	}

	// Send to selected peers
	for _, peer := range peers {
		go gp.sendGossip(peer, req)
	}
}

// BroadcastImmediate immediately broadcasts current state to all peers
// Used when critical state changes need to be propagated quickly
func (gp *GossipProtocol) BroadcastImmediate() {
	gp.doGossipRound()
}

// BroadcastAndVerifyAlive broadcasts ALIVE status and verifies peers have acknowledged it
// Retries up to maxRetries times with retryInterval between attempts
// Returns true if at least one peer has us marked as ALIVE in their registry
func (gp *GossipProtocol) BroadcastAndVerifyAlive(maxRetries int, retryInterval time.Duration) bool {
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Broadcast current state
		gp.doGossipRound()

		// Short delay to allow gossip to propagate
		time.Sleep(retryInterval)

		// Send gossip to peers and check their response to see if they have us as ALIVE
		peers := gp.selectRandomPeers(3) // Check a few peers
		verifiedCount := 0

		for _, peer := range peers {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

			req := &GossipRequest{
				SourceNodeId: gp.nodeID,
				Nodes:        gp.registry.GetAll(),
				Incarnation:  gp.incarnation,
			}

			resp, err := gp.client.SendGossip(ctx, peer.NodeId, req)
			cancel()

			if err != nil {
				log.Debug().Err(err).Uint64("peer", peer.NodeId).Msg("Failed to verify with peer")
				continue
			}

			// Check if the peer's view includes us as ALIVE
			for _, node := range resp.Nodes {
				if node.NodeId == gp.nodeID && node.Status == NodeStatus_ALIVE {
					verifiedCount++
					break
				}
			}
		}

		if verifiedCount > 0 {
			log.Info().
				Int("attempt", attempt+1).
				Int("verified_peers", verifiedCount).
				Msg("ALIVE status verified by peers")
			return true
		}

		log.Debug().
			Int("attempt", attempt+1).
			Int("max_retries", maxRetries).
			Msg("Retrying ALIVE broadcast - peers haven't acknowledged yet")
	}

	log.Warn().Msg("Failed to verify ALIVE status with peers after max retries")
	return false
}

// sendGossip sends a gossip message to a peer
func (gp *GossipProtocol) sendGossip(peer *NodeState, req *GossipRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := gp.client.SendGossip(ctx, peer.NodeId, req)
	if err != nil {
		gp.registry.MarkSuspect(peer.NodeId)
		return
	}

	// Merge received node states
	for _, nodeState := range resp.Nodes {
		gp.registry.Update(nodeState)
	}
}

// selectRandomPeers selects random peers for gossiping
// Note: Include ALL nodes except DEAD for gossip (ALIVE + JOINING + SUSPECT)
func (gp *GossipProtocol) selectRandomPeers(n int) []*NodeState {
	allNodes := gp.registry.GetAll()

	// Filter out self and DEAD nodes
	peers := make([]*NodeState, 0)
	for _, node := range allNodes {
		if node.NodeId != gp.nodeID && node.Status != NodeStatus_DEAD {
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
	// Skip if it's ourselves or no address provided
	if node.NodeId == gp.nodeID || node.Address == "" {
		return
	}

	// Attempt to establish connection
	if err := gp.client.Connect(node.NodeId, node.Address); err != nil {
		log.Error().
			Err(err).
			Uint64("node_id", node.NodeId).
			Str("address", node.Address).
			Msg("Failed to establish connection to node - will retry on next gossip round")
		return
	}

	log.Info().
		Uint64("node_id", node.NodeId).
		Str("address", node.Address).
		Msg("Successfully connected to node")
}

// GetNodeRegistry returns the node registry for accessing cluster membership
func (gp *GossipProtocol) GetNodeRegistry() *NodeRegistry {
	return gp.registry
}

// GetClient returns the gRPC client for connection management
func (gp *GossipProtocol) GetClient() *Client {
	return gp.client
}
