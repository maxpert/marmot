package grpc

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/telemetry"
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
		SuspectTimeout: 15 * time.Second,
		DeadTimeout:    30 * time.Second,
	}
}

// GossipConfigFromCluster creates gossip config from cluster configuration
func GossipConfigFromCluster(cluster cfg.ClusterConfiguration) GossipConfig {
	config := DefaultGossipConfig()

	if cluster.GossipIntervalMS > 0 {
		config.Interval = time.Duration(cluster.GossipIntervalMS) * time.Millisecond
	}
	if cluster.GossipFanout > 0 {
		config.Fanout = cluster.GossipFanout
	}
	if cluster.SuspectTimeoutMS > 0 {
		config.SuspectTimeout = time.Duration(cluster.SuspectTimeoutMS) * time.Millisecond
	}
	if cluster.DeadTimeoutMS > 0 {
		config.DeadTimeout = time.Duration(cluster.DeadTimeoutMS) * time.Millisecond
	}

	return config
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
	telemetry.GossipRoundsTotal.Inc()
	peers := gp.selectRandomPeers(gp.fanout)
	if len(peers) == 0 {
		// Log registry state when no peers found - helps debug membership issues
		allNodes := gp.registry.GetAll()
		log.Debug().
			Uint64("node_id", gp.nodeID).
			Int("total_registry_nodes", len(allNodes)).
			Msg("GOSSIP: No peers selected for gossip round")
		for _, n := range allNodes {
			log.Debug().
				Uint64("node_id", gp.nodeID).
				Uint64("registry_node", n.NodeId).
				Str("status", n.Status.String()).
				Msg("GOSSIP: Registry state when no peers")
		}
		return
	}

	// Prepare gossip message with our view of the cluster
	allNodes := gp.registry.GetAll()
	req := &GossipRequest{
		SourceNodeId: gp.nodeID,
		Nodes:        allNodes,
		Incarnation:  gp.incarnation,
	}

	// Log what we're sending
	for _, node := range allNodes {
		log.Debug().
			Uint64("node_id", gp.nodeID).
			Uint64("target_node", node.NodeId).
			Str("status", node.Status.String()).
			Uint64("incarnation", node.Incarnation).
			Msg("GOSSIP: Sending node state")
	}

	// Send to selected peers
	for _, peer := range peers {
		log.Debug().
			Uint64("node_id", gp.nodeID).
			Uint64("peer", peer.NodeId).
			Str("peer_status", peer.Status.String()).
			Msg("GOSSIP: Sending to peer")
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
	// For DEAD nodes, try to reconnect first - they might have recovered
	if peer.Status == NodeStatus_DEAD && peer.Address != "" {
		if err := gp.client.Connect(peer.NodeId, peer.Address); err != nil {
			log.Debug().
				Err(err).
				Uint64("peer", peer.NodeId).
				Msg("GOSSIP: Failed to reconnect to DEAD node")
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	telemetry.GossipMessagesTotal.With("sent").Inc()
	resp, err := gp.client.SendGossip(ctx, peer.NodeId, req)
	if err != nil {
		telemetry.GossipFailuresTotal.Inc()
		log.Debug().
			Err(err).
			Uint64("node_id", gp.nodeID).
			Uint64("peer", peer.NodeId).
			Str("peer_status", peer.Status.String()).
			Msg("GOSSIP: SendGossip FAILED")
		// Don't immediately mark suspect - let time-based CheckTimeouts handle it
		// Immediate marking causes flapping when transient failures occur
		// CheckTimeouts will mark SUSPECT after 15s of no contact (suspectTimeout)
		return
	}

	telemetry.GossipMessagesTotal.With("received").Inc()
	log.Debug().
		Uint64("node_id", gp.nodeID).
		Uint64("peer", peer.NodeId).
		Int("nodes_received", len(resp.Nodes)).
		Msg("GOSSIP: SendGossip SUCCESS")

	// Merge received node states
	for _, nodeState := range resp.Nodes {
		log.Debug().
			Uint64("node_id", gp.nodeID).
			Uint64("from_peer", peer.NodeId).
			Uint64("update_node", nodeState.NodeId).
			Str("status", nodeState.Status.String()).
			Uint64("incarnation", nodeState.Incarnation).
			Msg("GOSSIP: Applying update from peer")
		gp.registry.Update(nodeState)
	}
}

// selectRandomPeers selects random peers for gossiping
// Note: Include ALL nodes including DEAD - this allows recovery when dead nodes restart
// The SWIM protocol relies on gossip to propagate state, and excluding DEAD nodes
// permanently prevents restarted nodes from being re-discovered
func (gp *GossipProtocol) selectRandomPeers(n int) []*NodeState {
	allNodes := gp.registry.GetAll()

	// Filter out only self - include ALL other nodes (ALIVE, JOINING, SUSPECT, DEAD)
	// Dead nodes might have recovered, and gossip is how we discover this
	peers := make([]*NodeState, 0)
	for _, node := range allNodes {
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
	log.Debug().
		Uint64("node_id", gp.nodeID).
		Strs("seeds", seedAddresses).
		Str("advertise_address", advertiseAddress).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("BOOT: JoinCluster called")

	for i, addr := range seedAddresses {
		// Use temporary node ID for seeds (will be replaced with actual)
		seedNodeID := uint64(1000 + i)

		log.Debug().
			Uint64("node_id", gp.nodeID).
			Str("seed_addr", addr).
			Str("timestamp", time.Now().Format("15:04:05.000")).
			Msg("BOOT: Attempting to connect to seed")

		// Connect to seed
		if err := gp.client.Connect(seedNodeID, addr); err != nil {
			log.Debug().
				Err(err).
				Uint64("node_id", gp.nodeID).
				Str("address", addr).
				Str("timestamp", time.Now().Format("15:04:05.000")).
				Msg("BOOT: Failed to connect to seed node")
			continue
		}

		log.Debug().
			Uint64("node_id", gp.nodeID).
			Str("seed_addr", addr).
			Str("timestamp", time.Now().Format("15:04:05.000")).
			Msg("BOOT: Connected to seed, sending join request")

		// Retry join request with increasing timeout (lazy gRPC connection may need time)
		var resp *JoinResponse
		var err error
		maxRetries := 3
		baseTimeout := 5 * time.Second

		for attempt := 1; attempt <= maxRetries; attempt++ {
			timeout := time.Duration(attempt) * baseTimeout // 5s, 10s, 15s
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			req := &JoinRequest{
				NodeId:  gp.nodeID,
				Address: advertiseAddress,
			}

			resp, err = gp.client.SendJoin(ctx, seedNodeID, req)
			cancel()

			if err == nil {
				break
			}

			log.Debug().
				Err(err).
				Uint64("node_id", gp.nodeID).
				Str("address", addr).
				Int("attempt", attempt).
				Int("max_retries", maxRetries).
				Dur("timeout", timeout).
				Str("timestamp", time.Now().Format("15:04:05.000")).
				Msg("BOOT: Join request failed, retrying")

			// Small delay before retry
			if attempt < maxRetries {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if err != nil {
			telemetry.ClusterJoinTotal.With("failed").Inc()
			log.Debug().
				Err(err).
				Uint64("node_id", gp.nodeID).
				Str("address", addr).
				Str("timestamp", time.Now().Format("15:04:05.000")).
				Msg("BOOT: Failed to send join request after all retries")
			continue
		}

		if !resp.Success {
			telemetry.ClusterJoinTotal.With("rejected").Inc()
			log.Debug().
				Uint64("node_id", gp.nodeID).
				Str("address", addr).
				Str("timestamp", time.Now().Format("15:04:05.000")).
				Msg("BOOT: Join request rejected")
			continue
		}

		telemetry.ClusterJoinTotal.With("success").Inc()
		log.Debug().
			Uint64("node_id", gp.nodeID).
			Int("cluster_nodes_count", len(resp.ClusterNodes)).
			Str("timestamp", time.Now().Format("15:04:05.000")).
			Msg("BOOT: Join successful, received cluster nodes")

		// Add received cluster nodes to registry
		for _, node := range resp.ClusterNodes {
			log.Debug().
				Uint64("local_node_id", gp.nodeID).
				Uint64("received_node_id", node.NodeId).
				Str("received_status", node.Status.String()).
				Str("received_address", node.Address).
				Str("timestamp", time.Now().Format("15:04:05.000")).
				Msg("BOOT: Processing cluster node from join response")

			// Skip adding ourselves - we already have our own entry as ALIVE
			if node.NodeId == gp.nodeID {
				log.Debug().
					Uint64("node_id", node.NodeId).
					Str("timestamp", time.Now().Format("15:04:05.000")).
					Msg("BOOT: Skipping self in cluster nodes")
				continue
			}

			gp.registry.Add(node)
			log.Debug().
				Uint64("local_node_id", gp.nodeID).
				Uint64("added_node_id", node.NodeId).
				Str("timestamp", time.Now().Format("15:04:05.000")).
				Msg("BOOT: Added node to registry from join response")

			// Connect to new nodes
			if node.Address != "" {
				if err := gp.client.Connect(node.NodeId, node.Address); err != nil {
					log.Debug().
						Err(err).
						Uint64("node_id", node.NodeId).
						Str("timestamp", time.Now().Format("15:04:05.000")).
						Msg("BOOT: Failed to connect to cluster node")
				} else {
					log.Debug().
						Uint64("local_node_id", gp.nodeID).
						Uint64("connected_to", node.NodeId).
						Str("timestamp", time.Now().Format("15:04:05.000")).
						Msg("BOOT: Connected to cluster node")
				}
			}
		}

		// Log final registry state
		allNodes := gp.registry.GetAll()
		log.Debug().
			Uint64("node_id", gp.nodeID).
			Int("total_nodes_in_registry", len(allNodes)).
			Str("timestamp", time.Now().Format("15:04:05.000")).
			Msg("BOOT: Join complete, final registry state")
		for _, n := range allNodes {
			log.Debug().
				Uint64("node_id", gp.nodeID).
				Uint64("registry_node", n.NodeId).
				Str("status", n.Status.String()).
				Str("address", n.Address).
				Msg("BOOT: Registry entry")
		}

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
