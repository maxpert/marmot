package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// Server implements the gRPC server for Marmot
type Server struct {
	UnimplementedMarmotServiceServer

	nodeID   uint64
	address  string
	port     int
	server   *grpc.Server
	listener net.Listener

	// Components
	gossip             *GossipProtocol
	registry           *NodeRegistry
	hasher             *ConsistentHash // NOTE: Not used for full database replication. Reserved for future multi-DB partitioning.
	replicationHandler *ReplicationHandler

	mu sync.RWMutex
}

// ServerConfig holds configuration for the gRPC server
type ServerConfig struct {
	NodeID  uint64
	Address string
	Port    int
}

// NewServer creates a new gRPC server
func NewServer(config ServerConfig) (*Server, error) {
	s := &Server{
		nodeID:  config.NodeID,
		address: config.Address,
		port:    config.Port,
	}

	// Initialize components
	s.registry = NewNodeRegistry(config.NodeID)
	// NOTE: Consistent hashing not currently used for full database replication
	// Reserved for future multi-database partitioning support
	s.hasher = NewConsistentHash(3, 150)
	s.gossip = NewGossipProtocol(config.NodeID, s.registry)

	return s, nil
}

// Start starts the gRPC server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.address, s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = listener
	s.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(100*1024*1024), // 100MB
		grpc.MaxSendMsgSize(100*1024*1024), // 100MB
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second, // Minimum time between client pings
			PermitWithoutStream: true,            // Allow pings even when no streams
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    60 * time.Second, // Ping client if no activity for 60s
			Timeout: 10 * time.Second, // Wait 10s for ping ack before closing connection
		}),
	)

	// Register service
	RegisterMarmotServiceServer(s.server, s)

	// Enable reflection for debugging
	reflection.Register(s.server)

	log.Info().
		Str("address", addr).
		Uint64("node_id", s.nodeID).
		Msg("Starting gRPC server")

	// Start in goroutine
	go func() {
		if err := s.server.Serve(listener); err != nil {
			log.Error().Err(err).Msg("gRPC server failed")
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server
func (s *Server) Stop() {
	if s.server != nil {
		log.Info().Msg("Stopping gRPC server")
		s.server.GracefulStop()
	}
}

// =======================
// GOSSIP PROTOCOL METHODS
// =======================

// Gossip handles gossip protocol messages
func (s *Server) Gossip(ctx context.Context, req *GossipRequest) (*GossipResponse, error) {
	log.Debug().
		Uint64("from_node", req.SourceNodeId).
		Int("nodes", len(req.Nodes)).
		Msg("Received gossip message")

	// Merge received node states
	for _, nodeState := range req.Nodes {
		s.registry.Update(nodeState)
	}

	// Return our view of the cluster
	nodes := s.registry.GetAll()
	return &GossipResponse{
		Nodes: nodes,
	}, nil
}

// Join handles node join requests
func (s *Server) Join(ctx context.Context, req *JoinRequest) (*JoinResponse, error) {
	log.Info().
		Uint64("node_id", req.NodeId).
		Str("address", req.Address).
		Msg("Node joining cluster")

	// Add node to registry
	nodeState := &NodeState{
		NodeId:      req.NodeId,
		Address:     req.Address,
		Status:      NodeStatus_ALIVE,
		Incarnation: 0,
	}
	s.registry.Add(nodeState)

	// Add to consistent hash
	s.hasher.AddNode(req.NodeId)

	// Notify gossip protocol to connect to new node
	if s.gossip != nil {
		s.gossip.OnNodeJoin(nodeState)
	}

	// Return current cluster state
	nodes := s.registry.GetAll()
	return &JoinResponse{
		Success:      true,
		ClusterNodes: nodes,
	}, nil
}

// Ping handles health check requests
func (s *Server) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	return &PingResponse{
		NodeId: s.nodeID,
		Status: NodeStatus_ALIVE,
	}, nil
}

// =======================
// REPLICATION METHODS (Stubs for now)
// =======================

// ReplicateTransaction handles transaction replication (Phase 5 - WIRED UP)
func (s *Server) ReplicateTransaction(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	s.mu.RLock()
	handler := s.replicationHandler
	s.mu.RUnlock()

	if handler == nil {
		log.Warn().Msg("ReplicateTransaction called but handler not initialized")
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "replication handler not initialized",
		}, nil
	}

	return handler.HandleReplicateTransaction(ctx, req)
}

// Read handles quorum read requests
// Future Phase: This will support distributed quorum reads across replicas.
// Currently reads are handled locally via coordinator.ReadCoordinator.
func (s *Server) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	log.Warn().Msg("Read RPC not yet implemented - use local reads via ReadCoordinator")
	return &ReadResponse{}, nil
}

// StreamChanges handles change streaming for catch-up
// Future Phase: This will stream transaction logs to enable fast catch-up for recovering nodes.
// Useful for bringing new replicas up to speed or recovering from network partitions.
func (s *Server) StreamChanges(req *StreamRequest, stream MarmotService_StreamChangesServer) error {
	log.Warn().Msg("StreamChanges not yet implemented - will support transaction log streaming")
	return nil
}

// =======================
// SNAPSHOT METHODS (Stubs for now)
// =======================

// GetSnapshotInfo returns snapshot metadata
// Future Phase: Returns metadata about available snapshots for bulk node bootstrap.
// Enables new nodes to request full database snapshots instead of replaying all transactions.
func (s *Server) GetSnapshotInfo(ctx context.Context, req *SnapshotInfoRequest) (*SnapshotInfoResponse, error) {
	log.Warn().Msg("GetSnapshotInfo not yet implemented - will support snapshot metadata queries")
	return &SnapshotInfoResponse{}, nil
}

// StreamSnapshot handles snapshot chunk streaming
// Future Phase: Streams database snapshot chunks to enable fast node bootstrap.
// Complements GetSnapshotInfo by providing the actual snapshot data transfer.
func (s *Server) StreamSnapshot(req *SnapshotRequest, stream MarmotService_StreamSnapshotServer) error {
	log.Warn().Msg("StreamSnapshot not yet implemented - will support snapshot data streaming")
	return nil
}

// GetNodeRegistry returns the node registry (for testing/debugging)
func (s *Server) GetNodeRegistry() *NodeRegistry {
	return s.registry
}

// GetConsistentHash returns the consistent hash (for testing/debugging)
func (s *Server) GetConsistentHash() *ConsistentHash {
	return s.hasher
}

// GetGossipProtocol returns the gossip protocol instance
func (s *Server) GetGossipProtocol() *GossipProtocol {
	return s.gossip
}

// SetReplicationHandler sets the replication handler for transaction processing
func (s *Server) SetReplicationHandler(handler *ReplicationHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationHandler = handler
}
