package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Client manages gRPC connections to peer nodes
type Client struct {
	nodeID      uint64
	connections map[uint64]*grpc.ClientConn
	clients     map[uint64]MarmotServiceClient
	mu          sync.RWMutex
}

// NewClient creates a new gRPC client manager
func NewClient(nodeID uint64) *Client {
	return &Client{
		nodeID:      nodeID,
		connections: make(map[uint64]*grpc.ClientConn),
		clients:     make(map[uint64]MarmotServiceClient),
	}
}

// Connect establishes a connection to a peer node
func (c *Client) Connect(nodeID uint64, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already connected
	if _, exists := c.connections[nodeID]; exists {
		return nil
	}

	log.Debug().
		Uint64("node_id", nodeID).
		Str("address", address).
		Msg("Connecting to peer")

	// Get keepalive settings from config
	keepaliveTime := 10 * time.Second
	keepaliveTimeout := 3 * time.Second
	if cfg.Config != nil {
		keepaliveTime = time.Duration(cfg.Config.GRPCClient.KeepaliveTimeSeconds) * time.Second
		keepaliveTimeout = time.Duration(cfg.Config.GRPCClient.KeepaliveTimeoutSeconds) * time.Second
	}

	// Create connection with keepalive
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to node %d: %w", nodeID, err)
	}

	c.connections[nodeID] = conn
	c.clients[nodeID] = NewMarmotServiceClient(conn)

	log.Info().
		Uint64("node_id", nodeID).
		Str("address", address).
		Msg("Connected to peer")

	return nil
}

// GetClient returns a MarmotServiceClient for a node
func (c *Client) GetClient(nodeID uint64) (MarmotServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	client, exists := c.clients[nodeID]
	if !exists {
		return nil, fmt.Errorf("not connected to node %d", nodeID)
	}

	return client, nil
}

// Disconnect closes the connection to a peer node
func (c *Client) Disconnect(nodeID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, exists := c.connections[nodeID]
	if !exists {
		return nil
	}

	log.Debug().Uint64("node_id", nodeID).Msg("Disconnecting from peer")

	if err := conn.Close(); err != nil {
		return err
	}

	delete(c.connections, nodeID)
	delete(c.clients, nodeID)

	return nil
}

// Close closes all connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for nodeID, conn := range c.connections {
		log.Debug().Uint64("node_id", nodeID).Msg("Closing connection")
		if err := conn.Close(); err != nil {
			log.Error().Err(err).Uint64("node_id", nodeID).Msg("Failed to close connection")
		}
	}

	c.connections = make(map[uint64]*grpc.ClientConn)
	c.clients = make(map[uint64]MarmotServiceClient)

	return nil
}

// SendGossip sends a gossip message to a peer
func (c *Client) SendGossip(ctx context.Context, nodeID uint64, req *GossipRequest) (*GossipResponse, error) {
	client, err := c.GetClient(nodeID)
	if err != nil {
		return nil, err
	}

	return client.Gossip(ctx, req)
}

// SendJoin sends a join request to a peer
func (c *Client) SendJoin(ctx context.Context, nodeID uint64, req *JoinRequest) (*JoinResponse, error) {
	client, err := c.GetClient(nodeID)
	if err != nil {
		return nil, err
	}

	return client.Join(ctx, req)
}

// SendPing sends a ping to a peer
func (c *Client) SendPing(ctx context.Context, nodeID uint64) (*PingResponse, error) {
	client, err := c.GetClient(nodeID)
	if err != nil {
		return nil, err
	}

	req := &PingRequest{
		SourceNodeId: c.nodeID,
	}

	return client.Ping(ctx, req)
}

// ReplicateTransaction sends a transaction to a peer for replication
func (c *Client) ReplicateTransaction(ctx context.Context, nodeID uint64, req *TransactionRequest) (*TransactionResponse, error) {
	client, err := c.GetClient(nodeID)
	if err != nil {
		return nil, err
	}

	return client.ReplicateTransaction(ctx, req)
}

// Read sends a read request to a peer
func (c *Client) Read(ctx context.Context, nodeID uint64, req *ReadRequest) (*ReadResponse, error) {
	client, err := c.GetClient(nodeID)
	if err != nil {
		return nil, err
	}

	return client.Read(ctx, req)
}
