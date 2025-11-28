package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	grpcpool "github.com/processout/grpc-go-pool"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Client manages gRPC connection pools to peer nodes
type Client struct {
	nodeID uint64
	pools  map[uint64]*grpcpool.Pool
	addrs  map[uint64]string // Track addresses for reconnection
	mu     sync.RWMutex
}

// NewClient creates a new gRPC client manager
func NewClient(nodeID uint64) *Client {
	return &Client{
		nodeID: nodeID,
		pools:  make(map[uint64]*grpcpool.Pool),
		addrs:  make(map[uint64]string),
	}
}

// getPoolConfig returns pool configuration from config or defaults
func getPoolConfig() (poolSize int, idleTimeout, maxLifetime time.Duration) {
	poolSize = 4
	idleTimeout = 60 * time.Second
	maxLifetime = 3600 * time.Second // 1 hour

	if cfg.Config != nil {
		if cfg.Config.GRPCClient.ConnectionPoolSize > 0 {
			poolSize = cfg.Config.GRPCClient.ConnectionPoolSize
		}
		if cfg.Config.GRPCClient.PoolIdleTimeoutSeconds > 0 {
			idleTimeout = time.Duration(cfg.Config.GRPCClient.PoolIdleTimeoutSeconds) * time.Second
		}
		if cfg.Config.GRPCClient.PoolMaxLifetimeSeconds > 0 {
			maxLifetime = time.Duration(cfg.Config.GRPCClient.PoolMaxLifetimeSeconds) * time.Second
		}
	}
	return
}

// createDialOptions returns common gRPC dial options
func createDialOptions() []grpc.DialOption {
	keepaliveTime := 10 * time.Second
	keepaliveTimeout := 3 * time.Second
	if cfg.Config != nil {
		keepaliveTime = time.Duration(cfg.Config.GRPCClient.KeepaliveTimeSeconds) * time.Second
		keepaliveTimeout = time.Duration(cfg.Config.GRPCClient.KeepaliveTimeoutSeconds) * time.Second
	}

	return []grpc.DialOption{
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
		grpc.WithChainUnaryInterceptor(UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(StreamClientInterceptor()),
	}
}

// Connect establishes a connection pool to a peer node
// Idempotent: safe to call multiple times for the same node
func (c *Client) Connect(nodeID uint64, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Idempotent: return success if already connected
	if _, exists := c.pools[nodeID]; exists {
		log.Debug().
			Uint64("node_id", nodeID).
			Msg("Pool already exists, skipping")
		return nil
	}

	log.Debug().
		Uint64("node_id", nodeID).
		Str("address", address).
		Msg("Creating connection pool")

	poolSize, idleTimeout, maxLifetime := getPoolConfig()
	dialOpts := createDialOptions()

	// Factory function for creating connections
	factory := func() (*grpc.ClientConn, error) {
		return grpc.NewClient(address, dialOpts...)
	}

	// Create pool with init=1 (start with 1 connection), capacity=poolSize
	pool, err := grpcpool.New(factory, 1, poolSize, idleTimeout, maxLifetime)
	if err != nil {
		return fmt.Errorf("failed to create connection pool for node %d: %w", nodeID, err)
	}

	c.pools[nodeID] = pool
	c.addrs[nodeID] = address

	log.Info().
		Uint64("node_id", nodeID).
		Str("address", address).
		Int("pool_size", poolSize).
		Msg("Connection pool created")

	return nil
}

// getConn gets a connection from the pool for a node
func (c *Client) getConn(nodeID uint64) (*grpcpool.ClientConn, error) {
	c.mu.RLock()
	pool, exists := c.pools[nodeID]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("not connected to node %d", nodeID)
	}

	conn, err := pool.Get(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get connection from pool for node %d: %w", nodeID, err)
	}

	return conn, nil
}

// GetClient returns a MarmotServiceClient for a node
// IMPORTANT: Caller must call ReleaseClient when done to return connection to pool
func (c *Client) GetClient(nodeID uint64) (MarmotServiceClient, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}
	// Note: This creates a lightweight wrapper, actual connection is managed by pool
	// The conn will be returned to pool when the caller calls ReleaseClient
	return NewMarmotServiceClient(conn), nil
}

// withPooledClient executes a function with a pooled connection
// The connection is automatically returned to the pool after the function completes
func (c *Client) withPooledClient(nodeID uint64, fn func(MarmotServiceClient) error) error {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return err
	}
	defer conn.Close() // Returns connection to pool

	client := NewMarmotServiceClient(conn)
	return fn(client)
}

// Disconnect closes the connection pool for a peer node
func (c *Client) Disconnect(nodeID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	pool, exists := c.pools[nodeID]
	if !exists {
		return nil
	}

	log.Debug().Uint64("node_id", nodeID).Msg("Closing connection pool")

	pool.Close()
	delete(c.pools, nodeID)
	delete(c.addrs, nodeID)

	return nil
}

// Close closes all connection pools
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for nodeID, pool := range c.pools {
		log.Debug().Uint64("node_id", nodeID).Msg("Closing connection pool")
		pool.Close()
	}

	c.pools = make(map[uint64]*grpcpool.Pool)
	c.addrs = make(map[uint64]string)

	return nil
}

// SendGossip sends a gossip message to a peer
func (c *Client) SendGossip(ctx context.Context, nodeID uint64, req *GossipRequest) (*GossipResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	return client.Gossip(ctx, req)
}

// SendJoin sends a join request to a peer
func (c *Client) SendJoin(ctx context.Context, nodeID uint64, req *JoinRequest) (*JoinResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	return client.Join(ctx, req)
}

// SendPing sends a ping to a peer
func (c *Client) SendPing(ctx context.Context, nodeID uint64) (*PingResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	req := &PingRequest{
		SourceNodeId: c.nodeID,
	}
	return client.Ping(ctx, req)
}

// ReplicateTransaction sends a transaction to a peer for replication
func (c *Client) ReplicateTransaction(ctx context.Context, nodeID uint64, req *TransactionRequest) (*TransactionResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	return client.ReplicateTransaction(ctx, req)
}

// Read sends a read request to a peer
func (c *Client) Read(ctx context.Context, nodeID uint64, req *ReadRequest) (*ReadResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	return client.Read(ctx, req)
}

// GetClientByAddress returns a MarmotServiceClient for an address
// If not already connected, it creates a connection pool using a temporary node ID
func (c *Client) GetClientByAddress(address string) (MarmotServiceClient, error) {
	c.mu.RLock()
	// Check if we already have a pool for this address
	for nodeID, addr := range c.addrs {
		if addr == address {
			c.mu.RUnlock()
			return c.GetClient(nodeID)
		}
	}
	c.mu.RUnlock()

	// No existing pool, create one with temporary node ID
	// Use a hash of the address as a pseudo node ID
	tempNodeID := uint64(0)
	for _, b := range []byte(address) {
		tempNodeID = tempNodeID*31 + uint64(b)
	}

	if err := c.Connect(tempNodeID, address); err != nil {
		return nil, err
	}

	return c.GetClient(tempNodeID)
}

// PoolStats returns statistics about connection pools
func (c *Client) PoolStats() map[uint64]PoolStat {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := make(map[uint64]PoolStat)
	for nodeID, pool := range c.pools {
		stats[nodeID] = PoolStat{
			Available: pool.Available(),
			Capacity:  pool.Capacity(),
		}
	}
	return stats
}

// PoolStat holds statistics for a connection pool
type PoolStat struct {
	Available int
	Capacity  int
}

// RegisterTestConnection registers a pre-existing connection for testing
// This bypasses the normal pool mechanism and stores a single connection
func (c *Client) RegisterTestConnection(nodeID uint64, conn *grpc.ClientConn) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a minimal pool with just this connection
	factory := func() (*grpc.ClientConn, error) {
		return conn, nil
	}

	pool, err := grpcpool.New(factory, 1, 1, time.Hour, time.Hour)
	if err != nil {
		return
	}

	c.pools[nodeID] = pool
}
