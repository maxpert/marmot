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

// Client manages gRPC connections to peer nodes.
// Uses a single shared *grpc.ClientConn per peer - gRPC/HTTP2 handles multiplexing internally.
type Client struct {
	nodeID uint64
	conns  map[uint64]*grpc.ClientConn
	addrs  map[uint64]string
	mu     sync.RWMutex
}

// NewClient creates a new gRPC client manager
func NewClient(nodeID uint64) *Client {
	return &Client{
		nodeID: nodeID,
		conns:  make(map[uint64]*grpc.ClientConn),
		addrs:  make(map[uint64]string),
	}
}

// createDialOptions returns common gRPC dial options
func createDialOptions() []grpc.DialOption {
	keepaliveTime := 10 * time.Second
	keepaliveTimeout := 3 * time.Second
	if cfg.Config != nil {
		keepaliveTime = time.Duration(cfg.Config.GRPCClient.KeepaliveTimeSeconds) * time.Second
		keepaliveTimeout = time.Duration(cfg.Config.GRPCClient.KeepaliveTimeoutSeconds) * time.Second
	}

	// Build call options with compression if enabled
	callOpts := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(100 * 1024 * 1024), // 100MB
		grpc.MaxCallSendMsgSize(100 * 1024 * 1024),
	}
	if compressor := GetCompressionName(); compressor != "" {
		callOpts = append(callOpts, grpc.UseCompressor(compressor))
	}

	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(callOpts...),
		grpc.WithChainUnaryInterceptor(UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(StreamClientInterceptor()),
	}
}

// Connect establishes a connection to a peer node
// Idempotent: safe to call multiple times for the same node
func (c *Client) Connect(nodeID uint64, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Idempotent: return success if already connected
	if _, exists := c.conns[nodeID]; exists {
		log.Debug().
			Uint64("node_id", nodeID).
			Msg("Connection already exists, skipping")
		return nil
	}

	log.Debug().
		Uint64("node_id", nodeID).
		Str("address", address).
		Msg("Creating gRPC connection")

	dialOpts := createDialOptions()
	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to create connection for node %d: %w", nodeID, err)
	}

	c.conns[nodeID] = conn
	c.addrs[nodeID] = address

	log.Info().
		Uint64("node_id", nodeID).
		Str("address", address).
		Msg("gRPC connection created")

	return nil
}

// getConn gets the shared connection for a node
func (c *Client) getConn(nodeID uint64) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn, exists := c.conns[nodeID]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("not connected to node %d", nodeID)
	}

	return conn, nil
}

// GetClient returns a MarmotServiceClient for a node
func (c *Client) GetClient(nodeID uint64) (MarmotServiceClient, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}
	return NewMarmotServiceClient(conn), nil
}

// Disconnect closes the connection for a peer node
func (c *Client) Disconnect(nodeID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, exists := c.conns[nodeID]
	if !exists {
		return nil
	}

	log.Debug().Uint64("node_id", nodeID).Msg("Closing gRPC connection")

	conn.Close()
	delete(c.conns, nodeID)
	delete(c.addrs, nodeID)

	return nil
}

// Close closes all connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for nodeID, conn := range c.conns {
		log.Debug().Uint64("node_id", nodeID).Msg("Closing gRPC connection")
		conn.Close()
	}

	c.conns = make(map[uint64]*grpc.ClientConn)
	c.addrs = make(map[uint64]string)

	return nil
}

// SendGossip sends a gossip message to a peer
func (c *Client) SendGossip(ctx context.Context, nodeID uint64, req *GossipRequest) (*GossipResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	client := NewMarmotServiceClient(conn)
	return client.Gossip(ctx, req)
}

// SendJoin sends a join request to a peer
func (c *Client) SendJoin(ctx context.Context, nodeID uint64, req *JoinRequest) (*JoinResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	client := NewMarmotServiceClient(conn)
	return client.Join(ctx, req)
}

// SendPing sends a ping to a peer
func (c *Client) SendPing(ctx context.Context, nodeID uint64) (*PingResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}

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

	client := NewMarmotServiceClient(conn)
	return client.ReplicateTransaction(ctx, req)
}

// Read sends a read request to a peer
func (c *Client) Read(ctx context.Context, nodeID uint64, req *ReadRequest) (*ReadResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	client := NewMarmotServiceClient(conn)
	return client.Read(ctx, req)
}

// GetClientByAddress returns a MarmotServiceClient for an address
// If not already connected, it creates a connection using a temporary node ID
func (c *Client) GetClientByAddress(address string) (MarmotServiceClient, error) {
	c.mu.RLock()
	// Check if we already have a connection for this address
	for nodeID, addr := range c.addrs {
		if addr == address {
			c.mu.RUnlock()
			return c.GetClient(nodeID)
		}
	}
	c.mu.RUnlock()

	// No existing connection, create one with temporary node ID
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

// RegisterTestConnection registers a pre-existing connection for testing
func (c *Client) RegisterTestConnection(nodeID uint64, conn *grpc.ClientConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conns[nodeID] = conn
}

// estimateStatementSize estimates the byte size of a statement's CDC payload
func estimateStatementSize(stmt *Statement) int {
	size := len(stmt.TableName)
	if rc := stmt.GetRowChange(); rc != nil {
		size += len(rc.IntentKey)
		for _, v := range rc.OldValues {
			size += len(v)
		}
		for _, v := range rc.NewValues {
			size += len(v)
		}
	}
	return size
}

// TransactionStream sends a large transaction using client-streaming RPC.
// Chunks statements by size and sends them followed by a commit message.
// chunkSize specifies the maximum byte size for each chunk.
func (c *Client) TransactionStream(
	ctx context.Context,
	nodeID uint64,
	txnID uint64,
	database string,
	statements []*Statement,
	chunkSize int,
	timestamp *HLC,
	sourceNodeID uint64,
) (*TransactionResponse, error) {
	conn, err := c.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	client := NewMarmotServiceClient(conn)
	stream, err := client.TransactionStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction stream: %w", err)
	}

	// Send statements in size-based chunks
	chunkIndex := uint32(0)
	i := 0
	for i < len(statements) {
		// Accumulate statements until size would exceed chunkSize
		currentSize := 0
		chunkStart := i
		for i < len(statements) {
			stmtSize := estimateStatementSize(statements[i])
			// If single statement exceeds chunkSize, send it as its own chunk
			if currentSize > 0 && currentSize+stmtSize > chunkSize {
				break
			}
			currentSize += stmtSize
			i++
		}

		chunkMsg := &TransactionStreamMessage{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      txnID,
					Database:   database,
					Statements: statements[chunkStart:i],
					ChunkIndex: chunkIndex,
				},
			},
		}

		if err := stream.Send(chunkMsg); err != nil {
			return nil, fmt.Errorf("failed to send chunk %d: %w", chunkIndex, err)
		}
		chunkIndex++
	}

	// Send commit message
	commitMsg := &TransactionStreamMessage{
		Payload: &TransactionStreamMessage_Commit{
			Commit: &TransactionCommit{
				TxnId:        txnID,
				Database:     database,
				Timestamp:    timestamp,
				SourceNodeId: sourceNodeID,
			},
		},
	}

	if err := stream.Send(commitMsg); err != nil {
		return nil, fmt.Errorf("failed to send commit: %w", err)
	}

	// Close and receive response
	return stream.CloseAndRecv()
}
