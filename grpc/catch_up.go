package grpc

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CatchUpClient handles the client-side of node catch-up
type CatchUpClient struct {
	nodeID    uint64
	dataDir   string
	registry  *NodeRegistry
	seedAddrs []string
}

// NewCatchUpClient creates a new catch-up client
func NewCatchUpClient(nodeID uint64, dataDir string, registry *NodeRegistry, seedAddrs []string) *CatchUpClient {
	return &CatchUpClient{
		nodeID:    nodeID,
		dataDir:   dataDir,
		registry:  registry,
		seedAddrs: seedAddrs,
	}
}

// CatchUp performs the full catch-up process
// Returns the snapshot txn_id after which normal replication can begin
func (c *CatchUpClient) CatchUp(ctx context.Context) (uint64, error) {
	// Mark ourselves as JOINING
	c.registry.MarkJoining(c.nodeID)

	log.Info().
		Uint64("node_id", c.nodeID).
		Msg("Starting catch-up process")

	// Find a seed node to catch up from
	seedAddr, err := c.findAvailableSeed(ctx)
	if err != nil {
		return 0, fmt.Errorf("no available seed node: %w", err)
	}

	log.Info().Str("seed", seedAddr).Msg("Catching up from seed node")

	// Connect to seed
	conn, err := grpc.DialContext(ctx, seedAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to seed: %w", err)
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)

	// Step 1: Get snapshot info
	snapshotInfo, err := client.GetSnapshotInfo(ctx, &SnapshotInfoRequest{
		RequestingNodeId: c.nodeID,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get snapshot info: %w", err)
	}

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Int64("size_bytes", snapshotInfo.SnapshotSizeBytes).
		Int32("total_chunks", snapshotInfo.TotalChunks).
		Int("databases", len(snapshotInfo.Databases)).
		Msg("Received snapshot info")

	// Step 2: Stream and apply snapshot
	if err := c.applySnapshot(ctx, client, snapshotInfo); err != nil {
		return 0, fmt.Errorf("failed to apply snapshot: %w", err)
	}

	// Step 3: Apply delta changes (transactions after snapshot)
	// Note: For full snapshot-based sync, this may not be needed immediately
	// The snapshot itself should be consistent

	log.Info().
		Uint64("snapshot_txn_id", snapshotInfo.SnapshotTxnId).
		Msg("Catch-up completed successfully")

	// Mark ourselves as ALIVE
	c.registry.MarkAlive(c.nodeID)

	return snapshotInfo.SnapshotTxnId, nil
}

// findAvailableSeed finds an available seed node to catch up from
func (c *CatchUpClient) findAvailableSeed(ctx context.Context) (string, error) {
	// Try configured seed addresses first
	for _, addr := range c.seedAddrs {
		if c.checkNodeAvailable(ctx, addr) {
			return addr, nil
		}
	}

	// Try nodes from registry
	for _, node := range c.registry.GetAlive() {
		if node.NodeId == c.nodeID {
			continue // Skip self
		}
		if c.checkNodeAvailable(ctx, node.Address) {
			return node.Address, nil
		}
	}

	return "", fmt.Errorf("no available seed nodes")
}

// checkNodeAvailable checks if a node is available for catch-up
func (c *CatchUpClient) checkNodeAvailable(ctx context.Context, addr string) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := NewMarmotServiceClient(conn)
	_, err = client.Ping(ctx, &PingRequest{SourceNodeId: c.nodeID})
	return err == nil
}

// applySnapshot downloads and applies a snapshot from the seed node
func (c *CatchUpClient) applySnapshot(ctx context.Context, client MarmotServiceClient, info *SnapshotInfoResponse) error {
	log.Info().Msg("Downloading snapshot")

	// Create data directory structure
	if err := os.MkdirAll(c.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(c.dataDir, "databases"), 0755); err != nil {
		return fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Stream snapshot
	stream, err := client.StreamSnapshot(ctx, &SnapshotRequest{
		RequestingNodeId: c.nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to start snapshot stream: %w", err)
	}

	// Track open files for each database
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	chunksReceived := 0
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk: %w", err)
		}

		// Verify checksum
		actualChecksum := fmt.Sprintf("%x", md5.Sum(chunk.Data))
		if actualChecksum != chunk.Checksum {
			return fmt.Errorf("checksum mismatch for %s chunk %d", chunk.Filename, chunk.ChunkIndex)
		}

		// Get or create file
		file, exists := openFiles[chunk.Filename]
		if !exists {
			filePath := filepath.Join(c.dataDir, chunk.Filename)

			// Remove existing file if present
			os.Remove(filePath)
			os.Remove(filePath + "-wal")
			os.Remove(filePath + "-shm")

			file, err = os.Create(filePath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", filePath, err)
			}
			openFiles[chunk.Filename] = file
		}

		// Write chunk
		if _, err := file.Write(chunk.Data); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}

		chunksReceived++

		// Close file if this is the last chunk
		if chunk.IsLastForFile {
			file.Close()
			delete(openFiles, chunk.Filename)
			log.Debug().Str("file", chunk.Filename).Msg("Finished receiving file")
		}

		// Progress logging
		if chunksReceived%100 == 0 {
			log.Info().
				Int("chunks_received", chunksReceived).
				Int32("total_chunks", chunk.TotalChunks).
				Msg("Snapshot download progress")
		}
	}

	log.Info().
		Int("chunks_received", chunksReceived).
		Msg("Snapshot download completed")

	return nil
}

// NeedsCatchUp checks if this node needs to catch up (e.g., empty data directory)
func (c *CatchUpClient) NeedsCatchUp() bool {
	// Check if system database exists
	systemDBPath := filepath.Join(c.dataDir, "__marmot_system.db")
	if _, err := os.Stat(systemDBPath); os.IsNotExist(err) {
		return true
	}

	// Check if default database exists
	defaultDBPath := filepath.Join(c.dataDir, "databases", "marmot.db")
	if _, err := os.Stat(defaultDBPath); os.IsNotExist(err) {
		return true
	}

	return false
}
