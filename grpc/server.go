package grpc

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const (
	// SnapshotChunkSize is the size of each snapshot chunk (4MB)
	SnapshotChunkSize = 4 * 1024 * 1024
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
	replicationHandler *ReplicationHandler
	dbManager          *db.DatabaseManager

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
// Streams committed transactions from a given txn_id for delta sync
func (s *Server) StreamChanges(req *StreamRequest, stream MarmotService_StreamChangesServer) error {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		return fmt.Errorf("database manager not initialized")
	}

	log.Info().
		Uint64("from_txn_id", req.FromTxnId).
		Uint64("requesting_node", req.RequestingNodeId).
		Str("database", req.Database).
		Msg("Starting change stream")

	// Get databases to stream from
	var databases []string
	if req.Database != "" {
		databases = []string{req.Database}
	} else {
		databases = dbManager.ListDatabases()
	}

	// Stream changes from each database
	for _, dbName := range databases {
		mdb, err := dbManager.GetDatabase(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to get database for streaming")
			continue
		}

		// Query committed transactions after from_txn_id with statements
		rows, err := mdb.GetDB().Query(`
			SELECT txn_id, commit_ts_wall, commit_ts_logical,
			       COALESCE(statements_json, '[]'), COALESCE(database_name, '')
			FROM __marmot__txn_records
			WHERE status = 'COMMITTED' AND txn_id > ?
			ORDER BY txn_id
		`, req.FromTxnId)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to query transactions")
			continue
		}

		for rows.Next() {
			var txnID uint64
			var commitWall int64
			var commitLogical int32
			var statementsJSON string
			var databaseName string

			if err := rows.Scan(&txnID, &commitWall, &commitLogical, &statementsJSON, &databaseName); err != nil {
				log.Warn().Err(err).Msg("Failed to scan transaction")
				continue
			}

			// Parse statements from JSON
			var statements []*Statement
			if statementsJSON != "" && statementsJSON != "[]" {
				var rawStatements []struct {
					SQL       string `json:"SQL"`
					Type      int    `json:"Type"`
					TableName string `json:"TableName"`
					Database  string `json:"Database"`
				}
				if err := json.Unmarshal([]byte(statementsJSON), &rawStatements); err != nil {
					log.Warn().Err(err).Uint64("txn_id", txnID).Msg("Failed to parse statements JSON")
				} else {
					for _, s := range rawStatements {
						statements = append(statements, &Statement{
							Sql:       s.SQL,
							TableName: s.TableName,
							Database:  s.Database,
						})
					}
				}
			}

			event := &ChangeEvent{
				TxnId: txnID,
				Timestamp: &HLC{
					WallTime: commitWall,
					Logical:  commitLogical,
				},
				Statements: statements,
				Database:   databaseName,
			}

			if err := stream.Send(event); err != nil {
				rows.Close()
				return fmt.Errorf("failed to send change event: %w", err)
			}
		}
		rows.Close()
	}

	log.Info().
		Uint64("from_txn_id", req.FromTxnId).
		Msg("Change stream completed")

	return nil
}

// GetReplicationState returns current replication state for anti-entropy
// Returns per-database replication progress for the requesting peer
func (s *Server) GetReplicationState(ctx context.Context, req *ReplicationStateRequest) (*ReplicationStateResponse, error) {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	log.Debug().
		Uint64("requesting_node", req.RequestingNodeId).
		Str("database_filter", req.Database).
		Msg("Replication state requested")

	var states []*DatabaseReplicationState

	// Get list of databases to query
	var databases []string
	if req.Database != "" {
		// Specific database requested
		databases = []string{req.Database}
	} else {
		// All databases
		databases = dbManager.ListDatabases()
	}

	// Query replication state for each database
	for _, dbName := range databases {
		// Get last applied txn_id from replication_state table for this peer
		repState, err := dbManager.GetReplicationState(req.RequestingNodeId, dbName)
		var lastAppliedTxnID uint64
		var lastAppliedTS *HLC
		var lastSyncTime int64
		var syncStatus string

		if err != nil {
			// No replication state yet for this peer/database - use defaults
			lastAppliedTxnID = 0
			lastAppliedTS = &HLC{WallTime: 0, Logical: 0, NodeId: s.nodeID}
			lastSyncTime = 0
			syncStatus = "SYNCED"
		} else {
			lastAppliedTxnID = repState.LastAppliedTxnID
			lastAppliedTS = &HLC{
				WallTime: repState.LastAppliedTSWall,
				Logical:  repState.LastAppliedTSLog,
				NodeId:   s.nodeID,
			}
			lastSyncTime = repState.LastSyncTime
			syncStatus = repState.SyncStatus
		}

		// Get current max txn_id in this database
		maxTxnID, err := dbManager.GetMaxTxnID(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to get max txn_id")
			maxTxnID = 0
		}

		states = append(states, &DatabaseReplicationState{
			DatabaseName:         dbName,
			LastAppliedTxnId:     lastAppliedTxnID,
			LastAppliedTimestamp: lastAppliedTS,
			LastSyncTime:         lastSyncTime,
			SyncStatus:           syncStatus,
			CurrentMaxTxnId:      maxTxnID,
		})
	}

	return &ReplicationStateResponse{
		States: states,
	}, nil
}

// =======================
// SNAPSHOT METHODS
// =======================

// GetSnapshotInfo returns snapshot metadata for bootstrap
func (s *Server) GetSnapshotInfo(ctx context.Context, req *SnapshotInfoRequest) (*SnapshotInfoResponse, error) {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	log.Info().
		Uint64("requesting_node", req.RequestingNodeId).
		Msg("Snapshot info requested")

	// Take snapshot (checkpoints all databases)
	snapshots, maxTxnID, err := dbManager.TakeSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to take snapshot: %w", err)
	}

	// Calculate total size and chunks
	var totalSize int64
	var dbInfos []*DatabaseFileInfo
	for _, snap := range snapshots {
		totalSize += snap.Size
		dbInfos = append(dbInfos, &DatabaseFileInfo{
			Name:      snap.Name,
			Filename:  snap.Filename,
			SizeBytes: snap.Size,
		})
	}

	totalChunks := int32((totalSize + SnapshotChunkSize - 1) / SnapshotChunkSize)

	return &SnapshotInfoResponse{
		SnapshotTxnId:     maxTxnID,
		SnapshotSizeBytes: totalSize,
		TotalChunks:       totalChunks,
		Databases:         dbInfos,
	}, nil
}

// StreamSnapshot streams snapshot chunks to requesting node
func (s *Server) StreamSnapshot(req *SnapshotRequest, stream MarmotService_StreamSnapshotServer) error {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		return fmt.Errorf("database manager not initialized")
	}

	log.Info().
		Uint64("requesting_node", req.RequestingNodeId).
		Msg("Starting snapshot stream")

	// Take snapshot
	snapshots, _, err := dbManager.TakeSnapshot()
	if err != nil {
		return fmt.Errorf("failed to take snapshot: %w", err)
	}

	// Calculate total chunks across all files
	var totalChunks int32
	for _, snap := range snapshots {
		fileChunks := int32((snap.Size + SnapshotChunkSize - 1) / SnapshotChunkSize)
		if fileChunks == 0 {
			fileChunks = 1 // At least one chunk for empty files
		}
		totalChunks += fileChunks
	}

	chunkIndex := int32(0)

	// Stream each database file
	for _, snap := range snapshots {
		file, err := os.Open(snap.FullPath)
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", snap.Filename, err)
		}

		buf := make([]byte, SnapshotChunkSize)
		for {
			n, err := file.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return fmt.Errorf("failed to read %s: %w", snap.Filename, err)
			}

			// Calculate checksum
			checksum := fmt.Sprintf("%x", md5.Sum(buf[:n]))

			// Check if this is the last chunk for this file
			pos, _ := file.Seek(0, io.SeekCurrent)
			info, _ := file.Stat()
			isLastForFile := pos >= info.Size()

			chunk := &SnapshotChunk{
				ChunkIndex:    chunkIndex,
				TotalChunks:   totalChunks,
				Data:          buf[:n],
				Checksum:      checksum,
				Filename:      snap.Filename,
				IsLastForFile: isLastForFile,
			}

			if err := stream.Send(chunk); err != nil {
				file.Close()
				return fmt.Errorf("failed to send chunk: %w", err)
			}

			chunkIndex++
		}

		file.Close()
		log.Debug().Str("file", snap.Filename).Msg("Finished streaming file")
	}

	log.Info().
		Uint64("requesting_node", req.RequestingNodeId).
		Int32("total_chunks", totalChunks).
		Msg("Snapshot stream completed")

	return nil
}

// GetNodeRegistry returns the node registry (for testing/debugging)
func (s *Server) GetNodeRegistry() *NodeRegistry {
	return s.registry
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

// SetDatabaseManager sets the database manager for snapshot operations
func (s *Server) SetDatabaseManager(manager *db.DatabaseManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dbManager = manager
}
