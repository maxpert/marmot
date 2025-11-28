package grpc

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
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
	mux      cmux.CMux

	// Components
	gossip             *GossipProtocol
	registry           *NodeRegistry
	replicationHandler *ReplicationHandler
	dbManager          *db.DatabaseManager
	metricsHandler     http.Handler

	mu sync.RWMutex
}

// ServerConfig holds configuration for the gRPC server
type ServerConfig struct {
	NodeID           uint64
	Address          string
	Port             int
	AdvertiseAddress string
}

// NewServer creates a new gRPC server
func NewServer(config ServerConfig) (*Server, error) {
	s := &Server{
		nodeID:  config.NodeID,
		address: config.Address,
		port:    config.Port,
	}

	// Initialize components with advertise address
	s.registry = NewNodeRegistry(config.NodeID, config.AdvertiseAddress)
	s.gossip = NewGossipProtocol(config.NodeID, s.registry)

	// Set up callback to connect to nodes when they become ALIVE
	s.registry.SetOnNodeAlive(func(node *NodeState) {
		// Defensive checks
		if node == nil {
			log.Error().Msg("BUG: OnNodeAlive called with nil node")
			return
		}
		if node.Address == "" {
			log.Warn().
				Uint64("node_id", node.NodeId).
				Msg("Skipping connection - node has no address")
			return
		}
		if s.gossip == nil {
			log.Warn().Msg("Gossip protocol not initialized, skipping connection")
			return
		}

		log.Info().
			Uint64("node_id", node.NodeId).
			Str("address", node.Address).
			Msg("Node became ALIVE, establishing connection")
		s.gossip.OnNodeJoin(node)
	})

	// Set up callback to disconnect from nodes when they become DEAD
	s.registry.SetOnNodeDead(func(node *NodeState) {
		// Defensive checks
		if node == nil {
			log.Error().Msg("BUG: OnNodeDead called with nil node")
			return
		}
		if s.gossip == nil {
			log.Warn().Msg("Gossip protocol not initialized, skipping disconnect")
			return
		}

		log.Info().
			Uint64("node_id", node.NodeId).
			Msg("Node became DEAD, closing connection")
		s.gossip.GetClient().Disconnect(node.NodeId)
	})

	return s, nil
}

// Start starts the gRPC server (and optional HTTP metrics on same port)
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
		grpc.ChainUnaryInterceptor(UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(StreamServerInterceptor()),
	)

	// Register service
	RegisterMarmotServiceServer(s.server, s)

	// Enable reflection for debugging
	reflection.Register(s.server)

	log.Info().
		Str("address", addr).
		Uint64("node_id", s.nodeID).
		Msg("Starting gRPC server")

	// Always use cmux to multiplex HTTP (pprof + optional metrics) and gRPC on same port
	log.Info().Msg("Multiplexing HTTP (pprof) and gRPC on same port")
	s.mux = cmux.New(listener)

	// Match HTTP requests (for /debug/pprof and optionally /metrics)
	httpListener := s.mux.Match(cmux.HTTP1Fast())

	// Match gRPC requests (everything else)
	grpcListener := s.mux.Match(cmux.Any())

	// Start HTTP server for pprof (and optionally metrics)
	httpMux := http.NewServeMux()

	// Register pprof handlers for profiling
	httpMux.HandleFunc("/debug/pprof/", pprof.Index)
	httpMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	httpMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	httpMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	httpMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Optionally add metrics handler
	if s.metricsHandler != nil {
		httpMux.Handle("/metrics", s.metricsHandler)
		log.Info().Msg("Metrics endpoint enabled at /metrics")
	}

	httpServer := &http.Server{
		Handler: httpMux,
	}

	go func() {
		if err := httpServer.Serve(httpListener); err != nil {
			log.Error().Err(err).Msg("HTTP server failed")
		}
	}()

	// Start gRPC server
	go func() {
		if err := s.server.Serve(grpcListener); err != nil {
			log.Error().Err(err).Msg("gRPC server failed")
		}
	}()

	// Start cmux
	go func() {
		if err := s.mux.Serve(); err != nil {
			log.Error().Err(err).Msg("cmux failed")
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
	// Merge received node states - this includes the source node's state
	// which allows restarted nodes to be re-discovered
	for _, nodeState := range req.Nodes {
		s.registry.Update(nodeState)
	}

	// Update lastSeen for the source node after Update() to ensure
	// the node exists in registry (it should be in req.Nodes)
	s.registry.mu.Lock()
	if _, exists := s.registry.nodes[req.SourceNodeId]; exists {
		s.registry.lastSeen[req.SourceNodeId] = time.Now()
	}
	s.registry.mu.Unlock()

	// Return our view of the cluster
	nodes := s.registry.GetAll()
	return &GossipResponse{
		Nodes: nodes,
	}, nil
}

// Join handles node join requests
func (s *Server) Join(ctx context.Context, req *JoinRequest) (*JoinResponse, error) {
	log.Debug().
		Uint64("local_node_id", s.nodeID).
		Uint64("joining_node_id", req.NodeId).
		Str("joining_address", req.Address).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("SEED: Received join request")

	// Add node to registry with JOINING status
	// The node will transition to ALIVE after it completes catch-up
	nodeState := &NodeState{
		NodeId:      req.NodeId,
		Address:     req.Address,
		Status:      NodeStatus_JOINING,
		Incarnation: 0,
	}
	s.registry.Add(nodeState)

	// Notify gossip protocol to connect to new node
	if s.gossip != nil {
		s.gossip.OnNodeJoin(nodeState)
	}

	// Return current cluster state
	nodes := s.registry.GetAll()

	log.Debug().
		Uint64("local_node_id", s.nodeID).
		Int("cluster_nodes_count", len(nodes)).
		Str("timestamp", time.Now().Format("15:04:05.000")).
		Msg("SEED: Returning cluster nodes to joining node")
	for _, n := range nodes {
		log.Debug().
			Uint64("local_node_id", s.nodeID).
			Uint64("returning_node_id", n.NodeId).
			Str("returning_status", n.Status.String()).
			Str("returning_address", n.Address).
			Msg("SEED: Cluster node in response")
	}

	return &JoinResponse{
		Success:      true,
		ClusterNodes: nodes,
	}, nil
}

// Ping handles health check requests
func (s *Server) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	// Return actual node status from registry, not hardcoded ALIVE
	node, exists := s.registry.Get(s.nodeID)
	status := NodeStatus_ALIVE // Default to ALIVE if not found
	if exists {
		status = node.Status
	}

	return &PingResponse{
		NodeId: s.nodeID,
		Status: status,
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

			// Parse statements from JSON - must include CDC data (NewValues, OldValues, RowKey)
			var statements []*Statement
			if statementsJSON != "" && statementsJSON != "[]" {
				var rawStatements []struct {
					SQL       string            `json:"SQL"`
					Type      int               `json:"Type"`
					TableName string            `json:"TableName"`
					Database  string            `json:"Database"`
					RowKey    string            `json:"RowKey"`
					OldValues map[string][]byte `json:"OldValues"`
					NewValues map[string][]byte `json:"NewValues"`
				}
				if err := json.Unmarshal([]byte(statementsJSON), &rawStatements); err != nil {
					log.Warn().Err(err).Uint64("txn_id", txnID).Msg("Failed to parse statements JSON")
				} else {
					for _, s := range rawStatements {
						stmt := &Statement{
							Type:      StatementType(s.Type),
							TableName: s.TableName,
							Database:  s.Database,
						}

						// For DML with CDC data, use RowChange payload
						// For DDL or DML without CDC, use DdlChange payload with SQL
						isDML := s.Type == int(StatementType_INSERT) ||
							s.Type == int(StatementType_UPDATE) ||
							s.Type == int(StatementType_DELETE) ||
							s.Type == int(StatementType_REPLACE)

						if isDML && (len(s.NewValues) > 0 || len(s.OldValues) > 0) {
							// CDC path: send row data
							stmt.Payload = &Statement_RowChange{
								RowChange: &RowChange{
									RowKey:    s.RowKey,
									OldValues: s.OldValues,
									NewValues: s.NewValues,
								},
							}
							log.Debug().
								Uint64("txn_id", txnID).
								Str("table", s.TableName).
								Str("row_key", s.RowKey).
								Int("new_values", len(s.NewValues)).
								Int("old_values", len(s.OldValues)).
								Msg("STREAM: Sending CDC data for anti-entropy")
						} else {
							// DDL or DML without CDC: send SQL
							stmt.Payload = &Statement_DdlChange{
								DdlChange: &DDLChange{
									Sql: s.SQL,
								},
							}
							log.Debug().
								Uint64("txn_id", txnID).
								Str("sql_prefix", func() string {
									if len(s.SQL) > 50 {
										return s.SQL[:50]
									}
									return s.SQL
								}()).
								Msg("STREAM: Sending SQL for anti-entropy")
						}

						statements = append(statements, stmt)
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

		// Get committed transaction count for data completeness comparison
		txnCount, err := dbManager.GetCommittedTxnCount(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to get committed txn count")
			txnCount = 0
		}

		states = append(states, &DatabaseReplicationState{
			DatabaseName:         dbName,
			LastAppliedTxnId:     lastAppliedTxnID,
			LastAppliedTimestamp: lastAppliedTS,
			LastSyncTime:         lastSyncTime,
			SyncStatus:           syncStatus,
			CurrentMaxTxnId:      maxTxnID,
			CommittedTxnCount:    txnCount,
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

// SetMetricsHandler sets the Prometheus metrics HTTP handler
func (s *Server) SetMetricsHandler(handler http.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricsHandler = handler
}

// SetDatabaseManager sets the database manager for snapshot operations
func (s *Server) SetDatabaseManager(manager *db.DatabaseManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dbManager = manager
}

// =======================
// PROMOTION CHECKER
// =======================

// RunPromotionChecker periodically checks if a JOINING node is ready to be promoted to ALIVE
// This implements the automatic JOINING → ALIVE transition based on promotion criteria
func (s *Server) RunPromotionChecker(ctx context.Context, checkInterval time.Duration, minHealthyDuration time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	// Track when node first became eligible for promotion
	var eligibleSince *time.Time

	log.Debug().
		Dur("check_interval", checkInterval).
		Dur("min_healthy_duration", minHealthyDuration).
		Msg("Started promotion checker")

	// Helper function to run promotion check
	runCheck := func() {
		// Check if we're currently in JOINING state
		currentNode, exists := s.registry.Get(s.nodeID)
		if !exists || currentNode.Status != NodeStatus_JOINING {
			eligibleSince = nil
			return
		}

		// Check promotion criteria
		eligible := s.checkPromotionCriteria()

		if eligible {
			// Track how long we've been eligible
			if eligibleSince == nil {
				now := time.Now()
				eligibleSince = &now
				log.Debug().Msg("Node became eligible for promotion")
			}

			// Check if we've been healthy long enough
			if time.Since(*eligibleSince) >= minHealthyDuration {
				log.Info().Msg("Promoting node from JOINING to ALIVE")

				// Promote to ALIVE
				s.registry.MarkAlive(s.nodeID)

				// Immediately broadcast ALIVE status to accelerate propagation
				if s.gossip != nil {
					s.gossip.BroadcastImmediate()
				}

				// Reset eligibility timer (we're now ALIVE)
				eligibleSince = nil
			}
		} else {
			// Not eligible, reset timer
			if eligibleSince != nil {
				eligibleSince = nil
			}
		}
	}

	// Run initial check immediately
	runCheck()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Promotion checker stopped")
			return
		case <-ticker.C:
			runCheck()
		}
	}
}

// checkPromotionCriteria checks if the node meets all criteria for promotion to ALIVE
func (s *Server) checkPromotionCriteria() bool {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		return false
	}

	// Get list of databases from seed node (first ALIVE node we find)
	aliveNodes := s.registry.GetReplicationEligible()
	if len(aliveNodes) == 0 {
		// No ALIVE nodes yet - we might be the first node (seed)
		return true
	}

	// Check that we have at least one database
	localDatabases := dbManager.ListDatabases()
	return len(localDatabases) > 0
}

// =======================
// DELTA SYNC DETECTION
// =======================

// GetLatestTxnIDs returns the latest transaction ID for each database
// This is used by peers to determine if they need to catch up
func (s *Server) GetLatestTxnIDs(ctx context.Context, req *LatestTxnIDsRequest) (*LatestTxnIDsResponse, error) {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	log.Debug().
		Uint64("requesting_node", req.RequestingNodeId).
		Msg("Latest transaction IDs requested")

	txnIDs := make(map[string]uint64)

	// Get all databases
	databases := dbManager.ListDatabases()

	// Query max txn_id for each database
	for _, dbName := range databases {
		maxTxnID, err := dbManager.GetMaxTxnID(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to get max txn_id")
			continue
		}
		txnIDs[dbName] = maxTxnID
	}

	log.Debug().
		Uint64("requesting_node", req.RequestingNodeId).
		Int("databases", len(txnIDs)).
		Msg("Returning latest transaction IDs")

	return &LatestTxnIDsResponse{
		DatabaseTxnIds: txnIDs,
	}, nil
}
