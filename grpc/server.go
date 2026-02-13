package grpc

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// errStopIteration is a sentinel error used to exit early from transaction iteration.
var errStopIteration = errors.New("stop iteration")

// snapshotCacheEntry represents a cached snapshot for a database
type snapshotCacheEntry struct {
	snapshotInfo db.SnapshotInfo
	maxTxnID     uint64
	createdAt    time.Time
	expiresAt    time.Time
	tempDir      string
}

// Server implements the gRPC server for Marmot
type Server struct {
	nodeID   uint64
	address  string
	port     int
	server   *grpc.Server
	listener net.Listener
	mux      cmux.CMux
	httpMux  *http.ServeMux // HTTP mux for admin endpoints

	// Components
	gossip             *GossipProtocol
	registry           *NodeRegistry
	replicationHandler *ReplicationHandler
	dbManager          *db.DatabaseManager
	metricsHandler     http.Handler

	// Write forwarding for read-only replicas
	forwardSessionMgr *ForwardSessionManager
	forwardHandler    *ForwardHandler

	// CDC signal-based change streaming
	cdcSubscriber db.CDCSubscriber

	// Snapshot caching
	snapshotCache   map[string]*snapshotCacheEntry
	snapshotCacheMu sync.RWMutex

	mu sync.RWMutex

	UnimplementedMarmotServiceServer
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
		nodeID:        config.NodeID,
		address:       config.Address,
		port:          config.Port,
		httpMux:       http.NewServeMux(),
		snapshotCache: make(map[string]*snapshotCacheEntry),
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

	// Use the pre-created HTTP mux
	httpMux := s.httpMux

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

	// Admin endpoints will be registered externally to avoid import cycles

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

	// Start background snapshot cache cleanup
	go s.runSnapshotCacheCleanup()

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

	// Treat gossip as implicit heartbeat from source node
	s.registry.TouchLastSeen(req.SourceNodeId)

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

	// Reject REMOVED nodes - they must be explicitly allowed to rejoin via admin API
	if s.registry.IsRemoved(req.NodeId) {
		log.Warn().
			Uint64("node_id", req.NodeId).
			Msg("Rejecting join from REMOVED node - use admin API to allow rejoin")
		return &JoinResponse{
			Success: false,
		}, nil
	}

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
	// Treat ping as implicit heartbeat from source node
	if req.SourceNodeId != 0 {
		s.registry.TouchLastSeen(req.SourceNodeId)
	}

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
	// Treat replication as implicit heartbeat from source node
	if req.SourceNodeId != 0 {
		s.registry.TouchLastSeen(req.SourceNodeId)
	}

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

// ForwardQuery handles forwarded write queries from read-only replicas
func (s *Server) ForwardQuery(ctx context.Context, req *ForwardQueryRequest) (*ForwardQueryResponse, error) {
	s.mu.RLock()
	handler := s.forwardHandler
	s.mu.RUnlock()

	if handler == nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "write forwarding not enabled on this node",
		}, nil
	}

	return handler.HandleForwardQuery(ctx, req)
}

// ForwardLoadData handles forwarded LOAD DATA LOCAL INFILE payloads from read-only replicas.
func (s *Server) ForwardLoadData(ctx context.Context, req *ForwardLoadDataRequest) (*ForwardQueryResponse, error) {
	s.mu.RLock()
	handler := s.forwardHandler
	s.mu.RUnlock()

	if handler == nil {
		return &ForwardQueryResponse{
			Success:      false,
			ErrorMessage: "write forwarding not enabled on this node",
		}, nil
	}

	return handler.HandleForwardLoadData(ctx, req)
}

// GetLoadDataChunk returns a staged chunk for pull-based LOAD DATA replication.
func (s *Server) GetLoadDataChunk(ctx context.Context, req *LoadDataChunkRequest) (*LoadDataChunkResponse, error) {
	if req.LoadId == "" {
		return &LoadDataChunkResponse{}, nil
	}
	chunk, total, ok := getLoadDataChunk(req.LoadId, req.Offset, req.MaxBytes)
	if !ok {
		return &LoadDataChunkResponse{}, nil
	}
	return &LoadDataChunkResponse{
		Data:      chunk,
		TotalSize: total,
	}, nil
}

// Read handles quorum read requests
// Future Phase: This will support distributed quorum reads across replicas.
// Currently reads are handled locally via coordinator.ReadCoordinator.
func (s *Server) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	log.Warn().Msg("Read RPC not yet implemented - use local reads via ReadCoordinator")
	return &ReadResponse{}, nil
}

// sendChangeEvent converts a TransactionRecord to ChangeEvent and sends it on the stream.
// Handles both CDC (Change Data Capture) path with row data and DDL path with SQL statements.
// Reads CDC entries from MetaStore for the transaction and converts them to Statement objects.
func (s *Server) sendChangeEvent(rec *db.TransactionRecord, metaStore db.MetaStore, stream MarmotService_StreamChangesServer) error {
	// Build statements from CDC entries in MetaStore
	var statements []*Statement

	cursor, err := metaStore.IterateCapturedRows(rec.TxnID)
	if err != nil {
		log.Warn().Err(err).Uint64("txn_id", rec.TxnID).Msg("Failed to iterate captured rows for streaming")
	} else {
		defer cursor.Close()
		for cursor.Next() {
			_, data := cursor.Row()
			row, err := db.DecodeRow(data)
			if err != nil {
				log.Warn().Err(err).Uint64("txn_id", rec.TxnID).Msg("Failed to decode captured row")
				continue
			}

			// Convert OpType to wire StatementType
			stmtCode := db.OpTypeToStatementType(db.OpType(row.Op))
			wireType := common.MustToWireType(stmtCode)

			var stmt *Statement
			switch db.OpType(row.Op) {
			case db.OpTypeDDL:
				// DDL statement - use DDLChange payload
				stmt = &Statement{
					Type:      wireType,
					TableName: row.Table,
					Database:  rec.DatabaseName,
					Payload: &Statement_DdlChange{
						DdlChange: &DDLChange{
							Sql: row.DDLSQL,
						},
					},
				}
			case db.OpTypeLoadData:
				stmt = &Statement{
					Type:      wireType,
					TableName: row.Table,
					Database:  rec.DatabaseName,
					Payload: &Statement_LoadDataChange{
						LoadDataChange: &LoadDataChange{
							Sql:  row.LoadSQL,
							Data: row.LoadData,
						},
					},
				}
			default:
				// DML statement - use RowChange payload
				stmt = &Statement{
					Type:      wireType,
					TableName: row.Table,
					Database:  rec.DatabaseName,
					Payload: &Statement_RowChange{
						RowChange: &RowChange{
							IntentKey: row.IntentKey,
							OldValues: row.OldValues,
							NewValues: row.NewValues,
						},
					},
				}
			}
			statements = append(statements, stmt)
		}
	}

	event := &ChangeEvent{
		TxnId:  rec.TxnID,
		SeqNum: rec.SeqNum,
		Timestamp: &HLC{
			WallTime: rec.CommitTSWall,
			Logical:  rec.CommitTSLogical,
		},
		Statements:            statements,
		Database:              rec.DatabaseName,
		RequiredSchemaVersion: rec.RequiredSchemaVersion,
	}

	return stream.Send(event)
}

// StreamChanges handles change streaming for catch-up and live push.
// Phase 1: Streams historical transactions for delta sync
// Phase 2: Subscribes to CDC signals for real-time push (no polling)
func (s *Server) StreamChanges(req *StreamRequest, stream MarmotService_StreamChangesServer) error {
	// Clean up forwarded sessions when this replica disconnects
	defer func() {
		if s.forwardSessionMgr != nil && req.RequestingNodeId != 0 {
			s.forwardSessionMgr.RemoveSessionsForReplica(req.RequestingNodeId)
		}
	}()

	// Treat stream request as implicit heartbeat from requesting node
	if req.RequestingNodeId != 0 {
		s.registry.TouchLastSeen(req.RequestingNodeId)
	}

	s.mu.RLock()
	dbManager := s.dbManager
	cdcSubscriber := s.cdcSubscriber
	s.mu.RUnlock()

	if dbManager == nil {
		return fmt.Errorf("database manager not initialized")
	}

	if cdcSubscriber == nil {
		return fmt.Errorf("CDC subscriber not initialized")
	}

	log.Info().
		Uint64("from_txn_id", req.FromTxnId).
		Uint64("requesting_node", req.RequestingNodeId).
		Str("database", req.Database).
		Msg("Starting change stream (signal-based push mode)")

	// Track last sent transaction ID per database
	lastSentTxn := make(map[string]uint64)

	// Subscribe to CDC signals BEFORE streaming historical to avoid missing changes
	filter := db.CDCFilter{}
	if req.Database != "" {
		filter.Databases = []string{req.Database}
	}
	signals, cancel := cdcSubscriber.Subscribe(filter)
	defer cancel()

	// Get databases to stream from for historical catch-up
	var databases []string
	if req.Database != "" {
		databases = []string{req.Database}
	} else {
		databases = dbManager.ListDatabases()
	}

	// Phase 1: Stream historical transactions (catch-up)
	if err := s.streamHistoricalTransactions(dbManager, databases, req.FromTxnId, stream, lastSentTxn); err != nil {
		return err
	}

	log.Info().
		Uint64("from_txn_id", req.FromTxnId).
		Uint64("requesting_node", req.RequestingNodeId).
		Msg("Historical transactions streamed, entering signal-based push mode")

	// Phase 2: Live streaming via CDC signals
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case sig := <-signals:
			// Pull and stream the transaction
			if err := s.pullAndStreamTransaction(sig, lastSentTxn, stream); err != nil {
				return err
			}
		}
	}
}

// pullAndStreamTransaction pulls a transaction from MetaStore and streams it
func (s *Server) pullAndStreamTransaction(sig db.CDCSignal, lastSentTxn map[string]uint64, stream MarmotService_StreamChangesServer) error {
	// Skip if already sent
	if sig.TxnID <= lastSentTxn[sig.Database] {
		return nil
	}

	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		log.Debug().
			Str("database", sig.Database).
			Uint64("txn_id", sig.TxnID).
			Msg("Skipping CDC signal - database manager not initialized")
		return nil
	}

	mdb, err := dbManager.GetDatabase(sig.Database)
	if err != nil {
		log.Debug().
			Str("database", sig.Database).
			Uint64("txn_id", sig.TxnID).
			Msg("Skipping CDC signal - database dropped")
		return nil
	}

	metaStore := mdb.GetMetaStore()
	if metaStore == nil {
		log.Debug().
			Str("database", sig.Database).
			Uint64("txn_id", sig.TxnID).
			Msg("Skipping CDC signal - MetaStore unavailable")
		return nil
	}

	// Pull the specific transaction using StreamCommittedTransactions with tight range
	var rec *db.TransactionRecord
	err = metaStore.StreamCommittedTransactions(sig.TxnID-1, func(r *db.TransactionRecord) error {
		if r.TxnID == sig.TxnID {
			rec = r
			return errStopIteration
		}
		return nil
	})

	// Ignore stop iteration sentinel - it's our way to exit early
	if err != nil && !errors.Is(err, errStopIteration) {
		log.Debug().
			Err(err).
			Str("database", sig.Database).
			Uint64("txn_id", sig.TxnID).
			Msg("Skipping CDC signal - failed to read transaction")
		return nil
	}

	if rec == nil {
		log.Debug().
			Str("database", sig.Database).
			Uint64("txn_id", sig.TxnID).
			Msg("Skipping CDC signal - transaction not found (may be GC'd)")
		return nil
	}

	if err := s.sendChangeEvent(rec, metaStore, stream); err != nil {
		return err
	}

	lastSentTxn[sig.Database] = sig.TxnID
	return nil
}

// streamHistoricalTransactions streams all historical transactions for initial catch-up
func (s *Server) streamHistoricalTransactions(
	dbManager *db.DatabaseManager,
	databases []string,
	fromTxnId uint64,
	stream MarmotService_StreamChangesServer,
	lastSentTxn map[string]uint64,
) error {
	for _, dbName := range databases {
		mdb, err := dbManager.GetDatabase(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to get database for streaming")
			continue
		}

		metaStore := mdb.GetMetaStore()
		if metaStore == nil {
			log.Warn().Str("database", dbName).Msg("MetaStore not available for streaming")
			continue
		}

		// Stream historical transactions
		lastSentTxn[dbName] = fromTxnId
		err = metaStore.StreamCommittedTransactions(fromTxnId, func(rec *db.TransactionRecord) error {
			if err := s.sendChangeEvent(rec, metaStore, stream); err != nil {
				return err
			}
			lastSentTxn[dbName] = rec.TxnID
			return nil
		})
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to stream transactions")
			continue
		}
	}
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

		if err != nil || repState == nil {
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

		// Get max sequence number for gap detection
		maxSeqNum, err := dbManager.GetMaxSeqNum(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("Failed to get max seq_num")
			maxSeqNum = 0
		}

		states = append(states, &DatabaseReplicationState{
			DatabaseName:         dbName,
			LastAppliedTxnId:     lastAppliedTxnID,
			LastAppliedTimestamp: lastAppliedTS,
			LastSyncTime:         lastSyncTime,
			SyncStatus:           syncStatus,
			CurrentMaxTxnId:      maxTxnID,
			CommittedTxnCount:    txnCount,
			MaxSeqNum:            maxSeqNum,
		})
	}

	return &ReplicationStateResponse{
		States: states,
	}, nil
}

// =======================
// SNAPSHOT METHODS
// =======================

// GetSnapshotInfo returns snapshot metadata for bootstrap.
// NOTE: This returns estimated info. The actual snapshot is taken atomically
// during StreamSnapshot to ensure consistency.
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

	// Use TakeSnapshot for metadata estimation only
	// The actual atomic snapshot is taken in StreamSnapshot
	snapshots, maxTxnID, err := dbManager.TakeSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot info: %w", err)
	}

	// Calculate total size and chunks (estimates)
	var totalSize int64
	var dbInfos []*DatabaseFileInfo
	dbMetadata := make([]*DatabaseSnapshotMetadata, 0, len(snapshots))

	for _, snap := range snapshots {
		totalSize += snap.Size
		dbInfos = append(dbInfos, &DatabaseFileInfo{
			Name:           snap.Name,
			Filename:       snap.Filename,
			SizeBytes:      snap.Size,
			Sha256Checksum: snap.SHA256,
		})

		// Get per-database txn ID
		txnID, err := dbManager.GetMaxTxnID(snap.Name)
		if err != nil {
			log.Warn().Err(err).Str("database", snap.Name).Msg("Failed to get max txn ID")
			txnID = 0
		}

		dbMetadata = append(dbMetadata, &DatabaseSnapshotMetadata{
			DatabaseName:   snap.Name,
			SnapshotTxnId:  txnID,
			SizeBytes:      snap.Size,
			Sha256Checksum: snap.SHA256,
		})
	}

	chunkSize := coordinator.GetStreamChunkSize()
	totalChunks := int32((totalSize + int64(chunkSize) - 1) / int64(chunkSize))

	return &SnapshotInfoResponse{
		SnapshotTxnId:     maxTxnID,
		SnapshotSizeBytes: totalSize,
		TotalChunks:       totalChunks,
		Databases:         dbInfos,
		DatabaseMetadata:  dbMetadata,
	}, nil
}

// StreamSnapshot streams snapshot chunks to requesting node.
// Uses atomic snapshot: copies files to temp dir under write lock, then streams from temp.
// Implements caching with TTL to avoid repeated checkpoints when multiple replicas request same snapshot.
func (s *Server) StreamSnapshot(req *SnapshotRequest, stream MarmotService_StreamSnapshotServer) error {
	s.mu.RLock()
	dbManager := s.dbManager
	dataDir := ""
	if dbManager != nil {
		dataDir = dbManager.GetDataDir()
	}
	s.mu.RUnlock()

	if dbManager == nil {
		return fmt.Errorf("database manager not initialized")
	}

	log.Info().
		Uint64("requesting_node", req.RequestingNodeId).
		Str("database", req.Database).
		Msg("Starting snapshot stream")

	var snapshots []db.SnapshotInfo
	var maxTxnID uint64
	var tempDir string
	var shouldCleanup bool

	if req.Database != "" {
		// Single database snapshot - use cache
		cacheKey := req.Database

		// Check cache first
		s.snapshotCacheMu.RLock()
		cached, exists := s.snapshotCache[cacheKey]
		s.snapshotCacheMu.RUnlock()

		if exists && time.Now().Before(cached.expiresAt) {
			// Cache hit - use cached snapshot
			log.Debug().
				Str("database", req.Database).
				Uint64("cached_txn_id", cached.maxTxnID).
				Time("expires_at", cached.expiresAt).
				Msg("Serving cached snapshot")

			snapshots = []db.SnapshotInfo{cached.snapshotInfo}
			maxTxnID = cached.maxTxnID
			tempDir = cached.tempDir
			shouldCleanup = false
		} else {
			// Cache miss or expired - create new snapshot
			if exists {
				log.Debug().
					Str("database", req.Database).
					Msg("Cache expired, creating new snapshot")
				// Clean up old cache entry
				s.snapshotCacheMu.Lock()
				delete(s.snapshotCache, cacheKey)
				s.snapshotCacheMu.Unlock()
				os.RemoveAll(cached.tempDir)
			} else {
				log.Debug().
					Str("database", req.Database).
					Msg("Cache miss, creating new snapshot")
			}

			// Create temp directory for atomic snapshot
			var err error
			tempDir, err = os.MkdirTemp(dataDir, "snapshot-export-")
			if err != nil {
				return fmt.Errorf("failed to create temp directory: %w", err)
			}

			snapshot, txnID, err := dbManager.TakeSnapshotForDatabase(tempDir, req.Database)
			if err != nil {
				os.RemoveAll(tempDir)
				return fmt.Errorf("failed to snapshot database %s: %w", req.Database, err)
			}
			snapshots = []db.SnapshotInfo{snapshot}
			maxTxnID = txnID

			// Cache the snapshot
			ttl := time.Duration(cfg.Config.Replica.SnapshotCacheTTLSec) * time.Second
			now := time.Now()
			s.snapshotCacheMu.Lock()
			s.snapshotCache[cacheKey] = &snapshotCacheEntry{
				snapshotInfo: snapshot,
				maxTxnID:     txnID,
				createdAt:    now,
				expiresAt:    now.Add(ttl),
				tempDir:      tempDir,
			}
			s.snapshotCacheMu.Unlock()

			log.Info().
				Str("database", req.Database).
				Uint64("snapshot_txn_id", txnID).
				Dur("ttl", ttl).
				Msg("Single database snapshot created and cached")

			shouldCleanup = false
		}

	} else {
		// All databases snapshot (backward compatible, no caching for full snapshots)
		var err error
		tempDir, err = os.MkdirTemp(dataDir, "snapshot-export-")
		if err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}

		snapshots, maxTxnID, err = dbManager.TakeSnapshotToDir(tempDir)
		if err != nil {
			os.RemoveAll(tempDir)
			return fmt.Errorf("failed to take snapshot: %w", err)
		}

		log.Info().
			Int("databases", len(snapshots)).
			Uint64("snapshot_txn_id", maxTxnID).
			Msg("Full snapshot created (no caching)")

		shouldCleanup = true
	}

	// Cleanup temp directory when done (only for non-cached snapshots)
	if shouldCleanup {
		defer os.RemoveAll(tempDir)
	}

	log.Info().
		Uint64("requesting_node", req.RequestingNodeId).
		Uint64("snapshot_txn_id", maxTxnID).
		Int("databases", len(snapshots)).
		Msg("Atomic snapshot created, streaming files")

	// Get configured chunk size
	chunkSize := coordinator.GetStreamChunkSize()

	// Calculate total chunks across all files
	var totalChunks int32
	for _, snap := range snapshots {
		fileChunks := int32((snap.Size + int64(chunkSize) - 1) / int64(chunkSize))
		if fileChunks == 0 {
			fileChunks = 1 // At least one chunk for empty files
		}
		totalChunks += fileChunks
	}

	chunkIndex := int32(0)

	// Stream each database file from temp directory
	for _, snap := range snapshots {
		file, err := os.Open(snap.FullPath)
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", snap.Filename, err)
		}

		// Use closure to ensure defer works per-iteration
		err = func() error {
			defer file.Close()

			buf := make([]byte, chunkSize)
			for {
				n, err := file.Read(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
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
					return fmt.Errorf("failed to send chunk: %w", err)
				}

				chunkIndex++
			}
			return nil
		}()
		if err != nil {
			return err
		}

		log.Debug().Str("file", snap.Filename).Msg("Finished streaming file")
	}

	log.Info().
		Uint64("requesting_node", req.RequestingNodeId).
		Int32("total_chunks", totalChunks).
		Uint64("snapshot_txn_id", maxTxnID).
		Msg("Snapshot stream completed")

	return nil
}

// GetNodeRegistry returns the node registry (for testing/debugging)
func (s *Server) GetNodeRegistry() *NodeRegistry {
	return s.registry
}

// GetRegistry returns the node registry (alias for GetNodeRegistry)
func (s *Server) GetRegistry() *NodeRegistry {
	return s.registry
}

// GetGossipProtocol returns the gossip protocol instance
func (s *Server) GetGossipProtocol() *GossipProtocol {
	return s.gossip
}

// GetNodeID returns the local node ID
func (s *Server) GetNodeID() uint64 {
	return s.nodeID
}

// GetDatabaseManager returns the database manager
func (s *Server) GetDatabaseManager() *db.DatabaseManager {
	return s.dbManager
}

// GetHTTPMux returns the HTTP mux for registering additional routes
func (s *Server) GetHTTPMux() *http.ServeMux {
	return s.httpMux
}

// SetReplicationHandler sets the replication handler for transaction processing
func (s *Server) SetReplicationHandler(handler *ReplicationHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationHandler = handler
}

// SetForwardHandler sets the write forwarding handler for read-only replicas
func (s *Server) SetForwardHandler(handler *ForwardHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forwardHandler = handler
}

// SetForwardSessionManager sets the session manager for write forwarding
func (s *Server) SetForwardSessionManager(mgr *ForwardSessionManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forwardSessionMgr = mgr
}

// GetForwardSessionManager returns the forward session manager
func (s *Server) GetForwardSessionManager() *ForwardSessionManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.forwardSessionMgr
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

// SetCDCSubscriber sets the CDC subscriber for change streaming
func (s *Server) SetCDCSubscriber(subscriber db.CDCSubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cdcSubscriber = subscriber
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
	replicationHandler := s.replicationHandler
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
	if len(localDatabases) == 0 {
		return false
	}

	// Verify schema versions match or exceed all ALIVE peers
	// This prevents premature promotion before DDL replication completes
	if replicationHandler == nil {
		return false
	}

	localSchemaVersions, err := replicationHandler.GetAllSchemaVersions()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get local schema versions for promotion check")
		return false
	}

	// Build a set of local database names for quick lookup
	localDBSet := make(map[string]bool)
	for _, dbName := range localDatabases {
		localDBSet[dbName] = true
	}

	// Check each ALIVE peer's schema versions
	for _, peer := range aliveNodes {
		peerSchemaVersions := peer.DatabaseSchemaVersions
		if peerSchemaVersions == nil {
			continue
		}

		// For each database the peer has, check if our schema is at least as recent
		for dbName, peerVersion := range peerSchemaVersions {
			// Skip system database - it's not subject to DDL replication
			if dbName == db.SystemDatabaseName {
				continue
			}

			localVersion, hasSchemaEntry := localSchemaVersions[dbName]
			hasLocalDB := localDBSet[dbName]

			if !hasLocalDB {
				// Peer has a database we don't have yet - we'll get it via replication
				// This is acceptable, continue checking
				continue
			}

			// If we have the database locally but no schema version entry,
			// treat it as version 0 (fresh database, no DDL applied yet)
			if !hasSchemaEntry {
				localVersion = 0
			}

			// If peer has higher schema version, we're behind on DDL
			if localVersion < peerVersion {
				log.Debug().
					Str("database", dbName).
					Uint64("local_version", localVersion).
					Uint64("peer_version", peerVersion).
					Uint64("peer_id", peer.NodeId).
					Msg("Schema version behind peer, delaying promotion")
				return false
			}
		}
	}

	return true
}

// =======================
// SNAPSHOT CACHE CLEANUP
// =======================

// runSnapshotCacheCleanup periodically removes expired snapshot cache entries
func (s *Server) runSnapshotCacheCleanup() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	log.Debug().Msg("Started snapshot cache cleanup goroutine")

	for range ticker.C {
		s.cleanupExpiredSnapshots()
	}
}

// cleanupExpiredSnapshots removes expired cache entries and their temp directories
func (s *Server) cleanupExpiredSnapshots() {
	now := time.Now()
	var toDelete []string

	// Collect expired entries
	s.snapshotCacheMu.RLock()
	for dbName, entry := range s.snapshotCache {
		if now.After(entry.expiresAt) {
			toDelete = append(toDelete, dbName)
		}
	}
	s.snapshotCacheMu.RUnlock()

	if len(toDelete) == 0 {
		return
	}

	// Delete expired entries
	s.snapshotCacheMu.Lock()
	for _, dbName := range toDelete {
		entry, exists := s.snapshotCache[dbName]
		if !exists {
			continue
		}

		// Remove temp directory
		if err := os.RemoveAll(entry.tempDir); err != nil {
			log.Warn().Err(err).Str("database", dbName).Str("temp_dir", entry.tempDir).Msg("Failed to cleanup expired snapshot cache")
		}

		delete(s.snapshotCache, dbName)

		log.Debug().
			Str("database", dbName).
			Time("created_at", entry.createdAt).
			Time("expired_at", entry.expiresAt).
			Msg("Cleaned up expired snapshot cache entry")
	}
	s.snapshotCacheMu.Unlock()

	log.Debug().Int("cleaned", len(toDelete)).Msg("Snapshot cache cleanup completed")
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
	dbInfoList := make([]*DatabaseInfo, 0, len(dbManager.ListDatabases()))

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

		// Get database size and created_at
		dbPath := filepath.Join(dbManager.GetDataDir(), "databases", dbName+".db")
		var sizeBytes int64
		var createdAt int64

		if info, err := os.Stat(dbPath); err == nil {
			sizeBytes = info.Size()
			createdAt = info.ModTime().Unix()
		}

		dbInfoList = append(dbInfoList, &DatabaseInfo{
			Name:      dbName,
			MaxTxnId:  maxTxnID,
			SizeBytes: sizeBytes,
			CreatedAt: createdAt,
		})
	}

	log.Debug().
		Uint64("requesting_node", req.RequestingNodeId).
		Int("databases", len(txnIDs)).
		Msg("Returning latest transaction IDs")

	return &LatestTxnIDsResponse{
		DatabaseTxnIds: txnIDs,
		DatabaseInfo:   dbInfoList,
	}, nil
}

// GetClusterNodes returns cluster membership for readonly replicas
func (s *Server) GetClusterNodes(ctx context.Context, req *GetClusterNodesRequest) (*GetClusterNodesResponse, error) {
	return &GetClusterNodesResponse{Nodes: s.registry.GetAll()}, nil
}

// TransactionStream handles streaming CDC payloads for large transactions (>=128KB).
// Client streams TransactionChunk messages containing CDC data, followed by a
// TransactionCommit message to finalize. This avoids single large RPC payloads.
func (s *Server) TransactionStream(stream grpc.ClientStreamingServer[TransactionStreamMessage, TransactionResponse]) error {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()

	if dbManager == nil {
		return stream.SendAndClose(&TransactionResponse{
			Success:      false,
			ErrorMessage: "database manager not initialized",
		})
	}

	var txnID uint64
	var database string
	var sourceNodeID uint64
	var stmtSeq uint64

	// Track metadata for commit
	var metaStore db.MetaStore
	var txnMgr *db.TransactionManager

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&TransactionResponse{
				Success:      false,
				ErrorMessage: "stream ended without commit message",
			})
		}
		if err != nil {
			log.Error().Err(err).Msg("TransactionStream receive error")
			return err
		}

		switch payload := msg.GetPayload().(type) {
		case *TransactionStreamMessage_Chunk:
			chunk := payload.Chunk
			if chunk == nil {
				continue
			}

			// First chunk establishes transaction context
			if txnID == 0 {
				txnID = chunk.TxnId
				database = chunk.Database

				dbInstance, err := dbManager.GetDatabase(database)
				if err != nil {
					return stream.SendAndClose(&TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("database %s not found: %v", database, err),
					})
				}
				metaStore = dbInstance.GetMetaStore()
				txnMgr = dbInstance.GetTransactionManager()
			} else if chunk.TxnId != txnID {
				return stream.SendAndClose(&TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("txn_id mismatch: expected %d, got %d", txnID, chunk.TxnId),
				})
			}

			// Process each statement in the chunk
			for _, stmt := range chunk.Statements {
				stmtSeq++
				rowChange := stmt.GetRowChange()
				if rowChange == nil {
					// DDL statements don't use WriteIntentEntry - they're handled separately
					continue
				}

				// Convert wire type to internal OpType
				stmtCode := common.MustFromWireType(stmt.Type)
				op := db.StatementTypeToOpType(stmtCode)

				// Store CDC entry to PebbleDB via metaStore
				err := metaStore.WriteIntentEntry(
					txnID,
					stmtSeq,
					uint8(op),
					stmt.TableName,
					string(rowChange.IntentKey),
					rowChange.OldValues,
					rowChange.NewValues,
				)
				if err != nil {
					log.Error().Err(err).
						Uint64("txn_id", txnID).
						Uint64("seq", stmtSeq).
						Str("table", stmt.TableName).
						Msg("Failed to write CDC intent entry during stream")
					return stream.SendAndClose(&TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("failed to store CDC entry: %v", err),
					})
				}
			}

		case *TransactionStreamMessage_Commit:
			commit := payload.Commit
			if commit == nil {
				return stream.SendAndClose(&TransactionResponse{
					Success:      false,
					ErrorMessage: "received empty commit message",
				})
			}

			// Validate txn_id matches
			if txnID != 0 && commit.TxnId != txnID {
				return stream.SendAndClose(&TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("commit txn_id mismatch: expected %d, got %d", txnID, commit.TxnId),
				})
			}

			// Handle case where commit comes without prior chunks (empty transaction)
			if txnID == 0 {
				txnID = commit.TxnId
				database = commit.Database

				dbInstance, err := dbManager.GetDatabase(database)
				if err != nil {
					return stream.SendAndClose(&TransactionResponse{
						Success:      false,
						ErrorMessage: fmt.Sprintf("database %s not found: %v", database, err),
					})
				}
				txnMgr = dbInstance.GetTransactionManager()
			}

			sourceNodeID = commit.SourceNodeId

			// Touch heartbeat for source node
			if sourceNodeID != 0 {
				s.registry.TouchLastSeen(sourceNodeID)
			}

			// Get the pending transaction from TransactionManager
			txn := txnMgr.GetTransaction(txnID)
			if txn == nil {
				log.Warn().
					Uint64("txn_id", txnID).
					Str("database", database).
					Msg("TransactionStream commit: transaction not found")
				return stream.SendAndClose(&TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("transaction %d not found", txnID),
				})
			}

			// Commit the transaction
			if err := txnMgr.CommitTransaction(txn); err != nil {
				log.Error().Err(err).
					Uint64("txn_id", txnID).
					Str("database", database).
					Msg("TransactionStream commit failed")
				return stream.SendAndClose(&TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("commit failed: %v", err),
				})
			}

			log.Debug().
				Uint64("txn_id", txnID).
				Str("database", database).
				Uint64("statements", stmtSeq).
				Msg("TransactionStream committed successfully")

			return stream.SendAndClose(&TransactionResponse{
				Success: true,
			})

		default:
			return stream.SendAndClose(&TransactionResponse{
				Success:      false,
				ErrorMessage: "unknown message payload type",
			})
		}
	}
}
