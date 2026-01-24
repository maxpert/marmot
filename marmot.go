package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/maxpert/marmot/admin"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/publisher"
	"github.com/maxpert/marmot/replica"
	"github.com/maxpert/marmot/telemetry"

	// Register CDC sink and transformer factories via init()
	_ "github.com/maxpert/marmot/publisher/sink"
	_ "github.com/maxpert/marmot/publisher/transformer"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// publisherAdapter adapts publisher.Registry to coordinator.PublisherRegistry interface
type publisherAdapter struct {
	registry *publisher.Registry
}

func (p *publisherAdapter) AppendCDC(txnID uint64, database string, entries []common.CDCEntry, commitTSNanos int64, nodeID uint64) error {
	return p.registry.AppendCDC(txnID, database, entries, commitTSNanos, nodeID)
}

func main() {
	flag.Parse()

	// Load configuration
	err := cfg.Load(*cfg.ConfigPathFlag)
	if err != nil {
		panic(err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("Invalid configuration: %v", err))
	}

	// Setup logging
	var writer io.Writer = zerolog.NewConsoleWriter()
	if cfg.Config.Logging.Format == "json" {
		writer = os.Stdout
	}
	gLog := zerolog.New(writer).
		With().
		Timestamp().
		Uint64("node_id", cfg.Config.NodeID).
		Logger()

	if cfg.Config.Logging.Verbose {
		log.Logger = gLog.Level(zerolog.DebugLevel)
	} else {
		log.Logger = gLog.Level(zerolog.InfoLevel)
	}

	// Branch based on operating mode
	if cfg.IsReplicaMode() {
		log.Info().Msg("Marmot v2.0 - Read-Only Replica Mode")
		log.Info().
			Strs("follow_addresses", cfg.Config.Replica.FollowAddresses).
			Msg("Following cluster nodes")

		// Warn if cluster authentication is not configured
		if !cfg.IsClusterAuthEnabled() {
			log.Warn().Msg("WARNING: Cluster authentication is disabled. Set cluster_secret in config or MARMOT_CLUSTER_SECRET env var for production use.")
		}

		// Run replica mode (does not return until shutdown)
		replica.Run()
		return
	}

	// Cluster mode
	log.Info().Msg("Marmot v2.0 - Leaderless SQLite Replication")

	// Warn if cluster authentication is not configured
	if !cfg.IsClusterAuthEnabled() {
		log.Warn().Msg("WARNING: Cluster authentication is disabled. Set cluster_secret in config or MARMOT_CLUSTER_SECRET env var for production use.")
	}

	log.Debug().Msg("Initializing telemetry")
	telemetry.InitializeTelemetry()
	telemetry.InitMetrics()

	// Phase 1: Initialize gRPC server with gossip
	log.Info().Msg("Initializing gRPC server")
	grpcServer, err := initializeGRPCServer()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize gRPC server")
		return
	}

	// Set metrics handler if Prometheus is enabled
	if metricsHandler := telemetry.GetMetricsHandler(); metricsHandler != nil {
		grpcServer.SetMetricsHandler(metricsHandler)
	}

	// Start gRPC server
	if err := grpcServer.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start gRPC server")
		return
	}
	defer grpcServer.Stop()

	// Start gossip protocol
	log.Info().Msg("Starting gossip protocol")
	client, gossip := startGossip(grpcServer)

	// Initialize extension manager if configured (must be before any DB connections)
	if cfg.Config.Extensions.Directory != "" || len(cfg.Config.Extensions.AlwaysLoaded) > 0 {
		log.Info().
			Str("directory", cfg.Config.Extensions.Directory).
			Strs("always_loaded", cfg.Config.Extensions.AlwaysLoaded).
			Msg("Initializing SQLite extension manager")
		if err := db.InitExtensionManager(cfg.Config.Extensions.Directory, cfg.Config.Extensions.AlwaysLoaded); err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize extension manager")
			return
		}
	}

	// Phase 5: Initialize Database Manager
	log.Info().Msg("Initializing Database Manager")
	clock := hlc.NewClock(cfg.Config.NodeID)

	// Check if we're joining an existing cluster or starting as seed
	isJoiningCluster := len(cfg.Config.Cluster.SeedNodes) > 0

	// Create catch-up client for both seed nodes (for anti-entropy) and joining nodes
	catchUpClient := marmotgrpc.NewCatchUpClient(
		cfg.Config.NodeID,
		cfg.Config.DataDir,
		grpcServer.GetNodeRegistry(),
		cfg.Config.Cluster.SeedNodes,
	)

	// If joining cluster, determine if we need to catch up (even if we have data)
	// This fixes the bug where restarted nodes with stale data skip catch-up
	var catchUpDecision *marmotgrpc.CatchUpDecision

	if isJoiningCluster {

		log.Info().Msg("Determining catch-up strategy by comparing with cluster state...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		decision, err := catchUpClient.DetermineCatchUpStrategy(ctx)
		cancel()

		if err != nil {
			log.Warn().Err(err).Msg("Failed to determine catch-up strategy - will attempt to continue")
		} else {
			catchUpDecision = decision
			switch decision.Strategy {
			case marmotgrpc.NO_CATCHUP:
				log.Info().Msg("Node is up to date - no catch-up needed")

			case marmotgrpc.FULL_SNAPSHOT:
				log.Info().
					Str("peer", decision.PeerAddr).
					Msg("Full snapshot required - downloading from cluster")
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				snapshotTxnID, err := catchUpClient.CatchUp(ctx)
				cancel()
				if err != nil {
					log.Fatal().Err(err).Msg("Failed to download snapshot from cluster")
					return
				}
				log.Info().Uint64("snapshot_txn_id", snapshotTxnID).Msg("Snapshot download completed")

			case marmotgrpc.DELTA_SYNC:
				log.Info().
					Int("databases_behind", len(decision.DatabaseDeltas)).
					Str("peer", decision.PeerAddr).
					Msg("Delta sync required - will catch up after database initialization")
				// Delta sync will be performed after database manager is initialized
				// We store the decision for later use
			}
		}
	}

	dbMgr, err := db.NewDatabaseManager(cfg.Config.DataDir, cfg.Config.NodeID, clock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Database Manager")
		return
	}
	defer dbMgr.Close()

	// Initialize CDC Publisher if enabled
	var publisherRegistry *publisher.Registry
	if cfg.Config.Publisher.Enabled {
		log.Info().Msg("Initializing CDC Publisher")

		publisherRegistry, err = publisher.NewRegistry(publisher.RegistryConfig{
			DataDir:     cfg.Config.DataDir,
			DBManager:   dbMgr,
			SinkConfigs: cfg.Config.Publisher.Sinks,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize CDC Publisher")
			return
		}

		// Start publisher
		if err := publisherRegistry.Start(); err != nil {
			log.Fatal().Err(err).Msg("Failed to start publisher")
			return
		}
		defer publisherRegistry.Stop()

		log.Info().
			Int("sinks", len(cfg.Config.Publisher.Sinks)).
			Msg("CDC Publisher initialized")
	}

	// Register admin HTTP endpoints under /admin
	adminHandlers := admin.NewAdminHandlers(grpcServer, dbMgr)
	admin.RegisterRoutes(grpcServer.GetHTTPMux(), adminHandlers)

	// Import existing databases from data_dir if:
	// 1. Starting as seed node (no seeds configured), OR
	// 2. Joining cluster but no catch-up needed (peer has no data), OR
	// 3. Joining cluster but catch-up determination failed (anti-entropy will reconcile)
	shouldImport := !isJoiningCluster ||
		catchUpDecision == nil || // catch-up determination failed
		catchUpDecision.Strategy == marmotgrpc.NO_CATCHUP
	if shouldImport {
		imported, err := dbMgr.ImportExistingDatabases(cfg.Config.DataDir)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to import existing databases")
		} else if imported > 0 {
			log.Info().Int("count", imported).Msg("Imported existing databases from data directory")
		}
	}

	// Phase 5: Initialize schema version manager using system database's MetaStore
	log.Info().Msg("Initializing schema version manager")
	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get system database for schema versioning")
		return
	}
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	// Wire up replication handlers
	log.Info().Msg("Wiring up replication handlers")

	replicationHandler := marmotgrpc.NewReplicationHandler(
		cfg.Config.NodeID,
		dbMgr,
		clock,
		schemaVersionMgr,
	)
	grpcServer.SetReplicationHandler(replicationHandler)
	grpcServer.SetDatabaseManager(dbMgr)

	log.Info().Msg("Database Manager and replication handlers initialized")

	// Phase 6: Setup anti-entropy service for catching up lagging nodes
	log.Info().Msg("Setting up anti-entropy service")
	deltaSync := marmotgrpc.NewDeltaSyncClient(marmotgrpc.DeltaSyncConfig{
		NodeID:           cfg.Config.NodeID,
		Client:           client,
		DBManager:        dbMgr,
		Clock:            clock,
		ApplyTxnsFn:      replicationHandler.HandleReplicateTransaction,
		SchemaVersionMgr: schemaVersionMgr,
	})

	// If we determined we need delta sync, perform it now that database manager is initialized
	if isJoiningCluster && catchUpDecision != nil && catchUpDecision.Strategy == marmotgrpc.DELTA_SYNC {
		log.Info().Msg("Performing delta sync now that database manager is initialized")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		err := catchUpClient.PerformDeltaSync(ctx, catchUpDecision, deltaSync)
		cancel()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to perform delta sync")
			return
		}
		log.Info().Msg("Delta sync completed successfully")
	}

	// Create snapshot function for anti-entropy
	snapshotFunc := func(ctx context.Context, peerNodeID uint64, peerAddr string, database string) error {
		// Download snapshot to disk
		if err := catchUpClient.CatchUpFromPeer(ctx, peerNodeID, peerAddr, database); err != nil {
			return err
		}

		// Reload the database connection to pick up the new snapshot file
		// This is critical: the old connection still points to the old data
		if err := dbMgr.ReopenDatabase(database); err != nil {
			log.Error().Err(err).Str("database", database).Msg("Failed to reload database after snapshot")
			return fmt.Errorf("database reload failed after snapshot: %w", err)
		}

		log.Info().Str("database", database).Msg("Database reloaded after snapshot download")
		return nil
	}

	antiEntropy := marmotgrpc.NewAntiEntropyServiceFromConfig(
		cfg.Config.NodeID,
		grpcServer.GetNodeRegistry(),
		client,
		dbMgr,
		deltaSync,
		clock,
		snapshotFunc,
		schemaVersionMgr,
	)

	// Wire anti-entropy refresh to DatabaseManager for GC watermark freshness
	// This ensures GC queries fresh peer states before making deletion decisions
	dbMgr.SetRefreshReplicationStatesFunc(antiEntropy.RefreshPeerReplicationStates)

	// Start anti-entropy service
	antiEntropy.Start()
	defer antiEntropy.Stop()

	log.Info().Msg("Anti-entropy service initialized")

	// Phase 7: Setup transaction coordinators for full database replication
	log.Info().Msg("Setting up transaction coordinators")
	nodeProvider := marmotgrpc.NewGossipNodeProvider(gossip.GetNodeRegistry())
	replicator := marmotgrpc.NewGRPCReplicator(client)

	writeTimeout := time.Duration(cfg.Config.Replication.WriteTimeoutMS) * time.Millisecond
	readTimeout := time.Duration(cfg.Config.Replication.ReadTimeoutMS) * time.Millisecond

	// Initialize LocalReplicator for WriteCoordinator
	localReplicator := db.NewLocalReplicator(cfg.Config.NodeID, dbMgr, clock)

	writeCoordinator := coordinator.NewWriteCoordinator(
		cfg.Config.NodeID,
		nodeProvider,
		replicator,
		localReplicator,
		writeTimeout,
		clock,
	)

	// Initialize LocalReader for ReadCoordinator
	localReader := db.NewLocalReader(dbMgr)

	readCoordinator := coordinator.NewReadCoordinator(
		cfg.Config.NodeID,
		nodeProvider,
		localReader,
		readTimeout,
	)

	log.Info().Msg("Transaction coordinators initialized")

	// Initialize DDL lock manager for cluster-wide DDL serialization
	ddlLockLease := time.Duration(cfg.Config.DDL.LockLeaseSeconds) * time.Second
	ddlLockMgr := coordinator.NewDDLLockManager(ddlLockLease)

	// Phase 8: Initialize MySQL protocol server
	log.Info().Msg("Initializing MySQL protocol server")

	// Create handler to bridge MySQL protocol and coordinators
	// Wrap node registry to provide []any interface (avoids import cycle)
	registryAdapter := marmotgrpc.NewNodeRegistryAdapter(gossip.GetNodeRegistry())

	handler := coordinator.NewCoordinatorHandler(
		cfg.Config.NodeID,
		writeCoordinator,
		readCoordinator,
		clock,
		dbMgr,
		ddlLockMgr,
		schemaVersionMgr,
		registryAdapter,
	)

	// Set publisher registry if enabled
	if publisherRegistry != nil {
		// Create adapter to bridge type mismatch between coordinator and publisher packages
		adapter := &publisherAdapter{registry: publisherRegistry}
		handler.SetPublisherRegistry(adapter)
	}

	// Initialize query pipeline with configured values and ID generator
	var idGen id.Generator
	if cfg.Config.MySQL.AutoIDMode == "extended" {
		log.Info().Msg("Using extended 64-bit ID generation")
		idGen = id.NewHLCGenerator(clock)
	} else {
		log.Info().Msg("Using compact 53-bit ID generation")
		idGen = id.NewCompactGenerator(cfg.Config.NodeID)
	}
	if err := protocol.InitializePipeline(
		cfg.Config.QueryPipeline.TranspilerCacheSize,
		cfg.Config.QueryPipeline.ValidatorPoolSize,
		idGen,
	); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize query pipeline")
		return
	}

	// Create and start MySQL server
	var unixSocketPerm os.FileMode = 0660
	if cfg.Config.MySQL.UnixSocketPerm != 0 {
		unixSocketPerm = os.FileMode(cfg.Config.MySQL.UnixSocketPerm)
	}
	mysqlServer := protocol.NewMySQLServer(
		fmt.Sprintf("%s:%d", cfg.Config.MySQL.BindAddress, cfg.Config.MySQL.Port),
		cfg.Config.MySQL.UnixSocket,
		unixSocketPerm,
		handler,
	)

	if err := mysqlServer.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start MySQL server")
		return
	}
	defer mysqlServer.Stop()

	// Now that all services are initialized and running, handle node state
	if isJoiningCluster {
		// Start promotion checker to automatically transition from JOINING → ALIVE
		// when node is ready (has databases and is healthy)
		checkInterval := time.Duration(cfg.Config.Cluster.Promotion.CheckIntervalSeconds) * time.Second
		minHealthyDuration := time.Duration(cfg.Config.Cluster.Promotion.MinHealthyDurationSec) * time.Second

		// Create cancellable context for graceful shutdown
		promotionCtx, promotionCancel := context.WithCancel(context.Background())
		defer promotionCancel() // Cancel on shutdown

		go grpcServer.RunPromotionChecker(promotionCtx, checkInterval, minHealthyDuration)

		log.Info().Msg("Node fully initialized - staying in JOINING, promotion checker running")
	} else {
		// Seed node is immediately ALIVE (no need to catch up)
		grpcServer.GetNodeRegistry().MarkAlive(cfg.Config.NodeID)
		log.Info().Msg("Seed node fully initialized - now ALIVE")
	}

	log.Info().Msg("Marmot v2.0 started successfully")
	log.Info().
		Uint64("node_id", cfg.Config.NodeID).
		Int("grpc_port", cfg.Config.Cluster.GRPCPort).
		Str("data_dir", cfg.Config.DataDir).
		Msg("Node is operational")

	// Keep running
	select {}
}

func initializeGRPCServer() (*marmotgrpc.Server, error) {
	config := marmotgrpc.ServerConfig{
		NodeID:           cfg.Config.NodeID,
		Address:          cfg.Config.Cluster.GRPCBindAddress,
		Port:             cfg.Config.Cluster.GRPCPort,
		AdvertiseAddress: cfg.Config.Cluster.GRPCAdvertiseAddress,
	}

	server, err := marmotgrpc.NewServer(config)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func startGossip(server *marmotgrpc.Server) (*marmotgrpc.Client, *marmotgrpc.GossipProtocol) {
	gossipConfig := marmotgrpc.DefaultGossipConfig()

	// Get gossip protocol from server (already initialized)
	gossip := server.GetGossipProtocol()

	// Create and set client
	client := marmotgrpc.NewClient(cfg.Config.NodeID)
	gossip.SetClient(client)

	// Join cluster if seed nodes are configured
	if len(cfg.Config.Cluster.SeedNodes) > 0 {
		log.Info().Strs("seeds", cfg.Config.Cluster.SeedNodes).Msg("Joining cluster")
		if err := gossip.JoinCluster(cfg.Config.Cluster.SeedNodes, cfg.Config.Cluster.GRPCAdvertiseAddress); err != nil {
			log.Warn().Err(err).Msg("Failed to join cluster, starting as single node")
		}
	} else {
		log.Info().Msg("No seed nodes configured, starting as single-node cluster")
	}

	// Start gossip protocol
	gossip.Start(gossipConfig)

	return client, gossip
}
