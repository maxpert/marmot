package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

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

	log.Info().Msg("Marmot v2.0 - Leaderless SQLite Replication")
	log.Debug().Msg("Initializing telemetry")
	telemetry.InitializeTelemetry()

	// Phase 1: Initialize gRPC server with gossip
	log.Info().Msg("Initializing gRPC server")
	grpcServer, err := initializeGRPCServer()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize gRPC server")
		return
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

	// Phase 5: Initialize Database Manager
	log.Info().Msg("Initializing Database Manager")
	clock := hlc.NewClock(cfg.Config.NodeID)

	// Migrate legacy database if it exists
	if err := db.MigrateFromLegacy(cfg.Config.DBPath, cfg.Config.DataDir, cfg.Config.NodeID, clock); err != nil {
		log.Error().Err(err).Msg("Failed to migrate legacy database")
	}

	dbMgr, err := db.NewDatabaseManager(cfg.Config.DataDir, cfg.Config.NodeID, clock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Database Manager")
		return
	}
	defer dbMgr.Close()

	// Get default database for backward compatibility with replication handler
	defaultDB, err := dbMgr.GetDatabase(db.DefaultDatabaseName)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get default database")
		return
	}

	// Phase 5: Wire up replication handlers
	log.Info().Msg("Wiring up replication handlers")
	replicationHandler := marmotgrpc.NewReplicationHandler(
		cfg.Config.NodeID,
		defaultDB.GetDB(),
		defaultDB.GetTransactionManager(),
		clock,
	)
	grpcServer.SetReplicationHandler(replicationHandler)
	grpcServer.SetDatabaseManager(dbMgr)

	log.Info().Msg("Database Manager and replication handlers initialized")

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

	// Phase 8: Initialize MySQL protocol server
	log.Info().Msg("Initializing MySQL protocol server")

	// Create handler to bridge MySQL protocol and coordinators
	handler := coordinator.NewCoordinatorHandler(
		cfg.Config.NodeID,
		writeCoordinator,
		readCoordinator,
		clock,
		dbMgr,
	)

	// Create and start MySQL server
	mysqlServer := protocol.NewMySQLServer(
		fmt.Sprintf("%s:%d", cfg.Config.MySQL.BindAddress, cfg.Config.MySQL.Port),
		handler,
	)

	if err := mysqlServer.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start MySQL server")
		return
	}
	defer mysqlServer.Stop()

	log.Info().Msg("Marmot v2.0 started successfully")
	log.Info().
		Uint64("node_id", cfg.Config.NodeID).
		Int("grpc_port", cfg.Config.Cluster.GRPCPort).
		Str("db_path", cfg.Config.DBPath).
		Msg("Node is operational")

	// Keep running
	select {}
}

func initializeGRPCServer() (*marmotgrpc.Server, error) {
	config := marmotgrpc.ServerConfig{
		NodeID:  cfg.Config.NodeID,
		Address: cfg.Config.Cluster.GRPCBindAddress,
		Port:    cfg.Config.Cluster.GRPCPort,
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
