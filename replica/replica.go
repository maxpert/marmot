package replica

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"

	"github.com/rs/zerolog/log"
)

// ReplicaState represents the current state of the replica
type ReplicaState int32

const (
	StateInitializing ReplicaState = iota
	StateBootstrapping
	StateStreaming
	StateReconnecting
	StateStale
	StateStopped
)

func (s ReplicaState) String() string {
	switch s {
	case StateInitializing:
		return "INITIALIZING"
	case StateBootstrapping:
		return "BOOTSTRAPPING"
	case StateStreaming:
		return "STREAMING"
	case StateReconnecting:
		return "RECONNECTING"
	case StateStale:
		return "STALE"
	case StateStopped:
		return "STOPPED"
	default:
		return "UNKNOWN"
	}
}

// Replica is the main read-only replica orchestrator
type Replica struct {
	masterAddr   string
	nodeID       uint64
	dataDir      string
	dbManager    *db.DatabaseManager
	clock        *hlc.Clock
	streamClient *StreamClient
	handler      *ReadOnlyHandler

	state     atomic.Int32
	connected atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewReplica creates a new replica instance
func NewReplica(masterAddr string, nodeID uint64, dataDir string) *Replica {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Replica{
		masterAddr: masterAddr,
		nodeID:     nodeID,
		dataDir:    dataDir,
		ctx:        ctx,
		cancel:     cancel,
	}
	r.state.Store(int32(StateInitializing))
	return r
}

// State returns the current replica state
func (r *Replica) State() ReplicaState {
	return ReplicaState(r.state.Load())
}

// SetState sets the replica state
func (r *Replica) SetState(state ReplicaState) {
	old := ReplicaState(r.state.Swap(int32(state)))
	if old != state {
		log.Info().
			Str("from", old.String()).
			Str("to", state.String()).
			Msg("Replica state changed")
	}
}

// IsConnected returns whether the replica is connected to the master
func (r *Replica) IsConnected() bool {
	return r.connected.Load()
}

// SetConnected sets the connection status
func (r *Replica) SetConnected(connected bool) {
	r.connected.Store(connected)
}

// Run starts the read-only replica and blocks until shutdown
func Run() {
	log.Debug().Msg("Initializing telemetry")
	telemetry.InitializeTelemetry()

	// Create replica instance
	replica := NewReplica(
		cfg.Config.Replica.MasterAddress,
		cfg.Config.NodeID,
		cfg.Config.DataDir,
	)

	// Initialize HLC clock
	replica.clock = hlc.NewClock(cfg.Config.NodeID)

	// Migrate legacy single-database layout if present
	legacyDBPath := filepath.Join(cfg.Config.DataDir, "marmot.db")
	if err := db.MigrateFromLegacy(legacyDBPath, cfg.Config.DataDir, cfg.Config.NodeID, replica.clock); err != nil {
		log.Warn().Err(err).Msg("Legacy database migration failed")
	}

	// Initialize database manager
	log.Info().Msg("Initializing database manager")
	dbMgr, err := db.NewDatabaseManager(cfg.Config.DataDir, cfg.Config.NodeID, replica.clock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database manager")
		return
	}
	replica.dbManager = dbMgr
	defer dbMgr.Close()

	// Initialize stream client
	log.Info().Msg("Initializing stream client")
	replica.streamClient = NewStreamClient(
		replica.masterAddr,
		replica.nodeID,
		dbMgr,
		replica.clock,
		replica,
	)

	// Bootstrap: sync initial data from master
	replica.SetState(StateBootstrapping)
	log.Info().Msg("Starting bootstrap from master")

	bootstrapCtx, bootstrapCancel := context.WithTimeout(
		replica.ctx,
		time.Duration(cfg.Config.Replica.InitialSyncTimeoutMin)*time.Minute,
	)
	if err := replica.streamClient.Bootstrap(bootstrapCtx); err != nil {
		bootstrapCancel()
		log.Fatal().Err(err).Msg("Failed to bootstrap from master")
		return
	}
	bootstrapCancel()
	log.Info().Msg("Bootstrap completed successfully")

	// Start streaming changes in background
	replica.SetState(StateStreaming)
	go replica.streamClient.Start(replica.ctx)

	// Initialize read-only handler
	replica.handler = NewReadOnlyHandler(dbMgr, replica.clock, replica)

	// Initialize query pipeline
	if err := protocol.InitializePipeline(
		cfg.Config.QueryPipeline.TranspilerCacheSize,
		cfg.Config.QueryPipeline.ValidatorPoolSize,
	); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize query pipeline")
		return
	}

	// Start MySQL server with read-only handler
	log.Info().Msg("Starting MySQL server (read-only mode)")
	mysqlServer := protocol.NewMySQLServer(
		fmt.Sprintf("%s:%d", cfg.Config.MySQL.BindAddress, cfg.Config.MySQL.Port),
		replica.handler,
	)

	if err := mysqlServer.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start MySQL server")
		return
	}
	defer mysqlServer.Stop()

	log.Info().
		Uint64("node_id", cfg.Config.NodeID).
		Str("master", cfg.Config.Replica.MasterAddress).
		Int("mysql_port", cfg.Config.MySQL.Port).
		Str("data_dir", cfg.Config.DataDir).
		Msg("Read-only replica is operational")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info().Msg("Shutdown signal received, stopping replica...")
	replica.SetState(StateStopped)
	replica.cancel()
	replica.streamClient.Stop()

	log.Info().Msg("Read-only replica stopped")
}
