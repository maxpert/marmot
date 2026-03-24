package main

import (
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/maxpert/marmot/notify"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/publisher"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

// ShutdownOrchestrator coordinates graceful shutdown of all cluster-mode services
// in the correct order, allowing in-flight requests to complete before teardown.
type ShutdownOrchestrator struct {
	grpcServer         *marmotgrpc.Server
	gossip             *marmotgrpc.GossipProtocol
	registry           *marmotgrpc.NodeRegistry
	mysqlServer        *protocol.MySQLServer
	ddlLockMgr         *coordinator.DDLLockManager
	dbManager          *db.DatabaseManager
	notifierHub        *notify.Hub
	publisherReg       *publisher.Registry
	antiEntropy        *marmotgrpc.AntiEntropyService
	metricsCollector   *telemetry.MetricsCollector
	forwardSessMgr     *marmotgrpc.ForwardSessionManager
	coordinatorHandler *coordinator.CoordinatorHandler
	gracePeriod        time.Duration
}

// Shutdown executes the ordered graceful shutdown sequence.
// Each step is wrapped to prevent a single failure from aborting the rest.
func (o *ShutdownOrchestrator) Shutdown() {
	// Mark self LEAVING so peers stop routing new work here.
	o.runStep("mark self leaving", func() {
		if err := o.registry.MarkSelfLeaving(); err != nil {
			log.Error().Err(err).Msg("Failed to mark self leaving")
		}
	})

	// Reject new mutations with ER_SERVER_SHUTDOWN.
	if o.coordinatorHandler != nil {
		o.runStep("set coordinator draining", func() {
			o.coordinatorHandler.SetDraining(true)
		})
	}

	// Broadcast departure to peers (2 rounds for propagation).
	o.runStep("broadcast departure", func() {
		o.gossip.BroadcastDeparture(2)
	})

	// Stop MySQL listener — no new connections accepted.
	o.runStep("stop MySQL listener", func() {
		o.mysqlServer.Stop()
	})

	// Release all DDL locks so other coordinators can proceed.
	o.runStep("release DDL locks", func() {
		o.ddlLockMgr.ReleaseAll()
	})

	// Drain active MySQL connections — waits up to gracePeriod for in-flight
	// queries to finish, then force-closes any remaining connections.
	o.runStep("drain MySQL connections", func() {
		o.mysqlServer.GracefulDrain(o.gracePeriod)
	})

	// Stop gossip — no more membership messages.
	o.runStep("stop gossip protocol", func() {
		o.gossip.Stop()
	})

	// Stop anti-entropy — no more sync rounds.
	o.runStep("stop anti-entropy", func() {
		o.antiEntropy.Stop()
	})

	if o.forwardSessMgr != nil {
		o.runStep("stop forward session manager", func() {
			o.forwardSessMgr.Stop()
		})
	}

	if o.publisherReg != nil {
		o.runStep("stop publisher registry", func() {
			o.publisherReg.Stop()
		})
	}

	if o.metricsCollector != nil {
		o.runStep("stop metrics collector", func() {
			o.metricsCollector.Stop()
		})
	}

	if o.notifierHub != nil {
		o.runStep("close CDC notifier hub", func() {
			o.notifierHub.Close()
		})
	}

	// Close database manager — all SQLite + PebbleDB stores.
	o.runStep("close database manager", func() {
		if err := o.dbManager.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing database manager")
		}
	})

	// Stop gRPC server last — other services may need gRPC during drain.
	o.runStep("stop gRPC server", func() {
		o.grpcServer.Stop()
	})

	log.Info().Msg("Shutdown complete")
}

// runStep executes fn, recovering from any panic so a single step failure does
// not abort the remaining shutdown sequence.
func (o *ShutdownOrchestrator) runStep(name string, fn func()) {
	log.Info().Str("step", name).Msg("Shutdown step")
	defer func() {
		if r := recover(); r != nil {
			log.Error().Str("step", name).Interface("panic", r).Msg("Panic during shutdown step")
		}
	}()
	fn()
}
