package db

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
)

// LocalReplicator implements coordinator.Replicator for local application
type LocalReplicator struct {
	nodeID uint64
	dbMgr  DatabaseProvider
	clock  *hlc.Clock
	engine *ReplicationEngine
}

// NewLocalReplicator creates a new local replicator
func NewLocalReplicator(nodeID uint64, dbMgr DatabaseProvider, clock *hlc.Clock) *LocalReplicator {
	return &LocalReplicator{
		nodeID: nodeID,
		dbMgr:  dbMgr,
		clock:  clock,
		engine: NewReplicationEngine(nodeID, dbMgr, clock),
	}
}

// ReplicateTransaction applies transaction locally
func (lr *LocalReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Update clock
	lr.clock.Update(req.StartTS)

	switch req.Phase {
	case coordinator.PhasePrep:
		return lr.handlePrepare(ctx, req)
	case coordinator.PhaseCommit:
		return lr.handleCommit(ctx, req)
	case coordinator.PhaseAbort:
		return lr.handleAbort(ctx, req)
	default:
		return &coordinator.ReplicationResponse{
			Success: false,
			Error:   fmt.Sprintf("unknown phase: %v", req.Phase),
		}, nil
	}
}

func (lr *LocalReplicator) handlePrepare(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Convert coordinator.ReplicationRequest to PrepareRequest
	engineReq := &PrepareRequest{
		TxnID:      req.TxnID,
		NodeID:     req.NodeID,
		StartTS:    req.StartTS,
		Database:   req.Database,
		Statements: req.Statements,
	}

	// Call engine
	result := lr.engine.Prepare(ctx, engineReq)

	// Convert result back to coordinator.ReplicationResponse
	return result.ToCoordinatorResponse(), nil
}

func (lr *LocalReplicator) handleCommit(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	engineReq := &CommitRequest{
		TxnID:      req.TxnID,
		Database:   req.Database,
		Statements: req.Statements, // CDC data deferred from PREPARE phase
	}

	result := lr.engine.Commit(ctx, engineReq)
	return result.ToCoordinatorResponse(), nil
}

func (lr *LocalReplicator) handleAbort(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	engineReq := &AbortRequest{
		TxnID:    req.TxnID,
		Database: req.Database,
	}

	result := lr.engine.Abort(ctx, engineReq)
	return result.ToCoordinatorResponse(), nil
}
