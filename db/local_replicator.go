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
}

// NewLocalReplicator creates a new local replicator
func NewLocalReplicator(nodeID uint64, dbMgr DatabaseProvider, clock *hlc.Clock) *LocalReplicator {
	return &LocalReplicator{
		nodeID: nodeID,
		dbMgr:  dbMgr,
		clock:  clock,
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
	// Get database
	mvccDB, err := lr.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}, nil
	}

	txnMgr := mvccDB.GetTransactionManager()
	txn, err := txnMgr.BeginTransaction(req.NodeID)
	if err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	// Override ID
	originalID := txn.ID
	txn.ID = req.TxnID
	txn.StartTS = req.StartTS

	txnMgr.UpdateTransactionID(originalID, req.TxnID)

	// Update DB record
	_, err = mvccDB.GetDB().Exec(`
		UPDATE __marmot__txn_records
		SET txn_id = ?
		WHERE txn_id = ?
	`, req.TxnID, originalID)
	if err != nil {
		txnMgr.AbortTransaction(txn)
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	for _, stmt := range req.Statements {
		if err := txnMgr.AddStatement(txn, stmt); err != nil {
			txnMgr.AbortTransaction(txn)
			return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
		}

		rowKey := extractRowKeyFromStatement(stmt)
		dataSnapshot, _ := SerializeData(map[string]interface{}{
			"sql":       stmt.SQL,
			"type":      stmt.Type,
			"timestamp": req.StartTS.WallTime,
		})

		if err := txnMgr.WriteIntent(txn, stmt.TableName, rowKey, stmt, dataSnapshot); err != nil {
			txnMgr.AbortTransaction(txn)
			return &coordinator.ReplicationResponse{
				Success:          false,
				Error:            err.Error(),
				ConflictDetected: true,
				ConflictDetails:  err.Error(),
			}, nil
		}
	}

	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (lr *LocalReplicator) handleCommit(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Get database
	mvccDB, err := lr.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}, nil
	}

	txnMgr := mvccDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(req.TxnID)
	if txn == nil {
		return &coordinator.ReplicationResponse{Success: false, Error: "transaction not found"}, nil
	}

	if err := txnMgr.CommitTransaction(txn); err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (lr *LocalReplicator) handleAbort(ctx context.Context, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Get database
	mvccDB, err := lr.dbMgr.GetDatabase(req.Database)
	if err != nil {
		// If database doesn't exist, consider abort successful
		return &coordinator.ReplicationResponse{Success: true}, nil
	}

	txnMgr := mvccDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(req.TxnID)
	if txn == nil {
		return &coordinator.ReplicationResponse{Success: true}, nil
	}

	if err := txnMgr.AbortTransaction(txn); err != nil {
		return &coordinator.ReplicationResponse{Success: false, Error: err.Error()}, nil
	}

	return &coordinator.ReplicationResponse{Success: true}, nil
}
