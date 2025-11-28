package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// WriteCoordinator orchestrates distributed writes with quorum requirements
// Implements pre-commit replication: replicate FIRST, then commit locally
// Uses full database replication model where ALL nodes receive ALL writes
//
// Architecture Notes:
// - Full database replication: Every node gets a complete copy
// - Quorum-based commits: Returns success after majority ACK
// - Straggler catch-up: Dead nodes sync via snapshots + delta logs
// - Extensible: Can support multiple SQLite databases per node (future)
type WriteCoordinator struct {
	nodeID          uint64
	nodeProvider    NodeProvider
	replicator      Replicator
	localReplicator Replicator
	timeout         time.Duration
	clock           *hlc.Clock
}

// Replicator sends replication requests to remote nodes
type Replicator interface {
	ReplicateTransaction(ctx context.Context, nodeID uint64, req *ReplicationRequest) (*ReplicationResponse, error)
}

// Transaction represents a distributed transaction
type Transaction struct {
	ID                    uint64
	NodeID                uint64
	Statements            []protocol.Statement
	StartTS               hlc.Timestamp
	CommitTS              hlc.Timestamp
	WriteConsistency      protocol.ConsistencyLevel
	ReadConsistency       protocol.ConsistencyLevel
	Database              string // Target database name
	RequiredSchemaVersion uint64 // Minimum schema version required to execute this transaction
	// MutationGuards holds per-table hash lists for conflict detection
	// Key is table name, value is XXH64 hash list + expected row count
	MutationGuards map[string]*MutationGuard
	// LocalExecutionDone indicates that local execution was already performed
	// (e.g., via ExecuteLocalWithHooks). Skip local replication in WriteTransaction.
	LocalExecutionDone bool
}

// MutationGuard contains hash list data for conflict detection
// Uses XXH64 hashes for exact conflict detection (no false positives)
type MutationGuard struct {
	KeyHashes        []uint64 // XXH64 hashes of affected row keys
	ExpectedRowCount int64
}

// ReplicationRequest is sent to replica nodes
type ReplicationRequest struct {
	TxnID      uint64
	NodeID     uint64
	Statements []protocol.Statement
	StartTS    hlc.Timestamp
	// Phase indicates transaction phase
	Phase ReplicationPhase
	// Target database name
	Database string
	// Minimum schema version required to execute this transaction
	RequiredSchemaVersion uint64
	// MutationGuards for conflict detection (MutationGuard protocol)
	MutationGuards map[string]*MutationGuard
}

// ReplicationPhase indicates which phase of 2PC
type ReplicationPhase int

const (
	PhasePrep   ReplicationPhase = iota // Prepare: create write intents
	PhaseCommit                         // Commit: finalize transaction
	PhaseAbort                          // Abort: rollback transaction
)

// ReplicationResponse from replica nodes
type ReplicationResponse struct {
	Success bool
	Error   string
	// ConflictDetected indicates write-write conflict
	ConflictDetected bool
	ConflictDetails  string
}

// NewWriteCoordinator creates a new write coordinator for full database replication
func NewWriteCoordinator(nodeID uint64, nodeProvider NodeProvider, replicator Replicator, localReplicator Replicator,
	timeout time.Duration, clock *hlc.Clock) *WriteCoordinator {

	return &WriteCoordinator{
		nodeID:          nodeID,
		nodeProvider:    nodeProvider,
		replicator:      replicator,
		localReplicator: localReplicator,
		timeout:         timeout,
		clock:           clock,
	}
}

// WriteTransaction executes a distributed write transaction with full database replication
// This is the CRITICAL path: Pre-commit quorum writes with conflict detection
//
// Replication Model:
// - Coordinator broadcasts to ALL nodes (including self)
// - Waits for QUORUM acknowledgments (e.g., 2 out of 3)
// - Commits locally + returns success after quorum
// - Background replication continues to stragglers
// - Dead nodes catch up via snapshot + delta logs when they rejoin
func (wc *WriteCoordinator) WriteTransaction(ctx context.Context, txn *Transaction) error {
	log.Trace().
		Uint64("txn_id", txn.ID).
		Int("stmt_count", len(txn.Statements)).
		Msg("2PC: WriteTransaction started")

	// Get all alive nodes for full replication
	allNodes, err := wc.nodeProvider.GetAliveNodes()
	if err != nil {
		return fmt.Errorf("failed to get alive nodes: %w", err)
	}

	clusterSize := len(allNodes)
	if clusterSize == 0 {
		return fmt.Errorf("no alive nodes in cluster")
	}

	log.Trace().
		Uint64("txn_id", txn.ID).
		Int("cluster_size", clusterSize).
		Msg("2PC: Cluster state")

	// Validate consistency level against cluster size
	if err := ValidateConsistencyLevel(txn.WriteConsistency, clusterSize); err != nil {
		return fmt.Errorf("invalid write consistency: %w", err)
	}

	// Calculate required quorum BEFORE replication
	requiredQuorum := QuorumSize(txn.WriteConsistency, clusterSize)

	// Separate other nodes from self for replication
	// Self will be handled separately (we're the coordinator)
	otherNodes := make([]uint64, 0, clusterSize-1)
	for _, nodeID := range allNodes {
		if nodeID != wc.nodeID {
			otherNodes = append(otherNodes, nodeID)
		}
	}

	// ====================
	// PHASE 1: PREPARE (Create Write Intents)
	// ====================
	// Single-attempt prepare with MutationGuard conflict detection.
	// On conflict: abort and return MySQL 1213 to client for retry.
	// No internal retry - client handles retry at application level.

	prepReq := &ReplicationRequest{
		TxnID:                 txn.ID,
		NodeID:                wc.nodeID,
		Statements:            txn.Statements,
		StartTS:               txn.StartTS,
		Phase:                 PhasePrep,
		Database:              txn.Database,
		RequiredSchemaVersion: txn.RequiredSchemaVersion,
		MutationGuards:        txn.MutationGuards,
	}

	skipLocalReplication := txn.LocalExecutionDone

	// Execute prepare phase - single attempt, no retry
	prepResponses, conflictErr := wc.executePreparePhase(ctx, txn, prepReq, otherNodes, skipLocalReplication)

	// If local execution already done, count self as an ACK
	if skipLocalReplication {
		prepResponses[wc.nodeID] = &ReplicationResponse{Success: true}
	}

	if conflictErr != nil {
		// Write-write conflict detected - abort and signal client to retry
		log.Debug().
			Uint64("txn_id", txn.ID).
			Err(conflictErr).
			Msg("Conflict detected - aborting transaction, client should retry")
		wc.abortTransaction(ctx, allNodes, txn.ID, txn.Database)
		// Return MySQL 1213 (ER_LOCK_DEADLOCK) - standard signal for client retry
		return protocol.ErrDeadlock()
	}

	// Check if quorum was achieved
	totalAcks := len(prepResponses)
	if totalAcks < requiredQuorum {
		log.Warn().
			Uint64("txn_id", txn.ID).
			Int("total_acks", totalAcks).
			Int("required_quorum", requiredQuorum).
			Int("cluster_size", clusterSize).
			Msg("Prepare quorum not achieved - aborting transaction")

		wc.abortTransaction(ctx, allNodes, txn.ID, txn.Database)
		return fmt.Errorf("prepare quorum not achieved: got %d acks, need %d out of %d nodes",
			totalAcks, requiredQuorum, clusterSize)
	}

	// ====================
	// PHASE 2: COMMIT
	// ====================
	// Quorum of write intents created successfully with no conflicts
	// Commit to quorum nodes (excluding self if local execution done) and return success
	// Background replication continues to remaining nodes via anti-entropy
	commitReq := &ReplicationRequest{
		TxnID:    txn.ID,
		Phase:    PhaseCommit,
		StartTS:  txn.StartTS,
		NodeID:   wc.nodeID,
		Database: txn.Database,
	}

	// Count nodes to commit (exclude self if local execution already done)
	commitNodes := 0
	for nodeID := range prepResponses {
		if nodeID != wc.nodeID || !skipLocalReplication {
			commitNodes++
		}
	}

	// Send commit to all prepared nodes (excluding self if local execution done)
	commitChan := make(chan response, commitNodes)
	for nodeID := range prepResponses {
		// Skip local commit if local execution already done - pendingExec.Commit() handles it
		if nodeID == wc.nodeID && skipLocalReplication {
			continue
		}
		go func(nid uint64) {
			var resp *ReplicationResponse
			var err error
			if nid == wc.nodeID {
				resp, err = wc.localReplicator.ReplicateTransaction(ctx, nid, commitReq)
			} else {
				resp, err = wc.replicator.ReplicateTransaction(ctx, nid, commitReq)
			}
			commitChan <- response{nodeID: nid, resp: resp, err: err}
		}(nodeID)
	}

	// Wait for QUORUM commits only (not all prepared nodes)
	commitResponses := make(map[uint64]*ReplicationResponse)

	// If local execution done, count self as committed (will be committed by pendingExec.Commit())
	if skipLocalReplication {
		commitResponses[wc.nodeID] = &ReplicationResponse{Success: true}
	}

	// Collect commit responses until quorum achieved or all responses received
	for i := 0; i < commitNodes && len(commitResponses) < requiredQuorum; i++ {
		select {
		case r := <-commitChan:
			if r.err == nil && r.resp != nil && r.resp.Success {
				commitResponses[r.nodeID] = r.resp
			} else {
				log.Error().Err(r.err).Uint64("node_id", r.nodeID).Msg("Commit failed")
			}
		case <-time.After(wc.timeout):
			log.Warn().Msg("Commit response timed out. Will be repaired by anti-entropy.")
		}
	}

	totalCommitAcks := len(commitResponses)

	// Commit phase also requires quorum for durability guarantees.
	// If commit quorum fails, transaction is partially committed -
	// anti-entropy will eventually repair stragglers.
	if totalCommitAcks < requiredQuorum {
		return fmt.Errorf("commit quorum degraded: got %d acks, expected %d (will be repaired by anti-entropy)",
			totalCommitAcks, requiredQuorum)
	}

	return nil
}

// abortTransaction sends abort message to all replica nodes
func (wc *WriteCoordinator) abortTransaction(ctx context.Context, nodeIDs []uint64, txnID uint64, database string) {
	abortReq := &ReplicationRequest{
		TxnID:    txnID,
		Phase:    PhaseAbort,
		Database: database,
	}

	// Best effort - send abort to all nodes
	abortTimeout := 2 * time.Second // Default
	if cfg.Config != nil {
		abortTimeout = time.Duration(cfg.Config.Coordinator.AbortTimeoutMS) * time.Millisecond
	}

	for _, nodeID := range nodeIDs {
		go func(nid uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), abortTimeout)
			defer cancel()
			if nid == wc.nodeID {
				wc.localReplicator.ReplicateTransaction(ctx, nid, abortReq)
			} else {
				wc.replicator.ReplicateTransaction(ctx, nid, abortReq)
			}
		}(nodeID)
	}
}

// response is a helper struct for collecting async replication responses
type response struct {
	nodeID uint64
	resp   *ReplicationResponse
	err    error
}

// executePreparePhase broadcasts prepare requests to all nodes and collects responses.
// Returns successful responses and a conflict error if any node reports a conflict.
func (wc *WriteCoordinator) executePreparePhase(ctx context.Context, txn *Transaction, prepReq *ReplicationRequest,
	otherNodes []uint64, skipLocalReplication bool) (map[uint64]*ReplicationResponse, error) {

	// Calculate total nodes to contact
	totalNodes := len(otherNodes)
	if !skipLocalReplication {
		totalNodes++ // Include self
	}

	if totalNodes == 0 {
		return nil, fmt.Errorf("no nodes to replicate to")
	}

	// Channel for collecting responses
	prepChan := make(chan response, totalNodes)

	// Send to other nodes
	for _, nodeID := range otherNodes {
		go func(nid uint64) {
			resp, err := wc.replicator.ReplicateTransaction(ctx, nid, prepReq)
			prepChan <- response{nodeID: nid, resp: resp, err: err}
		}(nodeID)
	}

	// Send to self if not skipped
	if !skipLocalReplication {
		go func() {
			resp, err := wc.localReplicator.ReplicateTransaction(ctx, wc.nodeID, prepReq)
			prepChan <- response{nodeID: wc.nodeID, resp: resp, err: err}
		}()
	}

	// Collect responses
	prepResponses := make(map[uint64]*ReplicationResponse)
	var conflictErr error

	for i := 0; i < totalNodes; i++ {
		select {
		case r := <-prepChan:
			if r.err != nil {
				log.Debug().
					Uint64("txn_id", txn.ID).
					Uint64("node_id", r.nodeID).
					Err(r.err).
					Msg("Prepare failed for node")
				continue
			}
			if r.resp == nil {
				continue
			}
			if r.resp.ConflictDetected {
				// Any conflict -> return error, client will retry
				conflictErr = fmt.Errorf("conflict on node %d: %s", r.nodeID, r.resp.ConflictDetails)
				log.Debug().
					Uint64("txn_id", txn.ID).
					Uint64("node_id", r.nodeID).
					Str("details", r.resp.ConflictDetails).
					Msg("Write conflict detected - client should retry")
				continue
			}
			if r.resp.Success {
				prepResponses[r.nodeID] = r.resp
			} else {
				log.Debug().
					Uint64("txn_id", txn.ID).
					Uint64("node_id", r.nodeID).
					Str("error", r.resp.Error).
					Msg("Prepare returned failure")
			}
		case <-time.After(wc.timeout):
			log.Warn().
				Uint64("txn_id", txn.ID).
				Msg("Prepare phase timeout waiting for responses")
		}
	}

	// If any conflict was detected, return the error
	if conflictErr != nil {
		return prepResponses, conflictErr
	}

	return prepResponses, nil
}
