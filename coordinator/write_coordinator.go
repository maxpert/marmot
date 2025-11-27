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
	conflictHandler ConflictHandler
	timeout         time.Duration
}

// Replicator sends replication requests to remote nodes
type Replicator interface {
	ReplicateTransaction(ctx context.Context, nodeID uint64, req *ReplicationRequest) (*ReplicationResponse, error)
}

// ConflictHandler handles write-write conflicts
type ConflictHandler interface {
	OnConflict(txn *Transaction, conflictErr error) error
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
	timeout time.Duration) *WriteCoordinator {

	return &WriteCoordinator{
		nodeID:          nodeID,
		nodeProvider:    nodeProvider,
		replicator:      replicator,
		localReplicator: localReplicator,
		conflictHandler: &DefaultConflictHandler{}, // Initialized conflict handler
		timeout:         timeout,
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
	// Broadcast to ALL nodes (self + others, unless local execution already done)
	// Wait for QUORUM acknowledgments (includes self)

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

	// Replicate to other nodes
	// We use a channel to collect responses
	type response struct {
		nodeID uint64
		resp   *ReplicationResponse
		err    error
	}

	// Determine if we need to replicate to self
	// If LocalExecutionDone is true, skip local replication - pendingExec will handle commit
	skipLocalReplication := txn.LocalExecutionDone
	totalNodes := len(otherNodes)
	if !skipLocalReplication {
		totalNodes++ // Include self
	}

	respChan := make(chan response, totalNodes)

	// Send to other nodes
	for _, nodeID := range otherNodes {
		go func(nid uint64) {
			resp, err := wc.replicator.ReplicateTransaction(ctx, nid, prepReq)
			respChan <- response{nodeID: nid, resp: resp, err: err}
		}(nodeID)
	}

	// Send to self (local) only if local execution not already done
	if !skipLocalReplication {
		go func() {
			resp, err := wc.localReplicator.ReplicateTransaction(ctx, wc.nodeID, prepReq)
			respChan <- response{nodeID: wc.nodeID, resp: resp, err: err}
		}()
	}

	// Collect responses from all nodes before proceeding to commit phase
	prepResponses := make(map[uint64]*ReplicationResponse)

	// If local execution already done, count self as an ACK
	if skipLocalReplication {
		prepResponses[wc.nodeID] = &ReplicationResponse{Success: true}
	}

	// Wait for all nodes to respond to ensure consistent commit phase
	// Note: Early exit optimization was removed because it caused nodes that responded
	// after quorum to be excluded from the commit phase, leading to failures
	for i := 0; i < totalNodes; i++ {
		select {
		case r := <-respChan:
			if r.err != nil {
				log.Error().Err(r.err).Uint64("node_id", r.nodeID).Msg("Prepare failed")
			} else if r.resp != nil && r.resp.Success {
				prepResponses[r.nodeID] = r.resp
			} else {
				errorMsg := "unknown"
				if r.resp != nil {
					errorMsg = r.resp.Error
				}
				log.Warn().
					Uint64("node_id", r.nodeID).
					Uint64("txn_id", txn.ID).
					Str("error", errorMsg).
					Msg("Prepare returned unsuccessful response")
			}
		case <-ctx.Done():
			return fmt.Errorf("prepare timeout")
		}
	}

	// Check for conflicts from remote nodes
	for nodeID, resp := range prepResponses {
		if resp.ConflictDetected {
			log.Warn().
				Uint64("node_id", nodeID).
				Uint64("txn_id", txn.ID).
				Str("conflict_details", resp.ConflictDetails).
				Msg("Write-write conflict detected - aborting transaction")

			wc.abortTransaction(ctx, allNodes, txn.ID)
			return &protocol.MySQLError{
				Code:     1205, // ER_LOCK_WAIT_TIMEOUT
				SQLState: "HY000",
				Message:  fmt.Sprintf("write-write conflict detected on node %d: %s (TRANSACTION ABORTED)", nodeID, resp.ConflictDetails),
			}
		}
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

		wc.abortTransaction(ctx, allNodes, txn.ID)
		return fmt.Errorf("prepare quorum not achieved: got %d acks, need %d out of %d nodes (TRANSACTION ABORTED)",
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

	for i := 0; i < commitNodes; i++ {
		select {
		case r := <-commitChan:
			if r.err == nil && r.resp != nil && r.resp.Success {
				commitResponses[r.nodeID] = r.resp

				// OPTIMIZATION: Return early once quorum achieved
				if len(commitResponses) >= requiredQuorum {
					goto commitQuorumAchieved
				}
			} else {
				log.Error().Err(r.err).Uint64("node_id", r.nodeID).Msg("Commit failed")
			}
		case <-time.After(wc.timeout):
			// Timeout on commit - check if we already have quorum
			if len(commitResponses) >= requiredQuorum {
				log.Warn().Msg("Commit timeout for some nodes, but quorum achieved")
				goto commitQuorumAchieved
			}
			log.Warn().Msg("Commit response timed out for one or more nodes. Will be repaired by anti-entropy.")
		}
	}

commitQuorumAchieved:

	// Count commit acks
	totalCommitAcks := len(commitResponses)

	// We already achieved quorum in PREPARE, so commit phase is best-effort
	// As long as we have majority, we're good
	// Stragglers will catch up via anti-entropy or snapshots
	if totalCommitAcks < requiredQuorum {
		// This is a critical failure - some nodes committed, some didn't
		// Log warning but don't fail the transaction (quorum was achieved in PREPARE)
		// Anti-entropy will fix stragglers
		return fmt.Errorf("commit quorum degraded: got %d acks, expected %d (PARTIAL COMMIT - will be repaired by anti-entropy)",
			totalCommitAcks, requiredQuorum)
	}

	return nil
}

// abortTransaction sends abort message to all replica nodes
func (wc *WriteCoordinator) abortTransaction(ctx context.Context, nodeIDs []uint64, txnID uint64) {
	abortReq := &ReplicationRequest{
		TxnID: txnID,
		Phase: PhaseAbort,
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
			wc.replicator.ReplicateTransaction(ctx, nid, abortReq)
		}(nodeID)
	}
}
