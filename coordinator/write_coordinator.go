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
}

// ReplicationRequest is sent to replica nodes
type ReplicationRequest struct {
	TxnID                 uint64
	NodeID                uint64
	Statements            []protocol.Statement
	StartTS               hlc.Timestamp
	// Phase indicates transaction phase
	Phase                 ReplicationPhase
	// Target database name
	Database              string
	// Minimum schema version required to execute this transaction
	RequiredSchemaVersion uint64
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
	// Get all alive nodes for full replication
	allNodes, err := wc.nodeProvider.GetAliveNodes()
	if err != nil {
		return fmt.Errorf("failed to get alive nodes: %w", err)
	}

	clusterSize := len(allNodes)
	if clusterSize == 0 {
		return fmt.Errorf("no alive nodes in cluster")
	}

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
	// Broadcast to ALL nodes (self + others)
	// Wait for QUORUM acknowledgments (includes self)

	prepReq := &ReplicationRequest{
		TxnID:                 txn.ID,
		NodeID:                wc.nodeID,
		Statements:            txn.Statements,
		StartTS:               txn.StartTS,
		Phase:                 PhasePrep,
		Database:              txn.Database,
		RequiredSchemaVersion: txn.RequiredSchemaVersion,
	}

	// Replicate to other nodes
	// We use a channel to collect responses
	type response struct {
		nodeID uint64
		resp   *ReplicationResponse
		err    error
	}
	respChan := make(chan response, len(otherNodes)+1) // +1 for local

	// Send to other nodes
	for _, nodeID := range otherNodes {
		go func(nid uint64) {
			resp, err := wc.replicator.ReplicateTransaction(ctx, nid, prepReq)
			respChan <- response{nodeID: nid, resp: resp, err: err}
		}(nodeID)
	}

	// Send to self (local)
	go func() {
		resp, err := wc.localReplicator.ReplicateTransaction(ctx, wc.nodeID, prepReq)
		respChan <- response{nodeID: wc.nodeID, resp: resp, err: err}
	}()

	// Collect responses
	prepResponses := make(map[uint64]*ReplicationResponse)
	for i := 0; i < len(otherNodes)+1; i++ {
		select {
		case r := <-respChan:
			if r.err != nil {
				log.Error().Err(r.err).Uint64("node_id", r.nodeID).Msg("Prepare failed")
				continue
			}
			// Only count successful responses
			if r.resp != nil && r.resp.Success {
				prepResponses[r.nodeID] = r.resp
			} else {
				log.Warn().Uint64("node_id", r.nodeID).Msg("Prepare returned unsuccessful response")
			}
		case <-ctx.Done():
			return fmt.Errorf("prepare timeout")
		}
	}

	// Check for conflicts from remote nodes
	for nodeID, resp := range prepResponses {
		if resp.ConflictDetected {
			// Write-write conflict detected on remote node - ABORT
			wc.abortTransaction(ctx, allNodes, txn.ID) // Abort all nodes, including self if it prepared
			return fmt.Errorf("write-write conflict detected on node %d: %s (TRANSACTION ABORTED)",
				nodeID, resp.ConflictDetails)
		}
	}

	// Count responses: remote nodes + self
	totalAcks := len(prepResponses)

	// Check if quorum was achieved
	if totalAcks < requiredQuorum {
		// Quorum not achieved - abort
		wc.abortTransaction(ctx, allNodes, txn.ID) // Abort all nodes, including self if it prepared
		return fmt.Errorf("prepare quorum not achieved: got %d acks, need %d out of %d nodes (TRANSACTION ABORTED)",
			totalAcks, requiredQuorum, clusterSize)
	}

	// Quorum achieved! Proceed to commit phase

	// ====================
	// PHASE 2: COMMIT
	// ====================
	// Quorum of write intents created successfully with no conflicts
	// Commit to quorum nodes (including self) and return success
	// Background replication continues to remaining nodes via anti-entropy
	commitReq := &ReplicationRequest{
		TxnID:    txn.ID,
		Phase:    PhaseCommit,
		StartTS:  txn.StartTS,
		NodeID:   wc.nodeID,
		Database: txn.Database,
	}

	// Send commit to all prepared nodes (including self)
	commitChan := make(chan response, len(prepResponses))
	for nodeID := range prepResponses {
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

	// Wait for commits (best effort)
	commitResponses := make(map[uint64]*ReplicationResponse)
	for i := 0; i < len(prepResponses); i++ {
		select {
		case r := <-commitChan:
			if r.err == nil {
				commitResponses[r.nodeID] = r.resp
			} else {
				log.Error().Err(r.err).Uint64("node_id", r.nodeID).Msg("Commit failed")
			}
		case <-time.After(wc.timeout):
			// Timeout on commit is okay, we assume eventual consistency via recovery
			log.Warn().Msg("Commit response timed out for one or more nodes. Will be repaired by anti-entropy.")
		}
	}

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

	// Success! Transaction committed on quorum of nodes
	// Coordinator returns success to client
	// Background: Stragglers will catch up via anti-entropy
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
