package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"
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

// StagedPayloadCleaner is implemented by replicators that stage large payloads.
// CleanupStagedPayload is called when a transaction finishes (commit or abort).
type StagedPayloadCleaner interface {
	CleanupStagedPayload(txnID uint64)
}

// StreamReplicator extends Replicator with streaming capability for large payloads
type StreamReplicator interface {
	Replicator
	StreamReplicateTransaction(ctx context.Context, nodeID uint64, req *ReplicationRequest) (*ReplicationResponse, error)
}

// StreamChunkSizeDefault is the default payload size for streaming chunks (1MB)
const StreamChunkSizeDefault = 1024 * 1024

// GetStreamChunkSize returns the configured streaming chunk size in bytes.
// Falls back to StreamChunkSizeDefault (1MB) if not configured.
func GetStreamChunkSize() int {
	if cfg.Config != nil && cfg.Config.Replication.StreamChunkSizeKB > 0 {
		return cfg.Config.Replication.StreamChunkSizeKB * 1024
	}
	return StreamChunkSizeDefault
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
	// Rejected indicates the participant refused the statement itself (for example
	// DDL SQLite cannot apply). Timeouts and storage failures leave it false so they
	// stay retryable missing ACKs rather than a final verdict.
	Rejected bool
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
	metrics := NewTxnMetrics("write")
	telemetry.ActiveTransactions.Inc()
	defer telemetry.ActiveTransactions.Dec()
	defer wc.cleanupStagedPayloads(txn.ID)

	log.Trace().
		Uint64("txn_id", txn.ID).
		Int("stmt_count", len(txn.Statements)).
		Msg("2PC: WriteTransaction started")

	// 1. Validate statements
	if err := wc.validateStatements(txn); err != nil {
		return metrics.RecordFailure("failed", err)
	}

	// 2. Get cluster state
	cluster, err := GetClusterState(wc.nodeProvider, txn.WriteConsistency)
	if err != nil {
		return metrics.RecordFailure("failed", err)
	}

	log.Trace().
		Uint64("txn_id", txn.ID).
		Int("target_nodes", len(cluster.AliveNodes)).
		Int("total_membership", cluster.TotalMembership).
		Msg("2PC: Cluster state")

	// Separate other nodes from self for replication
	// Self will be handled separately (we're the coordinator)
	otherNodes := make([]uint64, 0, len(cluster.AliveNodes)-1)
	for _, nodeID := range cluster.AliveNodes {
		if nodeID != wc.nodeID {
			otherNodes = append(otherNodes, nodeID)
		}
	}

	// 3. Prepare phase
	prepResponses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)
	if err != nil {
		wc.abortTransaction(ctx, cluster.AliveNodes, txn.ID, txn.Database)
		return metrics.RecordFailure(wc.classifyPrepareError(err), err)
	}

	// 4. Commit phase - remote-first commit to prevent coordinator-only commits
	if err := wc.runCommitPhase(ctx, txn, cluster, prepResponses); err != nil {
		return metrics.RecordFailure("failed", err)
	}

	return metrics.RecordSuccess()
}

func (wc *WriteCoordinator) cleanupStagedPayloads(txnID uint64) {
	if cleaner, ok := wc.replicator.(StagedPayloadCleaner); ok {
		cleaner.CleanupStagedPayload(txnID)
	}
	if cleaner, ok := wc.localReplicator.(StagedPayloadCleaner); ok {
		cleaner.CleanupStagedPayload(txnID)
	}
}

// validateStatements validates that all CDC statements have required fields for replication
func (wc *WriteCoordinator) validateStatements(txn *Transaction) error {
	for i, stmt := range txn.Statements {
		isDML := isDMLStatement(stmt.Type)
		if isDML && len(stmt.EncodedRow) == 0 {
			return fmt.Errorf("DML statement missing encoded CDC row (stmt %d)", i)
		}
		if isDML && stmt.TableName == "" {
			log.Error().
				Uint64("txn_id", txn.ID).
				Int("stmt_index", i).
				Str("sql", stmt.SQL).
				Msg("CRITICAL: CDC statement missing TableName - this would fail on remote nodes")
			return fmt.Errorf("CDC statement missing TableName (stmt %d)", i)
		}
	}
	return nil
}

// buildPrepareRequest creates a ReplicationRequest for the prepare phase.
// DML CDC row images are included because PREPARE is the 2PC durability point:
// a participant that ACKs PREPARE must be able to commit after a crash.
func (wc *WriteCoordinator) buildPrepareRequest(txn *Transaction) *ReplicationRequest {
	prepareStmts := make([]protocol.Statement, len(txn.Statements))
	for i, stmt := range txn.Statements {
		isDML := isDMLStatement(stmt.Type)
		prepareStmts[i] = protocol.Statement{
			Type:         stmt.Type,
			TableName:    stmt.TableName,
			Database:     stmt.Database,
			IntentKey:    stmt.IntentKey,
			Operation:    stmt.Operation,
			EncodedRow:   stmt.EncodedRow,
			EncodedCodec: stmt.EncodedCodec,
		}
		if !isDML {
			prepareStmts[i].SQL = stmt.SQL
		}
		if isVectorControlStatement(stmt.Type) {
			prepareStmts[i].VectorIndexName = stmt.VectorIndexName
			prepareStmts[i].VectorColumnName = stmt.VectorColumnName
			prepareStmts[i].VectorMetric = stmt.VectorMetric
			prepareStmts[i].VectorDim = stmt.VectorDim
			prepareStmts[i].VectorNlist = stmt.VectorNlist
			prepareStmts[i].VectorNprobe = stmt.VectorNprobe
			prepareStmts[i].VectorMaxNorm = stmt.VectorMaxNorm
			prepareStmts[i].VectorIndexChange = stmt.VectorIndexChange
		}
		// LOAD DATA requires payload bytes during PREPARE so replicas can persist
		// durable intents with the same content before COMMIT.
		if stmt.Type == protocol.StatementLoadData {
			prepareStmts[i].LoadDataPayload = stmt.LoadDataPayload
		}
	}
	return &ReplicationRequest{
		TxnID:                 txn.ID,
		NodeID:                wc.nodeID,
		Statements:            prepareStmts,
		StartTS:               txn.StartTS,
		Phase:                 PhasePrep,
		Database:              txn.Database,
		RequiredSchemaVersion: txn.RequiredSchemaVersion,
	}
}

func isVectorControlStatement(stmtType protocol.StatementCode) bool {
	switch stmtType {
	case protocol.StatementCreateVectorIndex, protocol.StatementDropVectorIndex,
		protocol.StatementReindexVectorIndex, protocol.StatementVectorIndexControl:
		return true
	default:
		return false
	}
}

// buildCommitRequest creates a ReplicationRequest for the commit phase.
// DML CDC data was made durable during PREPARE; COMMIT only carries decision
// metadata. Non-DML statements still carry their control payloads.
func (wc *WriteCoordinator) buildCommitRequest(txn *Transaction) *ReplicationRequest {
	commitStmts := make([]protocol.Statement, len(txn.Statements))
	for i, stmt := range txn.Statements {
		commitStmts[i] = protocol.Statement{
			Type:      stmt.Type,
			TableName: stmt.TableName,
			Database:  stmt.Database,
			IntentKey: stmt.IntentKey,
		}
		if !isDMLStatement(stmt.Type) {
			commitStmts[i].SQL = stmt.SQL
			commitStmts[i].LoadDataPayload = stmt.LoadDataPayload
			commitStmts[i].VectorIndexName = stmt.VectorIndexName
			commitStmts[i].VectorColumnName = stmt.VectorColumnName
			commitStmts[i].VectorMetric = stmt.VectorMetric
			commitStmts[i].VectorDim = stmt.VectorDim
			commitStmts[i].VectorNlist = stmt.VectorNlist
			commitStmts[i].VectorNprobe = stmt.VectorNprobe
			commitStmts[i].VectorMaxNorm = stmt.VectorMaxNorm
			commitStmts[i].VectorIndexChange = stmt.VectorIndexChange
		}
	}
	return &ReplicationRequest{
		TxnID:      txn.ID,
		Phase:      PhaseCommit,
		StartTS:    txn.StartTS,
		NodeID:     wc.nodeID,
		Database:   txn.Database,
		Statements: commitStmts,
	}
}

func isDMLStatement(stmtType protocol.StatementCode) bool {
	switch stmtType {
	case protocol.StatementInsert, protocol.StatementUpdate, protocol.StatementDelete, protocol.StatementReplace:
		return true
	default:
		return false
	}
}

// txnHasDDL reports whether any statement is DDL. PREPARE validates DDL by
// executing the real statement in a rolled-back transaction, which can take
// far longer than a DML write, so a transaction carrying DDL needs the DDL
// validation timeout rather than the regular write timeout.
func txnHasDDL(stmts []protocol.Statement) bool {
	for _, stmt := range stmts {
		if stmt.Type == protocol.StatementDDL {
			return true
		}
	}
	return false
}

// estimateCDCPayloadSize estimates the CDC payload size for a transaction.
// Used to decide between regular RPC and streaming for large payloads.
func estimateCDCPayloadSize(statements []protocol.Statement) int {
	var size int
	for _, stmt := range statements {
		size += len(stmt.EncodedRow)
		size += len(stmt.IntentKey)
		size += len(stmt.TableName)
	}
	return size
}

// runPreparePhase executes the prepare phase and validates quorum requirements
func (wc *WriteCoordinator) runPreparePhase(ctx context.Context, txn *Transaction, cluster *ClusterState, otherNodes []uint64) (map[uint64]*ReplicationResponse, error) {
	prepReq := wc.buildPrepareRequest(txn)

	// Execute prepare phase on all nodes (including self) - single attempt, no retry
	// All nodes now participate uniformly - no skipLocalReplication
	prepResponses, remoteRejection, prepErr := wc.executePreparePhase(ctx, txn, prepReq, otherNodes, false)
	telemetry.TwoPhaseQuorumAcks.With("prepare").Observe(float64(len(prepResponses)))

	// A local rejection (invalid DDL, missing table, ...) is final - surface the
	// underlying reason so the client gets the matching protocol error instead of
	// a retry signal.
	var localPrepareErr *LocalPrepareError
	if errors.As(prepErr, &localPrepareErr) {
		// Logged above Debug because the error returned to the client carries only
		// the statement-level reason: this is where txn_id and reason are tied
		// together for operators. Rejections are rare, so this is not a hot path.
		log.Warn().
			Uint64("txn_id", txn.ID).
			Str("reason", localPrepareErr.Reason).
			Msg("Local PREPARE rejected transaction - aborting")

		return nil, &CoordinatorNotParticipatedError{TxnID: txn.ID, Err: localPrepareErr}
	}

	if prepErr != nil {
		// Write-write conflict detected - abort and signal client to retry
		telemetry.WriteConflictsTotal.With("intent", "slow").Inc()
		log.Debug().
			Uint64("txn_id", txn.ID).
			Err(prepErr).
			Msg("Conflict detected - aborting transaction, client should retry")
		// Return MySQL 1213 (ER_LOCK_DEADLOCK) - standard signal for client retry
		return nil, protocol.ErrDeadlock()
	}

	// Check if quorum was achieved
	totalAcks := len(prepResponses)
	if totalAcks < cluster.RequiredQuorum {
		// A remote rejection carries no veto on its own - it only replaces the
		// retry signal once quorum has separately failed to form. Retrying a
		// transaction one participant deterministically refused cannot succeed,
		// so surface that participant's own reason instead of a generic
		// quorum-not-achieved error the client would otherwise retry forever.
		if remoteRejection != nil {
			log.Warn().
				Uint64("txn_id", txn.ID).
				Uint64("node_id", remoteRejection.NodeID).
				Str("reason", remoteRejection.Reason).
				Int("total_acks", totalAcks).
				Int("required_quorum", cluster.RequiredQuorum).
				Msg("Prepare quorum not achieved and a remote participant rejected the transaction - aborting")

			return nil, remoteRejection
		}

		log.Warn().
			Uint64("txn_id", txn.ID).
			Int("total_acks", totalAcks).
			Int("required_quorum", cluster.RequiredQuorum).
			Int("total_membership", cluster.TotalMembership).
			Int("alive_nodes", len(cluster.AliveNodes)).
			Msg("Prepare quorum not achieved - aborting transaction")

		return nil, &QuorumNotAchievedError{
			Phase:           "prepare",
			AcksReceived:    totalAcks,
			QuorumRequired:  cluster.RequiredQuorum,
			TotalMembership: cluster.TotalMembership,
			AliveNodes:      len(cluster.AliveNodes),
			IsRemoteQuorum:  false,
		}
	}

	// CRITICAL: Verify coordinator participated in PREPARE phase
	// The coordinator MUST be part of the quorum for correctness
	_, selfPrepared := prepResponses[wc.nodeID]
	if !selfPrepared {
		log.Error().
			Uint64("txn_id", txn.ID).
			Int("total_acks", totalAcks).
			Int("required_quorum", cluster.RequiredQuorum).
			Msg("Coordinator PREPARE failed - coordinator must participate - aborting transaction")

		return nil, &CoordinatorNotParticipatedError{TxnID: txn.ID}
	}

	return prepResponses, nil
}

// sendRemoteCommits launches goroutines to send commit requests to remote nodes.
// Uses streaming for large payloads (≥128KB) if the replicator supports it.
// commitTimeout bounds each remote COMMIT RPC; the caller (runCommitPhase)
// decides its value from the transaction's statements so a DDL COMMIT gets
// the same DDL-aware budget PREPARE was given, instead of the fixed regular
// write timeout.
func (wc *WriteCoordinator) sendRemoteCommits(_ context.Context, preparedNodes map[uint64]*ReplicationResponse, req *ReplicationRequest, txnID uint64, commitTimeout time.Duration) chan response {
	// Count other prepared nodes (excluding self)
	otherPreparedNodes := 0
	for nodeID := range preparedNodes {
		if nodeID != wc.nodeID {
			otherPreparedNodes++
		}
	}

	// Check if we should use streaming for large payloads
	payloadSize := estimateCDCPayloadSize(req.Statements)
	streamThreshold := GetStreamChunkSize()
	useStreaming := payloadSize >= streamThreshold
	streamReplicator, hasStreaming := wc.replicator.(StreamReplicator)

	if useStreaming && hasStreaming {
		log.Debug().
			Uint64("txn_id", txnID).
			Int("payload_size", payloadSize).
			Int("threshold", streamThreshold).
			Msg("Using streaming for large payload")
	}

	// STEP 1: Send COMMIT to remote nodes first
	commitChan := make(chan response, otherPreparedNodes)
	for nodeID := range preparedNodes {
		if nodeID == wc.nodeID {
			continue // Don't commit locally yet
		}
		log.Debug().
			Uint64("txn_id", txnID).
			Uint64("target_node", nodeID).
			Msg("sending COMMIT to remote node")
		go func(nid uint64) {
			// Use detached context - remote commits should complete even if parent cancelled.
			// Bounded by commitTimeout (not wc.timeout): the remote already promised at
			// PREPARE it can commit, and for a DDL transaction that can take far longer
			// than the regular write timeout.
			commitCtx, commitCancel := context.WithTimeout(context.Background(), commitTimeout)
			defer commitCancel()

			var resp *ReplicationResponse
			var err error
			if useStreaming && hasStreaming {
				resp, err = streamReplicator.StreamReplicateTransaction(commitCtx, nid, req)
			} else {
				resp, err = wc.replicator.ReplicateTransaction(commitCtx, nid, req)
			}
			commitChan <- response{nodeID: nid, resp: resp, err: err}
		}(nodeID)
	}

	return commitChan
}

// waitForRemoteQuorum collects remote commit responses until quorum is achieved or timeout.
// commitTimeout must match the budget sendRemoteCommits gave each remote RPC:
// this collector gives up on a per-response basis with the same bound, so a
// mismatch here would abandon a response sendRemoteCommits is still waiting on.
func (wc *WriteCoordinator) waitForRemoteQuorum(commitChan chan response, otherPreparedNodes int, remoteQuorumNeeded int, txnID uint64, commitTimeout time.Duration) (map[uint64]*ReplicationResponse, int) {
	commitResponses := make(map[uint64]*ReplicationResponse)

	// STEP 2: Collect remote commit responses until we have enough for quorum
	remoteAcks := 0
	for i := 0; i < otherPreparedNodes; i++ {
		select {
		case r := <-commitChan:
			if r.err == nil && r.resp != nil && r.resp.Success {
				commitResponses[r.nodeID] = r.resp
				remoteAcks++
				log.Debug().
					Uint64("txn_id", txnID).
					Uint64("node_id", r.nodeID).
					Int("remote_acks", remoteAcks).
					Int("needed", remoteQuorumNeeded).
					Msg("Remote commit ACK received")
			} else {
				errMsg := ""
				if r.resp != nil {
					errMsg = r.resp.Error
				}
				log.Error().Err(r.err).Uint64("node_id", r.nodeID).Str("resp_error", errMsg).Msg("Remote commit failed")
			}
		case <-time.After(commitTimeout):
			log.Warn().
				Uint64("txn_id", txnID).
				Int("remote_acks", remoteAcks).
				Int("needed", remoteQuorumNeeded).
				Msg("Commit response timed out waiting for remote nodes")
		}

		// Check if we have enough remote ACKs to proceed with local commit
		if remoteAcks >= remoteQuorumNeeded {
			break
		}
	}

	return commitResponses, remoteAcks
}

// commitLocalAfterRemoteQuorum commits the transaction locally after remote quorum is achieved
func (wc *WriteCoordinator) commitLocalAfterRemoteQuorum(ctx context.Context, req *ReplicationRequest, txnID uint64) (*ReplicationResponse, error) {
	// STEP 4: Remote quorum achieved - now commit locally
	// This MUST succeed since we already PREPARED locally (we promised we can commit)
	// Note: selfPrepared is guaranteed to be true at this point (checked after PREPARE phase)
	log.Debug().
		Uint64("txn_id", txnID).
		Msg("Remote quorum achieved, committing locally")

	localResp, localErr := wc.localReplicator.ReplicateTransaction(ctx, wc.nodeID, req)
	if localErr != nil || localResp == nil || !localResp.Success {
		// This should NEVER happen - we PREPARED successfully, commit must succeed
		// If it does fail, we have a serious bug in PREPARE phase
		errMsg := ""
		if localResp != nil {
			errMsg = localResp.Error
		}
		log.Error().
			Err(localErr).
			Str("resp_error", errMsg).
			Uint64("txn_id", txnID).
			Msg("CRITICAL: Local commit failed after remote quorum achieved - node has diverged from the committed quorum")
		// CRITICAL: Don't abort remotes - they already committed. Return error but data is partially committed.
		// Prefer the participant's reported error: a failed COMMIT response carries
		// the reason in localResp.Error while localErr stays nil.
		reportedErr := localErr
		if reportedErr == nil && errMsg != "" {
			reportedErr = errors.New(errMsg)
		}
		return nil, &PartialCommitError{
			IsLocal:    true,
			LocalError: reportedErr,
		}
	}

	return localResp, nil
}

// runCommitPhase orchestrates the commit phase of 2PC.
// Quorum of write intents created successfully with no conflicts.
//
// CRITICAL: Commit to REMOTE nodes first, wait for quorum-1 ACKs, then commit locally.
// This ensures if remote quorum fails, coordinator hasn't committed yet (clean abort).
// After PREPARE ACK, commit MUST succeed (nodes promised they can commit).
func (wc *WriteCoordinator) runCommitPhase(ctx context.Context, txn *Transaction, cluster *ClusterState, prepResponses map[uint64]*ReplicationResponse) error {
	log.Debug().
		Uint64("txn_id", txn.ID).
		Int("prepared_nodes", len(prepResponses)).
		Msg("PREPARE phase complete, starting COMMIT")

	commitReq := wc.buildCommitRequest(txn)

	// Count other prepared nodes (excluding self)
	otherPreparedNodes := 0
	for nodeID := range prepResponses {
		if nodeID != wc.nodeID {
			otherPreparedNodes++
		}
	}

	// Calculate how many remote ACKs we need before committing locally
	// We need (quorum - 1) from remotes, then local commit gives us quorum
	// Note: selfPrepared is guaranteed to be true at this point (checked above)
	remoteQuorumNeeded := cluster.RequiredQuorum - 1 // We'll add local commit after

	// A transaction carrying DDL needs the same DDL-aware budget for COMMIT
	// that PREPARE was given: a participant that ACKed PREPARE promised it can
	// COMMIT, and applying DDL can take far longer than the regular write
	// timeout. Using the same writeTimeoutForStatements source of truth as
	// PREPARE keeps both phases consistent for a given transaction.
	commitTimeout := writeTimeoutForStatements(txn.Statements)

	// Send commit requests to remote nodes
	commitChan := wc.sendRemoteCommits(ctx, prepResponses, commitReq, txn.ID, commitTimeout)

	// Wait for remote quorum
	commitResponses, remoteAcks := wc.waitForRemoteQuorum(commitChan, otherPreparedNodes, remoteQuorumNeeded, txn.ID, commitTimeout)

	// STEP 3: Check if we got enough remote ACKs
	if remoteAcks < remoteQuorumNeeded {
		// CRITICAL BUG FIX: DO NOT abort after COMMIT phase has started!
		// Some nodes may have already committed. Aborting them is incorrect because:
		// 1. After successful PREPARE, nodes promised to commit
		// 2. Once they commit, their transaction state is cleaned up (txn=nil)
		// 3. Abort RPC on an already-committed node returns success without rollback
		// 4. This leads to partial commits and data inconsistency
		//
		// Correct behavior: Return error indicating partial commit.
		// The anti-entropy/recovery system will eventually resolve this inconsistency.
		log.Error().
			Uint64("txn_id", txn.ID).
			Int("remote_acks", remoteAcks).
			Int("needed", remoteQuorumNeeded).
			Int("remote_commits", len(commitResponses)).
			Msg("CRITICAL: Remote commit quorum not achieved - partial commit occurred")

		return &PartialCommitError{
			IsLocal:            false,
			RemoteAcks:         remoteAcks,
			RemoteQuorumNeeded: remoteQuorumNeeded,
		}
	}

	// Commit locally after remote quorum
	localResp, err := wc.commitLocalAfterRemoteQuorum(ctx, commitReq, txn.ID)
	if err != nil {
		return err
	}
	commitResponses[wc.nodeID] = localResp

	totalCommitAcks := len(commitResponses)
	telemetry.TwoPhaseQuorumAcks.With("commit").Observe(float64(totalCommitAcks))

	log.Debug().
		Uint64("txn_id", txn.ID).
		Int("total_acks", totalCommitAcks).
		Int("required", cluster.RequiredQuorum).
		Msg("COMMIT phase complete")

	return nil
}

// classifyPrepareError classifies the prepare phase error for telemetry
func (wc *WriteCoordinator) classifyPrepareError(err error) string {
	if mysqlErr, ok := err.(*protocol.MySQLError); ok && mysqlErr.Code == protocol.ErrCodeDeadlock {
		return "conflict"
	}
	return "failed"
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
			// Best-effort abort: errors are intentionally ignored because:
			// 1. Abort is fire-and-forget - we cannot recover from abort failures
			// 2. Nodes that don't receive abort will eventually timeout their prepared state
			// 3. Anti-entropy will resolve any inconsistencies
			if nid == wc.nodeID {
				_, _ = wc.localReplicator.ReplicateTransaction(ctx, nid, abortReq)
			} else {
				_, _ = wc.replicator.ReplicateTransaction(ctx, nid, abortReq)
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
// Returns successful responses, the first remote (non-local) explicit rejection seen (if
// any - the caller decides whether it matters, since a remote rejection alone must not
// veto a transaction that otherwise reaches quorum), and a conflict error if any node
// reports a conflict.
func (wc *WriteCoordinator) executePreparePhase(ctx context.Context, txn *Transaction, prepReq *ReplicationRequest,
	otherNodes []uint64, skipLocalReplication bool) (map[uint64]*ReplicationResponse, *RemotePrepareRejectedError, error) {

	// Calculate total nodes to contact
	totalNodes := len(otherNodes)
	if !skipLocalReplication {
		totalNodes++ // Include self
	}

	if totalNodes == 0 {
		// No nodes to contact - this is OK if local execution was already done
		// Return empty map, caller will add self if skipLocalReplication was true
		return make(map[uint64]*ReplicationResponse), nil, nil
	}

	// Channel for collecting responses
	prepChan := make(chan response, totalNodes)

	// A transaction carrying DDL needs the DDL validation timeout here, not the
	// regular write timeout: PREPARE executes the real statement in a
	// rolled-back transaction on every participant, including remote nodes -
	// the deadline set on prepCtx below propagates over gRPC as the incoming
	// context each participant validates under.
	prepTimeout := wc.timeout
	if txnHasDDL(txn.Statements) {
		prepTimeout = getDDLValidationTimeout()
	}

	// Every PREPARE call is bounded by the same timeout the collector gives up
	// after. Without it an abandoned call can still durably prepare a transaction
	// the coordinator has already stopped tracking. PREPARE does real work -
	// persisting intents and validating DDL - so this window is not theoretical.
	dispatch := func(nodeID uint64, replicator Replicator) {
		prepCtx, cancel := context.WithTimeout(ctx, prepTimeout)
		defer cancel()

		resp, err := replicator.ReplicateTransaction(prepCtx, nodeID, prepReq)
		prepChan <- response{nodeID: nodeID, resp: resp, err: err}
	}

	// Send to other nodes
	for _, nodeID := range otherNodes {
		go dispatch(nodeID, wc.replicator)
	}

	// Send to self if not skipped
	if !skipLocalReplication {
		go dispatch(wc.nodeID, wc.localReplicator)
	}

	// Collect responses
	prepResponses := make(map[uint64]*ReplicationResponse)
	var conflictErr error
	var localErr error
	var remoteRejection *RemotePrepareRejectedError

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
				// Only an explicit rejection is a final answer. Anything else - a
				// timeout, a storage error - must fall through to the quorum check so
				// it stays retryable, exactly as before DDL was validated here.
				if r.resp.Rejected {
					reason := r.resp.Error
					if r.nodeID == wc.nodeID {
						if reason == "" {
							reason = "local prepare rejected the transaction"
						}
						localErr = &LocalPrepareError{Reason: reason}
					} else if remoteRejection == nil {
						// A remote rejection does not by itself abort the transaction -
						// it has no veto over quorum. It is only surfaced by the caller
						// as a final answer if quorum also fails to form.
						if reason == "" {
							reason = "remote prepare rejected the transaction"
						}
						remoteRejection = &RemotePrepareRejectedError{NodeID: r.nodeID, Reason: reason}
					}
				}
			}
		case <-time.After(prepTimeout):
			log.Warn().
				Uint64("txn_id", txn.ID).
				Msg("Prepare phase timeout waiting for responses")
		}
	}

	// An explicit local rejection is deterministic, so it takes precedence over a
	// peer conflict: retrying the transaction cannot make it succeed. Transport
	// failures and missing responses are not rejections - they fall through to the
	// quorum check.
	if localErr != nil {
		return prepResponses, remoteRejection, localErr
	}

	// If any conflict was detected, return the error
	if conflictErr != nil {
		return prepResponses, remoteRejection, conflictErr
	}

	return prepResponses, remoteRejection, nil
}
