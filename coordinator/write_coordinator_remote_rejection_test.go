package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// ========================================
// Remote PREPARE rejection over the wire
//
// A remote participant's deterministic PREPARE rejection (e.g. DDL SQLite
// cannot apply) must not be treated as a mere missing ACK once quorum has
// already failed to form: retrying cannot change that participant's verdict.
// But the same rejection must carry no veto power while quorum is otherwise
// achieved - a single node cannot block cluster DDL.
// ========================================

// TestExecutePreparePhase_RemoteRejectionTracked verifies executePreparePhase
// surfaces a remote (non-local) explicit rejection via its third return value,
// while leaving the map/err results used for quorum counting unaffected - a
// remote rejection is not itself a final answer.
func TestExecutePreparePhase_RemoteRejectionTracked(t *testing.T) {
	InitTestTelemetry()

	mock := newMockReplicator()
	wc := &WriteCoordinator{
		nodeID:          1,
		replicator:      mock,
		localReplicator: mock,
		timeout:         100 * time.Millisecond,
	}

	txn := NewTxnBuilder().WithID(200).Build()
	req := &ReplicationRequest{
		TxnID:    txn.ID,
		NodeID:   wc.nodeID,
		Phase:    PhasePrep,
		Database: txn.Database,
	}

	mock.SetNodeResponse(2, CreateRejectionResponse("duplicate column name: creation_date"))
	otherNodes := []uint64{2, 3}

	ctx, cancel := WithTimeout(200)
	defer cancel()

	responses, remoteRejection, err := wc.executePreparePhase(ctx, txn, req, otherNodes, false)

	AssertNoError(t, err)
	if remoteRejection == nil {
		t.Fatal("expected a tracked remote rejection, got nil")
	}
	if remoteRejection.NodeID != 2 {
		t.Errorf("rejecting node: got %d, want 2", remoteRejection.NodeID)
	}
	if remoteRejection.Reason != "duplicate column name: creation_date" {
		t.Errorf("reason: got %q, want %q", remoteRejection.Reason, "duplicate column name: creation_date")
	}

	// The rejecting node must not appear as prepared, but the rejection is not
	// a final verdict at this layer - the map still holds the nodes that did
	// prepare successfully.
	AssertNodeNotInResponses(t, responses, 2)
	AssertResponseSuccess(t, responses, wc.nodeID)
	AssertResponseSuccess(t, responses, 3)
}

// TestRunPreparePhase_QuorumFailureWithRemoteRejectionIsFinal is the policy
// test: when quorum fails to form AND a remote participant explicitly
// rejected, the coordinator must return a final, non-retryable error carrying
// that participant's own reason - not the retryable QuorumNotAchievedError.
func TestRunPreparePhase_QuorumFailureWithRemoteRejectionIsFinal(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Quorum for 5 nodes is 3. Node 2 succeeds (2 total acks with coordinator).
	// Node 3 explicitly rejects (deterministic DDL verdict). Nodes 4,5 time out.
	remoteReplicator.SetNodeResponse(3, CreateRejectionResponse("duplicate column name: creation_date"))
	remoteReplicator.SetNodeResponse(4, nil)
	remoteReplicator.SetNodeResponse(5, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 50*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(201).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	var quorumErr *QuorumNotAchievedError
	if errors.As(err, &quorumErr) {
		t.Fatalf("expected a non-retryable RemotePrepareRejectedError, got the retryable QuorumNotAchievedError: %v", err)
	}

	var rejectedErr *RemotePrepareRejectedError
	if !errors.As(err, &rejectedErr) {
		t.Fatalf("expected *RemotePrepareRejectedError, got %T: %v", err, err)
	}
	if rejectedErr.NodeID != 3 {
		t.Errorf("rejecting node: got %d, want 3", rejectedErr.NodeID)
	}
	if rejectedErr.Reason != "duplicate column name: creation_date" {
		t.Errorf("reason: got %q, want %q", rejectedErr.Reason, "duplicate column name: creation_date")
	}

	// The client must receive the real statement error (1060/42S21), not a
	// retry signal (1213), or it retries forever against a verdict that can
	// never change.
	mysqlErr := protocol.ConvertToMySQLError(err)
	if mysqlErr.Code != protocol.ErrCodeDupFieldName {
		t.Errorf("MySQL error code: got %d, want %d (ErrCodeDupFieldName)", mysqlErr.Code, protocol.ErrCodeDupFieldName)
	}
	if mysqlErr.SQLState != protocol.SQLStateDupColumn {
		t.Errorf("MySQL SQLSTATE: got %q, want %q", mysqlErr.SQLState, protocol.SQLStateDupColumn)
	}
}

// TestRunPreparePhase_QuorumFailureWithoutRejectionStaysRetryable is the
// regression guard: quorum failing on plain timeouts/errors, with no
// participant issuing an explicit rejection, must keep returning the
// retryable QuorumNotAchievedError exactly as before this change.
func TestRunPreparePhase_QuorumFailureWithoutRejectionStaysRetryable(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	remoteReplicator.SetNodeResponse(3, nil)
	remoteReplicator.SetNodeResponse(4, nil)
	remoteReplicator.SetNodeResponse(5, nil)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 50*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(202).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4, 5}

	ctx := context.Background()
	_, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertQuorumNotAchievedError(t, err, "prepare", 2, 3)

	var rejectedErr *RemotePrepareRejectedError
	if errors.As(err, &rejectedErr) {
		t.Fatalf("did not expect a RemotePrepareRejectedError when nothing rejected: %v", err)
	}
}

// TestRunPreparePhase_RemoteRejectionDoesNotVetoAchievedQuorum is the "no veto
// power" half of the policy: a remote rejection must never abort a
// transaction that otherwise reaches quorum.
func TestRunPreparePhase_RemoteRejectionDoesNotVetoAchievedQuorum(t *testing.T) {
	InitTestTelemetry()

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	localReplicator := newMockReplicator()
	remoteReplicator := newMockReplicator()

	// Quorum for 5 nodes is 3. Node 2 explicitly rejects, but nodes 3 and 4
	// succeed alongside the coordinator, so quorum (3) is still reached.
	remoteReplicator.SetNodeResponse(2, CreateRejectionResponse("duplicate column name: creation_date"))

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, hlc.NewClock(1))

	txn := NewTxnBuilder().WithID(203).Build()
	cluster, _ := GetClusterState(nodeProvider, txn.WriteConsistency)
	otherNodes := []uint64{2, 3, 4}

	ctx := context.Background()
	responses, err := wc.runPreparePhase(ctx, txn, cluster, otherNodes)

	AssertNoError(t, err)
	AssertNodeNotInResponses(t, responses, 2)
	AssertResponseSuccess(t, responses, wc.nodeID)
	AssertResponseSuccess(t, responses, 3)
	AssertResponseSuccess(t, responses, 4)
}
