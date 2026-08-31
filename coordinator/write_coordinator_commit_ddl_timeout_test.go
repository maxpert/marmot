package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
)

// =============================================================================
// COMMIT-phase DDL-aware timeout tests.
//
// PREPARE for a transaction carrying DDL is bounded by the DDL validation
// timeout (getDDLValidationTimeout), not the regular write timeout, because
// PREPARE executes the real statement in a rolled-back transaction. A
// participant that ACKs PREPARE has promised it can COMMIT - so COMMIT must
// give that same participant at least as much time to actually apply the
// statement. These tests prove the COMMIT-phase remote RPC bound and the
// commit-response collector bound both follow the same DDL-aware budget as
// PREPARE, not the fixed regular write timeout (wc.timeout).
// =============================================================================

// TestSendRemoteCommits_UsesProvidedCommitTimeoutNotCoordinatorTimeout proves
// sendRemoteCommits bounds each remote COMMIT RPC using the commitTimeout
// explicitly passed by the caller, not wc.timeout. runCommitPhase is the
// caller that decides the DDL-aware budget; sendRemoteCommits must honor
// whatever it is given.
func TestSendRemoteCommits_UsesProvidedCommitTimeoutNotCoordinatorTimeout(t *testing.T) {
	defer CheckGoroutines(t)()
	InitTestTelemetry()

	recorder := &deadlineRecordingReplicator{}
	wc := &WriteCoordinator{
		nodeID:     1,
		replicator: recorder,
		// wc.timeout is deliberately tiny - the commit call must NOT be bounded by it.
		timeout: 10 * time.Millisecond,
	}

	preparedNodes := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(), // coordinator, excluded from remote commits
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}
	req := &ReplicationRequest{TxnID: 900, Phase: PhaseCommit}

	commitTimeout := 2 * time.Second
	start := time.Now()
	commitChan := wc.sendRemoteCommits(context.Background(), preparedNodes, req, 900, commitTimeout)

	// Drain the channel so goroutines complete before CheckGoroutines runs.
	for i := 0; i < 2; i++ {
		<-commitChan
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	if len(recorder.deadlines) != 2 {
		t.Fatalf("recorded deadlines: got %d, want 2", len(recorder.deadlines))
	}
	for i, deadline := range recorder.deadlines {
		budget := deadline.Sub(start)
		// A wide margin above wc.timeout (10ms) is required - not just
		// "greater than" - since scheduling jitter alone can push the recorded
		// budget past wc.timeout even when commitTimeout was NOT applied.
		if budget < 500*time.Millisecond {
			t.Errorf("call %d deadline budget %v is not comfortably above wc.timeout (%v) - commitTimeout was not applied", i, budget, wc.timeout)
		}
		if budget > commitTimeout+200*time.Millisecond {
			t.Errorf("call %d deadline budget %v exceeds the provided commitTimeout (%v)", i, budget, commitTimeout)
		}
	}
}

// TestRunCommitPhase_DDLTransactionSurvivesCommitDelayBeyondWriteTimeout is the
// ship-blocker regression test: a transaction carrying DDL that PREPAREs
// successfully across the cluster must also be able to COMMIT within the DDL
// validation timeout, even when a remote COMMIT RPC is slower than the
// regular write timeout. Before the fix, sendRemoteCommits bound every remote
// COMMIT RPC with a detached context.WithTimeout(context.Background(),
// wc.timeout) - the regular (short) write timeout - so a DDL COMMIT taking
// longer than that but well within the DDL validation timeout was abandoned,
// producing a PartialCommitError while the coordinator's own local commit
// still succeeded.
func TestRunCommitPhase_DDLTransactionSurvivesCommitDelayBeyondWriteTimeout(t *testing.T) {
	InitTestTelemetry()
	withDDLValidationTimeout(t, 2000) // 2s DDL validation timeout

	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()

	txn := NewTxnBuilder().WithID(910).WithNodeID(1).WithDatabase("test").
		WithDDLStatement("CREATE TABLE t (id INT)").Build()

	// Remote COMMIT for this txn takes 500ms: slower than the regular write
	// timeout (100ms) used below, well under the DDL validation timeout (2s).
	remoteReplicator.SetDelay(txn.ID, 500*time.Millisecond)

	// wc.timeout stands in for the regular write timeout (write_timeout_ms).
	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, nil)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}
	cluster := &ClusterState{AliveNodes: nodes, TotalMembership: 3, RequiredQuorum: 2}

	err := wc.runCommitPhase(context.Background(), txn, cluster, prepResponses)

	AssertNoError(t, err)

	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) != 1 {
		t.Errorf("expected exactly 1 local commit call, got %d", len(localCalls))
	}
}

// TestRunCommitPhase_DMLTransactionCommitStillTimesOutAtWriteTimeout is the
// control for the test above: a DML transaction (no DDL) must NOT inherit the
// DDL validation timeout for COMMIT. The same delay that a DDL transaction
// survives must still produce a PartialCommitError for a DML transaction,
// proving the regular write timeout still applies where it should.
func TestRunCommitPhase_DMLTransactionCommitStillTimesOutAtWriteTimeout(t *testing.T) {
	InitTestTelemetry()
	withDDLValidationTimeout(t, 2000)

	// writeTimeoutForStatements reads the regular write timeout from
	// cfg.Config (getWriteTimeout), not from wc.timeout directly - in
	// production the two are always the same value (marmot.go derives
	// wc.timeout from this same config field), so pin them together here too.
	originalWriteTimeoutMS := cfg.Config.Replication.WriteTimeoutMS
	cfg.Config.Replication.WriteTimeoutMS = 100
	t.Cleanup(func() { cfg.Config.Replication.WriteTimeoutMS = originalWriteTimeoutMS })

	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()

	txn := NewTxnBuilder().WithID(911).WithNodeID(1).WithDatabase("test").
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).Build()

	remoteReplicator.SetDelay(txn.ID, 500*time.Millisecond)

	wc := NewWriteCoordinator(1, nodeProvider, remoteReplicator, localReplicator, 100*time.Millisecond, nil)

	prepResponses := map[uint64]*ReplicationResponse{
		1: CreateSuccessResponse(),
		2: CreateSuccessResponse(),
		3: CreateSuccessResponse(),
	}
	cluster := &ClusterState{AliveNodes: nodes, TotalMembership: 3, RequiredQuorum: 2}

	err := wc.runCommitPhase(context.Background(), txn, cluster, prepResponses)

	AssertError(t, err, &PartialCommitError{})

	localCalls := localReplicator.GetPhaseCalls(PhaseCommit)
	if len(localCalls) > 0 {
		t.Errorf("expected local commit NOT to be attempted for a DML timeout, got %d calls", len(localCalls))
	}
}
