package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// Benchmark 13.1: validateStatements with 100 statements
func BenchmarkValidateStatements(b *testing.B) {
	stmts := CreateCDCStatements(100, "users")
	txn := NewTxnBuilder().
		WithStatements(stmts).
		Build()

	wc := &WriteCoordinator{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = wc.validateStatements(txn)
	}
}

// Benchmark 13.2: buildPrepareRequest with allocation tracking
func BenchmarkBuildPrepareRequest(b *testing.B) {
	stmts := CreateCDCStatements(50, "users")
	txn := NewTxnBuilder().
		WithStatements(stmts).
		WithRequiredSchemaVersion(42).
		Build()

	wc := &WriteCoordinator{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = wc.buildPrepareRequest(txn)
	}
}

// Benchmark 13.3: executePreparePhase with 10 nodes
func BenchmarkExecutePreparePhase(b *testing.B) {
	ctx := context.Background()
	stmts := CreateCDCStatements(10, "users")
	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithStatements(stmts).
		Build()

	// Create mock with 10 nodes
	mock := newMockReplicator()
	successResp := CreateSuccessResponse()
	for i := uint64(1); i <= 10; i++ {
		mock.SetNodeResponse(i, successResp)
	}

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	wc := NewWriteCoordinator(1, nodeProvider, mock, mock, 5*time.Second, hlc.NewClock(1))

	req := wc.buildPrepareRequest(txn)
	otherNodes := []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wc.executePreparePhase(ctx, txn, req, otherNodes, false)
	}
}

// Benchmark 13.4: waitForRemoteQuorum with high channel throughput
func BenchmarkWaitForRemoteQuorum(b *testing.B) {
	const numResponses = 100
	const quorumNeeded = 51

	nodeProvider := newMockNodeProvider([]uint64{1})
	wc := NewWriteCoordinator(1, nodeProvider, newMockReplicator(), newMockReplicator(), 5*time.Second, hlc.NewClock(1))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		commitChan := make(chan response, numResponses)

		// Pre-fill channel with responses
		for j := 0; j < numResponses; j++ {
			commitChan <- response{
				nodeID: uint64(j + 2),
				resp:   CreateSuccessResponse(),
				err:    nil,
			}
		}
		close(commitChan)
		b.StartTimer()

		_, _ = wc.waitForRemoteQuorum(commitChan, numResponses, quorumNeeded, 1)
	}
}

// Benchmark 13.5: Full write transaction end-to-end
func BenchmarkFullWriteTransaction(b *testing.B) {
	ctx := context.Background()
	stmts := CreateCDCStatements(20, "users")

	// Setup 5-node cluster
	mock := newMockReplicator()
	successResp := CreateSuccessResponse()
	for i := uint64(1); i <= 5; i++ {
		mock.SetNodeResponse(i, successResp)
		mock.SetNodeCommitResponse(i, successResp)
	}

	nodeProvider := newMockNodeProvider([]uint64{1, 2, 3, 4, 5})
	wc := NewWriteCoordinator(1, nodeProvider, mock, mock, 5*time.Second, hlc.NewClock(1))
	cluster, _ := GetClusterState(nodeProvider, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := NewTxnBuilder().
			WithID(uint64(i + 1)).
			WithNodeID(1).
			WithStatements(stmts).
			WithStartTS(hlc.Timestamp{WallTime: int64(time.Now().UnixNano()), NodeID: 1}).
			Build()

		_, _ = wc.runPreparePhase(ctx, txn, cluster, []uint64{2, 3, 4, 5})
		// Note: Full commit phase benchmarked separately to avoid cascading failures
	}
}
