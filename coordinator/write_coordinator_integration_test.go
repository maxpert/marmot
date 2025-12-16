package coordinator

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// Section 10: Full Transaction State Machines

// Test_10_6_SingleNode tests transaction flow in a single-node cluster
func Test_10_6_SingleNode(t *testing.T) {
	nodes := []uint64{1}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		WithDatabase("test").
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	AssertNoError(t, err)

	// Single node cluster: quorum = 1
	// Expect: 1 PREPARE + 1 COMMIT = 2 calls
	AssertCallCount(t, replicator, 2)
	AssertPhaseCallCount(t, replicator, PhasePrep, 1)
	AssertPhaseCallCount(t, replicator, PhaseCommit, 1)
}

// Test_10_7_TwoNode tests transaction flow in a two-node cluster
func Test_10_7_TwoNode(t *testing.T) {
	nodes := []uint64{1, 2}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		WithDatabase("test").
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	AssertNoError(t, err)

	// Two node cluster: quorum = 2 (both nodes must succeed)
	// Expect: 2 PREPARE + 2 COMMIT = 4 calls
	callCount := replicator.GetCallCount()
	if callCount < 4 {
		t.Errorf("Expected at least 4 calls for two-node cluster, got %d", callCount)
	}
}

// Test_10_7_TwoNode_OneNodeFails tests that two-node cluster fails when one node fails
func Test_10_7_TwoNode_OneNodeFails(t *testing.T) {
	nodes := []uint64{1, 2}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Node 2 fails during prepare
	replicator.SetNodeResponse(2, &ReplicationResponse{Success: false, Error: "node down"})

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		WithDatabase("test").
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail: only 1 node (coordinator), need 2 for quorum
	if err == nil {
		t.Fatal("Expected transaction to fail in two-node cluster when one node fails")
	}

	AssertError(t, err, &QuorumNotAchievedError{})
}

// Section 11: Network Partition & Failures

// Test_11_1_PartitionDuringPrepare tests network partition during prepare phase
func Test_11_1_PartitionDuringPrepare(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Partition nodes 3, 4, 5 mid-prepare (simulate timeout)
	replicator.SetNodeResponse(3, nil) // nil = timeout/unreachable
	replicator.SetNodeResponse(4, nil)
	replicator.SetNodeResponse(5, nil)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		100*time.Millisecond, // Short timeout
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail: coordinator + node 2 = 2 nodes, need 3 for quorum
	if err == nil {
		t.Fatal("Expected transaction to fail when 3 nodes partitioned during prepare")
	}

	AssertQuorumNotAchievedError(t, err, "prepare", 2, 3)
}

// Test_11_2_PartitionDuringCommit tests network partition during commit phase
func Test_11_2_PartitionDuringCommit(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// All nodes succeed during prepare
	// But nodes 3, 4, 5 fail during commit
	remoteReplicator.SetNodeCommitResponse(3, &ReplicationResponse{Success: false, Error: "partitioned"})
	remoteReplicator.SetNodeCommitResponse(4, &ReplicationResponse{Success: false, Error: "partitioned"})
	remoteReplicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "partitioned"})

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		remoteReplicator,
		localReplicator,
		100*time.Millisecond,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail: need 2 remote commits, only get 1 (node 2)
	// remoteQuorumNeeded = quorum(3) - 1 = 2
	if err == nil {
		t.Fatal("Expected transaction to fail when nodes partitioned during commit")
	}

	AssertPartialCommitError(t, err, false)

	// Verify no local commit happened
	localCalls := localReplicator.GetCalls()
	for _, call := range localCalls {
		if call.Request.Phase == PhaseCommit {
			t.Error("Local commit should not happen when remote quorum fails")
		}
	}
}

// Test_11_3_CoordinatorPartitioned tests coordinator isolation
func Test_11_3_CoordinatorPartitioned(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	remoteReplicator := newMockReplicator()
	localReplicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// All remote nodes fail (coordinator is isolated)
	remoteReplicator.SetNodeResponse(2, nil)
	remoteReplicator.SetNodeResponse(3, nil)
	remoteReplicator.SetNodeResponse(4, nil)
	remoteReplicator.SetNodeResponse(5, nil)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		remoteReplicator,
		localReplicator,
		100*time.Millisecond,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail: only coordinator (1 node), need 3 for quorum
	if err == nil {
		t.Fatal("Expected transaction to fail when coordinator is isolated")
	}

	AssertQuorumNotAchievedError(t, err, "prepare", 1, 3)
}

// Test_11_4_SplitBrain_3x3 tests split-brain prevention in 6-node cluster split evenly
func Test_11_4_SplitBrain_3x3(t *testing.T) {
	// Partition 1: nodes 1, 2, 3
	partitionNodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(partitionNodes)
	nodeProvider.SetTotalMembership(6) // Total cluster is 6 nodes

	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail: quorum requires 4 (majority of 6), but only 3 nodes reachable
	if err == nil {
		t.Fatal("Expected transaction to fail in 3x3 split-brain scenario")
	}

	AssertQuorumNotAchievedError(t, err, "prepare", 3, 4)

	t.Log("Split-brain prevention successful: 3x3 partition cannot achieve quorum")
}

// Test_11_5_SplitBrain_4x2 tests split-brain where 4-node partition succeeds
func Test_11_5_SplitBrain_4x2(t *testing.T) {
	// Partition 1: nodes 1, 2, 3, 4 (majority)
	partitionNodes := []uint64{1, 2, 3, 4}
	nodeProvider := newMockNodeProvider(partitionNodes)
	nodeProvider.SetTotalMembership(6) // Total cluster is 6 nodes

	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should succeed: quorum requires 4 (majority of 6), and 4 nodes are reachable
	AssertNoError(t, err)

	t.Log("Split-brain prevention successful: 4x2 partition can achieve quorum")
}

// Test_11_6_FlappingNode tests node that flaps during transaction
func Test_11_6_FlappingNode(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Node 5 succeeds on prepare but fails on commit (simulates flapping)
	replicator.SetNodeCommitResponse(5, &ReplicationResponse{Success: false, Error: "node flapped"})

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should succeed: coordinator + nodes 2,3,4 = 4 nodes (quorum = 3)
	// remoteQuorumNeeded = 2, and we get 3 remote acks during prepare
	// Even if node 5 fails during commit, we still have enough
	AssertNoError(t, err)
}

// Test_11_7_SlowNetwork tests transaction with high network latency
func Test_11_7_SlowNetwork(t *testing.T) {
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Configure 200ms latency for all operations
	replicator.mu.Lock()
	replicator.prepareLatency = 200 * time.Millisecond
	replicator.commitLatency = 200 * time.Millisecond
	replicator.mu.Unlock()

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		5*time.Second, // Long enough timeout
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	start := time.Now()
	err := coordinator.WriteTransaction(ctx, txn)

	AssertNoError(t, err)

	// Should take at least 200ms for prepare (parallel) + 200ms for commit
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Errorf("Expected transaction to take at least 200ms with slow network, took %v", elapsed)
	}
}

// Test_11_7_SlowNetwork_Timeout tests transaction timeout with slow network
func Test_11_7_SlowNetwork_Timeout(t *testing.T) {
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Configure 500ms latency (longer than timeout)
	replicator.mu.Lock()
	replicator.prepareLatency = 500 * time.Millisecond
	replicator.mu.Unlock()

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		100*time.Millisecond, // Timeout shorter than latency
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should fail due to timeout
	if err == nil {
		t.Fatal("Expected transaction to fail due to timeout")
	}
}

// Test_11_8_NetworkAsymmetry tests asymmetric network latency
func Test_11_8_NetworkAsymmetry(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Node 5 is very slow (300ms), but others are fast
	replicator.SetDelay(1, 300*time.Millisecond) // Only affects txn ID 1 on node 5

	// Configure specific response for node 5 with delay
	slowResponse := &ReplicationResponse{Success: true}
	replicator.SetNodeResponse(5, slowResponse)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second, // Timeout longer than slow node delay
		clock,
	)

	txn := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn)

	// Should succeed: quorum achieved before slow node responds
	// coordinator + nodes 2,3,4 = 4 nodes (quorum = 3)
	AssertNoError(t, err)
}

// Section 12: Concurrency & Races

// Test_12_3_ConcurrentMapAccess tests concurrent access to prepare responses
func Test_12_3_ConcurrentMapAccess(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	// Run 10 concurrent transactions
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			txn := NewTxnBuilder().
				WithID(txnID).
				WithNodeID(1).
				WithCDCStatement("users", map[string][]byte{"id": {byte(txnID)}}, map[string][]byte{"id": {byte(txnID + 1)}}).
				Build()

			ctx := context.Background()
			err := coordinator.WriteTransaction(ctx, txn)
			if err != nil {
				t.Logf("Transaction %d failed: %v", txnID, err)
			}
		}(uint64(i))
	}

	wg.Wait()

	t.Log("Concurrent map access test completed - run with -race flag")
}

// Test_12_4_ConcurrentTelemetry tests concurrent telemetry updates
func Test_12_4_ConcurrentTelemetry(t *testing.T) {
	InitTestTelemetry()

	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	// Run 100 concurrent transactions
	const concurrentTxns = 100
	var wg sync.WaitGroup
	var successCount, failureCount atomic.Int64

	for i := 0; i < concurrentTxns; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			txn := NewTxnBuilder().
				WithID(txnID).
				WithNodeID(1).
				WithCDCStatement("users", map[string][]byte{"id": {byte(txnID)}}, map[string][]byte{"id": {byte(txnID + 1)}}).
				Build()

			ctx := context.Background()
			err := coordinator.WriteTransaction(ctx, txn)
			if err != nil {
				failureCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(uint64(i))
	}

	wg.Wait()

	t.Logf("Completed %d concurrent transactions: %d success, %d failure",
		concurrentTxns, successCount.Load(), failureCount.Load())

	if successCount.Load()+failureCount.Load() != concurrentTxns {
		t.Errorf("Transaction count mismatch: %d + %d != %d",
			successCount.Load(), failureCount.Load(), concurrentTxns)
	}
}

// Test_12_5_GoroutineLeaks tests for goroutine leaks with many transactions
func Test_12_5_GoroutineLeaks(t *testing.T) {
	defer CheckGoroutines(t)()

	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		100*time.Millisecond, // Short timeout to speed up test
		clock,
	)

	// Run 1000 transactions to detect goroutine leaks
	const totalTxns = 1000
	var wg sync.WaitGroup

	for i := 0; i < totalTxns; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			txn := NewTxnBuilder().
				WithID(txnID).
				WithNodeID(1).
				WithCDCStatement("users", map[string][]byte{"id": {byte(txnID % 256)}}, map[string][]byte{"id": {byte((txnID + 1) % 256)}}).
				Build()

			ctx := context.Background()
			_ = coordinator.WriteTransaction(ctx, txn) // Error intentionally ignored - testing goroutine leaks
		}(uint64(i))
	}

	wg.Wait()

	t.Logf("Completed %d transactions", totalTxns)
	// CheckGoroutines deferred function will verify no leaks
}

// Test_12_1_ConcurrentWrites_DifferentRows tests concurrent writes to different rows
func Test_12_1_ConcurrentWrites_DifferentRows(t *testing.T) {
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	const concurrentWrites = 10
	var wg sync.WaitGroup
	var successCount atomic.Int64

	for i := 0; i < concurrentWrites; i++ {
		wg.Add(1)
		go func(rowID uint64) {
			defer wg.Done()

			txn := NewTxnBuilder().
				WithID(rowID).
				WithNodeID(1).
				WithCDCStatement("users",
					map[string][]byte{"id": {byte(rowID)}},
					map[string][]byte{"id": {byte(rowID)}, "name": []byte("user")}).
				Build()

			ctx := context.Background()
			err := coordinator.WriteTransaction(ctx, txn)
			if err == nil {
				successCount.Add(1)
			}
		}(uint64(i))
	}

	wg.Wait()

	// All writes to different rows should succeed
	if successCount.Load() != concurrentWrites {
		t.Errorf("Expected %d successful writes to different rows, got %d",
			concurrentWrites, successCount.Load())
	}
}

// Test_12_2_ConcurrentWrites_SameRow tests concurrent writes to same row (conflict detection)
func Test_12_2_ConcurrentWrites_SameRow(t *testing.T) {
	nodes := []uint64{1, 2, 3}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Configure conflicts on specific transactions to simulate same-row writes
	// First transaction (ID=1) succeeds, subsequent ones conflict
	for txnID := uint64(2); txnID <= 10; txnID++ {
		replicator.SetConflict(2, txnID, "write-write conflict on row 1")
	}

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	const concurrentWrites = 10
	var wg sync.WaitGroup
	var successCount, conflictCount, otherFailures atomic.Int64

	for i := 1; i <= concurrentWrites; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			txn := NewTxnBuilder().
				WithID(txnID).
				WithNodeID(1).
				WithCDCStatement("users",
					map[string][]byte{"id": {1}}, // Same row ID
					map[string][]byte{"id": {1}, "value": {byte(txnID)}}).
				Build()

			ctx := context.Background()
			err := coordinator.WriteTransaction(ctx, txn)
			if err == nil {
				successCount.Add(1)
			} else {
				errMsg := err.Error()
				if contains(errMsg, "deadlock") || contains(errMsg, "Deadlock") {
					conflictCount.Add(1)
				} else {
					otherFailures.Add(1)
				}
			}
		}(uint64(i))
	}

	wg.Wait()

	// At least one should succeed, others should conflict (deadlock)
	if successCount.Load() == 0 {
		t.Error("Expected at least one write to succeed")
	}

	if conflictCount.Load() == 0 {
		t.Error("Expected at least one write to conflict (deadlock)")
	}

	t.Logf("Same-row concurrent writes: %d success, %d conflicts, %d other failures",
		successCount.Load(), conflictCount.Load(), otherFailures.Load())
}

// Test_12_5_GoroutineLeaks_WithTimeouts tests goroutine cleanup with timeouts
func Test_12_5_GoroutineLeaks_WithTimeouts(t *testing.T) {
	defer CheckGoroutines(t)()

	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Make some nodes timeout
	replicator.SetNodeResponse(4, nil)
	replicator.SetNodeResponse(5, nil)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		50*time.Millisecond, // Short timeout
		clock,
	)

	// Run 100 transactions with timeouts
	const totalTxns = 100
	var wg sync.WaitGroup

	for i := 0; i < totalTxns; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			txn := NewTxnBuilder().
				WithID(txnID).
				WithNodeID(1).
				WithCDCStatement("users", map[string][]byte{"id": {byte(txnID % 256)}}, map[string][]byte{"id": {byte((txnID + 1) % 256)}}).
				Build()

			ctx := context.Background()
			_ = coordinator.WriteTransaction(ctx, txn) // Error intentionally ignored - testing goroutine leaks with timeouts
		}(uint64(i))
	}

	wg.Wait()

	t.Logf("Completed %d transactions with timeouts", totalTxns)
}

// Test_12_3_ConcurrentMapAccess_WithConflicts tests concurrent map access with conflicts
func Test_12_3_ConcurrentMapAccess_WithConflicts(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	// Configure conflicts on some transactions
	replicator.SetConflict(2, 2, "conflict on txn 2")
	replicator.SetConflict(3, 4, "conflict on txn 4")
	replicator.SetConflict(2, 6, "conflict on txn 6")

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	// Run 20 concurrent transactions with some conflicts
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			txn := NewTxnBuilder().
				WithID(txnID).
				WithNodeID(1).
				WithCDCStatement("users", map[string][]byte{"id": {byte(txnID)}}, map[string][]byte{"id": {byte(txnID + 1)}}).
				Build()

			ctx := context.Background()
			_ = coordinator.WriteTransaction(ctx, txn) // Error intentionally ignored - testing race conditions
		}(uint64(i))
	}

	wg.Wait()

	t.Log("Concurrent map access with conflicts test completed - run with -race flag")
}

// Test_NetworkPartition_Recovery tests recovery after partition heals
func Test_NetworkPartition_Recovery(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	nodeProvider := newMockNodeProvider(nodes)
	replicator := newMockReplicator()
	clock := hlc.NewClock(1)

	coordinator := NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		replicator,
		1*time.Second,
		clock,
	)

	// First transaction: nodes 3,4,5 are partitioned
	replicator.SetNodeResponse(3, nil)
	replicator.SetNodeResponse(4, nil)
	replicator.SetNodeResponse(5, nil)

	txn1 := NewTxnBuilder().
		WithID(1).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	ctx := context.Background()
	err := coordinator.WriteTransaction(ctx, txn1)

	// Should fail
	if err == nil {
		t.Fatal("Expected first transaction to fail during partition")
	}

	// Heal the partition
	replicator.mu.Lock()
	delete(replicator.responses, 3)
	delete(replicator.responses, 4)
	delete(replicator.responses, 5)
	replicator.mu.Unlock()

	// Second transaction: all nodes available
	txn2 := NewTxnBuilder().
		WithID(2).
		WithNodeID(1).
		WithCDCStatement("users", map[string][]byte{"id": {2}}, map[string][]byte{"id": {3}}).
		Build()

	err = coordinator.WriteTransaction(ctx, txn2)

	// Should succeed
	AssertNoError(t, err)

	t.Log("Partition recovery successful")
}
