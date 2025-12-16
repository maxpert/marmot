package coordinator

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

func TestTxnBuilder(t *testing.T) {
	t.Run("DefaultValues", func(t *testing.T) {
		txn := NewTxnBuilder().Build()

		if txn.ID != 1 {
			t.Errorf("default ID: got %d, want 1", txn.ID)
		}
		if txn.NodeID != 1 {
			t.Errorf("default NodeID: got %d, want 1", txn.NodeID)
		}
		if txn.Database != "test" {
			t.Errorf("default Database: got %s, want test", txn.Database)
		}
		if len(txn.Statements) != 0 {
			t.Errorf("default Statements: got %d, want 0", len(txn.Statements))
		}
	})

	t.Run("WithID", func(t *testing.T) {
		txn := NewTxnBuilder().WithID(42).Build()
		if txn.ID != 42 {
			t.Errorf("ID: got %d, want 42", txn.ID)
		}
	})

	t.Run("WithNodeID", func(t *testing.T) {
		txn := NewTxnBuilder().WithNodeID(5).Build()
		if txn.NodeID != 5 {
			t.Errorf("NodeID: got %d, want 5", txn.NodeID)
		}
	})

	t.Run("WithDatabase", func(t *testing.T) {
		txn := NewTxnBuilder().WithDatabase("mydb").Build()
		if txn.Database != "mydb" {
			t.Errorf("Database: got %s, want mydb", txn.Database)
		}
	})

	t.Run("WithCDCStatement", func(t *testing.T) {
		oldVals := map[string][]byte{"id": {1}, "name": {2}}
		newVals := map[string][]byte{"id": {1}, "name": {3}}
		txn := NewTxnBuilder().
			WithCDCStatement("users", oldVals, newVals).
			Build()

		if len(txn.Statements) != 1 {
			t.Fatalf("statement count: got %d, want 1", len(txn.Statements))
		}

		stmt := txn.Statements[0]
		if stmt.TableName != "users" {
			t.Errorf("TableName: got %s, want users", stmt.TableName)
		}
		if len(stmt.OldValues) != 2 {
			t.Errorf("OldValues length: got %d, want 2", len(stmt.OldValues))
		}
		if len(stmt.NewValues) != 2 {
			t.Errorf("NewValues length: got %d, want 2", len(stmt.NewValues))
		}
	})

	t.Run("WithDDLStatement", func(t *testing.T) {
		txn := NewTxnBuilder().
			WithDDLStatement("CREATE TABLE test (id INT)").
			Build()

		if len(txn.Statements) != 1 {
			t.Fatalf("statement count: got %d, want 1", len(txn.Statements))
		}

		stmt := txn.Statements[0]
		if stmt.SQL != "CREATE TABLE test (id INT)" {
			t.Errorf("SQL: got %s, want CREATE TABLE test (id INT)", stmt.SQL)
		}
	})

	t.Run("WithMultipleStatements", func(t *testing.T) {
		oldVals1 := map[string][]byte{"id": {1}}
		newVals1 := map[string][]byte{"id": {2}}
		oldVals2 := map[string][]byte{"id": {3}}
		newVals2 := map[string][]byte{"id": {4}}
		txn := NewTxnBuilder().
			WithCDCStatement("users", oldVals1, newVals1).
			WithCDCStatement("posts", oldVals2, newVals2).
			WithDDLStatement("CREATE TABLE test (id INT)").
			Build()

		if len(txn.Statements) != 3 {
			t.Errorf("statement count: got %d, want 3", len(txn.Statements))
		}
	})

	t.Run("WithWriteConsistency", func(t *testing.T) {
		txn := NewTxnBuilder().
			WithWriteConsistency(protocol.ConsistencyAll).
			Build()

		if txn.WriteConsistency != protocol.ConsistencyAll {
			t.Errorf("WriteConsistency: got %v, want %v", txn.WriteConsistency, protocol.ConsistencyAll)
		}
	})

	t.Run("WithRequiredSchemaVersion", func(t *testing.T) {
		txn := NewTxnBuilder().
			WithRequiredSchemaVersion(42).
			Build()

		if txn.RequiredSchemaVersion != 42 {
			t.Errorf("RequiredSchemaVersion: got %d, want 42", txn.RequiredSchemaVersion)
		}
	})

	t.Run("WithLocalExecutionDone", func(t *testing.T) {
		txn := NewTxnBuilder().
			WithLocalExecutionDone(true).
			Build()

		if !txn.LocalExecutionDone {
			t.Error("LocalExecutionDone: got false, want true")
		}
	})

	t.Run("FluentAPI", func(t *testing.T) {
		oldVals := map[string][]byte{"id": {1}}
		newVals := map[string][]byte{"id": {2}}
		txn := NewTxnBuilder().
			WithID(100).
			WithNodeID(2).
			WithDatabase("production").
			WithCDCStatement("users", oldVals, newVals).
			WithRequiredSchemaVersion(5).
			Build()

		if txn.ID != 100 {
			t.Errorf("ID: got %d, want 100", txn.ID)
		}
		if txn.NodeID != 2 {
			t.Errorf("NodeID: got %d, want 2", txn.NodeID)
		}
		if txn.Database != "production" {
			t.Errorf("Database: got %s, want production", txn.Database)
		}
		if len(txn.Statements) != 1 {
			t.Errorf("statement count: got %d, want 1", len(txn.Statements))
		}
		if txn.RequiredSchemaVersion != 5 {
			t.Errorf("RequiredSchemaVersion: got %d, want 5", txn.RequiredSchemaVersion)
		}
	})
}

func TestAssertPrepareRequest(t *testing.T) {
	t.Run("ValidRequest", func(t *testing.T) {
		req := &ReplicationRequest{
			TxnID:                 42,
			NodeID:                1,
			Phase:                 PhasePrep,
			Database:              "test",
			RequiredSchemaVersion: 5,
			Statements:            []protocol.Statement{{}, {}},
		}

		expected := expectedPrepareReq{
			TxnID:                 42,
			NodeID:                1,
			Phase:                 PhasePrep,
			Database:              "test",
			RequiredSchemaVersion: 5,
			StatementCount:        2,
		}

		AssertPrepareRequest(t, req, expected)
	})

	// Note: Testing nil request would require complex setup to catch t.Fatal
	// The helper correctly calls t.Fatal for nil, which is the expected behavior
}

func TestAssertCommitRequest(t *testing.T) {
	t.Run("ValidRequest", func(t *testing.T) {
		req := &ReplicationRequest{
			TxnID:      42,
			Phase:      PhaseCommit,
			Database:   "test",
			Statements: []protocol.Statement{{}, {}},
		}

		expected := expectedCommitReq{
			TxnID:          42,
			Phase:          PhaseCommit,
			Database:       "test",
			StatementCount: 2,
		}

		AssertCommitRequest(t, req, expected)
	})
}

func TestEnhancedMockReplicatorExtensions(t *testing.T) {
	t.Run("GetPhaseCalls", func(t *testing.T) {
		mock := newMockReplicator()

		// Simulate some calls
		req1 := &ReplicationRequest{TxnID: 1, Phase: PhasePrep}
		req2 := &ReplicationRequest{TxnID: 2, Phase: PhasePrep}
		req3 := &ReplicationRequest{TxnID: 3, Phase: PhaseCommit}

		ctx, cancel := WithCancellation()
		defer cancel()
		_, _ = mock.ReplicateTransaction(ctx, 1, req1)
		mock.mu.Lock()
		mock.calls = []ReplicationCall{
			{NodeID: 1, Request: req1},
			{NodeID: 2, Request: req2},
			{NodeID: 3, Request: req3},
		}
		mock.mu.Unlock()

		prepCalls := mock.GetPhaseCalls(PhasePrep)
		if len(prepCalls) != 2 {
			t.Errorf("prepare calls: got %d, want 2", len(prepCalls))
		}

		commitCalls := mock.GetPhaseCalls(PhaseCommit)
		if len(commitCalls) != 1 {
			t.Errorf("commit calls: got %d, want 1", len(commitCalls))
		}
	})

	t.Run("SetNetworkPartition", func(t *testing.T) {
		mock := newMockReplicator()
		ctx, cancel := WithCancellation()
		defer cancel()

		mock.SetNetworkPartition(2, true)

		req := &ReplicationRequest{TxnID: 1, Phase: PhasePrep}
		resp, _ := mock.ReplicateTransaction(ctx, 2, req)
		if resp != nil {
			t.Error("expected nil response for partitioned node")
		}

		mock.SetNetworkPartition(2, false)
		mock.mu.Lock()
		delete(mock.responses, 2)
		mock.mu.Unlock()

		resp, _ = mock.ReplicateTransaction(ctx, 2, req)
		if resp == nil || !resp.Success {
			t.Error("expected success response after partition healed")
		}
	})
}

func TestContextHelpers(t *testing.T) {
	t.Run("WithTimeout", func(t *testing.T) {
		ctx, cancel := WithTimeout(100)
		defer cancel()

		select {
		case <-ctx.Done():
			t.Error("context should not be done immediately")
		case <-time.After(10 * time.Millisecond):
			// OK
		}

		time.Sleep(150 * time.Millisecond)
		select {
		case <-ctx.Done():
			// OK
		default:
			t.Error("context should be done after timeout")
		}
	})

	t.Run("WithCancellation", func(t *testing.T) {
		ctx, cancel := WithCancellation()

		select {
		case <-ctx.Done():
			t.Error("context should not be done before cancel")
		default:
			// OK
		}

		cancel()

		select {
		case <-ctx.Done():
			// OK
		case <-time.After(10 * time.Millisecond):
			t.Error("context should be done after cancel")
		}
	})

	t.Run("CancelAfter", func(t *testing.T) {
		ctx, cancel := WithCancellation()
		defer cancel()

		CancelAfter(ctx, cancel, 50*time.Millisecond)

		select {
		case <-ctx.Done():
			t.Error("context should not be done immediately")
		case <-time.After(10 * time.Millisecond):
			// OK
		}

		time.Sleep(60 * time.Millisecond)
		select {
		case <-ctx.Done():
			// OK
		default:
			t.Error("context should be done after delay")
		}
	})
}

func TestChannelMocker(t *testing.T) {
	t.Run("SendResponse", func(t *testing.T) {
		mocker := NewChannelMocker()
		ch := make(chan response, 1)

		resp := CreateSuccessResponse()
		mocker.SendResponse(ch, 1, resp, 0)

		select {
		case r := <-ch:
			if r.nodeID != 1 {
				t.Errorf("nodeID: got %d, want 1", r.nodeID)
			}
			if !r.resp.Success {
				t.Error("expected success response")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("timeout waiting for response")
		}
	})

	t.Run("SendResponseWithDelay", func(t *testing.T) {
		mocker := NewChannelMocker()
		ch := make(chan response, 1)

		resp := CreateSuccessResponse()
		start := time.Now()
		mocker.SendResponse(ch, 1, resp, 50*time.Millisecond)

		select {
		case <-ch:
			elapsed := time.Since(start)
			if elapsed < 50*time.Millisecond {
				t.Errorf("response arrived too early: %v", elapsed)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("timeout waiting for response")
		}
	})

	t.Run("SendResponses", func(t *testing.T) {
		mocker := NewChannelMocker()
		ch := make(chan response, 3)

		responses := map[uint64]*ReplicationResponse{
			1: CreateSuccessResponse(),
			2: CreateSuccessResponse(),
			3: CreateSuccessResponse(),
		}
		delays := map[uint64]time.Duration{
			1: 0,
			2: 10 * time.Millisecond,
			3: 20 * time.Millisecond,
		}

		mocker.SendResponses(ch, responses, delays)

		received := 0
		timeout := time.After(200 * time.Millisecond)
		for received < 3 {
			select {
			case <-ch:
				received++
			case <-timeout:
				t.Fatalf("timeout, received %d/3 responses", received)
			}
		}
	})
}

func TestErrorAssertions(t *testing.T) {
	t.Run("AssertQuorumNotAchievedError", func(t *testing.T) {
		err := &QuorumNotAchievedError{
			Phase:          "prepare",
			AcksReceived:   2,
			QuorumRequired: 3,
		}

		AssertQuorumNotAchievedError(t, err, "prepare", 2, 3)
	})

	t.Run("AssertPartialCommitError", func(t *testing.T) {
		err := &PartialCommitError{
			IsLocal: true,
		}

		AssertPartialCommitError(t, err, true)
	})

	t.Run("AssertConflictDetected", func(t *testing.T) {
		err := protocol.ErrDeadlock()
		AssertConflictDetected(t, err)
	})
}

func TestResponseAssertions(t *testing.T) {
	t.Run("AssertResponseMapSize", func(t *testing.T) {
		responses := map[uint64]*ReplicationResponse{
			1: CreateSuccessResponse(),
			2: CreateSuccessResponse(),
		}

		AssertResponseMapSize(t, responses, 2)
	})

	t.Run("AssertResponseSuccess", func(t *testing.T) {
		responses := map[uint64]*ReplicationResponse{
			1: CreateSuccessResponse(),
		}

		AssertResponseSuccess(t, responses, 1)
	})

	t.Run("AssertNodeNotInResponses", func(t *testing.T) {
		responses := map[uint64]*ReplicationResponse{
			1: CreateSuccessResponse(),
		}

		AssertNodeNotInResponses(t, responses, 2)
	})
}

func TestStatementCreators(t *testing.T) {
	t.Run("CreateCDCStatements", func(t *testing.T) {
		stmts := CreateCDCStatements(5, "users")

		if len(stmts) != 5 {
			t.Errorf("statement count: got %d, want 5", len(stmts))
		}

		for i, stmt := range stmts {
			if stmt.TableName != "users" {
				t.Errorf("stmt[%d] TableName: got %s, want users", i, stmt.TableName)
			}
			if len(stmt.OldValues) == 0 {
				t.Errorf("stmt[%d] has empty OldValues", i)
			}
			if len(stmt.NewValues) == 0 {
				t.Errorf("stmt[%d] has empty NewValues", i)
			}
		}
	})

	t.Run("CreateDDLStatements", func(t *testing.T) {
		stmts := CreateDDLStatements(3)

		if len(stmts) != 3 {
			t.Errorf("statement count: got %d, want 3", len(stmts))
		}

		for i, stmt := range stmts {
			if stmt.SQL == "" {
				t.Errorf("stmt[%d] has empty SQL", i)
			}
		}
	})
}

func TestResponseCreators(t *testing.T) {
	t.Run("CreateSuccessResponse", func(t *testing.T) {
		resp := CreateSuccessResponse()
		if !resp.Success {
			t.Error("expected success=true")
		}
		if resp.ConflictDetected {
			t.Error("expected no conflict")
		}
	})

	t.Run("CreateConflictResponse", func(t *testing.T) {
		resp := CreateConflictResponse("write-write conflict")
		if !resp.Success {
			t.Error("expected success=true")
		}
		if !resp.ConflictDetected {
			t.Error("expected conflict detected")
		}
		if resp.ConflictDetails != "write-write conflict" {
			t.Errorf("conflict details: got %s, want write-write conflict", resp.ConflictDetails)
		}
	})

	t.Run("CreateErrorResponse", func(t *testing.T) {
		resp := CreateErrorResponse("network timeout")
		if resp.Success {
			t.Error("expected success=false")
		}
		if resp.Error != "network timeout" {
			t.Errorf("error: got %s, want network timeout", resp.Error)
		}
	})
}

func TestCallAssertions(t *testing.T) {
	t.Run("AssertCallCount", func(t *testing.T) {
		mock := newMockReplicator()
		ctx, cancel := WithCancellation()
		defer cancel()

		req := &ReplicationRequest{TxnID: 1, Phase: PhasePrep}
		_, _ = mock.ReplicateTransaction(ctx, 1, req)
		_, _ = mock.ReplicateTransaction(ctx, 2, req)

		mock.mu.Lock()
		mock.calls = []ReplicationCall{
			{NodeID: 1, Request: req},
			{NodeID: 2, Request: req},
		}
		mock.mu.Unlock()

		AssertCallCount(t, mock, 2)
	})

	t.Run("AssertPhaseCallCount", func(t *testing.T) {
		mock := newMockReplicator()

		req1 := &ReplicationRequest{TxnID: 1, Phase: PhasePrep}
		req2 := &ReplicationRequest{TxnID: 2, Phase: PhaseCommit}

		mock.mu.Lock()
		mock.calls = []ReplicationCall{
			{NodeID: 1, Request: req1},
			{NodeID: 2, Request: req1},
			{NodeID: 3, Request: req2},
		}
		mock.mu.Unlock()

		AssertPhaseCallCount(t, mock, PhasePrep, 2)
		AssertPhaseCallCount(t, mock, PhaseCommit, 1)
	})
}

func TestCheckGoroutines(t *testing.T) {
	t.Run("NoLeaks", func(t *testing.T) {
		defer CheckGoroutines(t)()

		// Do some work
		done := make(chan bool)
		go func() {
			time.Sleep(1 * time.Millisecond)
			done <- true
		}()
		<-done
	})
}

func TestWaitForCondition(t *testing.T) {
	t.Run("ConditionMet", func(t *testing.T) {
		var counter atomic.Int32
		go func() {
			time.Sleep(20 * time.Millisecond)
			counter.Store(1)
		}()

		WaitForCondition(t, 100*time.Millisecond, func() bool {
			return counter.Load() == 1
		}, "counter should be 1")
	})
}

func TestCountGoroutines(t *testing.T) {
	before := CountGoroutines()
	if before <= 0 {
		t.Error("expected positive goroutine count")
	}

	done := make(chan bool)
	go func() {
		time.Sleep(1 * time.Millisecond)
		done <- true
	}()

	during := CountGoroutines()
	if during <= before {
		t.Error("expected more goroutines during work")
	}

	<-done
}

func TestTxnBuilderWithTimestamps(t *testing.T) {
	t.Run("WithStartTS", func(t *testing.T) {
		ts := hlc.Timestamp{WallTime: 5000, Logical: 10, NodeID: 1}
		txn := NewTxnBuilder().WithStartTS(ts).Build()

		if txn.StartTS.WallTime != 5000 {
			t.Errorf("StartTS.WallTime: got %d, want 5000", txn.StartTS.WallTime)
		}
		if txn.StartTS.Logical != 10 {
			t.Errorf("StartTS.Logical: got %d, want 10", txn.StartTS.Logical)
		}
	})

	t.Run("WithCommitTS", func(t *testing.T) {
		ts := hlc.Timestamp{WallTime: 6000, Logical: 20, NodeID: 1}
		txn := NewTxnBuilder().WithCommitTS(ts).Build()

		if txn.CommitTS.WallTime != 6000 {
			t.Errorf("CommitTS.WallTime: got %d, want 6000", txn.CommitTS.WallTime)
		}
		if txn.CommitTS.Logical != 20 {
			t.Errorf("CommitTS.Logical: got %d, want 20", txn.CommitTS.Logical)
		}
	})
}
