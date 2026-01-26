package grpc

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var initOnce sync.Once

func initPipeline() {
	initOnce.Do(func() {
		gen := id.NewHLCGenerator(hlc.NewClock(1))
		if err := protocol.InitializePipeline(10000, gen); err != nil {
			panic("Failed to initialize pipeline: " + err.Error())
		}
	})
}

// TestForwardHandler_ValidateRequest tests request validation
func TestForwardHandler_ValidateRequest(t *testing.T) {
	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	// Mock DB manager that only has "testdb"
	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	// Create handler without coordinator (validation tests don't need it)
	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	tests := []struct {
		name        string
		req         *ForwardQueryRequest
		errContains string
	}{
		{
			name: "missing replica_node_id",
			req: &ForwardQueryRequest{
				SessionId: 100,
				Database:  "testdb",
			},
			errContains: "replica_node_id and session_id are required",
		},
		{
			name: "missing session_id",
			req: &ForwardQueryRequest{
				ReplicaNodeId: 1,
				Database:      "testdb",
			},
			errContains: "replica_node_id and session_id are required",
		},
		{
			name: "missing database",
			req: &ForwardQueryRequest{
				ReplicaNodeId: 1,
				SessionId:     100,
			},
			errContains: "database is required",
		},
		{
			name: "non-existent database",
			req: &ForwardQueryRequest{
				ReplicaNodeId: 1,
				SessionId:     100,
				Database:      "nonexistent",
			},
			errContains: "database nonexistent does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := handler.HandleForwardQuery(context.Background(), tc.req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.False(t, resp.Success)
			assert.Contains(t, resp.ErrorMessage, tc.errContains)
		})
	}
}

// TestForwardHandler_TransactionStateTracking tests Bug #2
// Verifies session transaction state tracking after BEGIN/COMMIT/ROLLBACK
func TestForwardHandler_TransactionStateTracking(t *testing.T) {
	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	replicaNodeID := uint64(2)
	sessionID := uint64(100)

	t.Run("BEGIN sets transaction active", func(t *testing.T) {
		req := &ForwardQueryRequest{
			ReplicaNodeId: replicaNodeID,
			SessionId:     sessionID,
			Database:      "testdb",
			TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
		}

		resp, err := handler.HandleForwardQuery(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Success, "BEGIN should succeed")

		// Verify session has active transaction
		key := ForwardSessionKey{ReplicaNodeID: replicaNodeID, SessionID: sessionID}
		session := handler.sessionMgr.GetOrCreateSession(key, "testdb")
		txn := session.GetTransaction()
		assert.NotNil(t, txn, "Session should have active transaction after BEGIN")
	})

	t.Run("ROLLBACK clears transaction", func(t *testing.T) {
		req := &ForwardQueryRequest{
			ReplicaNodeId: replicaNodeID,
			SessionId:     sessionID,
			Database:      "testdb",
			TxnControl:    ForwardTxnControl_FWD_TXN_ROLLBACK,
		}

		resp, err := handler.HandleForwardQuery(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Success, "ROLLBACK should succeed")

		// Verify session transaction is cleared
		key := ForwardSessionKey{ReplicaNodeID: replicaNodeID, SessionID: sessionID}
		session := handler.sessionMgr.GetOrCreateSession(key, "testdb")
		txn := session.GetTransaction()
		assert.Nil(t, txn, "Session should not have active transaction after ROLLBACK")
	})

	t.Run("BEGIN then COMMIT clears transaction", func(t *testing.T) {
		sessionID2 := uint64(101)

		// BEGIN
		beginReq := &ForwardQueryRequest{
			ReplicaNodeId: replicaNodeID,
			SessionId:     sessionID2,
			Database:      "testdb",
			TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
		}
		resp, err := handler.HandleForwardQuery(context.Background(), beginReq)
		require.NoError(t, err)
		require.True(t, resp.Success)

		// Verify transaction active
		key := ForwardSessionKey{ReplicaNodeID: replicaNodeID, SessionID: sessionID2}
		session := handler.sessionMgr.GetOrCreateSession(key, "testdb")
		assert.NotNil(t, session.GetTransaction())

		// COMMIT (empty transaction)
		commitReq := &ForwardQueryRequest{
			ReplicaNodeId: replicaNodeID,
			SessionId:     sessionID2,
			Database:      "testdb",
			TxnControl:    ForwardTxnControl_FWD_TXN_COMMIT,
		}
		resp, err = handler.HandleForwardQuery(context.Background(), commitReq)
		require.NoError(t, err)
		assert.True(t, resp.Success, "Empty COMMIT should succeed")

		// Verify transaction cleared
		assert.Nil(t, session.GetTransaction(), "Transaction should be cleared after COMMIT")
	})
}

// TestForwardHandler_TransactionBuffering tests statement buffering in transactions
func TestForwardHandler_TransactionBuffering(t *testing.T) {
	initPipeline() // Need pipeline for SQL parsing

	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	replicaNodeID := uint64(2)
	sessionID := uint64(102)

	// BEGIN
	beginReq := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
	}
	resp, err := handler.HandleForwardQuery(context.Background(), beginReq)
	require.NoError(t, err)
	require.True(t, resp.Success)

	// Buffer multiple statements
	statements := []string{
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
		"INSERT INTO users (id, name) VALUES (2, 'Bob')",
		"INSERT INTO users (id, name) VALUES (3, 'Charlie')",
	}

	for _, sql := range statements {
		stmtReq := &ForwardQueryRequest{
			ReplicaNodeId: replicaNodeID,
			SessionId:     sessionID,
			Database:      "testdb",
			Sql:           sql,
			TxnControl:    ForwardTxnControl_FWD_TXN_NONE,
		}
		resp, err = handler.HandleForwardQuery(context.Background(), stmtReq)
		require.NoError(t, err)
		require.True(t, resp.Success)
		assert.True(t, resp.InTransaction, "Should be in transaction")
	}

	// Verify statements were buffered
	key := ForwardSessionKey{ReplicaNodeID: replicaNodeID, SessionID: sessionID}
	session := handler.sessionMgr.GetOrCreateSession(key, "testdb")
	txn := session.GetTransaction()
	require.NotNil(t, txn)
	assert.Len(t, txn.Statements, len(statements), "Should have buffered all statements")

	// ROLLBACK
	rollbackReq := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_ROLLBACK,
	}
	resp, err = handler.HandleForwardQuery(context.Background(), rollbackReq)
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

// TestForwardHandler_CommitWithoutTransaction tests COMMIT without BEGIN
func TestForwardHandler_CommitWithoutTransaction(t *testing.T) {
	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	req := &ForwardQueryRequest{
		ReplicaNodeId: 2,
		SessionId:     103,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_COMMIT,
	}

	resp, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Success, "COMMIT without transaction should fail")
	assert.Contains(t, resp.ErrorMessage, "no active transaction")
}

// TestForwardHandler_RollbackWithoutTransaction tests ROLLBACK without BEGIN (no-op)
func TestForwardHandler_RollbackWithoutTransaction(t *testing.T) {
	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	req := &ForwardQueryRequest{
		ReplicaNodeId: 2,
		SessionId:     104,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_ROLLBACK,
	}

	resp, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success, "ROLLBACK without transaction should succeed (no-op)")
}

// TestForwardHandler_DoubleBegin tests BEGIN when transaction already active
func TestForwardHandler_DoubleBegin(t *testing.T) {
	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	replicaNodeID := uint64(2)
	sessionID := uint64(105)

	// First BEGIN
	req := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
	}
	resp, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Success)

	// Second BEGIN should fail
	resp, err = handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Success, "Second BEGIN should fail")
	assert.Contains(t, resp.ErrorMessage, "transaction already active")
}

// TestForwardHandler_InvalidParams tests invalid parameter serialization
func TestForwardHandler_InvalidParams(t *testing.T) {
	initPipeline() // Need pipeline for SQL parsing

	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	req := &ForwardQueryRequest{
		ReplicaNodeId: 2,
		SessionId:     106,
		Database:      "testdb",
		Sql:           "INSERT INTO users (id, name) VALUES (?, ?)",
		Params:        [][]byte{{0xd9}}, // Invalid msgpack (incomplete str8)
		TxnControl:    ForwardTxnControl_FWD_TXN_NONE,
	}

	resp, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Success)
	assert.Contains(t, resp.ErrorMessage, "failed to deserialize params")
}

// TestForwardHandler_ParamsWithTransaction tests params are preserved in buffered statements
func TestForwardHandler_ParamsWithTransaction(t *testing.T) {
	initPipeline() // Need pipeline for SQL parsing

	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	defer sessionMgr.Stop()

	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	handler := NewForwardHandler(nodeID, clock, sessionMgr, nil, mockDB)

	replicaNodeID := uint64(2)
	sessionID := uint64(107)

	// BEGIN
	beginReq := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
	}
	resp, err := handler.HandleForwardQuery(context.Background(), beginReq)
	require.NoError(t, err)
	require.True(t, resp.Success)

	// Insert with params
	params, err := SerializeParams([]interface{}{int64(1), "Alice"})
	require.NoError(t, err)

	stmtReq := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		Database:      "testdb",
		Sql:           "INSERT INTO users (id, name) VALUES (?, ?)",
		Params:        params,
		TxnControl:    ForwardTxnControl_FWD_TXN_NONE,
	}
	resp, err = handler.HandleForwardQuery(context.Background(), stmtReq)
	require.NoError(t, err)
	require.True(t, resp.Success)
	assert.True(t, resp.InTransaction)

	// Verify statement and params were buffered
	key := ForwardSessionKey{ReplicaNodeID: replicaNodeID, SessionID: sessionID}
	session := handler.sessionMgr.GetOrCreateSession(key, "testdb")
	txn := session.GetTransaction()
	require.NotNil(t, txn)
	require.Len(t, txn.Statements, 1)
	// Params in BufferedStatement are the deserialized []any
	assert.Len(t, txn.Statements[0].Params, 2, "Params should be preserved")

	// Cleanup
	rollbackReq := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_ROLLBACK,
	}
	handler.HandleForwardQuery(context.Background(), rollbackReq)
}

// mockForwardDBManager is a simple mock for ForwardDBManager
type mockForwardDBManager struct {
	databases map[string]bool
}

func (m *mockForwardDBManager) GetDatabaseConnection(name string) (*sql.DB, error) {
	return nil, nil
}

func (m *mockForwardDBManager) DatabaseExists(name string) bool {
	return m.databases[name]
}
