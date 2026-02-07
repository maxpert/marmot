package grpc

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
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

func newTestForwardHandler() (*ForwardHandler, *ForwardSessionManager) {
	nodeID := uint64(1)
	clock := hlc.NewClock(nodeID)
	sessionMgr := NewForwardSessionManager(60 * time.Second)
	mockDB := &mockForwardDBManager{
		databases: map[string]bool{"testdb": true},
	}

	coordHandler := coordinator.NewCoordinatorHandler(nodeID, nil, nil, clock, nil, nil, nil, nil)
	handler := NewForwardHandler(nodeID, clock, sessionMgr, coordHandler, mockDB)
	return handler, sessionMgr
}

func TestForwardHandler_ValidateRequest(t *testing.T) {
	initPipeline()

	handler, sessionMgr := newTestForwardHandler()
	defer sessionMgr.Stop()

	tests := []struct {
		name        string
		req         *ForwardQueryRequest
		errContains string
	}{
		{
			name: "missing replica_node_id",
			req: &ForwardQueryRequest{
				SessionId: 100,
				RequestId: 1,
				Database:  "testdb",
				Sql:       "INSERT INTO users VALUES (1)",
			},
			errContains: "replica_node_id and session_id are required",
		},
		{
			name: "missing request_id",
			req: &ForwardQueryRequest{
				ReplicaNodeId: 1,
				SessionId:     100,
				Database:      "testdb",
				Sql:           "INSERT INTO users VALUES (1)",
			},
			errContains: "request_id is required",
		},
		{
			name: "missing database",
			req: &ForwardQueryRequest{
				ReplicaNodeId: 1,
				SessionId:     100,
				RequestId:     1,
				Sql:           "INSERT INTO users VALUES (1)",
			},
			errContains: "database is required",
		},
		{
			name: "non-existent database",
			req: &ForwardQueryRequest{
				ReplicaNodeId: 1,
				SessionId:     100,
				RequestId:     1,
				Database:      "nonexistent",
				Sql:           "INSERT INTO users VALUES (1)",
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

func TestForwardHandler_TransactionStateTracking(t *testing.T) {
	initPipeline()
	handler, sessionMgr := newTestForwardHandler()
	defer sessionMgr.Stop()

	replicaNodeID := uint64(2)
	sessionID := uint64(100)

	beginReq := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		RequestId:     1,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
	}
	resp, err := handler.HandleForwardQuery(context.Background(), beginReq)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.True(t, resp.InTransaction)

	rollbackReq := &ForwardQueryRequest{
		ReplicaNodeId: replicaNodeID,
		SessionId:     sessionID,
		RequestId:     2,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_ROLLBACK,
	}
	resp, err = handler.HandleForwardQuery(context.Background(), rollbackReq)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.False(t, resp.InTransaction)
}

func TestForwardHandler_DedupeReturnsCachedResponse(t *testing.T) {
	handler, sessionMgr := newTestForwardHandler()
	defer sessionMgr.Stop()

	req := &ForwardQueryRequest{
		ReplicaNodeId: 2,
		SessionId:     200,
		RequestId:     1,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
	}
	resp1, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp1.Success)

	resp2, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp2.Success)
	assert.Equal(t, resp1.InTransaction, resp2.InTransaction)
	assert.Equal(t, resp1.RowsAffected, resp2.RowsAffected)
	assert.Equal(t, resp1.CommittedTxnId, resp2.CommittedTxnId)
}

func TestForwardHandler_CommitWithoutTransactionIsNoop(t *testing.T) {
	handler, sessionMgr := newTestForwardHandler()
	defer sessionMgr.Stop()

	req := &ForwardQueryRequest{
		ReplicaNodeId: 2,
		SessionId:     103,
		RequestId:     1,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_COMMIT,
	}

	resp, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success, "COMMIT without transaction should be a no-op")
	assert.False(t, resp.InTransaction)
}

func TestForwardHandler_DoubleBeginIsNoopLikeMySQL(t *testing.T) {
	handler, sessionMgr := newTestForwardHandler()
	defer sessionMgr.Stop()

	req := &ForwardQueryRequest{
		ReplicaNodeId: 2,
		SessionId:     105,
		RequestId:     1,
		Database:      "testdb",
		TxnControl:    ForwardTxnControl_FWD_TXN_BEGIN,
	}
	resp, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.True(t, resp.InTransaction)

	req.RequestId = 2
	resp, err = handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success, "Second BEGIN should be no-op")
	assert.True(t, resp.InTransaction)
}

func TestForwardHandler_InvalidParams(t *testing.T) {
	initPipeline()
	handler, sessionMgr := newTestForwardHandler()
	defer sessionMgr.Stop()

	req := &ForwardQueryRequest{
		ReplicaNodeId: 2,
		SessionId:     106,
		RequestId:     1,
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

	// Same request_id should return the cached response.
	resp2, err := handler.HandleForwardQuery(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp2)
	assert.Equal(t, resp.Success, resp2.Success)
	assert.Equal(t, resp.ErrorMessage, resp2.ErrorMessage)
}

// mockForwardDBManager is a simple mock for ForwardDBManager.
type mockForwardDBManager struct {
	databases map[string]bool
}

func (m *mockForwardDBManager) GetDatabaseConnection(name string) (*sql.DB, error) {
	return nil, nil
}

func (m *mockForwardDBManager) DatabaseExists(name string) bool {
	return m.databases[name]
}
