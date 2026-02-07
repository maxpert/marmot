package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForwardSessionManager_GetOrCreateSession(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}

	session1 := mgr.GetOrCreateSession(key, "testdb")
	require.NotNil(t, session1)
	assert.Equal(t, key, session1.Key)
	assert.Equal(t, "testdb", session1.Database)
	require.NotNil(t, session1.ConnSession)
	assert.Equal(t, uint64(100), session1.ConnSession.ConnID)
	assert.Equal(t, "testdb", session1.ConnSession.CurrentDatabase)

	session2 := mgr.GetOrCreateSession(key, "testdb")
	assert.Same(t, session1, session2)
}

func TestForwardSession_RequestDedupe(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	session := mgr.GetOrCreateSession(key, "testdb")

	state1, isNew := session.BeginRequest(42)
	require.True(t, isNew)
	require.NotNil(t, state1)

	state2, isNew := session.BeginRequest(42)
	require.False(t, isNew)
	assert.Equal(t, state1, state2)

	response := &ForwardQueryResponse{Success: true, RowsAffected: 5}
	session.CompleteRequest(42, state1, response)

	waited, err := session.WaitForRequest(context.Background(), state2)
	require.NoError(t, err)
	require.NotNil(t, waited)
	assert.True(t, waited.Success)
	assert.EqualValues(t, 5, waited.RowsAffected)
}

func TestForwardSession_ExecuteUpdatesDatabase(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	session := mgr.GetOrCreateSession(key, "db1")

	resp, err := session.Execute("db2", func(connSession *protocol.ConnectionSession) (*ForwardQueryResponse, error) {
		return &ForwardQueryResponse{Success: connSession.CurrentDatabase == "db2"}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)

	assert.Equal(t, "db2", session.Database)
	assert.Equal(t, "db2", session.ConnSession.CurrentDatabase)
}

func TestForwardSession_HasActiveTransaction(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	session := mgr.GetOrCreateSession(key, "testdb")
	assert.False(t, session.HasActiveTransaction())

	_, err := session.Execute("testdb", func(connSession *protocol.ConnectionSession) (*ForwardQueryResponse, error) {
		connSession.BeginTransaction(123, hlc.Timestamp{}, "testdb")
		return &ForwardQueryResponse{Success: true}, nil
	})
	require.NoError(t, err)
	assert.True(t, session.HasActiveTransaction())

	_, err = session.Execute("testdb", func(connSession *protocol.ConnectionSession) (*ForwardQueryResponse, error) {
		connSession.EndTransaction()
		return &ForwardQueryResponse{Success: true}, nil
	})
	require.NoError(t, err)
	assert.False(t, session.HasActiveTransaction())
}

func TestForwardSessionManager_RemoveSession(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	mgr.GetOrCreateSession(key, "testdb")

	mgr.RemoveSession(key)

	session := mgr.GetOrCreateSession(key, "testdb")
	require.NotNil(t, session)
}

func TestForwardSessionManager_RemoveSessionsForReplica(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key1 := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	key2 := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 101}
	key3 := ForwardSessionKey{ReplicaNodeID: 2, SessionID: 200}

	mgr.GetOrCreateSession(key1, "db1")
	mgr.GetOrCreateSession(key2, "db2")
	mgr.GetOrCreateSession(key3, "db3")

	mgr.RemoveSessionsForReplica(1)

	mgr.mu.RLock()
	_, exists1 := mgr.sessions[key1]
	_, exists2 := mgr.sessions[key2]
	_, exists3 := mgr.sessions[key3]
	mgr.mu.RUnlock()

	assert.False(t, exists1, "key1 should be removed")
	assert.False(t, exists2, "key2 should be removed")
	assert.True(t, exists3, "key3 should still exist")
}

func TestForwardSession_Touch(t *testing.T) {
	session := &ForwardSession{
		Key:          ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100},
		Database:     "testdb",
		LastActivity: time.Now().Add(-time.Hour),
	}

	oldTime := session.LastActivity
	session.Touch()

	assert.True(t, session.LastActivity.After(oldTime))
}

func TestForwardSessionManager_SessionTimeout(t *testing.T) {
	mgr := NewForwardSessionManager(50 * time.Millisecond)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	session := mgr.GetOrCreateSession(key, "testdb")
	require.NotNil(t, session)

	session.mu.Lock()
	session.LastActivity = time.Now().Add(-time.Hour)
	session.mu.Unlock()

	mgr.cleanupExpiredSessions()

	mgr.mu.RLock()
	_, exists := mgr.sessions[key]
	mgr.mu.RUnlock()
	assert.False(t, exists, "expired session should be removed")
}
