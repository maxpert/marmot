package grpc

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForwardSessionManager_GetOrCreateSession(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}

	// First call creates session
	session1 := mgr.GetOrCreateSession(key, "testdb")
	require.NotNil(t, session1)
	assert.Equal(t, key, session1.Key)
	assert.Equal(t, "testdb", session1.Database)

	// Second call returns same session
	session2 := mgr.GetOrCreateSession(key, "testdb")
	assert.Same(t, session1, session2)
}

func TestForwardSessionManager_RemoveSession(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	mgr.GetOrCreateSession(key, "testdb")

	// Remove session
	mgr.RemoveSession(key)

	// New call should create new session
	session := mgr.GetOrCreateSession(key, "testdb")
	require.NotNil(t, session)
}

func TestForwardSessionManager_RemoveSessionsForReplica(t *testing.T) {
	mgr := NewForwardSessionManager(60 * time.Second)
	defer mgr.Stop()

	// Create sessions for two replicas
	key1 := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	key2 := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 101}
	key3 := ForwardSessionKey{ReplicaNodeID: 2, SessionID: 200}

	mgr.GetOrCreateSession(key1, "db1")
	mgr.GetOrCreateSession(key2, "db2")
	mgr.GetOrCreateSession(key3, "db3")

	// Remove all sessions for replica 1
	mgr.RemoveSessionsForReplica(1)

	// Verify replica 1 sessions are gone (new sessions created)
	mgr.mu.RLock()
	_, exists1 := mgr.sessions[key1]
	_, exists2 := mgr.sessions[key2]
	_, exists3 := mgr.sessions[key3]
	mgr.mu.RUnlock()

	assert.False(t, exists1, "key1 should be removed")
	assert.False(t, exists2, "key2 should be removed")
	assert.True(t, exists3, "key3 should still exist")
}

func TestForwardSession_BeginTransaction(t *testing.T) {
	session := &ForwardSession{
		Key:      ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100},
		Database: "testdb",
	}

	clock := hlc.NewClock(1)
	ts := clock.Now()
	txnID := ts.ToTxnID()

	// Begin transaction
	err := session.BeginTransaction(txnID, ts, "testdb")
	require.NoError(t, err)

	// Verify transaction exists
	txn := session.GetTransaction()
	require.NotNil(t, txn)
	assert.Equal(t, txnID, txn.TxnID)
	assert.Equal(t, "testdb", txn.Database)

	// Second begin should fail
	err = session.BeginTransaction(txnID+1, ts, "testdb")
	assert.Error(t, err)
}

func TestForwardSession_AddStatement(t *testing.T) {
	session := &ForwardSession{
		Key:      ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100},
		Database: "testdb",
	}

	clock := hlc.NewClock(1)
	ts := clock.Now()
	txnID := ts.ToTxnID()

	// Add statement without transaction should fail
	sql := "INSERT INTO test VALUES (?)"
	params := []any{1}
	err := session.AddStatement(sql, params)
	assert.Error(t, err)

	// Begin transaction
	err = session.BeginTransaction(txnID, ts, "testdb")
	require.NoError(t, err)

	// Now add statement should succeed
	err = session.AddStatement(sql, params)
	require.NoError(t, err)

	// Verify statement was added
	txn := session.GetTransaction()
	require.NotNil(t, txn)
	assert.Len(t, txn.Statements, 1)
	assert.Equal(t, sql, txn.Statements[0].SQL)
	assert.Equal(t, params, txn.Statements[0].Params)
}

func TestForwardSession_ClearTransaction(t *testing.T) {
	session := &ForwardSession{
		Key:      ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100},
		Database: "testdb",
	}

	clock := hlc.NewClock(1)
	ts := clock.Now()
	txnID := ts.ToTxnID()

	// Begin transaction
	err := session.BeginTransaction(txnID, ts, "testdb")
	require.NoError(t, err)

	// Clear transaction
	session.ClearTransaction()

	// Verify transaction is gone
	assert.Nil(t, session.GetTransaction())

	// Can begin new transaction
	err = session.BeginTransaction(txnID+1, ts, "testdb")
	require.NoError(t, err)
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
	// Create manager with very short timeout for testing
	mgr := NewForwardSessionManager(50 * time.Millisecond)
	defer mgr.Stop()

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 100}
	session := mgr.GetOrCreateSession(key, "testdb")
	require.NotNil(t, session)

	// Manually set last activity to past
	session.mu.Lock()
	session.LastActivity = time.Now().Add(-time.Hour)
	session.mu.Unlock()

	// Run cleanup manually
	mgr.cleanupExpiredSessions()

	// Session should be removed
	mgr.mu.RLock()
	_, exists := mgr.sessions[key]
	mgr.mu.RUnlock()
	assert.False(t, exists, "expired session should be removed")
}
