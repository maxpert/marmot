package grpc

import (
	"context"
	"sync/atomic"
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

// TestCloseRemovedForwardSession_InvokesCloserWhenIdle is a regression guard
// for the ordinary (non-racing) eviction path: a session with no in-flight
// Execute call must still have its closer invoked, so the execMu fix below
// does not accidentally turn eviction into a no-op.
func TestCloseRemovedForwardSession_InvokesCloserWhenIdle(t *testing.T) {
	mgr := NewForwardSessionManager(time.Hour)
	defer mgr.Stop()

	var closed atomic.Bool
	mgr.SetSessionCloser(func(cs *protocol.ConnectionSession) {
		closed.Store(true)
	})

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 200}
	mgr.GetOrCreateSession(key, "testdb")

	mgr.RemoveSession(key)

	assert.True(t, closed.Load(), "RemoveSession must invoke the session closer for an idle session")
}

// TestCloseRemovedForwardSession_SerializesAgainstInFlightExecute reproduces
// the forward-session eviction race: cleanupExpiredSessions/RemoveSession/
// RemoveSessionsForReplica all reach closeRemovedForwardSession, which calls
// the coordinator's CloseSession to roll back and discard any pinned SQLite
// transaction (and its captured CDC entries) the session left open. Before
// the fix, that call was not synchronized against Execute's execMu at all,
// so eviction could run concurrently with an in-flight COMMIT: if eviction's
// CloseSession wins the race for the pinned state, the COMMIT sees no
// pinned state and no buffered statements and takes the "empty transaction"
// fast path, silently reporting success for a write that was never applied.
// A COMMIT merely being slow (lock contention, a busy 2PC round) is enough
// to make this reachable, since ForwardSession.LastActivity is set once at
// the start of Execute and does not move again until the call returns - see
// closeRemovedForwardSession's doc comment for the full deadlock analysis of
// the fix (taking session.execMu before invoking the closer).
//
// This test forces the interleaving deterministically with a fake closer and
// channels instead of sleeps/timing: it fails before the fix (the closer
// runs while Execute's callback is still parked mid-flight, simulating a
// slow COMMIT) and passes after (eviction blocks until Execute returns).
func TestCloseRemovedForwardSession_SerializesAgainstInFlightExecute(t *testing.T) {
	mgr := NewForwardSessionManager(time.Hour)
	defer mgr.Stop()

	execEntered := make(chan struct{})
	releaseExec := make(chan struct{})
	var closerRanDuringExecute atomic.Bool

	mgr.SetSessionCloser(func(*protocol.ConnectionSession) {
		select {
		case <-releaseExec:
			// Execute's callback had already returned when the closer ran -
			// correctly serialized after it.
		default:
			// Execute's callback is still parked mid-flight (simulated slow
			// COMMIT): the closer ran concurrently with it.
			closerRanDuringExecute.Store(true)
		}
	})

	key := ForwardSessionKey{ReplicaNodeID: 1, SessionID: 300}
	session := mgr.GetOrCreateSession(key, "testdb")

	execDone := make(chan struct{})
	go func() {
		defer close(execDone)
		_, _ = session.Execute("testdb", func(cs *protocol.ConnectionSession) (*ForwardQueryResponse, error) {
			close(execEntered)
			<-releaseExec // held open to simulate a slow in-flight COMMIT
			return &ForwardQueryResponse{Success: true}, nil
		})
	}()

	<-execEntered // Execute now holds execMu and is mid-flight

	evictDone := make(chan struct{})
	go func() {
		mgr.RemoveSession(key)
		close(evictDone)
	}()

	// Bounded window for an unsynchronized eviction to run to completion.
	// This does not gate correctness of the assertion below: releaseExec is
	// not closed until after this window, so if eviction is unsynchronized
	// and races ahead, its closer call is guaranteed to observe
	// releaseExec still open and record the race regardless of exactly how
	// much of the window it needed.
	select {
	case <-evictDone:
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseExec)
	<-execDone
	<-evictDone

	assert.False(t, closerRanDuringExecute.Load(),
		"session eviction must not invoke the session closer while an Execute call is still in flight on that session")
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
