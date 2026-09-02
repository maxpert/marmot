package grpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// ForwardSessionKey uniquely identifies a forwarding session from a replica
type ForwardSessionKey struct {
	ReplicaNodeID uint64
	SessionID     uint64
}

const maxCachedForwardRequests = 1024

type forwardRequestState struct {
	done chan struct{}
	resp *ForwardQueryResponse
}

// ForwardSession tracks a single client session on the leader for write forwarding
type ForwardSession struct {
	Key          ForwardSessionKey
	Database     string
	ConnSession  *protocol.ConnectionSession
	LastActivity time.Time

	execMu sync.Mutex
	mu     sync.Mutex

	requestStates map[uint64]*forwardRequestState
	requestOrder  []uint64
}

// ForwardSessionManager manages all active forwarding sessions on the leader
type ForwardSessionManager struct {
	sessions       map[ForwardSessionKey]*ForwardSession
	mu             sync.RWMutex
	sessionTimeout time.Duration
	stopCh         chan struct{}

	// onSessionRemoved, if set, is invoked with a removed session's
	// ConnSession whenever that session is evicted/removed from the manager
	// (idle timeout, explicit removal, or replica disconnect) without going
	// through an explicit COMMIT/ROLLBACK. This lets the coordinator release
	// any pinned SQLite transaction (and its row locks/writer hold) that the
	// session left open - see CoordinatorHandler.CloseSession, which mirrors
	// the cleanup protocol/server.go performs on direct connection close.
	onSessionRemoved func(*protocol.ConnectionSession)
}

// NewForwardSessionManager creates a new session manager and starts cleanup loop
func NewForwardSessionManager(timeout time.Duration) *ForwardSessionManager {
	m := &ForwardSessionManager{
		sessions:       make(map[ForwardSessionKey]*ForwardSession),
		sessionTimeout: timeout,
		stopCh:         make(chan struct{}),
	}
	go m.startCleanupLoop()
	return m
}

// SetSessionCloser registers a callback invoked with the ConnSession of every
// forward session removed from this manager, so any pinned transaction state
// held for that session's ConnID can be released. Must be called before the
// manager starts evicting sessions to avoid races with the cleanup loop.
func (m *ForwardSessionManager) SetSessionCloser(closer func(*protocol.ConnectionSession)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onSessionRemoved = closer
}

// GetOrCreateSession retrieves an existing session or creates a new one
func (m *ForwardSessionManager) GetOrCreateSession(key ForwardSessionKey, db string) *ForwardSession {
	m.mu.Lock()
	defer m.mu.Unlock()

	if session, exists := m.sessions[key]; exists {
		session.Touch()
		return session
	}

	session := &ForwardSession{
		Key:           key,
		Database:      db,
		ConnSession:   newForwardConnSession(key.SessionID, db),
		LastActivity:  time.Now(),
		requestStates: make(map[uint64]*forwardRequestState),
		requestOrder:  make([]uint64, 0, 16),
	}
	m.sessions[key] = session
	return session
}

// RemoveSession removes a specific session, closing any pinned transaction
// state left open on it.
func (m *ForwardSessionManager) RemoveSession(key ForwardSessionKey) {
	m.mu.Lock()
	session, ok := m.sessions[key]
	if ok {
		delete(m.sessions, key)
	}
	closer := m.onSessionRemoved
	m.mu.Unlock()

	if ok {
		closeRemovedForwardSession(closer, session)
	}
}

// RemoveSessionsForReplica removes all sessions for a given replica node,
// closing any pinned transaction state left open on each of them.
func (m *ForwardSessionManager) RemoveSessionsForReplica(replicaNodeID uint64) {
	m.mu.Lock()
	var removed []*ForwardSession
	for key, session := range m.sessions {
		if key.ReplicaNodeID == replicaNodeID {
			removed = append(removed, session)
			delete(m.sessions, key)
		}
	}
	closer := m.onSessionRemoved
	m.mu.Unlock()

	for _, session := range removed {
		closeRemovedForwardSession(closer, session)
	}
}

// startCleanupLoop runs periodic cleanup of expired sessions
func (m *ForwardSessionManager) startCleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupExpiredSessions()
		case <-m.stopCh:
			return
		}
	}
}

// cleanupExpiredSessions removes sessions that have been inactive beyond
// timeout, closing any pinned transaction state left open on each of them.
func (m *ForwardSessionManager) cleanupExpiredSessions() {
	m.mu.Lock()

	now := time.Now()
	expired := make([]*ForwardSession, 0)

	for key, session := range m.sessions {
		session.mu.Lock()
		lastActivity := session.LastActivity
		session.mu.Unlock()

		if now.Sub(lastActivity) > m.sessionTimeout {
			expired = append(expired, session)
			delete(m.sessions, key)
		}
	}
	closer := m.onSessionRemoved
	m.mu.Unlock()

	if len(expired) > 0 {
		for _, session := range expired {
			closeRemovedForwardSession(closer, session)
		}
		log.Debug().
			Int("count", len(expired)).
			Msg("cleaned up expired forward sessions")
	}
}

// closeRemovedForwardSession invokes closer with session's ConnSession, if
// both are non-nil, so the coordinator can release any pinned SQLite
// transaction (and writer lock) the session left open. Runs outside the
// manager's lock since CloseSession may block on coordinator-side state.
//
// It holds session.execMu for the duration of the call, the same lock
// Execute holds for a whole forwarded statement (including COMMIT). Without
// this, eviction (idle timeout, explicit removal, or replica disconnect) can
// run concurrently with an in-flight COMMIT: the coordinator's CloseSession
// rolls back and discards the pinned transaction's captured CDC entries
// while handleCommit is still trying to read them, so the write is silently
// lost even though the client is told COMMIT succeeded.
//
// This cannot deadlock: closer (CoordinatorHandler.CloseSession) only takes
// coordinator-side pinned-transaction state and ends the session's
// transaction - it never calls back into the ForwardSessionManager or any
// ForwardSession (the coordinator package does not import grpc, so no such
// call path can exist). Nor is the manager's own mutex held while this runs
// - every caller (cleanupExpiredSessions, RemoveSession,
// RemoveSessionsForReplica) releases it before invoking
// closeRemovedForwardSession. So this can only block behind an in-flight
// Execute call on this same session, and always makes progress once that
// call returns.
func closeRemovedForwardSession(closer func(*protocol.ConnectionSession), session *ForwardSession) {
	if closer == nil || session == nil {
		return
	}

	session.execMu.Lock()
	defer session.execMu.Unlock()

	session.mu.Lock()
	connSession := session.ConnSession
	session.mu.Unlock()

	if connSession != nil {
		closer(connSession)
	}
}

// Stop signals the cleanup loop to stop
func (m *ForwardSessionManager) Stop() {
	close(m.stopCh)
}

func newForwardConnSession(connID uint64, db string) *protocol.ConnectionSession {
	return &protocol.ConnectionSession{
		ConnID:               connID,
		CurrentDatabase:      db,
		TranspilationEnabled: false, // Forwarded SQL is already transpiled on replica
	}
}

// BeginRequest reserves a request slot for idempotent dedupe.
// Returns (state, true) for new requests, (state, false) for retries/duplicates.
func (s *ForwardSession) BeginRequest(requestID uint64) (*forwardRequestState, bool) {
	if requestID == 0 {
		return nil, true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if state, ok := s.requestStates[requestID]; ok {
		return state, false
	}

	state := &forwardRequestState{
		done: make(chan struct{}),
	}
	s.requestStates[requestID] = state
	s.requestOrder = append(s.requestOrder, requestID)
	s.LastActivity = time.Now()
	return state, true
}

// WaitForRequest waits until an in-flight duplicate request finishes.
func (s *ForwardSession) WaitForRequest(ctx context.Context, state *forwardRequestState) (*ForwardQueryResponse, error) {
	if state == nil {
		return nil, nil
	}

	select {
	case <-state.done:
		return cloneForwardResponse(state.resp), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CompleteRequest stores the response and unblocks duplicate waiters.
func (s *ForwardSession) CompleteRequest(requestID uint64, state *forwardRequestState, resp *ForwardQueryResponse) {
	if requestID == 0 || state == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state.resp = cloneForwardResponse(resp)
	close(state.done)
	s.LastActivity = time.Now()
	s.pruneCompletedRequestsLocked()
}

// Execute serializes all operations for a forwarded session, preserving
// single-connection ordering semantics and coordinator transaction state.
func (s *ForwardSession) Execute(database string, fn func(connSession *protocol.ConnectionSession) (*ForwardQueryResponse, error)) (*ForwardQueryResponse, error) {
	s.execMu.Lock()
	defer s.execMu.Unlock()

	s.mu.Lock()
	s.ensureDatabaseLocked(database)
	connSession := s.ConnSession
	s.LastActivity = time.Now()
	s.mu.Unlock()

	if connSession == nil {
		return nil, errors.New("forward session connection not initialized")
	}

	return fn(connSession)
}

func (s *ForwardSession) HasActiveTransaction() bool {
	s.mu.Lock()
	connSession := s.ConnSession
	s.mu.Unlock()

	if connSession == nil {
		return false
	}
	return connSession.InTransaction()
}

func (s *ForwardSession) ensureDatabaseLocked(db string) {
	if db == "" {
		return
	}
	s.Database = db
	if s.ConnSession == nil {
		s.ConnSession = newForwardConnSession(s.Key.SessionID, db)
		return
	}
	s.ConnSession.CurrentDatabase = db
}

func (s *ForwardSession) pruneCompletedRequestsLocked() {
	if len(s.requestStates) <= maxCachedForwardRequests {
		return
	}

	filtered := make([]uint64, 0, len(s.requestOrder))
	for _, requestID := range s.requestOrder {
		state, ok := s.requestStates[requestID]
		if !ok {
			continue
		}
		if len(s.requestStates) > maxCachedForwardRequests {
			select {
			case <-state.done:
				delete(s.requestStates, requestID)
				continue
			default:
			}
		}
		filtered = append(filtered, requestID)
	}
	s.requestOrder = filtered
}

func cloneForwardResponse(resp *ForwardQueryResponse) *ForwardQueryResponse {
	if resp == nil {
		return nil
	}
	cloned := *resp
	return &cloned
}

// Touch updates the last activity timestamp
func (s *ForwardSession) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActivity = time.Now()
}
