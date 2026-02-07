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

// RemoveSession removes a specific session
func (m *ForwardSessionManager) RemoveSession(key ForwardSessionKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, key)
}

// RemoveSessionsForReplica removes all sessions for a given replica node
func (m *ForwardSessionManager) RemoveSessionsForReplica(replicaNodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.sessions {
		if key.ReplicaNodeID == replicaNodeID {
			delete(m.sessions, key)
		}
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

// cleanupExpiredSessions removes sessions that have been inactive beyond timeout
func (m *ForwardSessionManager) cleanupExpiredSessions() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	expiredKeys := make([]ForwardSessionKey, 0)

	for key, session := range m.sessions {
		session.mu.Lock()
		lastActivity := session.LastActivity
		session.mu.Unlock()

		if now.Sub(lastActivity) > m.sessionTimeout {
			expiredKeys = append(expiredKeys, key)
		}
	}

	if len(expiredKeys) > 0 {
		for _, key := range expiredKeys {
			delete(m.sessions, key)
		}
		log.Debug().
			Int("count", len(expiredKeys)).
			Msg("cleaned up expired forward sessions")
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
