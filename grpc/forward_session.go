package grpc

import (
	"errors"
	"sync"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// ForwardSessionKey uniquely identifies a forwarding session from a replica
type ForwardSessionKey struct {
	ReplicaNodeID uint64
	SessionID     uint64
}

// BufferedStatement pairs a SQL statement with its parameters for deferred execution
type BufferedStatement struct {
	SQL    string
	Params []any
}

// ForwardTransaction represents an active transaction being built on the leader
type ForwardTransaction struct {
	TxnID      uint64
	StartTS    hlc.Timestamp
	Statements []BufferedStatement
	Database   string
}

// ForwardSession tracks a single client session on the leader for write forwarding
type ForwardSession struct {
	Key          ForwardSessionKey
	Database     string
	ActiveTxn    *ForwardTransaction
	LastActivity time.Time
	mu           sync.Mutex
}

// ForwardSessionManager manages all active forwarding sessions on the leader
type ForwardSessionManager struct {
	sessions       map[ForwardSessionKey]*ForwardSession
	mu             sync.RWMutex
	sessionTimeout time.Duration
	stopCh         chan struct{}
}

var (
	errNoActiveTransaction      = errors.New("no active transaction")
	errTransactionAlreadyActive = errors.New("transaction already active")
)

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
		Key:          key,
		Database:     db,
		LastActivity: time.Now(),
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

// BeginTransaction starts a new transaction in the session
func (s *ForwardSession) BeginTransaction(txnID uint64, startTS hlc.Timestamp, db string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ActiveTxn != nil {
		return errTransactionAlreadyActive
	}

	s.ActiveTxn = &ForwardTransaction{
		TxnID:      txnID,
		StartTS:    startTS,
		Statements: make([]BufferedStatement, 0),
		Database:   db,
	}
	s.LastActivity = time.Now()
	return nil
}

// AddStatement adds a statement with its params to the active transaction
func (s *ForwardSession) AddStatement(sql string, params []any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ActiveTxn == nil {
		return errNoActiveTransaction
	}

	s.ActiveTxn.Statements = append(s.ActiveTxn.Statements, BufferedStatement{
		SQL:    sql,
		Params: params,
	})
	s.LastActivity = time.Now()
	return nil
}

// GetTransaction returns the active transaction or nil
func (s *ForwardSession) GetTransaction() *ForwardTransaction {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ActiveTxn
}

// ClearTransaction clears the active transaction
func (s *ForwardSession) ClearTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ActiveTxn = nil
	s.LastActivity = time.Now()
}

// Touch updates the last activity timestamp
func (s *ForwardSession) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActivity = time.Now()
}
