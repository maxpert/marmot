package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/maxpert/marmot/protocol"
)

// Enhanced mock replicator with conflict simulation, delays, and call tracking
type enhancedMockReplicator struct {
	mu                  sync.RWMutex
	calls               []ReplicationCall
	responses           map[uint64]*ReplicationResponse // For SetNodeResponse compatibility
	conflicts           map[uint64]map[uint64]string    // nodeID -> txnID -> conflictMsg
	persistentConflicts map[uint64]string               // nodeID -> conflictMsg (applies to ALL txns)
	delays              map[uint64]time.Duration        // txnID -> delay
	activeLocks         map[uint64]uint64               // nodeID -> active txnID holding lock
	callCount           int                             // Simple counter for backward compatibility
	prepareLatency      time.Duration
	commitLatency       time.Duration
}

// ReplicationCall tracks a replication call for testing
type ReplicationCall struct {
	NodeID  uint64
	Request *ReplicationRequest
	Success bool
}

func newMockReplicator() *enhancedMockReplicator {
	return &enhancedMockReplicator{
		calls:               make([]ReplicationCall, 0),
		responses:           make(map[uint64]*ReplicationResponse),
		conflicts:           make(map[uint64]map[uint64]string),
		persistentConflicts: make(map[uint64]string),
		delays:              make(map[uint64]time.Duration),
		activeLocks:         make(map[uint64]uint64),
		prepareLatency:      1 * time.Millisecond,
		commitLatency:       1 * time.Millisecond,
	}
}

func (m *enhancedMockReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *ReplicationRequest) (*ReplicationResponse, error) {
	m.mu.Lock()

	// Track the call
	m.callCount++
	m.calls = append(m.calls, ReplicationCall{
		NodeID:  nodeID,
		Request: req,
		Success: true,
	})

	// Check for pre-configured response (for backward compatibility with SetNodeResponse)
	if resp, exists := m.responses[nodeID]; exists {
		m.mu.Unlock()
		return resp, nil
	}

	// Check for configured delay for this transaction
	delay, hasDelay := m.delays[req.TxnID]
	m.mu.Unlock()

	// Apply delay if configured
	if hasDelay {
		time.Sleep(delay)
	}

	// Simulate network latency
	if req.Phase == PhasePrep {
		time.Sleep(m.prepareLatency)
	} else if req.Phase == PhaseCommit {
		time.Sleep(m.commitLatency)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Handle PREPARE phase - check for conflicts
	if req.Phase == PhasePrep {
		// Check for persistent conflict first (applies to ALL transactions on this node)
		if conflictMsg, exists := m.persistentConflicts[nodeID]; exists {
			return &ReplicationResponse{
				Success:          true,
				ConflictDetected: true,
				ConflictDetails:  conflictMsg,
			}, nil
		}

		// Check if this node has a conflict configured for this specific txn
		if nodeConflicts, exists := m.conflicts[nodeID]; exists {
			if conflictMsg, hasConflict := nodeConflicts[req.TxnID]; hasConflict {
				// Conflict detected - return success with conflict flag
				// Success=true means the node responded successfully
				// ConflictDetected=true means there's a write-write conflict
				return &ReplicationResponse{
					Success:          true,
					ConflictDetected: true,
					ConflictDetails:  conflictMsg,
				}, nil
			}
		}

		// Acquire lock for this transaction on this node
		m.activeLocks[nodeID] = req.TxnID
	}

	// Handle COMMIT phase - release locks
	if req.Phase == PhaseCommit {
		delete(m.activeLocks, nodeID)
	}

	// Handle ABORT phase - release locks
	if req.Phase == PhaseAbort {
		delete(m.activeLocks, nodeID)
	}

	// Success response
	return &ReplicationResponse{
		Success: true,
	}, nil
}

// SetNodeResponse sets a fixed response for a node (backward compatibility)
func (m *enhancedMockReplicator) SetNodeResponse(nodeID uint64, resp *ReplicationResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[nodeID] = resp
}

// SetConflict configures a conflict for a specific node and transaction
func (m *enhancedMockReplicator) SetConflict(nodeID, txnID uint64, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conflicts[nodeID] == nil {
		m.conflicts[nodeID] = make(map[uint64]string)
	}
	m.conflicts[nodeID][txnID] = message
}

// SetPersistentNodeConflict configures a conflict that applies to ALL transactions on a node
func (m *enhancedMockReplicator) SetPersistentNodeConflict(nodeID uint64, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.persistentConflicts[nodeID] = message
}

// ClearPersistentNodeConflict removes a persistent conflict for a node
func (m *enhancedMockReplicator) ClearPersistentNodeConflict(nodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.persistentConflicts, nodeID)
}

// ClearConflict removes a configured conflict (simulates lock release)
func (m *enhancedMockReplicator) ClearConflict(nodeID, txnID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if nodeConflicts, exists := m.conflicts[nodeID]; exists {
		delete(nodeConflicts, txnID)
		if len(nodeConflicts) == 0 {
			delete(m.conflicts, nodeID)
		}
	}
}

// SetDelay configures a delay for a specific transaction (simulates slow execution)
func (m *enhancedMockReplicator) SetDelay(txnID uint64, delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delays[txnID] = delay
}

// GetCalls returns all replication calls made
func (m *enhancedMockReplicator) GetCalls() []ReplicationCall {
	m.mu.RLock()
	defer m.mu.RUnlock()

	calls := make([]ReplicationCall, len(m.calls))
	copy(calls, m.calls)
	return calls
}

// GetCallCount returns the number of replication calls
func (m *enhancedMockReplicator) GetCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.calls)
}

// Reset clears all state
func (m *enhancedMockReplicator) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls = make([]ReplicationCall, 0)
	m.conflicts = make(map[uint64]map[uint64]string)
	m.persistentConflicts = make(map[uint64]string)
	m.delays = make(map[uint64]time.Duration)
	m.activeLocks = make(map[uint64]uint64)
}

// IsLockHeld checks if a node currently holds a lock
func (m *enhancedMockReplicator) IsLockHeld(nodeID uint64) (bool, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	txnID, held := m.activeLocks[nodeID]
	return held, txnID
}

// Type aliases for convenience in tests
type MySQLError = protocol.MySQLError

// Helper functions for creating common errors
var (
	NewMySQLError      = protocol.NewMySQLError
	ErrLockWaitTimeout = protocol.ErrLockWaitTimeout
	ErrDeadlock        = protocol.ErrDeadlock
)
