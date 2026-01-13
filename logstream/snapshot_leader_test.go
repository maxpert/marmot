package logstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockMetaStore implements a mock for replicatorMetaStore
type MockMetaStore struct {
	mock.Mock
}

func (m *MockMetaStore) AcquireLease(name string, duration time.Duration) (bool, error) {
	args := m.Called(name, duration)
	return args.Bool(0), args.Error(1)
}

func TestSnapshotLeader_NewSnapshotLeader(t *testing.T) {
	t.Run("should create with default TTL when 0 is passed", func(t *testing.T) {
		leader := NewSnapshotLeader(123, nil, 0)

		assert.NotNil(t, leader)
		assert.Equal(t, uint64(123), leader.nodeID)
		assert.Equal(t, defaultLeaderTTL, leader.ttl)
		assert.Equal(t, defaultLeaderTTL/defaultHeartbeatRatio, leader.heartbeat)
		assert.False(t, leader.IsLeader())
	})

	t.Run("should create with custom TTL", func(t *testing.T) {
		customTTL := 60 * time.Second
		leader := NewSnapshotLeader(456, nil, customTTL)

		assert.NotNil(t, leader)
		assert.Equal(t, uint64(456), leader.nodeID)
		assert.Equal(t, customTTL, leader.ttl)
		assert.Equal(t, customTTL/defaultHeartbeatRatio, leader.heartbeat)
	})

	t.Run("should enforce minimum heartbeat of 1 second", func(t *testing.T) {
		veryShortTTL := 2 * time.Second // Would result in ~666ms heartbeat
		leader := NewSnapshotLeader(789, nil, veryShortTTL)

		assert.GreaterOrEqual(t, leader.heartbeat, time.Second)
	})
}

func TestSnapshotLeader_IsLeader(t *testing.T) {
	t.Run("should return false initially", func(t *testing.T) {
		leader := NewSnapshotLeader(123, nil, 10*time.Second)
		assert.False(t, leader.IsLeader())
	})

	t.Run("should return true after becoming leader", func(t *testing.T) {
		leader := NewSnapshotLeader(123, nil, 10*time.Second)
		leader.isLeader.Store(true)
		assert.True(t, leader.IsLeader())
	})
}

func TestSnapshotLeader_StartStop(t *testing.T) {
	t.Run("should start and stop cleanly without metaStore", func(t *testing.T) {
		// Note: This test verifies start/stop without a real metaStore
		// Integration tests with real NATS are needed for full coverage
		leader := NewSnapshotLeader(123, nil, 10*time.Second)

		// Manually test stop without starting (safe operation)
		assert.NotPanics(t, func() {
			leader.Stop()
		})
	})
}
