package logstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/wongfei2009/harmonylite/cfg"
)

const (
	snapshotLeaderKey     = "snapshot-leader"
	defaultLeaderTTL      = 30 * time.Second
	defaultHeartbeatRatio = 3 // heartbeat = TTL / ratio
)

// SnapshotLeader manages leader election for snapshot uploads.
// Only the leader node is responsible for uploading snapshots to object storage.
type SnapshotLeader struct {
	nodeID    uint64
	metaStore *replicatorMetaStore
	ttl       time.Duration
	heartbeat time.Duration

	isLeader atomic.Bool
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewSnapshotLeader creates a new SnapshotLeader instance.
// ttl is the lease time-to-live; if 0, defaults to 30 seconds.
func NewSnapshotLeader(nodeID uint64, metaStore *replicatorMetaStore, ttl time.Duration) *SnapshotLeader {
	if ttl == 0 {
		ttl = defaultLeaderTTL
	}

	heartbeat := ttl / defaultHeartbeatRatio
	if heartbeat < time.Second {
		heartbeat = time.Second
	}

	return &SnapshotLeader{
		nodeID:    nodeID,
		metaStore: metaStore,
		ttl:       ttl,
		heartbeat: heartbeat,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the leader election loop.
// This should be called once after creating the SnapshotLeader.
func (s *SnapshotLeader) Start() {
	s.wg.Add(1)
	go s.electionLoop()
	log.Info().
		Uint64("node_id", s.nodeID).
		Dur("ttl", s.ttl).
		Dur("heartbeat", s.heartbeat).
		Msg("Snapshot leader election started")
}

// Stop gracefully stops the leader election loop.
// If this node is the leader, it will release leadership.
func (s *SnapshotLeader) Stop() {
	close(s.stopCh)
	s.wg.Wait()

	if s.isLeader.Load() {
		log.Info().Uint64("node_id", s.nodeID).Msg("Releasing snapshot leadership on shutdown")
		s.isLeader.Store(false)
	}

	log.Info().Uint64("node_id", s.nodeID).Msg("Snapshot leader election stopped")
}

// IsLeader returns true if this node is currently the snapshot leader.
func (s *SnapshotLeader) IsLeader() bool {
	return s.isLeader.Load()
}

// electionLoop continuously attempts to acquire or maintain leadership.
func (s *SnapshotLeader) electionLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()

	// Try to acquire leadership immediately on start
	s.tryAcquireLeadership()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.tryAcquireLeadership()
		}
	}
}

// tryAcquireLeadership attempts to acquire or renew the leader lease.
func (s *SnapshotLeader) tryAcquireLeadership() {
	if s.metaStore == nil {
		log.Warn().Uint64("node_id", s.nodeID).Msg("Cannot acquire leadership: metaStore is nil")
		return
	}

	wasLeader := s.isLeader.Load()

	acquired, err := s.metaStore.AcquireLease(snapshotLeaderKey, s.ttl)
	if err != nil {
		log.Warn().
			Err(err).
			Uint64("node_id", s.nodeID).
			Bool("was_leader", wasLeader).
			Msg("Error during leader election")

		// If we were the leader and got an error, we may have lost leadership
		if wasLeader {
			s.isLeader.Store(false)
			log.Warn().Uint64("node_id", s.nodeID).Msg("Lost snapshot leadership due to error")
		}
		return
	}

	if acquired {
		if !wasLeader {
			log.Info().Uint64("node_id", s.nodeID).Msg("Became snapshot leader")
		}
		s.isLeader.Store(true)
	} else {
		if wasLeader {
			log.Info().Uint64("node_id", s.nodeID).Msg("Lost snapshot leadership")
		}
		s.isLeader.Store(false)
	}
}

// GetLeaderTTL returns the configured leader TTL from config, or default if not set.
func GetLeaderTTL() time.Duration {
	if cfg.Config.Snapshot.LeaderTTL > 0 {
		return time.Duration(cfg.Config.Snapshot.LeaderTTL) * time.Millisecond
	}
	return defaultLeaderTTL
}
