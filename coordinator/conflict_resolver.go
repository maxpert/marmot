package coordinator

import (
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
)

// ConflictResolutionStrategy determines how to resolve conflicts
type ConflictResolutionStrategy int

const (
	// LastWriteWins uses HLC timestamp to determine winner
	LastWriteWins ConflictResolutionStrategy = iota

	// FirstWriteWins keeps the first write
	FirstWriteWins
)

// ConflictResolver resolves conflicts between multiple versions using HLC
type ConflictResolver struct {
	strategy ConflictResolutionStrategy
}

// NewConflictResolver creates a new conflict resolver
func NewConflictResolver(strategy ConflictResolutionStrategy) *ConflictResolver {
	return &ConflictResolver{
		strategy: strategy,
	}
}

// ResolveTimestamps determines which timestamp wins in a conflict
// Returns true if timestamp1 wins, false if timestamp2 wins
func (cr *ConflictResolver) ResolveTimestamps(ts1, ts2 hlc.Timestamp) bool {
	switch cr.strategy {
	case LastWriteWins:
		// Higher timestamp wins
		cmp := hlc.Compare(ts1, ts2)
		return cmp >= 0 // ts1 wins if equal or greater

	case FirstWriteWins:
		// Lower timestamp wins
		cmp := hlc.Compare(ts1, ts2)
		return cmp <= 0 // ts1 wins if equal or lower

	default:
		// Default to LastWriteWins
		cmp := hlc.Compare(ts1, ts2)
		return cmp >= 0
	}
}

// SelectWinner selects the winning version from multiple candidates
// Returns the index of the winning candidate
func (cr *ConflictResolver) SelectWinner(timestamps []hlc.Timestamp) int {
	if len(timestamps) == 0 {
		return -1
	}

	if len(timestamps) == 1 {
		return 0
	}

	winnerIdx := 0
	winnerTS := timestamps[0]

	for i := 1; i < len(timestamps); i++ {
		if !cr.ResolveTimestamps(winnerTS, timestamps[i]) {
			// timestamps[i] wins
			winnerIdx = i
			winnerTS = timestamps[i]
		}
	}

	return winnerIdx
}

// MergeVersions merges multiple versions by selecting the latest
type Version struct {
	Timestamp hlc.Timestamp
	Data      interface{}
}

// SelectLatestVersion selects the version with the highest HLC timestamp
func (cr *ConflictResolver) SelectLatestVersion(versions []Version) *Version {
	if len(versions) == 0 {
		return nil
	}

	if len(versions) == 1 {
		return &versions[0]
	}

	latest := &versions[0]

	for i := 1; i < len(versions); i++ {
		if hlc.Compare(versions[i].Timestamp, latest.Timestamp) > 0 {
			latest = &versions[i]
		}
	}

	return latest
}

// IsConflict determines if two timestamps represent a conflict
// A conflict exists if timestamps are from different nodes but very close in time
func IsConflict(ts1, ts2 hlc.Timestamp, thresholdNanos int64) bool {
	// Same node, same logical clock -> no conflict (same operation)
	if ts1.NodeID == ts2.NodeID && ts1.Logical == ts2.Logical && ts1.WallTime == ts2.WallTime {
		return false
	}

	// Different nodes and within threshold -> potential conflict
	diff := ts1.WallTime - ts2.WallTime
	if diff < 0 {
		diff = -diff
	}

	return diff <= thresholdNanos
}

// getConflictWindow returns the conflict window duration in nanoseconds
func getConflictWindow() int64 {
	if cfg.Config != nil {
		return int64(cfg.Config.MVCC.ConflictWindowSeconds) * 1000 * 1000 * 1000
	}
	return 10 * 1000 * 1000 * 1000 // Default: 10 seconds
}

// DefaultConflictHandler handles write-write conflicts by aborting the transaction
type DefaultConflictHandler struct{}

// OnConflict handles write-write conflicts by returning the error (aborting)
func (h *DefaultConflictHandler) OnConflict(txn *Transaction, conflictErr error) error {
	return conflictErr
}
