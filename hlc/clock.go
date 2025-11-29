package hlc

import (
	"sync"
	"time"
)

// Clock implements a Hybrid Logical Clock for distributed causality tracking
type Clock struct {
	nodeID   uint64
	wallTime int64
	logical  int32
	lastMS   int64 // Last millisecond used for txn_id generation - logical resets when this changes
	mu       sync.Mutex
}

// Timestamp represents a point in time across the distributed system
type Timestamp struct {
	WallTime int64
	Logical  int32
	NodeID   uint64
}

// NewClock creates a new HLC instance
func NewClock(nodeID uint64) *Clock {
	now := time.Now().UnixNano()
	return &Clock{
		nodeID:   nodeID,
		wallTime: now,
		logical:  0,
		lastMS:   now / 1_000_000,
	}
}

// MaxLogical is the maximum value for logical counter before overflow
// 16 bits = 65535
const MaxLogical = LogicalMask

// Now generates a new timestamp for a local event
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get current physical time
	physicalNow := time.Now().UnixNano()
	currentMS := physicalNow / 1_000_000

	if physicalNow > c.wallTime {
		c.wallTime = physicalNow
	}

	// Reset logical when millisecond changes to prevent overflow into physical bits
	// ToTxnID uses 16 bits for logical (~65k IDs/ms), so we must reset per-millisecond
	if currentMS > c.lastMS {
		c.lastMS = currentMS
		c.logical = 0
	}

	// Overflow protection: if we've exhausted logical counter for this millisecond,
	// spin until the next millisecond. This prevents txn_id collisions.
	for c.logical >= MaxLogical {
		time.Sleep(100 * time.Microsecond)
		now := time.Now().UnixNano()
		nowMS := now / 1_000_000
		if nowMS > c.lastMS {
			c.wallTime = now
			c.lastMS = nowMS
			c.logical = 0
			break
		}
	}

	c.logical++

	return Timestamp{
		WallTime: c.wallTime,
		Logical:  c.logical,
		NodeID:   c.nodeID,
	}
}

// Update updates the clock based on a received timestamp
// Returns the updated current time
func (c *Clock) Update(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	physicalNow := time.Now().UnixNano()

	// Take maximum of all three: local wall, remote wall, physical now
	maxWall := c.wallTime
	if remote.WallTime > maxWall {
		maxWall = remote.WallTime
	}
	if physicalNow > maxWall {
		maxWall = physicalNow
	}

	// Track millisecond for txn_id uniqueness
	maxWallMS := maxWall / 1_000_000

	if maxWall == c.wallTime && maxWall == remote.WallTime {
		// Same wall time: increment logical to be greater than both
		if remote.Logical > c.logical {
			c.logical = remote.Logical + 1
		} else {
			c.logical++
		}
	} else if maxWall == remote.WallTime {
		// Remote wall time is ahead
		c.wallTime = remote.WallTime
		c.logical = remote.Logical + 1
	} else if maxWall == physicalNow {
		// Physical time advanced past both
		c.wallTime = physicalNow
		// Only reset logical if millisecond changed
		if maxWallMS > c.lastMS {
			c.logical = 0
		} else {
			c.logical++
		}
	} else {
		// Local wall time was ahead
		c.logical++
	}

	c.wallTime = maxWall
	c.lastMS = maxWallMS

	// Overflow protection: if logical counter exceeds max, wait for next millisecond
	for c.logical >= MaxLogical {
		time.Sleep(100 * time.Microsecond)
		now := time.Now().UnixNano()
		nowMS := now / 1_000_000
		if nowMS > c.lastMS {
			c.wallTime = now
			c.lastMS = nowMS
			c.logical = 1 // Start at 1 since we're generating a timestamp
			break
		}
	}

	return Timestamp{
		WallTime: c.wallTime,
		Logical:  c.logical,
		NodeID:   c.nodeID,
	}
}

// Compare compares two timestamps
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func Compare(a, b Timestamp) int {
	if a.WallTime < b.WallTime {
		return -1
	}
	if a.WallTime > b.WallTime {
		return 1
	}

	// Wall times are equal, compare logical
	if a.Logical < b.Logical {
		return -1
	}
	if a.Logical > b.Logical {
		return 1
	}

	// Both wall and logical are equal, use node ID as tiebreaker
	if a.NodeID < b.NodeID {
		return -1
	}
	if a.NodeID > b.NodeID {
		return 1
	}

	return 0
}

// Less returns true if a happened before b
func Less(a, b Timestamp) bool {
	return Compare(a, b) < 0
}

// Equal returns true if timestamps are equal
func Equal(a, b Timestamp) bool {
	return Compare(a, b) == 0
}

// After returns true if a happened after b
func After(a, b Timestamp) bool {
	return Compare(a, b) > 0
}

// PhysicalTime returns the physical time component as time.Time
func (t Timestamp) PhysicalTime() time.Time {
	return time.Unix(0, t.WallTime)
}

// String returns a human-readable representation
func (t Timestamp) String() string {
	return t.PhysicalTime().Format(time.RFC3339Nano)
}

// LogicalBits is the number of bits reserved for logical counter in txn IDs.
// 16 bits = ~65k IDs per millisecond per node.
const LogicalBits = 16

// LogicalMask masks the logical counter to 16 bits for ToTxnID
const LogicalMask = (1 << LogicalBits) - 1

// NodeIDBits is the number of bits reserved for node ID in txn IDs.
// 6 bits = 64 nodes maximum per cluster.
const NodeIDBits = 6

// NodeIDMask masks the node ID to 6 bits for ToTxnID
const NodeIDMask = (1 << NodeIDBits) - 1

// TotalShiftBits is the total bits to shift wall time (NodeIDBits + LogicalBits)
const TotalShiftBits = NodeIDBits + LogicalBits // 22 bits

// ToTxnID converts a timestamp to a unique transaction ID.
// Format: (physical_ms << 22) | (node_id << 16) | logical
//
// Bit allocation (64 bits total):
//   - 42 bits for wall time in milliseconds (~139 years from epoch)
//   - 6 bits for node ID (64 nodes max)
//   - 16 bits for logical counter (~65k per ms per node)
//
// This ensures unique IDs across nodes even when transactions
// occur at the exact same millisecond with the same logical counter.
func (t Timestamp) ToTxnID() uint64 {
	physicalMS := uint64(t.WallTime / 1_000_000)
	nodeID := t.NodeID & NodeIDMask
	logical := uint64(t.Logical) & LogicalMask
	return (physicalMS << TotalShiftBits) | (nodeID << LogicalBits) | logical
}
