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
	return &Clock{
		nodeID:   nodeID,
		wallTime: time.Now().UnixNano(),
		logical:  0,
	}
}

// Now generates a new timestamp for a local event
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get current physical time
	physicalNow := time.Now().UnixNano()

	if physicalNow > c.wallTime {
		// Physical time advanced, reset logical clock
		c.wallTime = physicalNow
		c.logical = 0
	} else {
		// Physical time hasn't advanced (or went backwards), increment logical
		c.logical++
	}

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
		c.logical = 0
	} else {
		// Local wall time was ahead
		c.logical++
	}

	c.wallTime = maxWall

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
