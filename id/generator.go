package id

import "github.com/maxpert/marmot/hlc"

// Generator provides unique IDs for distributed autoincrement columns.
// IDs are guaranteed unique across nodes and roughly time-ordered.
type Generator interface {
	NextID() uint64
}

// HLCGenerator generates unique IDs using the Hybrid Logical Clock.
// Thread-safe via HLC's internal mutex.
type HLCGenerator struct {
	clock *hlc.Clock
}

// NewHLCGenerator creates a new ID generator backed by the given HLC.
func NewHLCGenerator(clock *hlc.Clock) *HLCGenerator {
	return &HLCGenerator{clock: clock}
}

// NextID generates a unique 64-bit ID.
// Format: (physical_ms << 22) | (node_id << 16) | logical
// See hlc.Timestamp.ToTxnID for bit allocation details.
func (g *HLCGenerator) NextID() uint64 {
	return g.clock.Now().ToTxnID()
}
