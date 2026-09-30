package id

import (
	"sync"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// Generator provides unique IDs for distributed autoincrement columns.
// IDs are guaranteed unique across nodes and roughly time-ordered.
//
// It mints every id one statement needs in one call, and returns an error so
// that a generator which must coordinate to mint ids can refuse; every caller
// has to hand that error to the client.
type Generator interface {
	// NextIDs fills ids with distinct ids in ascending order.
	NextIDs(ids []uint64) error
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

// NextIDs fills ids with successive unique 64-bit ids. The clock is strictly
// monotonic, so they ascend.
// Format: (physical_ms << 22) | (node_id << 16) | logical
// See hlc.Timestamp.ToTxnID for bit allocation details.
func (g *HLCGenerator) NextIDs(ids []uint64) error {
	for i := range ids {
		ids[i] = g.clock.Now().ToTxnID()
	}
	return nil
}

// Compact ID format constants (53-bit)
const (
	CompactEpoch     int64 = 1735776000000 // Jan 2, 2025 00:00:00 UTC in ms
	CompactSeqBits         = 6
	CompactNodeBits        = 6
	CompactSeqMax          = (1 << CompactSeqBits) - 1        // 63
	CompactNodeMask        = (1 << CompactNodeBits) - 1       // 63
	CompactNodeShift       = CompactSeqBits                   // 6
	CompactTimeShift       = CompactSeqBits + CompactNodeBits // 12
)

// CompactGenerator generates compact 53-bit IDs.
// Format: | 41 bits: timestamp | 6 bits: node | 6 bits: sequence |
// Epoch: Jan 1, 2025 00:00:00 UTC = 1735689600000 ms
// Max ID: 2^53 - 1 = 9007199254740991 = Number.MAX_SAFE_INTEGER
type CompactGenerator struct {
	nodeID   uint64
	lastMS   int64
	sequence uint64
	mu       sync.Mutex
}

// NewCompactGenerator creates a new 53-bit ID generator.
// nodeID must be in range [0, 63]. Values outside this range are masked.
func NewCompactGenerator(nodeID uint64) *CompactGenerator {
	return &CompactGenerator{
		nodeID: nodeID & CompactNodeMask,
	}
}

// NextIDs fills ids with unique 53-bit ids safe for JavaScript. Thread-safe.
// IDs are monotonically increasing and unique; they are minted under one lock
// acquisition, so no other caller's id lands between them.
func (g *CompactGenerator) NextIDs(ids []uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range ids {
		ids[i] = g.next()
	}
	return nil
}

// next mints one id. The caller holds g.mu.
func (g *CompactGenerator) next() uint64 {
	nowMS := time.Now().UnixMilli()
	relativeMS := nowMS - CompactEpoch

	// Clock skew protection: never go backward
	if relativeMS < g.lastMS {
		relativeMS = g.lastMS
	}

	// Clamp negative (clock before epoch)
	if relativeMS < 0 {
		relativeMS = 0
	}

	if relativeMS == g.lastMS {
		g.sequence++
		if g.sequence > CompactSeqMax {
			// Wait for next millisecond
			for relativeMS <= g.lastMS {
				time.Sleep(100 * time.Microsecond)
				nowMS = time.Now().UnixMilli()
				relativeMS = nowMS - CompactEpoch
				if relativeMS < 0 {
					relativeMS = 0
				}
			}
			g.lastMS = relativeMS
			g.sequence = 0
		}
	} else {
		g.lastMS = relativeMS
		g.sequence = 0
	}

	return (uint64(relativeMS) << CompactTimeShift) |
		((g.nodeID & CompactNodeMask) << CompactNodeShift) |
		g.sequence
}
