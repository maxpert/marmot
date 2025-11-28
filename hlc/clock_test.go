package hlc

import (
	"sync"
	"testing"
	"time"
)

func TestClock_Now(t *testing.T) {
	clock := NewClock(1)

	ts1 := clock.Now()
	if ts1.NodeID != 1 {
		t.Errorf("Expected node ID 1, got %d", ts1.NodeID)
	}
	if ts1.WallTime == 0 {
		t.Error("Wall time should not be zero")
	}

	// Calling Now again should produce a strictly greater timestamp
	// (either by wall time or by logical counter within same ms)
	ts2 := clock.Now()
	if !After(ts2, ts1) {
		t.Errorf("Second timestamp should be after first: ts1=%+v, ts2=%+v", ts1, ts2)
	}
}

func TestClock_MonotonicIncrement(t *testing.T) {
	clock := NewClock(1)

	// Generate 100 timestamps rapidly
	timestamps := make([]Timestamp, 100)
	for i := 0; i < 100; i++ {
		timestamps[i] = clock.Now()
	}

	// Verify they're all monotonically increasing
	for i := 1; i < len(timestamps); i++ {
		if !After(timestamps[i], timestamps[i-1]) {
			t.Errorf("Timestamp %d not after %d", i, i-1)
		}
	}
}

func TestClock_Update(t *testing.T) {
	clock1 := NewClock(1)
	clock2 := NewClock(2)

	// Clock 1 generates a timestamp
	ts1 := clock1.Now()

	// Clock 2 receives it and updates
	ts2 := clock2.Update(ts1)

	// Clock 2's timestamp should be after clock 1's
	if !After(ts2, ts1) {
		t.Error("Updated timestamp should be after received timestamp")
	}

	if ts2.NodeID != 2 {
		t.Errorf("Node ID should be 2, got %d", ts2.NodeID)
	}
}

func TestClock_UpdateAdvancesTime(t *testing.T) {
	clock := NewClock(1)

	// Start with a timestamp
	ts1 := clock.Now()

	// Simulate receiving a timestamp from the future
	futureTS := Timestamp{
		WallTime: ts1.WallTime + 1000000000, // 1 second ahead
		Logical:  5,
		NodeID:   2,
	}

	ts2 := clock.Update(futureTS)

	// Our clock should jump forward
	if ts2.WallTime <= ts1.WallTime {
		t.Error("Clock should advance when receiving future timestamp")
	}

	// And should be ahead of the received timestamp
	if !After(ts2, futureTS) {
		t.Error("Updated timestamp should be after received future timestamp")
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		a    Timestamp
		b    Timestamp
		want int
	}{
		{
			name: "a before b (wall time)",
			a:    Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			b:    Timestamp{WallTime: 200, Logical: 0, NodeID: 1},
			want: -1,
		},
		{
			name: "a after b (wall time)",
			a:    Timestamp{WallTime: 200, Logical: 0, NodeID: 1},
			b:    Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			want: 1,
		},
		{
			name: "a before b (logical)",
			a:    Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			b:    Timestamp{WallTime: 100, Logical: 1, NodeID: 1},
			want: -1,
		},
		{
			name: "a after b (logical)",
			a:    Timestamp{WallTime: 100, Logical: 2, NodeID: 1},
			b:    Timestamp{WallTime: 100, Logical: 1, NodeID: 1},
			want: 1,
		},
		{
			name: "a before b (node ID tiebreaker)",
			a:    Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			b:    Timestamp{WallTime: 100, Logical: 0, NodeID: 2},
			want: -1,
		},
		{
			name: "equal",
			a:    Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			b:    Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Compare(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("Compare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLess(t *testing.T) {
	a := Timestamp{WallTime: 100, Logical: 0, NodeID: 1}
	b := Timestamp{WallTime: 200, Logical: 0, NodeID: 1}

	if !Less(a, b) {
		t.Error("a should be less than b")
	}

	if Less(b, a) {
		t.Error("b should not be less than a")
	}

	if Less(a, a) {
		t.Error("a should not be less than itself")
	}
}

func TestEqual(t *testing.T) {
	a := Timestamp{WallTime: 100, Logical: 5, NodeID: 1}
	b := Timestamp{WallTime: 100, Logical: 5, NodeID: 1}
	c := Timestamp{WallTime: 100, Logical: 6, NodeID: 1}

	if !Equal(a, b) {
		t.Error("a should equal b")
	}

	if Equal(a, c) {
		t.Error("a should not equal c")
	}
}

func TestAfter(t *testing.T) {
	a := Timestamp{WallTime: 200, Logical: 0, NodeID: 1}
	b := Timestamp{WallTime: 100, Logical: 0, NodeID: 1}

	if !After(a, b) {
		t.Error("a should be after b")
	}

	if After(b, a) {
		t.Error("b should not be after a")
	}

	if After(a, a) {
		t.Error("a should not be after itself")
	}
}

func TestTimestamp_PhysicalTime(t *testing.T) {
	now := time.Now()
	ts := Timestamp{
		WallTime: now.UnixNano(),
		Logical:  0,
		NodeID:   1,
	}

	physicalTime := ts.PhysicalTime()
	diff := physicalTime.Sub(now).Abs()

	// Should be within 1 millisecond (accounting for execution time)
	if diff > time.Millisecond {
		t.Errorf("Physical time extraction inaccurate: diff = %v", diff)
	}
}

func TestClock_ConcurrentAccess(t *testing.T) {
	clock := NewClock(1)
	done := make(chan bool)

	// Spawn 10 goroutines that each generate 100 timestamps
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				clock.Now()
			}
			done <- true
		}()
	}

	// Wait for all to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// If we reach here without deadlock or panic, test passes
}

func TestClock_UpdateConcurrent(t *testing.T) {
	clock := NewClock(1)
	done := make(chan bool)

	remoteTS := Timestamp{
		WallTime: time.Now().UnixNano(),
		Logical:  10,
		NodeID:   2,
	}

	// Spawn goroutines doing Now() and Update() concurrently
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 50; j++ {
				clock.Now()
			}
			done <- true
		}()

		go func() {
			for j := 0; j < 50; j++ {
				clock.Update(remoteTS)
			}
			done <- true
		}()
	}

	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestClock_LogicalOverflow(t *testing.T) {
	clock := NewClock(1)

	// Manually set logical clock high
	clock.logical = 2147483647 // Max int32

	// This should still work (will overflow but that's Go's defined behavior)
	ts := clock.Now()
	if ts.NodeID != 1 {
		t.Error("Clock should still function after logical overflow")
	}
}

func TestClock_UniqueTxnIDs(t *testing.T) {
	clock := NewClock(1)
	ids := make(chan uint64, 10000)

	// Spawn 100 goroutines each generating 100 txn_ids
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ts := clock.Now()
				ids <- ts.ToTxnID()
			}
		}()
	}

	go func() {
		wg.Wait()
		close(ids)
	}()

	seen := make(map[uint64]int)
	for id := range ids {
		seen[id]++
	}

	duplicates := 0
	for id, count := range seen {
		if count > 1 {
			t.Errorf("DUPLICATE: txn_id=%d appears %d times", id, count)
			duplicates++
		}
	}

	if duplicates > 0 {
		t.Errorf("Found %d duplicate txn_ids out of 10000", duplicates)
	}
	if len(seen) != 10000 {
		t.Errorf("Expected 10000 unique txn_ids, got %d", len(seen))
	}
}

// TestCrossNodeTxnIDUniqueness verifies that different nodes produce different
// txn IDs even when they have identical wall time and logical counter.
// This is the CRITICAL test for the multi-node collision bug fix.
func TestCrossNodeTxnIDUniqueness(t *testing.T) {
	// Create timestamps with same wall time and logical but different NodeID
	wallTime := int64(1732000000000000000) // Fixed timestamp
	logical := int32(1)

	// Test: Different nodes at exact same time must produce different IDs
	ts1 := Timestamp{WallTime: wallTime, Logical: logical, NodeID: 1}
	ts2 := Timestamp{WallTime: wallTime, Logical: logical, NodeID: 2}
	ts3 := Timestamp{WallTime: wallTime, Logical: logical, NodeID: 3}

	id1 := ts1.ToTxnID()
	id2 := ts2.ToTxnID()
	id3 := ts3.ToTxnID()

	if id1 == id2 {
		t.Errorf("COLLISION: Node 1 and Node 2 produced same txn_id=%d at same time!", id1)
	}
	if id2 == id3 {
		t.Errorf("COLLISION: Node 2 and Node 3 produced same txn_id=%d at same time!", id2)
	}
	if id1 == id3 {
		t.Errorf("COLLISION: Node 1 and Node 3 produced same txn_id=%d at same time!", id1)
	}

	t.Logf("Node 1 ID: %d", id1)
	t.Logf("Node 2 ID: %d", id2)
	t.Logf("Node 3 ID: %d", id3)
}

// TestTxnIDFormat verifies the bit layout of transaction IDs
func TestTxnIDFormat(t *testing.T) {
	// Create a known timestamp
	wallTime := int64(1000000000000) // 1000 seconds in nanoseconds
	logical := int32(5)
	nodeID := uint64(3)

	ts := Timestamp{WallTime: wallTime, Logical: logical, NodeID: nodeID}
	txnID := ts.ToTxnID()

	// Extract components
	wallMS := uint64(wallTime / 1_000_000) // = 1000
	expectedID := (wallMS << TotalShiftBits) | ((nodeID & NodeIDMask) << LogicalBits) | (uint64(logical) & LogicalMask)

	if txnID != expectedID {
		t.Errorf("TxnID format mismatch: got %d, expected %d", txnID, expectedID)
		t.Logf("wallMS=%d, nodeID=%d, logical=%d", wallMS, nodeID, logical)
		t.Logf("TotalShiftBits=%d, LogicalBits=%d", TotalShiftBits, LogicalBits)
	}

	// Verify node ID can be extracted
	extractedNodeID := (txnID >> LogicalBits) & NodeIDMask
	if extractedNodeID != nodeID {
		t.Errorf("Failed to extract nodeID: got %d, expected %d", extractedNodeID, nodeID)
	}

	// Verify logical can be extracted
	extractedLogical := txnID & LogicalMask
	if extractedLogical != uint64(logical) {
		t.Errorf("Failed to extract logical: got %d, expected %d", extractedLogical, logical)
	}
}

// TestTxnIDTimeOrdering verifies that IDs from the same node preserve time ordering
func TestTxnIDTimeOrdering(t *testing.T) {
	nodeID := uint64(1)

	// Earlier timestamp
	ts1 := Timestamp{WallTime: 1000000000000, Logical: 5, NodeID: nodeID}
	// Later timestamp (same ms, higher logical)
	ts2 := Timestamp{WallTime: 1000000000000, Logical: 6, NodeID: nodeID}
	// Even later (higher ms)
	ts3 := Timestamp{WallTime: 2000000000000, Logical: 1, NodeID: nodeID}

	id1 := ts1.ToTxnID()
	id2 := ts2.ToTxnID()
	id3 := ts3.ToTxnID()

	if id1 >= id2 {
		t.Errorf("Same-ms timestamps should be ordered by logical: id1=%d, id2=%d", id1, id2)
	}
	if id2 >= id3 {
		t.Errorf("Higher-ms timestamp should have higher ID: id2=%d, id3=%d", id2, id3)
	}
}

func BenchmarkClock_Now(b *testing.B) {
	clock := NewClock(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Now()
	}
}

func BenchmarkClock_Update(b *testing.B) {
	clock := NewClock(1)
	remoteTS := Timestamp{
		WallTime: time.Now().UnixNano(),
		Logical:  5,
		NodeID:   2,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Update(remoteTS)
	}
}

func BenchmarkCompare(b *testing.B) {
	ts1 := Timestamp{WallTime: 100, Logical: 5, NodeID: 1}
	ts2 := Timestamp{WallTime: 200, Logical: 3, NodeID: 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Compare(ts1, ts2)
	}
}
