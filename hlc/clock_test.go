package hlc

import (
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

	// Calling Now again immediately should increment logical
	ts2 := clock.Now()
	if ts2.WallTime != ts1.WallTime {
		// Physical time advanced - logical resets
		if ts2.Logical != 0 {
			t.Errorf("If wall time advanced, logical should reset to 0")
		}
	} else {
		// Same wall time - logical increments
		if ts2.Logical != ts1.Logical+1 {
			t.Errorf("Expected logical %d, got %d", ts1.Logical+1, ts2.Logical)
		}
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
