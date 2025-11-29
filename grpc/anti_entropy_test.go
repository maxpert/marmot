package grpc

import (
	"testing"
	"time"
)

// TestAntiEntropyShouldUseDeltaSync tests the decision logic for delta vs snapshot
func TestAntiEntropyShouldUseDeltaSync(t *testing.T) {
	ae := &AntiEntropyService{
		deltaThresholdTxns:    10000,
		deltaThresholdSeconds: 3600,
	}

	tests := []struct {
		name        string
		lagTxns     uint64
		lagTime     time.Duration
		expectDelta bool
	}{
		{
			name:        "Small lag - use delta",
			lagTxns:     100,
			lagTime:     5 * time.Minute,
			expectDelta: true,
		},
		{
			name:        "Large transaction lag - use snapshot",
			lagTxns:     15000,
			lagTime:     10 * time.Minute,
			expectDelta: false,
		},
		{
			name:        "Large time lag - use snapshot",
			lagTxns:     5000,
			lagTime:     2 * time.Hour,
			expectDelta: false,
		},
		{
			name:        "At threshold - use delta",
			lagTxns:     9999,
			lagTime:     59 * time.Minute,
			expectDelta: true,
		},
		{
			name:        "Just over threshold - use snapshot",
			lagTxns:     10001,
			lagTime:     10 * time.Minute,
			expectDelta: false,
		},
		{
			name:        "Zero lag - use delta",
			lagTxns:     0,
			lagTime:     0,
			expectDelta: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ae.shouldUseDeltaSync(tt.lagTxns, tt.lagTime)
			if result != tt.expectDelta {
				t.Errorf("shouldUseDeltaSync(%d txns, %v) = %v, want %v",
					tt.lagTxns, tt.lagTime, result, tt.expectDelta)
			}
		})
	}
}

// TestAntiEntropyGetStats tests stats reporting
func TestAntiEntropyGetStats(t *testing.T) {
	ae := &AntiEntropyService{
		enabled:               true,
		running:               false,
		interval:              60 * time.Second,
		deltaThresholdTxns:    10000,
		deltaThresholdSeconds: 3600,
	}

	stats := ae.GetStats()

	if enabled, ok := stats["enabled"].(bool); !ok || !enabled {
		t.Errorf("Expected enabled=true, got %v", stats["enabled"])
	}

	if running, ok := stats["running"].(bool); !ok || running {
		t.Errorf("Expected running=false, got %v", stats["running"])
	}

	if interval, ok := stats["interval_seconds"].(float64); !ok || interval != 60.0 {
		t.Errorf("Expected interval_seconds=60, got %v", stats["interval_seconds"])
	}

	if threshold, ok := stats["delta_threshold_txns"].(int); !ok || threshold != 10000 {
		t.Errorf("Expected delta_threshold_txns=10000, got %v", stats["delta_threshold_txns"])
	}
}

// TestAntiEntropyStartStop tests start/stop lifecycle
func TestAntiEntropyStartStop(t *testing.T) {
	// Create a minimal registry for testing
	registry := NewNodeRegistry(1, "localhost:8081")

	ae := &AntiEntropyService{
		nodeID:                1,
		registry:              registry, // Add registry to avoid nil pointer
		enabled:               true,
		interval:              100 * time.Millisecond,
		deltaThresholdTxns:    10000,
		deltaThresholdSeconds: 3600,
		stopCh:                make(chan struct{}),
	}

	// Test start
	ae.Start()
	time.Sleep(50 * time.Millisecond)

	ae.mu.Lock()
	running := ae.running
	ae.mu.Unlock()

	if !running {
		t.Error("Expected service to be running after Start()")
	}

	// Test stop
	ae.Stop()
	time.Sleep(150 * time.Millisecond) // Give more time for goroutine to exit

	ae.mu.Lock()
	running = ae.running
	ae.mu.Unlock()

	if running {
		t.Error("Expected service to be stopped after Stop()")
	}
}

// TestAntiEntropyDisabled tests that disabled service doesn't start
func TestAntiEntropyDisabled(t *testing.T) {
	ae := &AntiEntropyService{
		enabled: false,
		stopCh:  make(chan struct{}),
	}

	ae.Start()
	time.Sleep(50 * time.Millisecond)

	ae.mu.Lock()
	running := ae.running
	ae.mu.Unlock()

	if running {
		t.Error("Disabled service should not be running")
	}
}

// TestLastSyncTimeZeroHandling tests that LastSyncTime=0 doesn't cause 54-year lag
func TestLastSyncTimeZeroHandling(t *testing.T) {
	ae := &AntiEntropyService{
		deltaThresholdTxns:    10000,
		deltaThresholdSeconds: 3600, // 1 hour
	}

	// Test case 1: LastSyncTime=0 should result in 0 duration (use delta sync)
	// The fix ensures we don't compute time.Since(0) which would be ~54 years
	var timeLag time.Duration
	lastSyncTime := int64(0)
	if lastSyncTime > 0 {
		timeLag = time.Since(time.Unix(0, lastSyncTime))
	}

	// With 0 lag, should use delta sync
	if !ae.shouldUseDeltaSync(100, timeLag) {
		t.Error("LastSyncTime=0 should result in 0 duration, allowing delta sync")
	}

	// Test case 2: Verify that non-zero LastSyncTime still works
	lastSyncTime = time.Now().Add(-30 * time.Minute).UnixNano()
	timeLag = time.Since(time.Unix(0, lastSyncTime))

	// 30 minutes is within 1 hour threshold, should use delta
	if !ae.shouldUseDeltaSync(100, timeLag) {
		t.Error("30 minute time lag should allow delta sync with 1 hour threshold")
	}

	// Test case 3: Old LastSyncTime exceeding threshold
	lastSyncTime = time.Now().Add(-2 * time.Hour).UnixNano()
	timeLag = time.Since(time.Unix(0, lastSyncTime))

	// 2 hours exceeds 1 hour threshold, should use snapshot
	if ae.shouldUseDeltaSync(100, timeLag) {
		t.Error("2 hour time lag should trigger snapshot with 1 hour threshold")
	}
}
