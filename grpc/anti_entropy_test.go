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
	registry := NewNodeRegistry(1)

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
