package coordinator

import (
	"errors"
	"testing"
	"time"
)

func TestNewTxnMetrics(t *testing.T) {
	tests := []struct {
		name    string
		txnType string
	}{
		{
			name:    "write transaction",
			txnType: "write",
		},
		{
			name:    "read transaction",
			txnType: "read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := time.Now()
			m := NewTxnMetrics(tt.txnType)
			after := time.Now()

			if m == nil {
				t.Fatal("NewTxnMetrics returned nil")
			}

			if m.txnType != tt.txnType {
				t.Errorf("txnType = %q, want %q", m.txnType, tt.txnType)
			}

			if m.startTime.Before(before) || m.startTime.After(after) {
				t.Errorf("startTime not captured correctly: got %v, expected between %v and %v",
					m.startTime, before, after)
			}
		})
	}
}

func TestRecordPhase(t *testing.T) {
	tests := []struct {
		name     string
		phase    string
		duration time.Duration
	}{
		{
			name:     "prepare phase",
			phase:    "prepare",
			duration: 100 * time.Millisecond,
		},
		{
			name:     "commit phase",
			phase:    "commit",
			duration: 50 * time.Millisecond,
		},
		{
			name:     "unknown phase (should not panic)",
			phase:    "unknown",
			duration: 10 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewTxnMetrics("write")

			// Should not panic
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("RecordPhase panicked: %v", r)
				}
			}()

			m.RecordPhase(tt.phase, tt.duration)
		})
	}
}

func TestRecordSuccess(t *testing.T) {
	tests := []struct {
		name    string
		txnType string
	}{
		{
			name:    "write transaction success",
			txnType: "write",
		},
		{
			name:    "read transaction success",
			txnType: "read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewTxnMetrics(tt.txnType)

			// Add small delay to ensure duration is measurable
			time.Sleep(1 * time.Millisecond)

			err := m.RecordSuccess()

			// Should return nil for convenient use in return statements
			if err != nil {
				t.Errorf("RecordSuccess() returned error: %v, want nil", err)
			}
		})
	}
}

func TestRecordFailure(t *testing.T) {
	tests := []struct {
		name     string
		txnType  string
		errType  string
		inputErr error
	}{
		{
			name:     "write transaction conflict",
			txnType:  "write",
			errType:  "conflict",
			inputErr: errors.New("write-write conflict detected"),
		},
		{
			name:     "write transaction failed",
			txnType:  "write",
			errType:  "failed",
			inputErr: errors.New("replication failed"),
		},
		{
			name:     "read transaction failed",
			txnType:  "read",
			errType:  "failed",
			inputErr: errors.New("query execution failed"),
		},
		{
			name:     "nil error",
			txnType:  "write",
			errType:  "failed",
			inputErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewTxnMetrics(tt.txnType)

			// Add small delay to ensure duration is measurable
			time.Sleep(1 * time.Millisecond)

			err := m.RecordFailure(tt.errType, tt.inputErr)

			// CRITICAL: Verify error pass-through
			if err != tt.inputErr {
				t.Errorf("RecordFailure() returned different error: got %v, want %v", err, tt.inputErr)
			}

			// Verify error is unchanged (same reference)
			if tt.inputErr != nil && err.Error() != tt.inputErr.Error() {
				t.Errorf("RecordFailure() modified error message: got %q, want %q",
					err.Error(), tt.inputErr.Error())
			}
		})
	}
}

func TestRecordFailure_ErrorPassThrough(t *testing.T) {
	// Specific test to verify the pass-through pattern
	// This is critical for error handling in distributed systems
	tests := []struct {
		name     string
		inputErr error
	}{
		{
			name:     "specific error instance",
			inputErr: errors.New("specific conflict at row 42"),
		},
		{
			name:     "wrapped error",
			inputErr: errors.New("wrapped: original error"),
		},
		{
			name:     "nil error (edge case)",
			inputErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewTxnMetrics("write")

			returnedErr := m.RecordFailure("conflict", tt.inputErr)

			// Verify EXACT same error instance is returned
			if returnedErr != tt.inputErr {
				t.Errorf("RecordFailure did not pass through original error")
				t.Errorf("  got:  %p %v", returnedErr, returnedErr)
				t.Errorf("  want: %p %v", tt.inputErr, tt.inputErr)
			}
		})
	}
}

func TestRecordFailure_DifferentErrorTypes(t *testing.T) {
	// Test various error types that might occur in practice
	tests := []struct {
		name     string
		errType  string
		inputErr error
	}{
		{
			name:     "conflict error type",
			errType:  "conflict",
			inputErr: errors.New("write conflict"),
		},
		{
			name:     "failed error type",
			errType:  "failed",
			inputErr: errors.New("network timeout"),
		},
		{
			name:     "timeout error type",
			errType:  "timeout",
			inputErr: errors.New("context deadline exceeded"),
		},
		{
			name:     "unknown error type",
			errType:  "unknown",
			inputErr: errors.New("unexpected error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewTxnMetrics("write")

			err := m.RecordFailure(tt.errType, tt.inputErr)

			// Verify original error is returned
			if err != tt.inputErr {
				t.Errorf("RecordFailure() = %v, want %v", err, tt.inputErr)
			}
		})
	}
}

func TestTxnMetrics_MultipleOperations(t *testing.T) {
	// Test that multiple operations can be performed on the same metrics instance
	m := NewTxnMetrics("write")

	// Record prepare phase
	m.RecordPhase("prepare", 10*time.Millisecond)

	// Record commit phase
	m.RecordPhase("commit", 5*time.Millisecond)

	// Add delay to ensure duration is measurable
	time.Sleep(1 * time.Millisecond)

	// Record final outcome
	err := m.RecordSuccess()
	if err != nil {
		t.Errorf("RecordSuccess() returned error: %v", err)
	}
}

func TestTxnMetrics_DurationMeasurement(t *testing.T) {
	// Verify that duration is measured from construction time
	m := NewTxnMetrics("write")

	// Sleep to create measurable duration
	sleepDuration := 10 * time.Millisecond
	time.Sleep(sleepDuration)

	// Record success (this should measure duration from startTime)
	// We can't directly verify the metric value, but we can ensure no panic
	err := m.RecordSuccess()
	if err != nil {
		t.Errorf("RecordSuccess() returned error: %v", err)
	}

	// Verify another operation on same instance
	time.Sleep(5 * time.Millisecond)
	err = m.RecordFailure("failed", errors.New("test error"))
	if err == nil || err.Error() != "test error" {
		t.Errorf("RecordFailure() = %v, want 'test error'", err)
	}
}
