package coordinator

import (
	"time"

	"github.com/maxpert/marmot/telemetry"
)

// TxnMetrics provides centralized transaction telemetry recording.
// It tracks transaction timing and outcomes (success/failure/conflict)
// for both write and read transaction types.
type TxnMetrics struct {
	txnType   string // "write" or "read"
	startTime time.Time
}

// NewTxnMetrics creates a new transaction metrics recorder.
// txnType should be "write" or "read".
func NewTxnMetrics(txnType string) *TxnMetrics {
	return &TxnMetrics{
		txnType:   txnType,
		startTime: time.Now(),
	}
}

// RecordPhase records the duration of a specific transaction phase.
// Common phases: "prepare", "commit"
func (m *TxnMetrics) RecordPhase(phase string, duration time.Duration) {
	switch phase {
	case "prepare":
		telemetry.TwoPhasePrepareSeconds.Observe(duration.Seconds())
	case "commit":
		telemetry.TwoPhaseCommitSeconds.Observe(duration.Seconds())
	}
}

// RecordFailure records a transaction failure with the specified error type
// and returns the original error unchanged (pass-through).
// Common error types: "conflict", "failed"
func (m *TxnMetrics) RecordFailure(errType string, err error) error {
	telemetry.TxnTotal.With(m.txnType, errType).Inc()
	telemetry.TxnDurationSeconds.With(m.txnType).Observe(time.Since(m.startTime).Seconds())
	return err
}

// RecordSuccess records a successful transaction completion.
// Returns nil for convenient use in return statements.
func (m *TxnMetrics) RecordSuccess() error {
	telemetry.TxnTotal.With(m.txnType, "success").Inc()
	telemetry.TxnDurationSeconds.With(m.txnType).Observe(time.Since(m.startTime).Seconds())
	return nil
}
