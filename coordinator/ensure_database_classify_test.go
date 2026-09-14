//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/maxpert/marmot/protocol"
)

// TestIsRetryableEnsureDatabaseError is a white-box (package coordinator) unit
// test of EnsureDatabase's retry classifier: exactly two error shapes are
// retryable (a 1213 write-write conflict, and this node's own DDL lock being
// held by another in-flight transaction), classified by TYPE - errors.As /
// errors.Is - never by matching message text. Every other error, including
// ones EnsureDatabase's own code paths can plausibly produce (quorum
// failure, an unknown-database rejection) and ones that are not reachable
// through CoordinatorHandler today (protocol.ErrReadOnly, a bare context
// cancellation) but which R1 explicitly calls out as required to fail fast,
// must be classified as non-retryable.
func TestIsRetryableEnsureDatabaseError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"nil", nil, false},
		{"deadlock 1213", protocol.ErrDeadlock(), true},
		{"ddl lock held, bare sentinel", ErrDDLLockHeld, true},
		{
			"ddl lock held, wrapped once (AcquireLock's own wrap)",
			fmt.Errorf("%w: DDL lock for database 'x' is held by txn 1 (node 1)", ErrDDLLockHeld),
			true,
		},
		{
			"ddl lock held, double-wrapped (AcquireLock -> handleMutation's failed-to-acquire wrap)",
			fmt.Errorf("failed to acquire DDL lock: %w", fmt.Errorf("%w: DDL lock for database 'x' is held by txn 1 (node 1)", ErrDDLLockHeld)),
			true,
		},
		{"read-only 1290 (unreachable via CoordinatorHandler today, still must classify false)", protocol.ErrReadOnly(), false},
		{"server shutdown 1053 (the draining gate's actual error)", protocol.ErrServerShutdown(), false},
		{"unknown database 1049", protocol.ErrUnknownDatabase("x"), false},
		{"quorum not achieved", &QuorumNotAchievedError{Phase: "prepare", AcksReceived: 1, QuorumRequired: 2}, false},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
		{"plain unclassified error", errors.New("boom"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetryableEnsureDatabaseError(tt.err)
			if got != tt.retryable {
				t.Fatalf("isRetryableEnsureDatabaseError(%v) = %v, want %v", tt.err, got, tt.retryable)
			}
		})
	}
}
