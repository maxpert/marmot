package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// isContextError reports whether err was caused by the context being cancelled or
// timing out, rather than by the statement being invalid.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// ValidateDDLStatements verifies that DDL can be applied to this node before the
// participant ACKs PREPARE.
//
// PREPARE is the 2PC promise point: a node that ACKs it must be able to COMMIT.
// SQLite reports most DDL failures (duplicate column, missing table, invalid
// default) only when the statement executes, so the statements are executed here
// inside a transaction that is always rolled back. Statements run in request
// order so later DDL sees the schema produced by earlier DDL in the same
// transaction.
//
// Statements are executed exactly as COMMIT will execute them, so callers must
// pass the same SQL that will be applied - already rewritten for idempotency by
// protocol.RewriteDDLForIdempotency - or validation and apply can disagree.
//
// The underlying SQLite error is returned unwrapped so callers can map it to the
// matching MySQL error code.
func ValidateDDLStatements(ctx context.Context, dbConn *sql.DB, statements []string) error {
	if dbConn == nil || len(statements) == 0 {
		return nil
	}

	tx, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin DDL validation transaction: %w", err)
	}
	// Always discard the validation transaction - it exists only to surface errors.
	defer func() { _ = tx.Rollback() }()

	for _, stmt := range statements {
		if stmt == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}
