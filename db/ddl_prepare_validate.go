package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

// isContextError reports whether err was caused by the context being cancelled or
// timing out, rather than by the statement being invalid.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// transientSQLiteCodes are primary SQLite result codes that describe a
// resource or locking condition on this node right now, not a verdict on the
// statement. With cache=shared, a table lock held by another connection in
// this process (writeDB vs. hookDB) surfaces as SQLITE_LOCKED immediately -
// busy_timeout does not apply to it, so the statement can fail here on pure
// timing. A DDL that hits one of these must stay a retryable missing ACK: it
// may succeed on retry, or once the condition clears.
var transientSQLiteCodes = map[sqlite3.ErrNo]bool{
	sqlite3.ErrBusy:     true, // SQLITE_BUSY: whole-file lock held by another connection/process
	sqlite3.ErrLocked:   true, // SQLITE_LOCKED: table lock held by another connection, shared cache
	sqlite3.ErrNomem:    true, // SQLITE_NOMEM: transient allocation failure
	sqlite3.ErrIoErr:    true, // SQLITE_IOERR: transient disk I/O condition
	sqlite3.ErrFull:     true, // SQLITE_FULL: disk or database full
	sqlite3.ErrCantOpen: true, // SQLITE_CANTOPEN: could not open a required file
	sqlite3.ErrProtocol: true, // SQLITE_PROTOCOL: locking protocol contention
	sqlite3.ErrReadonly: true, // SQLITE_READONLY: database (or this connection) is read-only right now
}

// isDDLRejection reports whether err is a deterministic verdict on the DDL
// statement itself - SQLite refuses to ever apply it, such as a constraint
// violation or a schema conflict (duplicate column, missing table, syntax
// error) - rather than a transient condition on this node such as lock
// contention, a resource limit, or context cancellation.
//
// Only a rejection may be surfaced to the coordinator as a final refusal of
// the transaction; everything else must stay a retryable missing ACK, exactly
// as it was before DDL validation was added to PREPARE.
func isDDLRejection(ctx context.Context, err error) bool {
	if ctx.Err() != nil || isContextError(err) {
		return false
	}

	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		// Not a typed SQLite error - e.g. a wrapped BeginTx failure from
		// connection-pool exhaustion. Treat conservatively as a missing ACK
		// rather than assume it is a verdict on the statement.
		return false
	}

	return !transientSQLiteCodes[sqliteErr.Code]
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
