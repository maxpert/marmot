package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/protocol/query/transform"
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

// AutoIncWidthExceededError reports that MAX(id) in an existing table exceeds
// the range its explicitly declared AUTO_INCREMENT column can hold - e.g. an
// ALTER that narrows the column to TINYINT after 200 rows already exist.
//
// It is raised only after DDL validation has re-derived the column's marker
// from THIS node's own (in-transaction) sqlite_master, so - unlike a raw
// SQLite runtime error - it carries no ambiguity about transient node state:
// it is always a deterministic verdict on the statement, and isDDLRejection
// treats it as an unconditional rejection.
type AutoIncWidthExceededError struct {
	Table    string
	Column   string
	Max      uint64
	WidthMax uint64
}

func (e *AutoIncWidthExceededError) Error() string {
	return fmt.Sprintf(
		"table %s: existing max value %d of AUTO_INCREMENT column %s exceeds its declared width ceiling %d",
		e.Table, e.Max, e.Column, e.WidthMax)
}

// Unwrap exposes this rejection's MySQL error code (ER_WARN_DATA_OUT_OF_RANGE)
// through *transform.CodedError, the same coded-error mechanism a
// transformation rule uses to choose its own client-facing code.
// protocol.ConvertToMySQLError already recognises *transform.CodedError via
// errors.As, so this alone is enough for the width-ceiling rejection to reach
// the client with the right code - no separate mapping mechanism is needed,
// and protocol.ConvertToMySQLError never has to reference this db type by
// name, which would create an import cycle (db already imports protocol;
// protocol must not import db).
func (e *AutoIncWidthExceededError) Unwrap() error {
	return &transform.CodedError{Code: mysqlcode.ErrCodeDataOutOfRange, Message: e.Error()}
}

// isDDLRejection reports whether err is a deterministic verdict on the DDL
// statement itself - SQLite refuses to ever apply it, such as a constraint
// violation or a schema conflict (duplicate column, missing table, syntax
// error), or this node's own width-ceiling check found existing rows the
// declared column cannot hold - rather than a transient condition on this
// node such as lock contention, a resource limit, or context cancellation.
//
// Only a rejection may be surfaced to the coordinator as a final refusal of
// the transaction; everything else must stay a retryable missing ACK, exactly
// as it was before DDL validation was added to PREPARE.
func isDDLRejection(ctx context.Context, err error) bool {
	if ctx.Err() != nil || isContextError(err) {
		return false
	}

	var widthErr *AutoIncWidthExceededError
	if errors.As(err, &widthErr) {
		return true
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

// mysqlCodeForError returns the MySQL server error code an error names, or 0
// when it names none.
//
// A rejection crosses to the coordinator as a string (PrepareResult.Error), so
// the typed error is gone by the time anything maps it to a client-visible
// code. Reading the code here, where the typed error still exists, is what
// lets a participant's deterministic refusal keep its own code instead of
// being flattened to ER_UNKNOWN_ERROR.
func mysqlCodeForError(err error) uint16 {
	var coded *transform.CodedError
	if errors.As(err, &coded) {
		return coded.Code
	}
	return 0
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

	before, err := tableSchemaSnapshot(ctx, tx)
	if err != nil {
		return err
	}

	for _, stmt := range statements {
		if stmt == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	after, err := tableSchemaSnapshot(ctx, tx)
	if err != nil {
		return err
	}

	// Refuse the DDL when it leaves an explicitly declared AUTO_INCREMENT
	// column unable to hold ids the table already contains. Scoped to tables whose sqlite_master row this DDL actually changed
	// - derived from the schema itself, not by parsing the statement text - so
	// a CREATE TABLE for an unrelated table is never rejected over a
	// pre-existing condition on some other table it never touched.
	return checkAutoIncWidthCeilings(ctx, tx, changedTables(before, after))
}

// tableSchemaSnapshot reads every table's CREATE TABLE text from
// sqlite_master, keyed by name.
func tableSchemaSnapshot(ctx context.Context, tx *sql.Tx) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		return nil, fmt.Errorf("snapshot sqlite_master: %w", err)
	}
	defer rows.Close()

	snapshot := make(map[string]string)
	for rows.Next() {
		var name string
		var createSQL sql.NullString
		if err := rows.Scan(&name, &createSQL); err != nil {
			return nil, fmt.Errorf("scan sqlite_master row: %w", err)
		}
		snapshot[name] = createSQL.String
	}
	return snapshot, rows.Err()
}

// changedTables returns the names of tables in `after` whose CREATE TABLE
// text differs from (or is absent from) `before` - the set of tables this
// validation's DDL actually touched, derived from the schema rather than the
// statement text so it covers CREATE TABLE and every ALTER shape uniformly.
func changedTables(before, after map[string]string) []string {
	var touched []string
	for name, createSQL := range after {
		if before[name] != createSQL {
			touched = append(touched, name)
		}
	}
	return touched
}

// checkAutoIncWidthCeilings rejects the DDL when a table's explicitly
// declared AUTO_INCREMENT column can no longer hold the ids the table already
// contains. It runs inside the validation transaction, so it sees the NEW
// schema (this DDL's own effect) against the OLD rows the DDL has not
// touched, and is always rolled back with the rest of validation.
func checkAutoIncWidthCeilings(ctx context.Context, tx *sql.Tx, tables []string) error {
	for _, table := range tables {
		col, attrs, ok, err := autoIncMarkedColumn(tx, table)
		if err != nil {
			return fmt.Errorf("derive auto-increment column for %s: %w", table, err)
		}
		if !ok {
			continue
		}

		var max sql.NullInt64
		q := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(col), quoteIdent(table))
		if err := tx.QueryRowContext(ctx, q).Scan(&max); err != nil {
			return fmt.Errorf("compute existing max for %s.%s: %w", table, col, err)
		}
		if !max.Valid || max.Int64 < 0 {
			// No rows, or every id is negative: negative ids are never issued
			// by the allocator (it only ever advances upward from a
			// non-negative base), so they carry nothing for this ceiling to
			// check against.
			continue
		}

		widthMax := attrs.WidthMax()
		if uint64(max.Int64) > widthMax {
			return &AutoIncWidthExceededError{
				Table: table, Column: col, Max: uint64(max.Int64), WidthMax: widthMax,
			}
		}
	}
	return nil
}
