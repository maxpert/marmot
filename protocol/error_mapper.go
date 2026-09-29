package protocol

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/protocol/query/transform"
)

// ConvertToMySQLError converts any error to *MySQLError with appropriate MySQL error codes
func ConvertToMySQLError(err error) *MySQLError {
	if err == nil {
		return nil
	}

	// Already a MySQLError, return as-is
	if mysqlErr, ok := err.(*MySQLError); ok {
		return mysqlErr
	}

	// A transformation rule refused the statement and chose the code itself.
	var coded *transform.CodedError
	if errors.As(err, &coded) {
		return NewMySQLError(coded.Code, sqlStateForCode(coded.Code), coded.Message)
	}

	// The statement's SQLite transaction has already ended: an explicit
	// transaction's pinned session that outlived the lock wait was rolled
	// back. MySQL answers a rolled-back transaction with 1213, which tells the
	// client to run the whole transaction again.
	if errors.Is(err, sql.ErrTxDone) {
		return NewMySQLError(ErrCodeDeadlock, SQLStateDeadlock,
			"Deadlock found when trying to get lock; try restarting transaction ("+err.Error()+")")
	}

	// Try to extract sqlite3.Error
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return mapSQLiteError(sqliteErr, err.Error())
	}

	// Fallback to message-based detection
	return mapByMessage(err.Error())
}

// UnsupportedStatementError builds the error a client sees for a statement the
// query pipeline refused. A transformation rule that rejected the statement
// chose its own MySQL error code; a rule that failed for any other reason is
// ER_UNKNOWN_ERROR, because a rule failing internally is not a syntax error;
// anything else reached StatementUnsupported from the parser and is one.
func UnsupportedStatementError(stmt Statement) *MySQLError {
	if stmt.TranspileErr != nil {
		var coded *transform.CodedError
		if errors.As(stmt.TranspileErr, &coded) {
			return NewMySQLError(coded.Code, sqlStateForCode(coded.Code), coded.Message)
		}
		return NewMySQLError(ErrCodeUnknown, SQLStateGeneral, stmt.Error)
	}
	return NewMySQLError(ErrCodeParseError, SQLStateSyntax, stmt.Error)
}

// sqlStateForCode returns the SQLSTATE MySQL pairs with a server error code.
//
// Its one caller is the rule-rejection path: a transformation rule raised below
// the protocol layer names only a code, because the SQLSTATE constants are not
// visible from there. It is NOT a general inverse of the constructors in this
// file and must not be used as one - the pairing is not injective, since 1105
// occurs with both HY000 and SQLStateIntegrity. Anything that needs a leader's
// exact SQLSTATE has to carry it, not rebuild it.
func sqlStateForCode(code uint16) string {
	switch code {
	case ErrCodeDupEntry, ErrCodeBadNull, ErrCodeNoReferencedRow, ErrCodeCheckConstraint:
		return SQLStateIntegrity
	case ErrCodeDeadlock:
		return SQLStateDeadlock
	case ErrCodeNoSuchTable:
		return SQLStateNoSuchTable
	case ErrCodeTableExists:
		return SQLStateTableExists
	case ErrCodeBadField:
		return SQLStateNoSuchCol
	case ErrCodeDupFieldName:
		return SQLStateDupColumn
	case ErrCodeParseError, transform.ErrCodeNotSupportedYet, ErrCodeTableAccessDenied:
		return SQLStateSyntax
	case ErrCodeNoDB:
		return SQLStateNoDB
	case ErrCodeServerShutdown:
		return SQLStateConnFailure
	case ErrCodeDataOutOfRange:
		return SQLStateDataOutOfRange
	default:
		return SQLStateGeneral
	}
}

func mapSQLiteError(e sqlite3.Error, msg string) *MySQLError {
	// Check extended codes first (more specific)
	switch e.ExtendedCode {
	case sqlite3.ErrConstraintUnique:
		return NewMySQLError(ErrCodeDupEntry, SQLStateIntegrity, msg)
	case sqlite3.ErrConstraintPrimaryKey:
		return NewMySQLError(ErrCodeDupEntry, SQLStateIntegrity, msg)
	case sqlite3.ErrConstraintNotNull:
		return NewMySQLError(ErrCodeBadNull, SQLStateIntegrity, msg)
	case sqlite3.ErrConstraintForeignKey:
		return NewMySQLError(ErrCodeNoReferencedRow, SQLStateIntegrity, msg)
	case sqlite3.ErrConstraintCheck:
		return NewMySQLError(ErrCodeCheckConstraint, SQLStateIntegrity, msg)
	case sqlite3.ErrConstraintCommitHook:
		// A commit hook refused the commit and SQLite rolled the whole
		// transaction back. Marmot's only commit hook is a database's write
		// gate, which refuses while the database is out of service (restore,
		// drop, shutdown): transient, and nothing was written, so the client
		// restarts the transaction as after a deadlock (1213, SQLSTATE 40001).
		return NewMySQLError(ErrCodeDeadlock, SQLStateDeadlock,
			"Transaction rolled back while its database was out of service; try restarting transaction")
	}

	// Check primary codes
	switch e.Code {
	case sqlite3.ErrBusy:
		return NewMySQLError(ErrCodeLockTimeout, SQLStateGeneral,
			"Lock wait timeout exceeded; try restarting transaction")
	case sqlite3.ErrLocked:
		return NewMySQLError(ErrCodeDeadlock, SQLStateDeadlock,
			"Deadlock found when trying to get lock; try restarting transaction")
	case sqlite3.ErrTooBig:
		return NewMySQLError(ErrCodeTooBigRowsize, SQLStateGeneral, msg)
	case sqlite3.ErrConstraint:
		// Generic constraint error, try to determine type from message
		return mapConstraintByMessage(msg)
	}

	// Fallback to message-based detection
	return mapByMessage(msg)
}

func mapByMessage(msg string) *MySQLError {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "no such table"):
		return NewMySQLError(ErrCodeNoSuchTable, SQLStateNoSuchTable, msg)
	// SQLite reports "duplicate column name: <col>" for ALTER TABLE ADD COLUMN on
	// an existing column; clients expect MySQL's ER_DUP_FIELDNAME.
	case strings.Contains(lower, "duplicate column name"):
		return NewMySQLError(ErrCodeDupFieldName, SQLStateDupColumn, msg)
	case strings.Contains(lower, "already exists"):
		return NewMySQLError(ErrCodeTableExists, SQLStateTableExists, msg)
	case strings.Contains(lower, "no column named"), strings.Contains(lower, "no such column"):
		return NewMySQLError(ErrCodeBadField, SQLStateNoSuchCol, msg)
	case strings.Contains(lower, "syntax error"):
		return NewMySQLError(ErrCodeParseError, SQLStateSyntax, msg)
	case strings.Contains(lower, "unique constraint"):
		return NewMySQLError(ErrCodeDupEntry, SQLStateIntegrity, msg)
	case strings.Contains(lower, "not null constraint"):
		return NewMySQLError(ErrCodeBadNull, SQLStateIntegrity, msg)
	case strings.Contains(lower, "foreign key constraint"):
		return NewMySQLError(ErrCodeNoReferencedRow, SQLStateIntegrity, msg)
	default:
		return NewMySQLError(ErrCodeUnknown, SQLStateGeneral, msg)
	}
}

func mapConstraintByMessage(msg string) *MySQLError {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "unique"), strings.Contains(lower, "primary key"):
		return NewMySQLError(ErrCodeDupEntry, SQLStateIntegrity, msg)
	case strings.Contains(lower, "not null"):
		return NewMySQLError(ErrCodeBadNull, SQLStateIntegrity, msg)
	case strings.Contains(lower, "foreign key"):
		return NewMySQLError(ErrCodeNoReferencedRow, SQLStateIntegrity, msg)
	case strings.Contains(lower, "check"):
		return NewMySQLError(ErrCodeCheckConstraint, SQLStateIntegrity, msg)
	default:
		return NewMySQLError(ErrCodeUnknown, SQLStateIntegrity, msg)
	}
}
