package protocol

import (
	"errors"
	"strings"

	"github.com/mattn/go-sqlite3"
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

	// Try to extract sqlite3.Error
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return mapSQLiteError(sqliteErr, err.Error())
	}

	// Fallback to message-based detection
	return mapByMessage(err.Error())
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
