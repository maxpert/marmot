package protocol

import (
	"fmt"

	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// MySQL error code constants. Values live in protocol/mysqlcode so packages
// below protocol - which cannot import protocol without an import cycle -
// can reference the same numbers instead of duplicating them.
const (
	ErrCodeUnknown         = mysqlcode.ErrCodeUnknown
	ErrCodeBadNull         = mysqlcode.ErrCodeBadNull
	ErrCodeTableExists     = mysqlcode.ErrCodeTableExists
	ErrCodeBadField        = mysqlcode.ErrCodeBadField
	ErrCodeDupFieldName    = mysqlcode.ErrCodeDupFieldName
	ErrCodeDupEntry        = mysqlcode.ErrCodeDupEntry
	ErrCodeParseError      = mysqlcode.ErrCodeParseError
	ErrCodeTooBigRowsize   = mysqlcode.ErrCodeTooBigRowsize
	ErrCodeNoSuchTable     = mysqlcode.ErrCodeNoSuchTable
	ErrCodeNoDB            = mysqlcode.ErrCodeNoDB
	ErrCodeLockTimeout     = mysqlcode.ErrCodeLockTimeout
	ErrCodeDeadlock        = mysqlcode.ErrCodeDeadlock
	ErrCodeReadOnly        = mysqlcode.ErrCodeReadOnly
	ErrCodeServerShutdown  = mysqlcode.ErrCodeServerShutdown
	ErrCodeNoReferencedRow = mysqlcode.ErrCodeNoReferencedRow
	ErrCodeCheckConstraint = mysqlcode.ErrCodeCheckConstraint
)

// SQLSTATE constants. Values live in protocol/mysqlcode; see above.
const (
	SQLStateGeneral     = mysqlcode.SQLStateGeneral
	SQLStateIntegrity   = mysqlcode.SQLStateIntegrity
	SQLStateSyntax      = mysqlcode.SQLStateSyntax
	SQLStateDeadlock    = mysqlcode.SQLStateDeadlock
	SQLStateTableExists = mysqlcode.SQLStateTableExists
	SQLStateNoSuchTable = mysqlcode.SQLStateNoSuchTable
	SQLStateNoSuchCol   = mysqlcode.SQLStateNoSuchCol
	SQLStateDupColumn   = mysqlcode.SQLStateDupColumn
	SQLStateConnFailure = mysqlcode.SQLStateConnFailure
	SQLStateNoDB        = mysqlcode.SQLStateNoDB
)

// MySQLError represents a MySQL protocol error with error code and SQLSTATE
type MySQLError struct {
	Code     uint16
	SQLState string
	Message  string
}

func (e *MySQLError) Error() string {
	return fmt.Sprintf("ERROR %d (%s): %s", e.Code, e.SQLState, e.Message)
}

// NewMySQLError creates a new MySQL error
func NewMySQLError(code uint16, sqlState, message string) *MySQLError {
	return &MySQLError{
		Code:     code,
		SQLState: sqlState,
		Message:  message,
	}
}

// Common MySQL errors for transaction conflicts

// ErrLockWaitTimeout returns error 1205 - lock wait timeout exceeded
func ErrLockWaitTimeout() *MySQLError {
	return NewMySQLError(ErrCodeLockTimeout, SQLStateGeneral, "Lock wait timeout exceeded; try restarting transaction")
}

// ErrDeadlock returns error 1213 - deadlock detected
func ErrDeadlock() *MySQLError {
	return NewMySQLError(ErrCodeDeadlock, SQLStateDeadlock, "Deadlock found when trying to get lock; try restarting transaction")
}

// ErrReadOnly returns error 1290 - server is running with --read-only option
func ErrReadOnly() *MySQLError {
	return NewMySQLError(ErrCodeReadOnly, SQLStateGeneral, "The MySQL server is running with the --read-only option so it cannot execute this statement")
}

// ErrServerShutdown returns error 1053 - server shutdown in progress
func ErrServerShutdown() *MySQLError {
	return NewMySQLError(ErrCodeServerShutdown, SQLStateConnFailure, "Server shutdown in progress")
}
