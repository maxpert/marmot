package protocol

import "fmt"

// MySQL error code constants
const (
	ErrCodeUnknown         uint16 = 1105
	ErrCodeBadNull         uint16 = 1048
	ErrCodeTableExists     uint16 = 1050
	ErrCodeBadField        uint16 = 1054
	ErrCodeDupEntry        uint16 = 1062
	ErrCodeParseError      uint16 = 1064
	ErrCodeTooBigRowsize   uint16 = 1118
	ErrCodeNoSuchTable     uint16 = 1146
	ErrCodeLockTimeout     uint16 = 1205
	ErrCodeDeadlock        uint16 = 1213
	ErrCodeReadOnly        uint16 = 1290
	ErrCodeNoReferencedRow uint16 = 1452
	ErrCodeCheckConstraint uint16 = 3819
)

// SQLSTATE constants
const (
	SQLStateGeneral     = "HY000"
	SQLStateIntegrity   = "23000"
	SQLStateSyntax      = "42000"
	SQLStateDeadlock    = "40001"
	SQLStateTableExists = "42S01"
	SQLStateNoSuchTable = "42S02"
	SQLStateNoSuchCol   = "42S22"
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
