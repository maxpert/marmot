package protocol

import "fmt"

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
	return NewMySQLError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
}

// ErrDeadlock returns error 1213 - deadlock detected
func ErrDeadlock() *MySQLError {
	return NewMySQLError(1213, "40001", "Deadlock found when trying to get lock; try restarting transaction")
}

// IsRetryableError checks if an error is a retryable transaction error
func IsRetryableError(err error) bool {
	mysqlErr, ok := err.(*MySQLError)
	if !ok {
		return false
	}

	// Errors that indicate retry is safe
	switch mysqlErr.Code {
	case 1205, // Lock wait timeout
		1213: // Deadlock
		return true
	default:
		return false
	}
}
