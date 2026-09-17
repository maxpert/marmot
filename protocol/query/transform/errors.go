package transform

import (
	"fmt"

	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// MySQL server error codes raised by transformation rules.
//
// The value lives in protocol/mysqlcode, the shared leaf package for MySQL
// error-code constants, and is referenced here under its historical name so
// existing callers in this package's API are unaffected.
const (
	// ErrCodeNotSupportedYet is MySQL's ER_NOT_SUPPORTED_YET (1235): the
	// server understands the statement but does not implement this form of it.
	ErrCodeNotSupportedYet = mysqlcode.ErrCodeNotSupportedYet
)

// CodedError is the error a transformation rule returns when it refuses a
// statement it cannot safely rewrite. Code is the MySQL server error code the
// client must see; protocol.ConvertToMySQLError pairs it with its SQLSTATE and
// turns it into the error packet.
//
// A rule that fails for any other reason returns a plain error, which the
// protocol layer reports as ER_UNKNOWN_ERROR (1105).
type CodedError struct {
	Code    uint16
	Message string
}

func (e *CodedError) Error() string {
	return fmt.Sprintf("ERROR %d: %s", e.Code, e.Message)
}

// NewCodedError builds a rule rejection carrying the MySQL error code the
// client must see.
func NewCodedError(code uint16, format string, args ...interface{}) *CodedError {
	return &CodedError{Code: code, Message: fmt.Sprintf(format, args...)}
}
