package protocol

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestConvertToMySQLError_Nil(t *testing.T) {
	result := ConvertToMySQLError(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestConvertToMySQLError_PassthroughMySQLError(t *testing.T) {
	original := NewMySQLError(1234, "ABCDE", "test message")
	result := ConvertToMySQLError(original)
	if result != original {
		t.Errorf("expected same MySQLError instance to be returned")
	}
	if result.Code != 1234 || result.SQLState != "ABCDE" || result.Message != "test message" {
		t.Errorf("MySQLError fields changed unexpectedly")
	}
}

func TestConvertToMySQLError_SQLiteExtendedCodes(t *testing.T) {
	tests := []struct {
		name         string
		extCode      sqlite3.ErrNoExtended
		wantCode     uint16
		wantSQLState string
	}{
		{
			name:         "unique constraint",
			extCode:      sqlite3.ErrConstraintUnique,
			wantCode:     ErrCodeDupEntry,
			wantSQLState: SQLStateIntegrity,
		},
		{
			name:         "primary key constraint",
			extCode:      sqlite3.ErrConstraintPrimaryKey,
			wantCode:     ErrCodeDupEntry,
			wantSQLState: SQLStateIntegrity,
		},
		{
			name:         "not null constraint",
			extCode:      sqlite3.ErrConstraintNotNull,
			wantCode:     ErrCodeBadNull,
			wantSQLState: SQLStateIntegrity,
		},
		{
			name:         "foreign key constraint",
			extCode:      sqlite3.ErrConstraintForeignKey,
			wantCode:     ErrCodeNoReferencedRow,
			wantSQLState: SQLStateIntegrity,
		},
		{
			name:         "check constraint",
			extCode:      sqlite3.ErrConstraintCheck,
			wantCode:     ErrCodeCheckConstraint,
			wantSQLState: SQLStateIntegrity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sqlite3.Error{
				Code:         sqlite3.ErrConstraint,
				ExtendedCode: tt.extCode,
			}
			result := ConvertToMySQLError(err)
			if result.Code != tt.wantCode {
				t.Errorf("Code = %d, want %d", result.Code, tt.wantCode)
			}
			if result.SQLState != tt.wantSQLState {
				t.Errorf("SQLState = %s, want %s", result.SQLState, tt.wantSQLState)
			}
		})
	}
}

// TestConvertToMySQLError_CommitHookRefusalIsRetryable: a commit the write
// gate refused was rolled back whole while its database was out of service,
// so the client must restart the transaction (1213, SQLSTATE 40001), not
// treat it as an integrity violation. It is told apart by its extended code,
// not its text: a plain constraint error with the same message stays an
// integrity error.
//
// Mutation: drop the ErrConstraintCommitHook case. "a refused commit was not
// reported as retryable" fires.
func TestConvertToMySQLError_CommitHookRefusalIsRetryable(t *testing.T) {
	refused := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintCommitHook}
	for _, err := range []error{refused, fmt.Errorf("commit: %w", refused)} {
		result := ConvertToMySQLError(err)
		if result.Code != ErrCodeDeadlock || result.SQLState != SQLStateDeadlock {
			t.Errorf("a refused commit was not reported as retryable: %v gave %d/%s", err, result.Code, result.SQLState)
		}
	}

	plain := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrNoExtended(sqlite3.ErrConstraint)}
	result := ConvertToMySQLError(fmt.Errorf("%w: constraint failed", plain))
	if result.Code == ErrCodeDeadlock || result.SQLState != SQLStateIntegrity {
		t.Errorf("a plain constraint error was reported as %d/%s", result.Code, result.SQLState)
	}
}

func TestConvertToMySQLError_SQLitePrimaryCodes(t *testing.T) {
	tests := []struct {
		name         string
		code         sqlite3.ErrNo
		wantCode     uint16
		wantSQLState string
	}{
		{
			name:         "busy",
			code:         sqlite3.ErrBusy,
			wantCode:     ErrCodeLockTimeout,
			wantSQLState: SQLStateGeneral,
		},
		{
			name:         "locked",
			code:         sqlite3.ErrLocked,
			wantCode:     ErrCodeDeadlock,
			wantSQLState: SQLStateDeadlock,
		},
		{
			name:         "too big",
			code:         sqlite3.ErrTooBig,
			wantCode:     ErrCodeTooBigRowsize,
			wantSQLState: SQLStateGeneral,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sqlite3.Error{
				Code: tt.code,
			}
			result := ConvertToMySQLError(err)
			if result.Code != tt.wantCode {
				t.Errorf("Code = %d, want %d", result.Code, tt.wantCode)
			}
			if result.SQLState != tt.wantSQLState {
				t.Errorf("SQLState = %s, want %s", result.SQLState, tt.wantSQLState)
			}
		})
	}
}

func TestConvertToMySQLError_MessageBased(t *testing.T) {
	tests := []struct {
		name         string
		errMsg       string
		wantCode     uint16
		wantSQLState string
	}{
		{
			name:         "no such table",
			errMsg:       "no such table: users",
			wantCode:     ErrCodeNoSuchTable,
			wantSQLState: SQLStateNoSuchTable,
		},
		{
			name:         "table already exists",
			errMsg:       "table 'users' already exists",
			wantCode:     ErrCodeTableExists,
			wantSQLState: SQLStateTableExists,
		},
		{
			name:         "duplicate column name",
			errMsg:       "duplicate column name: creation_date",
			wantCode:     ErrCodeDupFieldName,
			wantSQLState: SQLStateDupColumn,
		},
		{
			name:         "duplicate column name wrapped by prepare failure",
			errMsg:       "local prepare failed: duplicate column name: creation_date",
			wantCode:     ErrCodeDupFieldName,
			wantSQLState: SQLStateDupColumn,
		},
		{
			name:         "no column named",
			errMsg:       "no column named foo",
			wantCode:     ErrCodeBadField,
			wantSQLState: SQLStateNoSuchCol,
		},
		{
			name:         "no such column",
			errMsg:       "no such column: bar",
			wantCode:     ErrCodeBadField,
			wantSQLState: SQLStateNoSuchCol,
		},
		{
			name:         "syntax error",
			errMsg:       "near \"SELEC\": syntax error",
			wantCode:     ErrCodeParseError,
			wantSQLState: SQLStateSyntax,
		},
		{
			name:         "unique constraint message",
			errMsg:       "UNIQUE constraint failed: users.email",
			wantCode:     ErrCodeDupEntry,
			wantSQLState: SQLStateIntegrity,
		},
		{
			name:         "not null constraint message",
			errMsg:       "NOT NULL constraint failed: users.name",
			wantCode:     ErrCodeBadNull,
			wantSQLState: SQLStateIntegrity,
		},
		{
			name:         "foreign key constraint message",
			errMsg:       "FOREIGN KEY constraint failed",
			wantCode:     ErrCodeNoReferencedRow,
			wantSQLState: SQLStateIntegrity,
		},
		{
			name:         "unknown error",
			errMsg:       "some random error",
			wantCode:     ErrCodeUnknown,
			wantSQLState: SQLStateGeneral,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			result := ConvertToMySQLError(err)
			if result.Code != tt.wantCode {
				t.Errorf("Code = %d, want %d", result.Code, tt.wantCode)
			}
			if result.SQLState != tt.wantSQLState {
				t.Errorf("SQLState = %s, want %s", result.SQLState, tt.wantSQLState)
			}
			if result.Message != tt.errMsg {
				t.Errorf("Message = %s, want %s", result.Message, tt.errMsg)
			}
		})
	}
}

func TestConvertToMySQLError_GenericConstraint(t *testing.T) {
	// Test generic constraint error (Code=ErrConstraint but no specific ExtendedCode)
	tests := []struct {
		name     string
		errMsg   string
		wantCode uint16
	}{
		{
			name:     "unique in message",
			errMsg:   "UNIQUE constraint failed",
			wantCode: ErrCodeDupEntry,
		},
		{
			name:     "primary key in message",
			errMsg:   "PRIMARY KEY constraint failed",
			wantCode: ErrCodeDupEntry,
		},
		{
			name:     "not null in message",
			errMsg:   "NOT NULL constraint failed",
			wantCode: ErrCodeBadNull,
		},
		{
			name:     "foreign key in message",
			errMsg:   "FOREIGN KEY constraint failed",
			wantCode: ErrCodeNoReferencedRow,
		},
		{
			name:     "check in message",
			errMsg:   "CHECK constraint failed",
			wantCode: ErrCodeCheckConstraint,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapConstraintByMessage(tt.errMsg)
			if result.Code != tt.wantCode {
				t.Errorf("Code = %d, want %d", result.Code, tt.wantCode)
			}
		})
	}
}

// TestConvertToMySQLError_EndedTransactionIsRetryableDeadlock: a statement of
// an explicit transaction whose SQLite transaction already ended (its pinned
// session outlived the lock wait and was rolled back) is answered with 1213,
// MySQL's "the transaction was rolled back, run it again", never the
// unknown error 1105 a client would not retry.
func TestConvertToMySQLError_EndedTransactionIsRetryableDeadlock(t *testing.T) {
	err := fmt.Errorf("DML execution failed: failed to execute statement: %w", sql.ErrTxDone)
	result := ConvertToMySQLError(err)
	if result.Code != ErrCodeDeadlock || result.SQLState != SQLStateDeadlock {
		t.Fatalf("got %d/%s (%s), want %d/%s", result.Code, result.SQLState, result.Message, ErrCodeDeadlock, SQLStateDeadlock)
	}
}

func TestConvertToMySQLError_WrappedError(t *testing.T) {
	// Test that errors.As works with wrapped errors
	sqliteErr := sqlite3.Error{
		Code:         sqlite3.ErrConstraint,
		ExtendedCode: sqlite3.ErrConstraintUnique,
	}
	wrapped := errors.Join(errors.New("operation failed"), sqliteErr)

	result := ConvertToMySQLError(wrapped)
	if result.Code != ErrCodeDupEntry {
		t.Errorf("Code = %d, want %d for wrapped sqlite error", result.Code, ErrCodeDupEntry)
	}
}
