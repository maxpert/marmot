package protocol

import (
	"errors"
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
