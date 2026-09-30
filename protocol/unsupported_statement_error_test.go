package protocol

import (
	"errors"
	"testing"

	"github.com/maxpert/marmot/protocol/query/transform"
)

// TestUnsupportedStatementError pins the MySQL error code a client sees for
// each way a statement can reach StatementUnsupported. Before the transpiler
// propagated rule errors, every one of them was 1064.
func TestUnsupportedStatementError(t *testing.T) {
	cases := []struct {
		name         string
		stmt         Statement
		wantCode     uint16
		wantSQLState string
		wantMessage  string
	}{
		{
			// Mutation: drop the errors.As branch in
			// UnsupportedStatementError; the rule's 1235 becomes 1105.
			name: "rule rejection keeps its own code",
			stmt: Statement{
				Type:         StatementUnsupported,
				Error:        "flattened text",
				TranspileErr: transform.NewCodedError(transform.ErrCodeNotSupportedYet, "cannot rewrite %s", "this"),
			},
			wantCode:     1235,
			wantSQLState: SQLStateSyntax,
			wantMessage:  "cannot rewrite this",
		},
		{
			// Mutation: fall through to ErrCodeParseError for a non-coded
			// transpile error; a rule failing internally is reported as a
			// client syntax error.
			name: "other transpile failure is ER_UNKNOWN_ERROR",
			stmt: Statement{
				Type:         StatementUnsupported,
				Error:        "rule blew up",
				TranspileErr: errors.New("rule blew up"),
			},
			wantCode:     ErrCodeUnknown,
			wantSQLState: SQLStateGeneral,
			wantMessage:  "rule blew up",
		},
		{
			// Mutation: route the nil-TranspileErr case through
			// ConvertToMySQLError; parse errors stop being 1064.
			name: "parse failure stays ER_PARSE_ERROR",
			stmt: Statement{
				Type:  StatementUnsupported,
				Error: "syntax error at position 12",
			},
			wantCode:     ErrCodeParseError,
			wantSQLState: SQLStateSyntax,
			wantMessage:  "syntax error at position 12",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := UnsupportedStatementError(tc.stmt)
			if got.Code != tc.wantCode {
				t.Errorf("Code = %d, want %d", got.Code, tc.wantCode)
			}
			if got.SQLState != tc.wantSQLState {
				t.Errorf("SQLState = %q, want %q", got.SQLState, tc.wantSQLState)
			}
			if got.Message != tc.wantMessage {
				t.Errorf("Message = %q, want %q", got.Message, tc.wantMessage)
			}
		})
	}
}

// TestConvertToMySQLErrorCodedError pins that a rule's rejection surviving
// anywhere else in the error path - a wrapped return, a forwarded query -
// still reaches the client with its own code.
// Mutation: remove the *transform.CodedError branch from ConvertToMySQLError;
// the message-based fallback returns 1105.
func TestConvertToMySQLErrorCodedError(t *testing.T) {
	err := transform.NewCodedError(transform.ErrCodeNotSupportedYet, "nope")
	got := ConvertToMySQLError(err)
	if got.Code != transform.ErrCodeNotSupportedYet {
		t.Errorf("Code = %d, want %d", got.Code, transform.ErrCodeNotSupportedYet)
	}
	if got.SQLState != SQLStateSyntax {
		t.Errorf("SQLState = %q, want %q", got.SQLState, SQLStateSyntax)
	}
}
