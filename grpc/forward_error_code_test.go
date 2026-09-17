package grpc

import (
	"errors"
	"testing"

	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/query/transform"
)

// TestForwardFailureCarriesTheMySQLCode pins the leader half of the forwarded
// error path: the response carries the same MySQL error code the coordinator
// would have put in its own ERR packet, so a replica can reproduce it instead of
// reporting ER_UNKNOWN_ERROR for every failure.
//
// The code is taken from protocol.ConvertToMySQLError rather than re-derived
// here, so the two paths cannot drift apart.
func TestForwardFailureCarriesTheMySQLCode(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wantCode     uint32
		wantSQLState string
		wantMessage  string
	}{
		{
			// The deliverable: a rule's rejection reaches the wire with 1235.
			// Mutation: set ErrorCode to 0, or drop the field assignment.
			name:         "a rule rejection is carried as 1235",
			err:          transform.NewCodedError(transform.ErrCodeNotSupportedYet, "nope"),
			wantCode:     1235,
			wantSQLState: protocol.SQLStateSyntax,
			wantMessage:  "nope",
		},
		{
			// A typed MySQL error keeps its own code.
			// Mutation: same as above.
			name:         "a typed MySQL error keeps its code",
			err:          protocol.NewMySQLError(protocol.ErrCodeDupEntry, protocol.SQLStateIntegrity, "Duplicate entry"),
			wantCode:     1062,
			wantSQLState: protocol.SQLStateIntegrity,
			wantMessage:  "Duplicate entry",
		},
		{
			// A plain error maps to ER_UNKNOWN_ERROR, which is what the
			// coordinator's own ERR packet would carry for it.
			// Mutation: hardcode a non-1105 default.
			name:         "a plain error becomes 1105",
			err:          errors.New("something broke"),
			wantCode:     1105,
			wantSQLState: protocol.SQLStateGeneral,
			wantMessage:  "something broke",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := forwardFailure(tc.err)

			if resp.Success {
				t.Error("forwardFailure produced Success = true")
			}
			if resp.ErrorCode != tc.wantCode {
				t.Errorf("ErrorCode = %d, want %d", resp.ErrorCode, tc.wantCode)
			}
			// The SQLSTATE travels too, because the code does not determine it:
			// 1105 pairs with both HY000 and 23000 on the leader.
			// Mutation: drop the SqlState assignment in forwardFailure.
			if resp.SqlState != tc.wantSQLState {
				t.Errorf("SqlState = %q, want %q", resp.SqlState, tc.wantSQLState)
			}
			// The message must be the mapped message, not err.Error(): a
			// *protocol.MySQLError formats its own code into Error(), and that
			// string would otherwise be sent as the message text.
			// Mutation: use err.Error() instead of mysqlErr.Message.
			if resp.ErrorMessage != tc.wantMessage {
				t.Errorf("ErrorMessage = %q, want %q", resp.ErrorMessage, tc.wantMessage)
			}
		})
	}
}
