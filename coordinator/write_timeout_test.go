package coordinator

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/protocol"
)

// TestGetDDLValidationTimeout_UsesConfiguredValue verifies the configured
// value is honored.
func TestGetDDLValidationTimeout_UsesConfiguredValue(t *testing.T) {
	withDDLValidationTimeout(t, 12345)

	got := getDDLValidationTimeout()
	want := 12345 * time.Millisecond
	if got != want {
		t.Errorf("getDDLValidationTimeout(): got %v, want %v", got, want)
	}
}

// TestGetDDLValidationTimeout_DefaultsWhenUnset verifies the 60s default is
// used when the config value is not set (<= 0), mirroring getWriteTimeout's
// defensive fallback for a nil or zero-value config.
func TestGetDDLValidationTimeout_DefaultsWhenUnset(t *testing.T) {
	withDDLValidationTimeout(t, 0)

	got := getDDLValidationTimeout()
	want := 60 * time.Second
	if got != want {
		t.Errorf("getDDLValidationTimeout(): got %v, want %v (default)", got, want)
	}
}

// TestWriteTimeoutForStatements verifies the timeout selection: any DDL
// statement in the transaction routes to the DDL validation timeout,
// otherwise the regular write timeout is used - including for an empty
// statement list and a transaction mixing DML and DDL.
func TestWriteTimeoutForStatements(t *testing.T) {
	withDDLValidationTimeout(t, 9000)
	originalWrite := cfg.Config.Replication.WriteTimeoutMS
	cfg.Config.Replication.WriteTimeoutMS = 500
	t.Cleanup(func() { cfg.Config.Replication.WriteTimeoutMS = originalWrite })

	ddlWant := 9000 * time.Millisecond
	writeWant := 500 * time.Millisecond

	cases := []struct {
		name  string
		stmts []protocol.Statement
		want  time.Duration
	}{
		{
			name:  "no statements",
			stmts: nil,
			want:  writeWant,
		},
		{
			name:  "single DML statement",
			stmts: []protocol.Statement{{Type: protocol.StatementInsert}},
			want:  writeWant,
		},
		{
			name:  "single DDL statement",
			stmts: []protocol.Statement{{Type: protocol.StatementDDL}},
			want:  ddlWant,
		},
		{
			name: "mixed DML and DDL",
			stmts: []protocol.Statement{
				{Type: protocol.StatementInsert},
				{Type: protocol.StatementDDL},
			},
			want: ddlWant,
		},
		{
			name:  "vector index control is not DDL",
			stmts: []protocol.Statement{{Type: protocol.StatementCreateVectorIndex}},
			want:  writeWant,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := writeTimeoutForStatements(tc.stmts)
			if got != tc.want {
				t.Errorf("writeTimeoutForStatements(%s): got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
