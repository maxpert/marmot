package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/protocol"
)

// TestCommitRequestCarriesClaimFlagNotPayload pins the split that makes the
// AUTO_INCREMENT range claim safe across the two phases.
//
// buildCommitRequest rebuilds every statement from a fixed list of fields, so a
// field that is not named there does not reach COMMIT. The claim FLAG must:
// it is what tells a participant to apply the claim it prepared, and without it
// every participant commits the transaction while leaving its base untouched,
// so the next claimant anywhere mints the same ids. The PAYLOAD must not: a
// participant reads the claim's values from the intent it wrote at PREPARE,
// after checking them against its own schema and its own stored base, and a
// payload on the COMMIT wire is an invitation to read them from an unchecked
// source instead.
//
// Mutations: drop the AutoIDClaim line from the struct literal (the first
// assertion fires); add AutoIDClaimPayload beside it (the second fires).
func TestCommitRequestCarriesClaimFlagNotPayload(t *testing.T) {
	wc := &WriteCoordinator{nodeID: 9}

	req := wc.buildCommitRequest(&Transaction{
		ID:       4242,
		Database: "testdb",
		Statements: []protocol.Statement{{
			// StatementInsert matters: a claim rides a DML statement type, so
			// it takes the branch that copies the fewest fields.
			Type:               protocol.StatementInsert,
			Database:           "testdb",
			TableName:          "__marmot__autoinc",
			IntentKey:          []byte("autoinc:testdb:users"),
			AutoIDClaim:        true,
			AutoIDClaimPayload: []byte("prepare-only"),
		}},
	})

	if len(req.Statements) != 1 {
		t.Fatalf("commit request carries %d statements, want 1", len(req.Statements))
	}
	stmt := req.Statements[0]
	if !stmt.AutoIDClaim {
		t.Error("the claim flag was dropped at COMMIT, so no participant would apply the claim it prepared")
	}
	if stmt.AutoIDClaimPayload != nil {
		t.Errorf("the claim payload reached COMMIT (%q); participants must read it from their own intent", stmt.AutoIDClaimPayload)
	}
	if len(stmt.IntentKey) == 0 {
		t.Error("the intent key was dropped, so the claim's intent could not be addressed")
	}
}
