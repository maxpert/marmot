package grpc

import (
	"testing"

	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/protocol"
)

// TestClaimFlagRoundTripsOnAnExistingStatementType pins the shape the design
// requires of a range claim on the wire: an EXISTING StatementType carrying a
// flag, a non-empty intent key, and deliberately NO row image.
//
// A new StatementType was rejected by the design because an older binary would
// reach a conversion that does not know it. A flag is ignored by an older
// binary, which then sees a DML statement with no row image and rejects it at
// the "DML prepare missing encoded CDC row" gate - a clean failed transaction
// (TestClaimIsRejectedByABinaryThatIgnoresTheFlag).
//
// Mutation: drop the AutoIdClaim assignment in convertStatementsToProto, or the
// GetAutoIdClaim read in protocolStatementFromProto. The round trip loses the
// flag and the claim is applied as ordinary DML.
func TestClaimFlagRoundTripsOnAnExistingStatementType(t *testing.T) {
	claim := protocol.Statement{
		Type:        protocol.StatementInsert,
		TableName:   "users",
		Database:    "testdb",
		IntentKey:   []byte("autoinc:testdb:users"),
		AutoIDClaim: true,
	}

	protoStmts, err := convertStatementsToProto([]protocol.Statement{claim}, "testdb", 1)
	if err != nil {
		t.Fatalf("convertStatementsToProto: %v", err)
	}
	if len(protoStmts) != 1 {
		t.Fatalf("converted %d statements, want 1", len(protoStmts))
	}
	wire := protoStmts[0]

	// An EXISTING type, not a new one.
	// Mutation: introduce a dedicated StatementType for claims.
	if wire.Type != pb.StatementType_INSERT {
		t.Errorf("wire type = %v, want the existing INSERT type", wire.Type)
	}
	if !wire.GetAutoIdClaim() {
		t.Error("the claim flag did not reach the wire")
	}
	// No row image: this is what makes an older binary reject rather than
	// mishandle. Mutation: attach a RowChange payload to the claim.
	if wire.GetRowChange() != nil {
		t.Error("a claim statement carries a row image; an older binary would not reject it at the CDC-row gate")
	}

	back, err := protocolStatementFromProto(wire)
	if err != nil {
		t.Fatalf("protocolStatementFromProto: %v", err)
	}
	if !back.AutoIDClaim {
		t.Error("the claim flag did not survive the round trip")
	}
	if len(back.EncodedRow) != 0 {
		t.Error("the claim gained a row image on the way back")
	}
	if string(back.IntentKey) != "autoinc:testdb:users" {
		t.Errorf("intent key = %q, want it preserved", back.IntentKey)
	}
}

// TestAutoIDStoredBaseSurvivesTransactionResponseConversion pins the
// coordinator-facing half of the rejected-claim retry plumbing: a
// db.PrepareResult's AutoIDStoredBase (the rejecting participant's own
// committed base) must survive the conversion to the wire TransactionResponse
// and back to coordinator.ReplicationResponse, or a claimant retrying above a
// participant's stored base loses the value it needs to retry with.
//
// Mutation: drop AutoIdStoredBase from convertTransactionResponse. The
// handlePrepare half is TestHandlePrepareReturnsTheStoredBaseOfARejectedClaim.
func TestAutoIDStoredBaseSurvivesTransactionResponseConversion(t *testing.T) {
	wire := &TransactionResponse{
		Success:      false,
		Rejected:     true,
		ErrorMessage: "auto-increment claim for orders is stale: this node holds base 77",
		// This is what ReplicationHandler.handlePrepare copies from
		// db.PrepareResult.AutoIDStoredBase.
		AutoIdStoredBase: 77,
	}

	resp := convertTransactionResponse(wire)

	if !resp.Rejected {
		t.Error("Rejected did not survive the conversion")
	}
	if resp.AutoIDStoredBase != 77 {
		t.Errorf("AutoIDStoredBase = %d, want 77", resp.AutoIDStoredBase)
	}
}
