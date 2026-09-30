package protocol

import (
	"testing"

	"github.com/maxpert/marmot/encoding"
)

// TestAutoIncClaimWireShapeIsPinned pins the exact msgpack encoding of an
// AutoIncClaim payload: this is the wire contract a claim intent's
// DataSnapshot carries between every participant in the cluster, and it must
// not silently change shape (e.g. a field reorder, rename, or a tag edit)
// without every node in a mixed-version cluster being able to decode it.
//
// The byte length below was captured from this exact field set - table
// "users", prev_base 100, new_base 164, size 64, membership 3 - encoded through
// encoding.Marshal. A change to this length signals an encoding change that
// needs a deliberate wire-compatibility decision, not a silent drift.
func TestAutoIncClaimWireShapeIsPinned(t *testing.T) {
	claim := AutoIncClaim{Table: "users", PrevBase: 100, NewBase: 164, Size: 64, Membership: 3}

	data, err := EncodeAutoIncClaim(claim)
	if err != nil {
		t.Fatalf("EncodeAutoIncClaim: %v", err)
	}

	const wantLen = 80
	if len(data) != wantLen {
		t.Fatalf("encoded length = %d, want %d (wire shape changed: %x)", len(data), wantLen, data)
	}

	got, err := DecodeAutoIncClaim(data)
	if err != nil {
		t.Fatalf("DecodeAutoIncClaim: %v", err)
	}
	if got != claim {
		t.Fatalf("round trip = %+v, want %+v", got, claim)
	}
}

// TestAutoIncClaimWithoutMembershipDecodesAsZero pins what a participant sees
// from a claimant that predates the membership field: the payload still
// decodes, with Membership 0, which the PREPARE condition refuses
// (db.prepareAutoIncClaim), rather than failing to decode or being read as a
// real size.
func TestAutoIncClaimWithoutMembershipDecodesAsZero(t *testing.T) {
	type legacyClaim struct {
		Table    string `msgpack:"table"`
		PrevBase uint64 `msgpack:"prev_base"`
		NewBase  uint64 `msgpack:"new_base"`
		Size     uint64 `msgpack:"size"`
	}
	data, err := encoding.Marshal(legacyClaim{Table: "users", PrevBase: 100, NewBase: 100, Size: 64})
	if err != nil {
		t.Fatalf("marshal legacy claim: %v", err)
	}
	got, err := DecodeAutoIncClaim(data)
	if err != nil {
		t.Fatalf("a claim without the membership field must still decode: %v", err)
	}
	want := AutoIncClaim{Table: "users", PrevBase: 100, NewBase: 100, Size: 64}
	if got != want {
		t.Fatalf("decoded %+v, want %+v with Membership 0", got, want)
	}
}
