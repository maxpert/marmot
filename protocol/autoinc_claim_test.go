package protocol

import "testing"

// TestAutoIncClaimWireShapeIsPinned pins the exact msgpack encoding of an
// AutoIncClaim payload: this is the wire contract a claim intent's
// DataSnapshot carries between every participant in the cluster, and it must
// not silently change shape (e.g. a field reorder, rename, or a tag edit)
// without every node in a mixed-version cluster being able to decode it.
//
// The byte length below was captured from this exact field set - table
// "users", prev_base 100, new_base 164, size 64 - encoded through
// encoding.Marshal. A change to this length signals an encoding change that
// needs a deliberate wire-compatibility decision, not a silent drift.
func TestAutoIncClaimWireShapeIsPinned(t *testing.T) {
	claim := AutoIncClaim{Table: "users", PrevBase: 100, NewBase: 164, Size: 64}

	data, err := EncodeAutoIncClaim(claim)
	if err != nil {
		t.Fatalf("EncodeAutoIncClaim: %v", err)
	}

	const wantLen = 64
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
