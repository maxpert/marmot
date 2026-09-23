package protocol

import (
	"fmt"

	"github.com/maxpert/marmot/encoding"
)

// AutoIncClaim is the payload a narrow AUTO_INCREMENT range claim carries in
// its intent's DataSnapshot. msgpack, like every other payload in this
// system.
//
// It lives here, rather than in coordinator or db, because coordinator builds
// claims (coordinator/autoinc_claim.go) and db applies them
// (db/autoinc_claim.go), but db already imports coordinator (for
// coordinator.Replicator), so coordinator importing db back would cycle.
// protocol imports neither db nor coordinator and both already import it,
// making it the neutral home for the type both sides share.
//
// It is what the COMMIT handler reads to write the new base, and what a
// participant evaluates at PREPARE. It carries no width: each participant
// derives widthMax from its OWN sqlite_master, because trusting the claimant
// would let a node with a stale schema be rubber-stamped.
type AutoIncClaim struct {
	Table    string `msgpack:"table"`
	PrevBase uint64 `msgpack:"prev_base"`
	NewBase  uint64 `msgpack:"new_base"`
	Size     uint64 `msgpack:"size"`
}

// EncodeAutoIncClaim renders a claim payload for an intent's DataSnapshot.
func EncodeAutoIncClaim(claim AutoIncClaim) ([]byte, error) {
	data, err := encoding.Marshal(claim)
	if err != nil {
		return nil, fmt.Errorf("encode auto-increment claim: %w", err)
	}
	return data, nil
}

// DecodeAutoIncClaim reads a claim payload back.
func DecodeAutoIncClaim(data []byte) (AutoIncClaim, error) {
	var claim AutoIncClaim
	if err := encoding.Unmarshal(data, &claim); err != nil {
		return AutoIncClaim{}, fmt.Errorf("decode auto-increment claim: %w", err)
	}
	return claim, nil
}

// AutoIncClaimKey is the intent key a claim locks on: one key per (database,
// table), so two claims for the same table serialise on the existing row lock
// while claims for different tables do not contend.
func AutoIncClaimKey(database, table string) string {
	return "autoinc:" + database + ":" + table
}
