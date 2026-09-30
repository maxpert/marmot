package grpc

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/maxpert/marmot/encoding"
	"github.com/rs/zerolog/log"
)

// membershipSnapshotFile is the snapshot's name inside the node's data directory.
const membershipSnapshotFile = "membership.msgpack"

// membershipSnapshotNode is one node's durable membership record.
//
// Only the fields a restart needs to reconstruct a quorum denominator and to
// keep SWIM's refutation logic working are stored. Liveness is deliberately NOT
// among them: a restored peer's status is decided by the restore, not by what it
// was when the snapshot was written (see restoreMembershipLocked).
type membershipSnapshotNode struct {
	NodeID      uint64 `msgpack:"node_id"`
	Address     string `msgpack:"address"`
	Incarnation uint64 `msgpack:"incarnation"`
	Status      int32  `msgpack:"status"`
}

// membershipSnapshot is the whole durable record.
type membershipSnapshot struct {
	Nodes []membershipSnapshotNode `msgpack:"nodes"`
}

// membershipStore persists cluster membership across restarts.
//
// It exists because quorum is a majority of TOTAL membership, and a registry
// that starts with only itself computes a majority of one. A node that has ever
// known a multi-node membership must not compute a quorum from fewer members
// than it last knew until it has re-learned membership from a live peer, and
// the only way to know what it last knew is to have written it down.
//
// It is a plain file, not the system Pebble store, because the registry is
// built inside the gRPC server (marmot.go initializeGRPCServer) before the
// database manager exists, so no Pebble store is open at that point.
type membershipStore struct {
	path string
}

// newMembershipStore returns a store rooted at dataDir, or nil when dataDir is
// empty. A nil store disables persistence, which is what the in-process tests
// and any embedded use without a data directory want; every method tolerates a
// nil receiver.
func newMembershipStore(dataDir string) *membershipStore {
	if dataDir == "" {
		return nil
	}
	return &membershipStore{path: filepath.Join(dataDir, membershipSnapshotFile)}
}

// save writes the snapshot atomically: a temp file in the same directory,
// fsynced, then renamed over the target, then the directory fsynced so the
// rename itself survives a crash. A half-written snapshot would be worse than
// none, because it would be read back as a smaller membership.
func (s *membershipStore) save(snapshot membershipSnapshot) error {
	if s == nil {
		return nil
	}

	// A stable order keeps the bytes identical for identical membership, so a
	// no-op save cannot look like a change to anything watching the file.
	sort.Slice(snapshot.Nodes, func(i, j int) bool {
		return snapshot.Nodes[i].NodeID < snapshot.Nodes[j].NodeID
	})

	data, err := encoding.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode membership snapshot: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, membershipSnapshotFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp membership snapshot: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write membership snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("fsync membership snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close membership snapshot: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename membership snapshot: %w", err)
	}

	// Without this the rename can be lost on a crash even though the file's
	// contents were synced.
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}

// load reads the snapshot. A missing or unreadable snapshot is not an error the
// caller should act on: it means "nothing known", and the registry starts with
// self only, exactly as it did before this file existed. Returning an error here
// instead would turn a corrupt file into a node that refuses to boot.
func (s *membershipStore) load() (membershipSnapshot, bool) {
	if s == nil {
		return membershipSnapshot{}, false
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Warn().Err(err).Str("path", s.path).
				Msg("Membership snapshot unreadable; starting with self-only membership")
		}
		return membershipSnapshot{}, false
	}

	var snapshot membershipSnapshot
	if err := encoding.Unmarshal(data, &snapshot); err != nil {
		log.Warn().Err(err).Str("path", s.path).
			Msg("Membership snapshot corrupt; starting with self-only membership")
		return membershipSnapshot{}, false
	}
	return snapshot, true
}
