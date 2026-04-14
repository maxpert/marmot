// Package store handles Pebble-backed persistence for the vecindex engine.
// It defines keyspace layout and provides key encoding helpers.
package store

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
)

// Key prefix bytes that partition the Pebble keyspace by record type.
const (
	// KeyPrefixCentroid stores serialized centroid vectors keyed by cluster ID.
	KeyPrefixCentroid byte = 0x01
	// KeyPrefixPosting stores posting-list entries: (clusterID, docID) → empty.
	KeyPrefixPosting byte = 0x02
	// KeyPrefixReverseMap stores docID → clusterID for fast cluster lookup.
	KeyPrefixReverseMap byte = 0x03
	// KeyPrefixClusterMeta stores per-cluster metadata (count, centroid epoch).
	KeyPrefixClusterMeta byte = 0x04
	// KeyPrefixExtToDoc maps externalID bytes → uint64 docID.
	KeyPrefixExtToDoc byte = 0x05
	// KeyPrefixDocToExt maps uint64 docID → externalID bytes.
	KeyPrefixDocToExt byte = 0x06
	// KeyPrefixSpec stores the serialized IVFSpec for the index.
	KeyPrefixSpec byte = 0x07
)

// Store wraps a Pebble database and provides typed key access for vecindex data.
type Store struct {
	db *pebble.DB
}

// New opens or creates a Pebble store at the given directory path.
func New(dir string, opts *pebble.Options) (*Store, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// DB returns the underlying Pebble database.
func (s *Store) DB() *pebble.DB {
	return s.db
}

// Close closes the underlying Pebble database.
func (s *Store) Close() error {
	return s.db.Close()
}

// EncodeCentroidKey returns the key for a centroid record.
// Layout: [0x01][clusterID uint32 big-endian]
func EncodeCentroidKey(clusterID uint32) []byte {
	key := make([]byte, 5)
	key[0] = KeyPrefixCentroid
	binary.BigEndian.PutUint32(key[1:], clusterID)
	return key
}

// EncodePostingKey returns the key for a posting-list entry.
// Layout: [0x02][clusterID uint32 big-endian][docID uint64 big-endian]
func EncodePostingKey(clusterID uint32, docID uint64) []byte {
	key := make([]byte, 13)
	key[0] = KeyPrefixPosting
	binary.BigEndian.PutUint32(key[1:5], clusterID)
	binary.BigEndian.PutUint64(key[5:], docID)
	return key
}

// EncodePostingPrefix returns a prefix for scanning all postings in a cluster.
// Layout: [0x02][clusterID uint32 big-endian]
func EncodePostingPrefix(clusterID uint32) []byte {
	key := make([]byte, 5)
	key[0] = KeyPrefixPosting
	binary.BigEndian.PutUint32(key[1:], clusterID)
	return key
}

// EncodeReverseKey returns the key for a reverse-map entry (docID → clusterID).
// Layout: [0x03][docID uint64 big-endian]
func EncodeReverseKey(docID uint64) []byte {
	key := make([]byte, 9)
	key[0] = KeyPrefixReverseMap
	binary.BigEndian.PutUint64(key[1:], docID)
	return key
}

// EncodeClusterMetaKey returns the key for cluster metadata.
// Layout: [0x04][clusterID uint32 big-endian]
func EncodeClusterMetaKey(clusterID uint32) []byte {
	key := make([]byte, 5)
	key[0] = KeyPrefixClusterMeta
	binary.BigEndian.PutUint32(key[1:], clusterID)
	return key
}

// EncodeExtToDocKey returns the key for the external-ID to doc-ID mapping.
// Layout: [0x05][externalID bytes]
func EncodeExtToDocKey(externalID []byte) []byte {
	key := make([]byte, 1+len(externalID))
	key[0] = KeyPrefixExtToDoc
	copy(key[1:], externalID)
	return key
}

// EncodeDocToExtKey returns the key for the doc-ID to external-ID mapping.
// Layout: [0x06][docID uint64 big-endian]
func EncodeDocToExtKey(docID uint64) []byte {
	key := make([]byte, 9)
	key[0] = KeyPrefixDocToExt
	binary.BigEndian.PutUint64(key[1:], docID)
	return key
}

// EncodeSpecKey returns the singleton key for the index spec record.
// Layout: [0x07]
func EncodeSpecKey() []byte {
	return []byte{KeyPrefixSpec}
}

// DecodeClusterIDFromPostingKey extracts the clusterID from a posting key.
// Callers must verify the key has prefix KeyPrefixPosting before calling.
func DecodeClusterIDFromPostingKey(key []byte) uint32 {
	return binary.BigEndian.Uint32(key[1:5])
}

// DecodeDocIDFromPostingKey extracts the docID from a posting key.
// Callers must verify the key has prefix KeyPrefixPosting before calling.
func DecodeDocIDFromPostingKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[5:13])
}

// DecodeDocIDFromReverseKey extracts the docID from a reverse-map key.
// Callers must verify the key has prefix KeyPrefixReverseMap before calling.
func DecodeDocIDFromReverseKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[1:9])
}
