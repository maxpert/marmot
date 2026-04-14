// Package store handles Pebble-backed persistence for the vecindex engine.
// It defines keyspace layout and provides key encoding helpers.
package store

import (
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
)

// ErrNotFound is returned when a requested key does not exist in the store.
var ErrNotFound = errors.New("store: key not found")

// Key prefix bytes that partition the Pebble keyspace by record type.
const (
	// KeyPrefixCentroid stores serialized centroid vectors keyed by cluster ID.
	KeyPrefixCentroid byte = 0x01
	// KeyPrefixPosting stores posting-list entries: (clusterID, docID) → inline vector.
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

// ClusterState represents the lifecycle state of an IVF cluster.
type ClusterState uint8

const (
	// ClusterStateActive is the normal operating state.
	ClusterStateActive ClusterState = iota
	// ClusterStateSplitting indicates the cluster is undergoing a split.
	ClusterStateSplitting
	// ClusterStateRetired indicates the cluster is no longer active.
	ClusterStateRetired
)

// ClusterMeta holds per-cluster metadata persisted in namespace 0x04.
type ClusterMeta struct {
	// Size is the number of live (non-tombstoned) vectors in the cluster.
	Size uint32
	// Epoch is the centroid generation when this cluster was last updated.
	Epoch uint64
	// TombstoneCount is the number of deleted vectors not yet compacted.
	TombstoneCount uint32
	// State is the current lifecycle state of the cluster.
	State ClusterState
}

// PostingEntry is a single result returned by ScanCluster.
type PostingEntry struct {
	// DocID is the internal document identifier.
	DocID uint64
	// Vector holds the inline float32 values for this posting.
	Vector []float32
}

// Batch wraps a pebble.Batch for atomic multi-namespace writes.
type Batch struct {
	b *pebble.Batch
}

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

// NewBatch creates a new atomic batch writer.
func (s *Store) NewBatch() *Batch {
	return &Batch{b: s.db.NewBatch()}
}

// Commit atomically applies the batch to the store.
func (b *Batch) Commit() error {
	return errors.New("not implemented: Batch.Commit")
}

// Close discards the batch without committing.
func (b *Batch) Close() error {
	return errors.New("not implemented: Batch.Close")
}

// PutCentroid stores a centroid vector for clusterID.
// Returns an error if vec has the wrong dimension (dim > 0 to validate).
func (s *Store) PutCentroid(clusterID uint32, vec []float32, dim int) error {
	return errors.New("not implemented: PutCentroid")
}

// GetCentroid retrieves the centroid vector for clusterID.
// Returns ErrNotFound if absent.
func (s *Store) GetCentroid(clusterID uint32) ([]float32, error) {
	return nil, errors.New("not implemented: GetCentroid")
}

// DeleteCentroid removes the centroid for clusterID.
func (s *Store) DeleteCentroid(clusterID uint32) error {
	return errors.New("not implemented: DeleteCentroid")
}

// ListCentroids returns all (clusterID, vector) pairs sorted by clusterID ascending.
func (s *Store) ListCentroids() ([]uint32, [][]float32, error) {
	return nil, nil, errors.New("not implemented: ListCentroids")
}

// PutPosting inserts an inline vector for (clusterID, docID) into namespace 0x02.
func (s *Store) PutPosting(clusterID uint32, docID uint64, vec []float32) error {
	return errors.New("not implemented: PutPosting")
}

// GetPosting retrieves the inline vector for (clusterID, docID).
// Returns ErrNotFound if absent.
func (s *Store) GetPosting(clusterID uint32, docID uint64) ([]float32, error) {
	return nil, errors.New("not implemented: GetPosting")
}

// DeletePosting removes the posting entry for (clusterID, docID).
func (s *Store) DeletePosting(clusterID uint32, docID uint64) error {
	return errors.New("not implemented: DeletePosting")
}

// ScanCluster iterates all posting entries for clusterID in docID ascending order.
func (s *Store) ScanCluster(clusterID uint32) ([]PostingEntry, error) {
	return nil, errors.New("not implemented: ScanCluster")
}

// PutReverseMap stores docID → clusterID in namespace 0x03.
func (s *Store) PutReverseMap(docID uint64, clusterID uint32) error {
	return errors.New("not implemented: PutReverseMap")
}

// GetClusterForDoc returns the clusterID assigned to docID.
// Returns ErrNotFound if absent.
func (s *Store) GetClusterForDoc(docID uint64) (uint32, error) {
	return 0, errors.New("not implemented: GetClusterForDoc")
}

// GetClusterMeta retrieves metadata for clusterID.
// Returns ErrNotFound if absent.
func (s *Store) GetClusterMeta(clusterID uint32) (ClusterMeta, error) {
	return ClusterMeta{}, errors.New("not implemented: GetClusterMeta")
}

// PutClusterMeta stores metadata for clusterID.
func (s *Store) PutClusterMeta(clusterID uint32, meta ClusterMeta) error {
	return errors.New("not implemented: PutClusterMeta")
}

// ListActiveClusters returns clusterIDs whose state is ClusterStateActive.
func (s *Store) ListActiveClusters() ([]uint32, error) {
	return nil, errors.New("not implemented: ListActiveClusters")
}

// PutExtToDoc stores a mapping from externalID → docID in namespace 0x05.
func (s *Store) PutExtToDoc(externalID []byte, docID uint64) error {
	return errors.New("not implemented: PutExtToDoc")
}

// GetExtToDoc retrieves the docID for externalID.
// Returns ErrNotFound if absent.
func (s *Store) GetExtToDoc(externalID []byte) (uint64, error) {
	return 0, errors.New("not implemented: GetExtToDoc")
}

// PutDocToExt stores a mapping from docID → externalID in namespace 0x06.
func (s *Store) PutDocToExt(docID uint64, externalID []byte) error {
	return errors.New("not implemented: PutDocToExt")
}

// GetDocToExt retrieves the externalID for docID.
// Returns ErrNotFound if absent.
func (s *Store) GetDocToExt(docID uint64) ([]byte, error) {
	return nil, errors.New("not implemented: GetDocToExt")
}

// DeleteExtMapping removes both 0x05 and 0x06 entries for the given pair atomically.
func (s *Store) DeleteExtMapping(externalID []byte, docID uint64) error {
	return errors.New("not implemented: DeleteExtMapping")
}

// CompactCluster runs pebble.Compact over the posting prefix for clusterID.
func (s *Store) CompactCluster(clusterID uint32) error {
	return errors.New("not implemented: CompactCluster")
}

// ShouldCompact returns true when the cluster's tombstone ratio exceeds 30%.
func ShouldCompact(meta ClusterMeta) bool {
	total := meta.Size + meta.TombstoneCount
	if total == 0 {
		return false
	}
	return float64(meta.TombstoneCount)/float64(total) > 0.3
}

// AllocateClusterID returns the next monotonically increasing cluster ID,
// persisting the watermark so it survives restarts.
func (s *Store) AllocateClusterID() (uint32, error) {
	return 0, errors.New("not implemented: AllocateClusterID")
}

// Checkpoint creates a hard-linked snapshot of the store at destDir using
// pebble.Checkpoint semantics.
func (s *Store) Checkpoint(destDir string) error {
	return errors.New("not implemented: Checkpoint")
}

// BatchPutPosting adds an inline-vector posting write to the batch.
func (b *Batch) BatchPutPosting(clusterID uint32, docID uint64, vec []float32) error {
	return errors.New("not implemented: Batch.BatchPutPosting")
}

// BatchDeletePosting adds a posting delete to the batch.
func (b *Batch) BatchDeletePosting(clusterID uint32, docID uint64) error {
	return errors.New("not implemented: Batch.BatchDeletePosting")
}

// BatchPutReverseMap adds a reverse-map write to the batch.
func (b *Batch) BatchPutReverseMap(docID uint64, clusterID uint32) error {
	return errors.New("not implemented: Batch.BatchPutReverseMap")
}

// BatchPutClusterMeta adds a cluster-meta write to the batch.
func (b *Batch) BatchPutClusterMeta(clusterID uint32, meta ClusterMeta) error {
	return errors.New("not implemented: Batch.BatchPutClusterMeta")
}

// BatchPutExtToDoc adds an ext→doc mapping write to the batch.
func (b *Batch) BatchPutExtToDoc(externalID []byte, docID uint64) error {
	return errors.New("not implemented: Batch.BatchPutExtToDoc")
}

// BatchPutDocToExt adds a doc→ext mapping write to the batch.
func (b *Batch) BatchPutDocToExt(docID uint64, externalID []byte) error {
	return errors.New("not implemented: Batch.BatchPutDocToExt")
}

// BatchDeleteExtMapping adds both ext→doc and doc→ext deletes to the batch.
func (b *Batch) BatchDeleteExtMapping(externalID []byte, docID uint64) error {
	return errors.New("not implemented: Batch.BatchDeleteExtMapping")
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
