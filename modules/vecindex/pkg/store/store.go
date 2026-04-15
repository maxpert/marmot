// Package store handles Pebble-backed persistence for the vecindex engine.
// It defines keyspace layout and provides key encoding helpers.
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog/log"
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

// keyClusterIDCounter is the sub-key within KeyPrefixSpec for the cluster ID watermark.
var keyClusterIDCounter = []byte{KeyPrefixSpec, 0x01}

// clusterMetaSize is the fixed binary size of a serialized ClusterMeta.
// Layout: Size(4) + Epoch(8) + TombstoneCount(4) + State(1) = 17 bytes.
const clusterMetaSize = 17

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
	db        *pebble.DB
	allocMu   sync.Mutex
	clusterID uint32 // in-memory watermark; persisted on each alloc
}

// New opens or creates a Pebble store at the given directory path.
// If opts.Cache is non-nil, New releases the caller's construction-time reference
// after pebble.Open takes its own, preventing a one-ref leak per open call (MR-04).
func New(dir string, opts *pebble.Options) (*Store, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("store: open pebble at %s: %w", dir, err)
	}
	if opts != nil && opts.Cache != nil {
		opts.Cache.Unref()
	}

	s := &Store{db: db}
	if err := s.loadClusterIDWatermark(); err != nil {
		_ = db.Close()
		return nil, err
	}

	log.Debug().Str("dir", dir).Msg("store: opened")
	return s, nil
}

// loadClusterIDWatermark reads the persisted cluster ID counter from disk.
func (s *Store) loadClusterIDWatermark() error {
	val, closer, err := s.db.Get(keyClusterIDCounter)
	if errors.Is(err, pebble.ErrNotFound) {
		s.clusterID = 0
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: load cluster ID watermark: %w", err)
	}
	defer closer.Close()
	s.clusterID = binary.BigEndian.Uint32(val)
	return nil
}

// DB returns the underlying Pebble database.
func (s *Store) DB() *pebble.DB {
	return s.db
}

// Close closes the underlying Pebble database.
func (s *Store) Close() error {
	log.Info().Msg("store: closing")
	return s.db.Close()
}

// NewBatch creates a new atomic batch writer.
func (s *Store) NewBatch() *Batch {
	return &Batch{b: s.db.NewBatch()}
}

// Commit atomically applies the batch to the store.
func (b *Batch) Commit() error {
	return b.b.Commit(pebble.NoSync)
}

// Close discards the batch without committing.
func (b *Batch) Close() error {
	return b.b.Close()
}

// PutCentroid stores a centroid vector for clusterID.
// Returns an error if vec has the wrong dimension (dim > 0 to validate).
func (s *Store) PutCentroid(clusterID uint32, vec []float32, dim int) error {
	if dim > 0 && len(vec) != dim {
		return fmt.Errorf("store: centroid dimension mismatch: got %d, want %d", len(vec), dim)
	}
	key := EncodeCentroidKey(clusterID)
	val := encodeFloat32Slice(vec)
	return s.db.Set(key, val, pebble.NoSync)
}

// GetCentroid retrieves the centroid vector for clusterID.
// Returns ErrNotFound if absent.
func (s *Store) GetCentroid(clusterID uint32) ([]float32, error) {
	key := EncodeCentroidKey(clusterID)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return decodeFloat32Slice(val), nil
}

// DeleteCentroid removes the centroid for clusterID.
func (s *Store) DeleteCentroid(clusterID uint32) error {
	key := EncodeCentroidKey(clusterID)
	return s.db.Delete(key, pebble.NoSync)
}

// ListCentroids returns all (clusterID, vector) pairs sorted by clusterID ascending.
func (s *Store) ListCentroids() ([]uint32, [][]float32, error) {
	lower := []byte{KeyPrefixCentroid}
	upper := []byte{KeyPrefixCentroid + 1}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	var ids []uint32
	var vecs [][]float32
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 5 || key[0] != KeyPrefixCentroid {
			break
		}
		clusterID := binary.BigEndian.Uint32(key[1:5])
		vec := decodeFloat32Slice(iter.Value())
		ids = append(ids, clusterID)
		vecs = append(vecs, vec)
	}
	if err := iter.Error(); err != nil {
		return nil, nil, err
	}
	return ids, vecs, nil
}

// PutPosting inserts an inline vector for (clusterID, docID) into namespace 0x02.
func (s *Store) PutPosting(clusterID uint32, docID uint64, vec []float32) error {
	key := EncodePostingKey(clusterID, docID)
	val := encodeFloat32Slice(vec)
	return s.db.Set(key, val, pebble.NoSync)
}

// GetPosting retrieves the inline vector for (clusterID, docID).
// Returns ErrNotFound if absent.
func (s *Store) GetPosting(clusterID uint32, docID uint64) ([]float32, error) {
	key := EncodePostingKey(clusterID, docID)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return decodeFloat32Slice(val), nil
}

// DeletePosting removes the posting entry for (clusterID, docID).
func (s *Store) DeletePosting(clusterID uint32, docID uint64) error {
	key := EncodePostingKey(clusterID, docID)
	return s.db.Delete(key, pebble.NoSync)
}

// ScanCluster iterates all posting entries for clusterID in docID ascending order.
func (s *Store) ScanCluster(clusterID uint32) ([]PostingEntry, error) {
	lower := EncodePostingPrefix(clusterID)
	upper := EncodePostingPrefix(clusterID + 1)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entries []PostingEntry
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 13 || key[0] != KeyPrefixPosting {
			break
		}
		docID := binary.BigEndian.Uint64(key[5:13])
		vec := decodeFloat32Slice(iter.Value())
		entries = append(entries, PostingEntry{DocID: docID, Vector: vec})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

// ScanClusterFunc iterates all posting entries for clusterID, calling fn for each
// entry with its docID and the raw little-endian float32 bytes of the vector.
// fn MUST NOT retain vecBytes beyond its return — the slice is owned by the Pebble
// iterator and becomes invalid after fn returns. Return a non-nil error from fn to
// abort iteration early; that error is propagated to the caller.
//
// Deprecated: ScanCluster materialises a full []PostingEntry slice; prefer
// ScanClusterFunc for search hot-paths to avoid 6KB-per-vector heap allocations.
func (s *Store) ScanClusterFunc(clusterID uint32, fn func(docID uint64, vecBytes []byte) error) error {
	lower := EncodePostingPrefix(clusterID)
	upper := EncodePostingPrefix(clusterID + 1)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 13 || key[0] != KeyPrefixPosting {
			break
		}
		docID := binary.BigEndian.Uint64(key[5:13])
		if err := fn(docID, iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

// PutReverseMap stores docID → clusterID in namespace 0x03.
func (s *Store) PutReverseMap(docID uint64, clusterID uint32) error {
	key := EncodeReverseKey(docID)
	val := make([]byte, 4)
	binary.BigEndian.PutUint32(val, clusterID)
	return s.db.Set(key, val, pebble.NoSync)
}

// GetClusterForDoc returns the clusterID assigned to docID.
// Returns ErrNotFound if absent.
func (s *Store) GetClusterForDoc(docID uint64) (uint32, error) {
	key := EncodeReverseKey(docID)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	return binary.BigEndian.Uint32(val), nil
}

// GetClusterMeta retrieves metadata for clusterID.
// Returns ErrNotFound if absent.
func (s *Store) GetClusterMeta(clusterID uint32) (ClusterMeta, error) {
	key := EncodeClusterMetaKey(clusterID)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return ClusterMeta{}, ErrNotFound
	}
	if err != nil {
		return ClusterMeta{}, err
	}
	defer closer.Close()
	return decodeClusterMeta(val)
}

// PutClusterMeta stores metadata for clusterID.
func (s *Store) PutClusterMeta(clusterID uint32, meta ClusterMeta) error {
	key := EncodeClusterMetaKey(clusterID)
	val := encodeClusterMeta(meta)
	return s.db.Set(key, val, pebble.NoSync)
}

// ListActiveClusters returns clusterIDs whose state is ClusterStateActive.
func (s *Store) ListActiveClusters() ([]uint32, error) {
	lower := []byte{KeyPrefixClusterMeta}
	upper := []byte{KeyPrefixClusterMeta + 1}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var ids []uint32
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 5 || key[0] != KeyPrefixClusterMeta {
			break
		}
		meta, decErr := decodeClusterMeta(iter.Value())
		if decErr != nil {
			continue
		}
		if meta.State == ClusterStateActive {
			clusterID := binary.BigEndian.Uint32(key[1:5])
			ids = append(ids, clusterID)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return ids, nil
}

// PutExtToDoc stores a mapping from externalID → docID in namespace 0x05.
func (s *Store) PutExtToDoc(externalID []byte, docID uint64) error {
	key := EncodeExtToDocKey(externalID)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, docID)
	return s.db.Set(key, val, pebble.NoSync)
}

// GetExtToDoc retrieves the docID for externalID.
// Returns ErrNotFound if absent.
func (s *Store) GetExtToDoc(externalID []byte) (uint64, error) {
	key := EncodeExtToDocKey(externalID)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	return binary.BigEndian.Uint64(val), nil
}

// PutDocToExt stores a mapping from docID → externalID in namespace 0x06.
func (s *Store) PutDocToExt(docID uint64, externalID []byte) error {
	key := EncodeDocToExtKey(docID)
	return s.db.Set(key, externalID, pebble.NoSync)
}

// GetDocToExt retrieves the externalID for docID.
// Returns ErrNotFound if absent.
func (s *Store) GetDocToExt(docID uint64) ([]byte, error) {
	key := EncodeDocToExtKey(docID)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	// Copy the value before closing
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

// DeleteExtMapping removes both 0x05 and 0x06 entries for the given pair atomically.
func (s *Store) DeleteExtMapping(externalID []byte, docID uint64) error {
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Delete(EncodeExtToDocKey(externalID), pebble.NoSync); err != nil {
		return err
	}
	if err := b.Delete(EncodeDocToExtKey(docID), pebble.NoSync); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// CompactCluster runs pebble.Compact over the posting prefix for clusterID.
func (s *Store) CompactCluster(clusterID uint32) error {
	postStart := EncodePostingPrefix(clusterID)
	postEnd := EncodePostingPrefix(clusterID + 1)
	if err := s.db.Compact(postStart, postEnd, true); err != nil {
		return fmt.Errorf("store: compact posting range for cluster %d: %w", clusterID, err)
	}

	centStart := EncodeCentroidKey(clusterID)
	centEnd := EncodeCentroidKey(clusterID + 1)
	if err := s.db.Compact(centStart, centEnd, true); err != nil {
		return fmt.Errorf("store: compact centroid range for cluster %d: %w", clusterID, err)
	}
	return nil
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
	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	next := s.clusterID + 1
	val := make([]byte, 4)
	binary.BigEndian.PutUint32(val, next)
	if err := s.db.Set(keyClusterIDCounter, val, pebble.Sync); err != nil {
		return 0, fmt.Errorf("store: persist cluster ID watermark: %w", err)
	}
	s.clusterID = next
	return next, nil
}

// Checkpoint creates a hard-linked snapshot of the store at destDir using
// pebble.Checkpoint semantics. Flushes memtables first so all data is visible
// in the snapshot.
func (s *Store) Checkpoint(destDir string) error {
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("store: flush before checkpoint: %w", err)
	}
	return s.db.Checkpoint(destDir)
}

// BatchPutPosting adds an inline-vector posting write to the batch.
func (b *Batch) BatchPutPosting(clusterID uint32, docID uint64, vec []float32) error {
	key := EncodePostingKey(clusterID, docID)
	val := encodeFloat32Slice(vec)
	return b.b.Set(key, val, pebble.NoSync)
}

// BatchDeletePosting adds a posting delete to the batch.
func (b *Batch) BatchDeletePosting(clusterID uint32, docID uint64) error {
	key := EncodePostingKey(clusterID, docID)
	return b.b.Delete(key, pebble.NoSync)
}

// BatchPutReverseMap adds a reverse-map write to the batch.
func (b *Batch) BatchPutReverseMap(docID uint64, clusterID uint32) error {
	key := EncodeReverseKey(docID)
	val := make([]byte, 4)
	binary.BigEndian.PutUint32(val, clusterID)
	return b.b.Set(key, val, pebble.NoSync)
}

// BatchPutClusterMeta adds a cluster-meta write to the batch.
func (b *Batch) BatchPutClusterMeta(clusterID uint32, meta ClusterMeta) error {
	key := EncodeClusterMetaKey(clusterID)
	val := encodeClusterMeta(meta)
	return b.b.Set(key, val, pebble.NoSync)
}

// BatchPutExtToDoc adds an ext→doc mapping write to the batch.
func (b *Batch) BatchPutExtToDoc(externalID []byte, docID uint64) error {
	key := EncodeExtToDocKey(externalID)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, docID)
	return b.b.Set(key, val, pebble.NoSync)
}

// BatchPutDocToExt adds a doc→ext mapping write to the batch.
func (b *Batch) BatchPutDocToExt(docID uint64, externalID []byte) error {
	key := EncodeDocToExtKey(docID)
	return b.b.Set(key, externalID, pebble.NoSync)
}

// BatchDeleteExtMapping adds both ext→doc and doc→ext deletes to the batch.
func (b *Batch) BatchDeleteExtMapping(externalID []byte, docID uint64) error {
	if err := b.b.Delete(EncodeExtToDocKey(externalID), pebble.NoSync); err != nil {
		return err
	}
	return b.b.Delete(EncodeDocToExtKey(docID), pebble.NoSync)
}

// BatchDeleteReverseMap adds a reverse-map delete to the batch.
func (b *Batch) BatchDeleteReverseMap(docID uint64) error {
	return b.b.Delete(EncodeReverseKey(docID), pebble.NoSync)
}

// BatchPutWatermark adds a watermark write for externalID to the batch.
// The watermark is stored in the 0x08 namespace.
func (b *Batch) BatchPutWatermark(externalID []byte, txnID, seqID uint64) error {
	key := EncodeWatermarkKey(externalID)
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[:8], txnID)
	binary.BigEndian.PutUint64(val[8:], seqID)
	return b.b.Set(key, val, pebble.NoSync)
}

// EncodeWatermarkKey encodes the 0x08-prefixed watermark key for externalID.
func EncodeWatermarkKey(externalID []byte) []byte {
	key := make([]byte, 1+len(externalID))
	key[0] = 0x08
	copy(key[1:], externalID)
	return key
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

// encodeFloat32Slice encodes a float32 slice as raw little-endian bytes.
func encodeFloat32Slice(vec []float32) []byte {
	buf := make([]byte, len(vec)*4)
	for i, v := range vec {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}

// decodeFloat32Slice decodes raw little-endian bytes into a float32 slice.
func decodeFloat32Slice(buf []byte) []float32 {
	n := len(buf) / 4
	vec := make([]float32, n)
	for i := range vec {
		bits := binary.LittleEndian.Uint32(buf[i*4:])
		vec[i] = math.Float32frombits(bits)
	}
	return vec
}

// encodeClusterMeta serializes a ClusterMeta to a fixed-size byte slice.
// Layout: [Size uint32 BE][Epoch uint64 BE][TombstoneCount uint32 BE][State uint8]
func encodeClusterMeta(m ClusterMeta) []byte {
	buf := make([]byte, clusterMetaSize)
	binary.BigEndian.PutUint32(buf[0:4], m.Size)
	binary.BigEndian.PutUint64(buf[4:12], m.Epoch)
	binary.BigEndian.PutUint32(buf[12:16], m.TombstoneCount)
	buf[16] = uint8(m.State)
	return buf
}

// decodeClusterMeta deserializes a ClusterMeta from its binary representation.
// Returns an error if the buffer is too short to be valid.
func decodeClusterMeta(buf []byte) (ClusterMeta, error) {
	if len(buf) < clusterMetaSize {
		return ClusterMeta{}, fmt.Errorf("store: cluster meta buffer too short: %d < %d", len(buf), clusterMetaSize)
	}
	return ClusterMeta{
		Size:           binary.BigEndian.Uint32(buf[0:4]),
		Epoch:          binary.BigEndian.Uint64(buf[4:12]),
		TombstoneCount: binary.BigEndian.Uint32(buf[12:16]),
		State:          ClusterState(buf[16]),
	}, nil
}
