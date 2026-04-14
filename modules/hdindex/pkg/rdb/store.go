package rdb

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
)

const maxPartitions = 256

// Entry represents a single RDB entry with its doc_id and reference distances.
type Entry struct {
	DocID    uint64
	RefDists []float32 // distances to each reference object
}

// Store manages the RDB (Reference Distance B+-tree) data in Pebble.
type Store struct {
	db       *pebble.DB
	refCount int // m: number of reference objects
}

// Open creates an RDB store backed by the given Pebble DB.
func Open(db *pebble.DB, refCount int) *Store {
	return &Store{db: db, refCount: refCount}
}

// Put inserts or updates an RDB entry. Uses the provided batch if non-nil.
func (s *Store) Put(batch *pebble.Batch, partitionID int, hilbertKey []byte, docID uint64, refDists []float32) error {
	if partitionID < 0 || partitionID >= maxPartitions {
		return fmt.Errorf("rdb: partitionID %d out of range [0, %d)", partitionID, maxPartitions)
	}
	if len(refDists) != s.refCount {
		return fmt.Errorf("rdb: refDists length %d != refCount %d", len(refDists), s.refCount)
	}
	key := rdbKey(partitionID, hilbertKey, docID)
	val := encodeRefDists(refDists)
	if batch != nil {
		return batch.Set(key, val, pebble.NoSync)
	}
	return s.db.Set(key, val, pebble.Sync)
}

// Delete removes an RDB entry. Uses the provided batch if non-nil.
func (s *Store) Delete(batch *pebble.Batch, partitionID int, hilbertKey []byte, docID uint64) error {
	if partitionID < 0 || partitionID >= maxPartitions {
		return fmt.Errorf("rdb: partitionID %d out of range [0, %d)", partitionID, maxPartitions)
	}
	key := rdbKey(partitionID, hilbertKey, docID)
	if batch != nil {
		return batch.Delete(key, pebble.NoSync)
	}
	return s.db.Delete(key, pebble.Sync)
}

// ScanNearest performs a bidirectional scan from the query's Hilbert key,
// collecting up to alpha entries with the closest Hilbert keys.
// Reference distances are decoded into a single pre-allocated arena to
// avoid per-entry heap allocations. Each Entry.RefDists is a subslice of
// the arena and must not be retained beyond the caller's scope.
func (s *Store) ScanNearest(partitionID int, queryHilbertKey []byte, alpha int) ([]Entry, error) {
	if partitionID < 0 || partitionID >= maxPartitions {
		return nil, fmt.Errorf("rdb: partitionID %d out of range [0, %d)", partitionID, maxPartitions)
	}
	prefix := rdbPrefix(partitionID)
	upper := prefixUpperBound(prefix)
	seekKey := rdbSeekKey(partitionID, queryHilbertKey)

	iterOpts := &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	}

	fwdIter, err := s.db.NewIter(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("rdb: new forward iter: %w", err)
	}
	defer fwdIter.Close()

	bwdIter, err := fwdIter.Clone(pebble.CloneOptions{})
	if err != nil {
		return nil, fmt.Errorf("rdb: clone backward iter: %w", err)
	}
	defer bwdIter.Close()

	fwdValid := fwdIter.SeekGE(seekKey)
	bwdValid := bwdIter.SeekLT(seekKey)

	// Pre-allocate arena for all ref distances: one contiguous buffer sliced per entry.
	refArena := make([]float32, alpha*s.refCount)
	entries := make([]Entry, 0, alpha)

	for len(entries) < alpha && (fwdValid || bwdValid) {
		if fwdValid {
			e, err := s.readEntryArena(fwdIter, refArena, len(entries))
			if err != nil {
				return nil, err
			}
			entries = append(entries, e)
			fwdValid = fwdIter.Next()
			if len(entries) >= alpha {
				break
			}
		}

		if bwdValid {
			e, err := s.readEntryArena(bwdIter, refArena, len(entries))
			if err != nil {
				return nil, err
			}
			entries = append(entries, e)
			bwdValid = bwdIter.Prev()
		}
	}

	if err := fwdIter.Error(); err != nil {
		return nil, fmt.Errorf("rdb: forward iter error: %w", err)
	}
	if err := bwdIter.Error(); err != nil {
		return nil, fmt.Errorf("rdb: backward iter error: %w", err)
	}

	return entries, nil
}

// readEntryArena decodes an RDB entry, writing ref distances into the
// pre-allocated arena at the given entry index.
func (s *Store) readEntryArena(iter *pebble.Iterator, arena []float32, entryIdx int) (Entry, error) {
	key := iter.Key()
	val, err := iter.ValueAndErr()
	if err != nil {
		return Entry{}, fmt.Errorf("rdb: read value: %w", err)
	}
	want := s.refCount * 4
	if len(val) < want {
		return Entry{}, fmt.Errorf("rdb: corrupt entry: got %d bytes, want %d", len(val), want)
	}
	docID := extractDocID(key)
	offset := entryIdx * s.refCount
	dst := arena[offset : offset+s.refCount]
	decodeRefDistsInto(val, dst)
	return Entry{DocID: docID, RefDists: dst}, nil
}

// rdbKey builds the Pebble key for an RDB entry.
// Format: "r/" + partitionID(1 byte) + "/" + hilbertKey + "/" + docID(8 bytes BE)
func rdbKey(partitionID int, hilbertKey []byte, docID uint64) []byte {
	// "r/" = 2 bytes, partitionID = 1 byte, "/" = 1 byte, hilbertKey, "/" = 1 byte, docID = 8 bytes
	key := make([]byte, 0, 2+1+1+len(hilbertKey)+1+8)
	key = append(key, 'r', '/')
	key = append(key, byte(partitionID))
	key = append(key, '/')
	key = append(key, hilbertKey...)
	key = append(key, '/')
	key = append(key, encodeUint64BE(docID)...)
	return key
}

// rdbPrefix returns the key prefix for a partition: "r/{partitionID}/"
func rdbPrefix(partitionID int) []byte {
	return []byte{'r', '/', byte(partitionID), '/'}
}

// rdbSeekKey returns the seek key for a query: "r/{partitionID}/{hilbertKey}"
// without the docID suffix, so SeekGE finds the first entry >= this Hilbert key.
func rdbSeekKey(partitionID int, hilbertKey []byte) []byte {
	key := make([]byte, 0, 2+1+1+len(hilbertKey))
	key = append(key, 'r', '/')
	key = append(key, byte(partitionID))
	key = append(key, '/')
	key = append(key, hilbertKey...)
	return key
}

// prefixUpperBound returns a prefix with its last byte incremented, used as an
// exclusive upper bound to constrain iteration within a partition.
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	upper[len(upper)-1]++
	return upper
}

// encodeRefDists encodes reference distances as little-endian float32 bytes.
func encodeRefDists(dists []float32) []byte {
	out := make([]byte, len(dists)*4)
	for i, d := range dists {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(d))
	}
	return out
}

// decodeRefDistsInto decodes reference distances from little-endian float32
// bytes into the pre-allocated dst buffer. len(dst) determines how many
// distances are decoded.
func decodeRefDistsInto(data []byte, dst []float32) {
	for i := range dst {
		dst[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))
	}
}

// extractDocID extracts the doc_id from the last 8 bytes of an RDB key.
func extractDocID(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(key)-8:])
}

// encodeUint64BE encodes a uint64 as 8-byte big-endian.
func encodeUint64BE(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
