package vecstore

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

const (
	prefixVector  = "v/"
	prefixE2D     = "e2d/"
	prefixD2E     = "d2e/"
	prefixD2R     = "d2r/"
	metaNextDocID = "meta/next_docid"
	metaVecCount  = "meta/vector_count"
	metaWatermark = "meta/watermark"
)

// Store manages vector storage and ID mappings in Pebble.
type Store struct {
	db         *pebble.DB
	dim        int
	initMu     sync.Mutex // guards one-time initialization of docCounter
	initDone   bool
	docCounter atomic.Uint64 // in-memory counter; DB write is for restart durability only
}

// Open opens or creates a vecstore backed by the given Pebble DB.
func Open(db *pebble.DB, dim int) *Store {
	return &Store{db: db, dim: dim}
}

// initDocCounter loads the persisted counter from DB on first call. All
// subsequent calls are no-ops. Safe for concurrent callers.
func (s *Store) initDocCounter() error {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	if s.initDone {
		return nil
	}
	val, closer, err := s.db.Get([]byte(metaNextDocID))
	if err == pebble.ErrNotFound {
		s.docCounter.Store(0)
	} else if err != nil {
		return err
	} else {
		if len(val) < 8 {
			closer.Close()
			return fmt.Errorf("vecstore: corrupt next_docid")
		}
		s.docCounter.Store(decodeUint64(val))
		closer.Close()
	}
	s.initDone = true
	return nil
}

// PutVector stores a vector keyed by doc_id. Uses the provided batch if non-nil.
func (s *Store) PutVector(batch *pebble.Batch, docID uint64, vec []float32) error {
	key := vectorKey(docID)
	val := encodeVector(vec)
	if batch != nil {
		return batch.Set(key, val, pebble.NoSync)
	}
	return s.db.Set(key, val, pebble.Sync)
}

// GetVector retrieves the vector for the given doc_id.
func (s *Store) GetVector(docID uint64) ([]float32, error) {
	val, closer, err := s.db.Get(vectorKey(docID))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return decodeVector(val, s.dim), nil
}

// GetVectors retrieves multiple vectors by doc_id. Returns a map of docID -> vector.
// Missing doc_ids are silently skipped.
func (s *Store) GetVectors(docIDs []uint64) (map[uint64][]float32, error) {
	result := make(map[uint64][]float32, len(docIDs))
	for _, id := range docIDs {
		vec, err := s.GetVector(id)
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		result[id] = vec
	}
	return result, nil
}

// PutIDMapping stores both e2d and d2e mappings. Uses the provided batch if non-nil.
func (s *Store) PutIDMapping(batch *pebble.Batch, externalID []byte, docID uint64) error {
	docIDBytes := encodeUint64(docID)
	if batch != nil {
		if err := batch.Set(e2dKey(externalID), docIDBytes, pebble.NoSync); err != nil {
			return err
		}
		return batch.Set(d2eKey(docID), externalID, pebble.NoSync)
	}
	if err := s.db.Set(e2dKey(externalID), docIDBytes, pebble.Sync); err != nil {
		return err
	}
	return s.db.Set(d2eKey(docID), externalID, pebble.Sync)
}

// GetDocID looks up the internal doc_id for an external ID.
func (s *Store) GetDocID(externalID []byte) (uint64, error) {
	val, closer, err := s.db.Get(e2dKey(externalID))
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(val) < 8 {
		return 0, fmt.Errorf("vecstore: corrupt e2d value for external id")
	}
	return decodeUint64(val), nil
}

// GetExternalID looks up the external ID for an internal doc_id.
func (s *Store) GetExternalID(docID uint64) ([]byte, error) {
	val, closer, err := s.db.Get(d2eKey(docID))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// DeleteIDMapping removes both e2d and d2e mappings. Uses the provided batch if non-nil.
func (s *Store) DeleteIDMapping(batch *pebble.Batch, externalID []byte, docID uint64) error {
	if batch != nil {
		if err := batch.Delete(e2dKey(externalID), pebble.NoSync); err != nil {
			return err
		}
		return batch.Delete(d2eKey(docID), pebble.NoSync)
	}
	if err := s.db.Delete(e2dKey(externalID), pebble.Sync); err != nil {
		return err
	}
	return s.db.Delete(d2eKey(docID), pebble.Sync)
}

// DeleteVector removes the vector for the given doc_id. Uses the provided batch if non-nil.
func (s *Store) DeleteVector(batch *pebble.Batch, docID uint64) error {
	key := vectorKey(docID)
	if batch != nil {
		return batch.Delete(key, pebble.NoSync)
	}
	return s.db.Delete(key, pebble.Sync)
}

// PutReverseHilbert stores the concatenated Hilbert keys for a doc_id.
func (s *Store) PutReverseHilbert(batch *pebble.Batch, docID uint64, hilbertKeys []byte) error {
	key := d2rKey(docID)
	if batch != nil {
		return batch.Set(key, hilbertKeys, pebble.NoSync)
	}
	return s.db.Set(key, hilbertKeys, pebble.Sync)
}

// GetReverseHilbert retrieves the concatenated Hilbert keys for a doc_id.
func (s *Store) GetReverseHilbert(docID uint64) ([]byte, error) {
	val, closer, err := s.db.Get(d2rKey(docID))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// DeleteReverseHilbert removes the reverse Hilbert mapping. Uses the provided batch if non-nil.
func (s *Store) DeleteReverseHilbert(batch *pebble.Batch, docID uint64) error {
	key := d2rKey(docID)
	if batch != nil {
		return batch.Delete(key, pebble.NoSync)
	}
	return s.db.Delete(key, pebble.Sync)
}

// NextDocID atomically allocates the next unique doc_id using an in-memory
// counter seeded from the persisted value on first call. The updated counter
// is recorded in batch (or written directly when batch is nil) so the
// highest-seen value survives process restarts. Gaps in the sequence are
// harmless — docIDs are internal identifiers only.
func (s *Store) NextDocID(batch *pebble.Batch) (uint64, error) {
	if err := s.initDocCounter(); err != nil {
		return 0, err
	}
	next := s.docCounter.Add(1)
	key := []byte(metaNextDocID)
	if batch != nil {
		if err := batch.Set(key, encodeUint64(next), pebble.NoSync); err != nil {
			return 0, err
		}
	} else {
		if err := s.db.Set(key, encodeUint64(next), pebble.Sync); err != nil {
			return 0, err
		}
	}
	return next, nil
}

// GetVectorCount returns the current vector count.
func (s *Store) GetVectorCount() (uint64, error) {
	val, closer, err := s.db.Get([]byte(metaVecCount))
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(val) < 8 {
		return 0, fmt.Errorf("vecstore: corrupt vector_count")
	}
	return decodeUint64(val), nil
}

// SetVectorCount sets the vector count. Uses the provided batch if non-nil.
func (s *Store) SetVectorCount(batch *pebble.Batch, count uint64) error {
	key := []byte(metaVecCount)
	val := encodeUint64(count)
	if batch != nil {
		return batch.Set(key, val, pebble.NoSync)
	}
	return s.db.Set(key, val, pebble.Sync)
}

// IncrementVectorCount reads the current count from committed state and writes
// the updated value into batch. Since GetVectorCount reads from DB (not batch),
// callers must ensure at most one pending SetVectorCount/IncrementVectorCount
// per batch to avoid reading stale data.
func (s *Store) IncrementVectorCount(batch *pebble.Batch, delta int64) error {
	current, err := s.GetVectorCount()
	if err != nil {
		return err
	}
	newCount := uint64(max(0, int64(current)+delta))
	return s.SetVectorCount(batch, newCount)
}

// GetWatermark returns the current (txnID, seqID) watermark.
func (s *Store) GetWatermark() (txnID, seqID uint64, err error) {
	val, closer, err := s.db.Get([]byte(metaWatermark))
	if err == pebble.ErrNotFound {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer closer.Close()
	if len(val) < 16 {
		return 0, 0, fmt.Errorf("vecstore: corrupt watermark")
	}
	txnID = decodeUint64(val[:8])
	seqID = decodeUint64(val[8:16])
	return txnID, seqID, nil
}

// SetWatermark stores the watermark. Uses the provided batch if non-nil.
func (s *Store) SetWatermark(batch *pebble.Batch, txnID, seqID uint64) error {
	key := []byte(metaWatermark)
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[:8], txnID)
	binary.BigEndian.PutUint64(val[8:], seqID)
	if batch != nil {
		return batch.Set(key, val, pebble.NoSync)
	}
	return s.db.Set(key, val, pebble.Sync)
}

// vectorKey returns the Pebble key for a vector: "v/" + 8-byte big-endian docID.
func vectorKey(docID uint64) []byte {
	key := make([]byte, len(prefixVector)+8)
	copy(key, prefixVector)
	binary.BigEndian.PutUint64(key[len(prefixVector):], docID)
	return key
}

// e2dKey returns the Pebble key for an external-to-doc_id mapping: "e2d/" + externalID.
func e2dKey(externalID []byte) []byte {
	key := make([]byte, len(prefixE2D)+len(externalID))
	copy(key, prefixE2D)
	copy(key[len(prefixE2D):], externalID)
	return key
}

// d2eKey returns the Pebble key for a doc_id-to-external mapping: "d2e/" + 8-byte big-endian docID.
func d2eKey(docID uint64) []byte {
	key := make([]byte, len(prefixD2E)+8)
	copy(key, prefixD2E)
	binary.BigEndian.PutUint64(key[len(prefixD2E):], docID)
	return key
}

// d2rKey returns the Pebble key for a reverse Hilbert mapping: "d2r/" + 8-byte big-endian docID.
func d2rKey(docID uint64) []byte {
	key := make([]byte, len(prefixD2R)+8)
	copy(key, prefixD2R)
	binary.BigEndian.PutUint64(key[len(prefixD2R):], docID)
	return key
}

// encodeUint64 encodes a uint64 as 8-byte big-endian.
func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// decodeUint64 decodes a uint64 from 8-byte big-endian.
func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// encodeVector encodes a []float32 as a concatenation of little-endian uint32 bytes.
func encodeVector(vec []float32) []byte {
	b := make([]byte, len(vec)*4)
	for i, v := range vec {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b
}

// decodeVector decodes a []float32 from a little-endian byte slice.
func decodeVector(b []byte, dim int) []float32 {
	n := min(dim, len(b)/4)
	vec := make([]float32, n)
	for i := range n {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return vec
}
