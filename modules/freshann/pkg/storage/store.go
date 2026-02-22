package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/maxpert/marmot/modules/freshann/pkg/graph"
)

// VectorRecord holds mutable state for a logical vector.
type VectorRecord struct {
	PartitionKey string            `json:"partition_key"`
	Tags         map[string]string `json:"tags,omitempty"`
	VectorFP32   []float32         `json:"vector_fp32"`
}

type IndexStore struct {
	db *pebble.DB
}

type OpenOptions struct {
	DisableWAL bool
}

type graphMeta struct {
	Metric api.Metric `json:"metric"`
	R      int        `json:"r"`
}

func Open(path string, opts OpenOptions) (*IndexStore, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	db, err := pebble.Open(path, &pebble.Options{DisableWAL: opts.DisableWAL})
	if err != nil {
		return nil, err
	}
	return &IndexStore{db: db}, nil
}

func (s *IndexStore) Close() error { return s.db.Close() }
func (s *IndexStore) Flush() error { return s.db.Flush() }

func (s *IndexStore) SaveSpec(spec api.IndexSpec) error {
	b, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return s.db.Set(keySpec, b, pebble.Sync)
}

func (s *IndexStore) LoadSpec() (api.IndexSpec, error) {
	var spec api.IndexSpec
	v, closer, err := s.db.Get(keySpec)
	if err != nil {
		return spec, err
	}
	defer closer.Close()
	if err := json.Unmarshal(v, &spec); err != nil {
		return spec, err
	}
	return spec, nil
}

func (s *IndexStore) SaveGraphState(state graph.State, wo *pebble.WriteOptions) error {
	meta := graphMeta{Metric: state.Metric, R: state.R}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	startBytes, err := json.Marshal(state.Start)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	// Replace all graph node keys atomically.
	lower, upper := prefixBounds(prefixGraphN)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			iter.Close()
			return err
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if err := batch.Set(keyGraphMeta, metaBytes, nil); err != nil {
		return err
	}
	if err := batch.Set(keyGraphHead, startBytes, nil); err != nil {
		return err
	}
	for nodeID, neighbors := range state.Adj {
		payload, err := json.Marshal(neighbors)
		if err != nil {
			return err
		}
		if err := batch.Set(encodeGraphNodeKey(nodeID), payload, nil); err != nil {
			return err
		}
	}
	return batch.Commit(wo)
}

func (s *IndexStore) LoadGraphState() (graph.State, bool, error) {
	var state graph.State
	metaBytes, closer, err := s.db.Get(keyGraphMeta)
	if errors.Is(err, pebble.ErrNotFound) {
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	var meta graphMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		closer.Close()
		return state, false, err
	}
	closer.Close()

	state.Metric = meta.Metric
	state.R = meta.R
	state.Adj = make(map[string][]string)

	headBytes, closer, err := s.db.Get(keyGraphHead)
	if err == nil {
		if err := json.Unmarshal(headBytes, &state.Start); err != nil {
			closer.Close()
			return graph.State{}, false, err
		}
		closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return graph.State{}, false, err
	}

	lower, upper := prefixBounds(prefixGraphN)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return graph.State{}, false, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var neighbors []string
		if err := json.Unmarshal(iter.Value(), &neighbors); err != nil {
			return graph.State{}, false, err
		}
		state.Adj[decodeGraphNodeID(iter.Key())] = neighbors
	}
	if err := iter.Error(); err != nil {
		return graph.State{}, false, err
	}
	return state, true, nil
}

func (s *IndexStore) IsApplied(token api.ApplyToken) (bool, error) {
	k := encodeAppliedKey(token.TxnID, token.SeqID)
	_, closer, err := s.db.Get(k)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

func (s *IndexStore) MarkApplied(token api.ApplyToken, wo *pebble.WriteOptions) error {
	k := encodeAppliedKey(token.TxnID, token.SeqID)
	v := make([]byte, 16)
	binary.BigEndian.PutUint64(v[:8], token.TxnID)
	binary.BigEndian.PutUint64(v[8:], token.SeqID)
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(k, v, nil); err != nil {
		return err
	}
	wm, err := s.Watermark()
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if tokenGreater(token, wm) {
		if err := b.Set(keyWatermark, v, nil); err != nil {
			return err
		}
	}
	return b.Commit(wo)
}

func (s *IndexStore) Watermark() (api.ApplyToken, error) {
	var tok api.ApplyToken
	v, closer, err := s.db.Get(keyWatermark)
	if err != nil {
		return tok, err
	}
	defer closer.Close()
	if len(v) != 16 {
		return tok, fmt.Errorf("invalid watermark payload")
	}
	tok.TxnID = binary.BigEndian.Uint64(v[:8])
	tok.SeqID = binary.BigEndian.Uint64(v[8:])
	return tok, nil
}

func (s *IndexStore) PutVector(externalID []byte, rec VectorRecord, wo *pebble.WriteOptions) error {
	old, exists, err := s.GetVector(externalID)
	if err != nil {
		return err
	}

	encoded, err := encodeVectorRecord(rec)
	if err != nil {
		return err
	}
	hash := hashExternalID(externalID)
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(encodeVectorKey(externalID), encoded, nil); err != nil {
		return err
	}
	if exists {
		if err := s.bitmapRemove(batch, encodePartitionKey(old.PartitionKey), hash); err != nil {
			return err
		}
		for k, v := range old.Tags {
			if err := s.bitmapRemove(batch, encodeTagKey(k, v), hash); err != nil {
				return err
			}
		}
	}
	if err := s.bitmapAdd(batch, encodePartitionKey(rec.PartitionKey), hash); err != nil {
		return err
	}
	for k, v := range rec.Tags {
		if err := s.bitmapAdd(batch, encodeTagKey(k, v), hash); err != nil {
			return err
		}
	}
	if err := s.idMapAdd(batch, hash, externalID); err != nil {
		return err
	}
	return batch.Commit(wo)
}

func (s *IndexStore) DeleteVector(externalID []byte, wo *pebble.WriteOptions) error {
	old, exists, err := s.GetVector(externalID)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	hash := hashExternalID(externalID)
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Delete(encodeVectorKey(externalID), nil); err != nil {
		return err
	}
	if err := s.bitmapRemove(batch, encodePartitionKey(old.PartitionKey), hash); err != nil {
		return err
	}
	for k, v := range old.Tags {
		if err := s.bitmapRemove(batch, encodeTagKey(k, v), hash); err != nil {
			return err
		}
	}
	if err := s.idMapRemove(batch, hash, externalID); err != nil {
		return err
	}
	return batch.Commit(wo)
}

func (s *IndexStore) GetVector(externalID []byte) (VectorRecord, bool, error) {
	key := encodeVectorKey(externalID)
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return VectorRecord{}, false, nil
	}
	if err != nil {
		return VectorRecord{}, false, err
	}
	defer closer.Close()
	rec, err := decodeVectorRecord(v)
	if err != nil {
		return VectorRecord{}, false, err
	}
	return rec, true, nil
}

func (s *IndexStore) CandidateExternalIDs(partition string, tags map[string]string) ([][]byte, error) {
	if partition == "" && len(tags) == 0 {
		return nil, nil
	}
	var bm *roaring64.Bitmap
	if partition != "" {
		var err error
		bm, err = s.getBitmap(encodePartitionKey(partition))
		if err != nil {
			return nil, err
		}
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		tbm, err := s.getBitmap(encodeTagKey(k, tags[k]))
		if err != nil {
			return nil, err
		}
		if bm == nil {
			bm = tbm
		} else {
			bm.And(tbm)
		}
	}
	if bm == nil {
		return nil, nil
	}
	out := make([][]byte, 0, bm.GetCardinality())
	it := bm.Iterator()
	for it.HasNext() {
		h := it.Next()
		ids, err := s.idMapGet(h)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			cp := make([]byte, len(id))
			copy(cp, id)
			out = append(out, cp)
		}
	}
	return out, nil
}

func (s *IndexStore) IterateVectors(fn func(externalID []byte, rec VectorRecord) error) error {
	lower, upper := prefixBounds(prefixVector)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		rec, err := decodeVectorRecord(iter.Value())
		if err != nil {
			return err
		}
		if err := fn(decodeVectorExternalID(iter.Key()), rec); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *IndexStore) SnapshotVectorsMap() (map[string]VectorRecord, error) {
	out := map[string]VectorRecord{}
	err := s.IterateVectors(func(externalID []byte, rec VectorRecord) error {
		out[string(externalID)] = rec
		return nil
	})
	return out, err
}

func (s *IndexStore) CountVectors() (uint64, error) {
	var count uint64
	err := s.IterateVectors(func(_ []byte, _ VectorRecord) error {
		count++
		return nil
	})
	return count, err
}

func (s *IndexStore) CountApplied() (uint64, error) {
	lower, upper := prefixBounds(prefixApplied)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var count uint64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count, iter.Error()
}

func tokenGreater(a, b api.ApplyToken) bool {
	if a.TxnID != b.TxnID {
		return a.TxnID > b.TxnID
	}
	return a.SeqID > b.SeqID
}

func encodeVectorRecord(rec VectorRecord) ([]byte, error) {
	tags := rec.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return nil, err
	}
	part := []byte(rec.PartitionKey)
	vecBytes := len(rec.VectorFP32) * 4
	total := 2 + len(part) + 4 + len(tagsJSON) + 4 + vecBytes
	out := make([]byte, total)
	off := 0
	binary.BigEndian.PutUint16(out[off:off+2], uint16(len(part)))
	off += 2
	copy(out[off:off+len(part)], part)
	off += len(part)
	binary.BigEndian.PutUint32(out[off:off+4], uint32(len(tagsJSON)))
	off += 4
	copy(out[off:off+len(tagsJSON)], tagsJSON)
	off += len(tagsJSON)
	binary.BigEndian.PutUint32(out[off:off+4], uint32(len(rec.VectorFP32)))
	off += 4
	for _, f := range rec.VectorFP32 {
		binary.LittleEndian.PutUint32(out[off:off+4], math.Float32bits(f))
		off += 4
	}
	return out, nil
}

func decodeVectorRecord(data []byte) (VectorRecord, error) {
	var rec VectorRecord
	off := 0
	if len(data) < 2 {
		return rec, fmt.Errorf("invalid vector record: short partition len")
	}
	partLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	if len(data) < off+partLen+4 {
		return rec, fmt.Errorf("invalid vector record: short partition body")
	}
	rec.PartitionKey = string(data[off : off+partLen])
	off += partLen
	tagsLen := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	if len(data) < off+tagsLen+4 {
		return rec, fmt.Errorf("invalid vector record: short tags body")
	}
	if err := json.Unmarshal(data[off:off+tagsLen], &rec.Tags); err != nil {
		return rec, err
	}
	off += tagsLen
	dim := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	if len(data) < off+dim*4 {
		return rec, fmt.Errorf("invalid vector record: vector payload truncated")
	}
	rec.VectorFP32 = make([]float32, dim)
	for i := 0; i < dim; i++ {
		rec.VectorFP32[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
	}
	return rec, nil
}

func hashExternalID(externalID []byte) uint64 { return xxhash.Sum64(externalID) }

func (s *IndexStore) getBitmap(key []byte) (*roaring64.Bitmap, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return roaring64.NewBitmap(), nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	bm := roaring64.NewBitmap()
	if len(v) == 0 {
		return bm, nil
	}
	if _, err := bm.ReadFrom(bytes.NewReader(v)); err != nil {
		return nil, err
	}
	return bm, nil
}

func (s *IndexStore) bitmapAdd(batch *pebble.Batch, key []byte, hash uint64) error {
	bm, err := s.getBitmap(key)
	if err != nil {
		return err
	}
	bm.Add(hash)
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return err
	}
	return batch.Set(key, buf.Bytes(), nil)
}

func (s *IndexStore) bitmapRemove(batch *pebble.Batch, key []byte, hash uint64) error {
	bm, err := s.getBitmap(key)
	if err != nil {
		return err
	}
	bm.Remove(hash)
	if bm.IsEmpty() {
		return batch.Delete(key, nil)
	}
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return err
	}
	return batch.Set(key, buf.Bytes(), nil)
}

func (s *IndexStore) idMapGet(hash uint64) ([][]byte, error) {
	key := encodeIDMapKey(hash)
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return decodeIDMapList(v)
}

func (s *IndexStore) idMapAdd(batch *pebble.Batch, hash uint64, externalID []byte) error {
	current, err := s.idMapGet(hash)
	if err != nil {
		return err
	}
	for _, existing := range current {
		if bytes.Equal(existing, externalID) {
			encoded, err := encodeIDMapList(current)
			if err != nil {
				return err
			}
			return batch.Set(encodeIDMapKey(hash), encoded, nil)
		}
	}
	cp := make([]byte, len(externalID))
	copy(cp, externalID)
	current = append(current, cp)
	encoded, err := encodeIDMapList(current)
	if err != nil {
		return err
	}
	return batch.Set(encodeIDMapKey(hash), encoded, nil)
}

func (s *IndexStore) idMapRemove(batch *pebble.Batch, hash uint64, externalID []byte) error {
	current, err := s.idMapGet(hash)
	if err != nil {
		return err
	}
	if len(current) == 0 {
		return nil
	}
	filtered := current[:0]
	for _, existing := range current {
		if !bytes.Equal(existing, externalID) {
			filtered = append(filtered, existing)
		}
	}
	if len(filtered) == 0 {
		return batch.Delete(encodeIDMapKey(hash), nil)
	}
	encoded, err := encodeIDMapList(filtered)
	if err != nil {
		return err
	}
	return batch.Set(encodeIDMapKey(hash), encoded, nil)
}

func encodeIDMapList(items [][]byte) ([]byte, error) {
	vals := make([]string, len(items))
	for i := range items {
		vals[i] = base64.StdEncoding.EncodeToString(items[i])
	}
	return json.Marshal(vals)
}

func decodeIDMapList(payload []byte) ([][]byte, error) {
	var vals []string
	if err := json.Unmarshal(payload, &vals); err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(vals))
	for _, s := range vals {
		v, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func PebblePath(indexDir string) string {
	return filepath.Join(indexDir, "meta.pebble")
}
