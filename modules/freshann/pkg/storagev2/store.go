package storagev2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/maxpert/marmot/modules/freshann/pkg/graphv2"
	"github.com/vmihailenco/msgpack/v5"
)

// VectorRecord is the logical row payload for a docID.
type VectorRecord struct {
	PartitionKey string
	Tags         map[string]string
	VectorFP32   []float32
}

// VectorMeta holds non-vector metadata split out of vec payloads in V2.
type VectorMeta struct {
	PartitionKey string            `msgpack:"partition_key,omitempty"`
	Tags         map[string]string `msgpack:"tags,omitempty"`
}

type IndexStore struct {
	db               *pebble.DB
	postingChunkSize uint64
	graphPageSize    uint64
}

type OpenOptions struct {
	DisableWAL       bool
	PebbleCacheBytes int64
	BloomBitsPerKey  int
	BytesPerSync     int
	PostingChunkSize int
	GraphPageSize    int
}

type VectorLookup struct {
	iter *pebble.Iterator
	db   *pebble.DB
}

type graphHead struct {
	Generation uint64     `msgpack:"generation"`
	PageSize   uint64     `msgpack:"page_size"`
	Metric     api.Metric `msgpack:"metric"`
	R          int        `msgpack:"r"`
	Start      []uint64   `msgpack:"start"`
	UpdatedAt  int64      `msgpack:"updated_at"`
}

type graphPageEntry struct {
	DocID     uint64
	Neighbors []uint64
}

func Open(path string, opts OpenOptions) (*IndexStore, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	if opts.PebbleCacheBytes <= 0 {
		opts.PebbleCacheBytes = 256 << 20
	}
	if opts.BloomBitsPerKey <= 0 {
		opts.BloomBitsPerKey = 10
	}
	if opts.BytesPerSync <= 0 {
		opts.BytesPerSync = 512 << 10
	}
	if opts.PostingChunkSize <= 0 {
		opts.PostingChunkSize = 65536
	}
	if opts.GraphPageSize <= 0 {
		opts.GraphPageSize = 64
	}

	cache := pebble.NewCache(opts.PebbleCacheBytes)
	defer cache.Unref()
	popts := &pebble.Options{
		DisableWAL:   opts.DisableWAL,
		BytesPerSync: opts.BytesPerSync,
		Cache:        cache,
	}
	for i := range popts.Levels {
		popts.Levels[i].FilterPolicy = bloom.FilterPolicy(opts.BloomBitsPerKey)
		popts.Levels[i].FilterType = pebble.TableFilter
	}
	db, err := pebble.Open(path, popts)
	if err != nil {
		return nil, err
	}
	return &IndexStore{db: db, postingChunkSize: uint64(opts.PostingChunkSize), graphPageSize: uint64(opts.GraphPageSize)}, nil
}

func (s *IndexStore) Close() error { return s.db.Close() }
func (s *IndexStore) Flush() error { return s.db.Flush() }

func (s *IndexStore) SaveSpec(spec api.IndexSpec) error {
	b, err := msgpack.Marshal(spec)
	if err != nil {
		return err
	}
	return s.db.Set(keySpecV2, b, pebble.Sync)
}

func (s *IndexStore) LoadSpec() (api.IndexSpec, error) {
	var spec api.IndexSpec
	v, closer, err := s.db.Get(keySpecV2)
	if err != nil {
		return spec, err
	}
	defer closer.Close()
	if err := msgpack.Unmarshal(v, &spec); err != nil {
		return spec, err
	}
	return spec, nil
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

func tokenGreater(a, b api.ApplyToken) bool {
	if a.TxnID != b.TxnID {
		return a.TxnID > b.TxnID
	}
	return a.SeqID > b.SeqID
}

func (s *IndexStore) nextDocID(batch *pebble.Batch) (uint64, error) {
	v, closer, err := s.db.Get(keyNextDocID)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return 0, err
	}
	var next uint64 = 1
	if err == nil {
		if len(v) != 8 {
			closer.Close()
			return 0, fmt.Errorf("invalid next doc id payload")
		}
		next = binary.BigEndian.Uint64(v)
		closer.Close()
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, next+1)
	if err := batch.Set(keyNextDocID, payload, nil); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *IndexStore) counter(key []byte) (uint64, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, fmt.Errorf("invalid counter payload for %s", string(key))
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *IndexStore) setCounter(batch *pebble.Batch, key []byte, value uint64) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, value)
	return batch.Set(key, v, nil)
}

func (s *IndexStore) deltaCounter(batch *pebble.Batch, key []byte, delta int64) error {
	cur, err := s.counter(key)
	if err != nil {
		return err
	}
	var next uint64
	if delta < 0 {
		d := uint64(-delta)
		if d > cur {
			next = 0
		} else {
			next = cur - d
		}
	} else {
		next = cur + uint64(delta)
	}
	return s.setCounter(batch, key, next)
}

func encodeToken(token api.ApplyToken) []byte {
	v := make([]byte, 16)
	binary.BigEndian.PutUint64(v[:8], token.TxnID)
	binary.BigEndian.PutUint64(v[8:], token.SeqID)
	return v
}

func (s *IndexStore) markAppliedInBatch(batch *pebble.Batch, token api.ApplyToken) error {
	if err := batch.Set(encodeAppliedKey(token.TxnID, token.SeqID), encodeToken(token), nil); err != nil {
		return err
	}
	if err := s.deltaCounter(batch, keyAppliedCnt, 1); err != nil {
		return err
	}
	wm, err := s.Watermark()
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if errors.Is(err, pebble.ErrNotFound) || tokenGreater(token, wm) {
		if err := batch.Set(keyWatermark, encodeToken(token), nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *IndexStore) DocIDForExternalID(externalID []byte) (uint64, bool, error) {
	v, closer, err := s.db.Get(encodeExtToDocKey(externalID))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, false, fmt.Errorf("invalid doc id payload")
	}
	return binary.BigEndian.Uint64(v), true, nil
}

func (s *IndexStore) ExternalIDForDocID(docID uint64) ([]byte, bool, error) {
	v, closer, err := s.db.Get(encodeDocToExtKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

func encodeVectorPayload(vec []float32) []byte {
	out := make([]byte, 4+len(vec)*4)
	binary.BigEndian.PutUint32(out[:4], uint32(len(vec)))
	off := 4
	for _, f := range vec {
		binary.LittleEndian.PutUint32(out[off:off+4], math.Float32bits(f))
		off += 4
	}
	return out
}

func decodeVectorPayload(data []byte) ([]float32, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid vector payload: short length")
	}
	dim := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) < 4+dim*4 {
		return nil, fmt.Errorf("invalid vector payload: truncated")
	}
	vec := make([]float32, dim)
	off := 4
	for i := 0; i < dim; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
	}
	return vec, nil
}

func encodeMetaPayload(meta VectorMeta) ([]byte, error) {
	return msgpack.Marshal(meta)
}

func decodeMetaPayload(data []byte) (VectorMeta, error) {
	var meta VectorMeta
	if len(data) == 0 {
		return meta, nil
	}
	if err := msgpack.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	return meta, nil
}

func (s *IndexStore) chunkID(docID uint64) uint64 {
	if s.postingChunkSize == 0 {
		return 0
	}
	return docID / s.postingChunkSize
}

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

func (s *IndexStore) bitmapAddDoc(batch *pebble.Batch, key []byte, docID uint64) error {
	bm, err := s.getBitmap(key)
	if err != nil {
		return err
	}
	bm.Add(docID)
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return err
	}
	return batch.Set(key, buf.Bytes(), nil)
}

func (s *IndexStore) bitmapRemoveDoc(batch *pebble.Batch, key []byte, docID uint64) error {
	bm, err := s.getBitmap(key)
	if err != nil {
		return err
	}
	bm.Remove(docID)
	if bm.IsEmpty() {
		return batch.Delete(key, nil)
	}
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return err
	}
	return batch.Set(key, buf.Bytes(), nil)
}

func (s *IndexStore) postingBitmap(prefix string, value string) (*roaring64.Bitmap, error) {
	base := encodePostingBase(prefix, value)
	lower, upper := prefixBounds(base)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := roaring64.NewBitmap()
	for iter.First(); iter.Valid(); iter.Next() {
		bm := roaring64.NewBitmap()
		if _, err := bm.ReadFrom(bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}
		out.Or(bm)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func sortedTagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *IndexStore) updatePostings(batch *pebble.Batch, docID uint64, oldMeta VectorMeta, newMeta VectorMeta) error {
	chunkID := s.chunkID(docID)
	if oldMeta.PartitionKey != "" {
		if err := s.bitmapRemoveDoc(batch, encodePostingChunk(prefixPostPart, oldMeta.PartitionKey, chunkID), docID); err != nil {
			return err
		}
	}
	for _, k := range sortedTagKeys(oldMeta.Tags) {
		kv := k + "=" + oldMeta.Tags[k]
		if err := s.bitmapRemoveDoc(batch, encodePostingChunk(prefixPostTag, kv, chunkID), docID); err != nil {
			return err
		}
	}

	if newMeta.PartitionKey != "" {
		if err := s.bitmapAddDoc(batch, encodePostingChunk(prefixPostPart, newMeta.PartitionKey, chunkID), docID); err != nil {
			return err
		}
	}
	for _, k := range sortedTagKeys(newMeta.Tags) {
		kv := k + "=" + newMeta.Tags[k]
		if err := s.bitmapAddDoc(batch, encodePostingChunk(prefixPostTag, kv, chunkID), docID); err != nil {
			return err
		}
	}
	return nil
}

func toMeta(rec VectorRecord) VectorMeta {
	return VectorMeta{PartitionKey: rec.PartitionKey, Tags: cloneTags(rec.Tags)}
}

func cloneTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// ApplyUpsert performs idempotent upsert + token apply in one atomic batch.
func (s *IndexStore) ApplyUpsert(token api.ApplyToken, externalID []byte, rec VectorRecord, wo *pebble.WriteOptions) (docID uint64, applied bool, err error) {
	appliedAlready, err := s.IsApplied(token)
	if err != nil {
		return 0, false, err
	}
	if appliedAlready {
		docID, _, err := s.DocIDForExternalID(externalID)
		return docID, false, err
	}

	docID, exists, err := s.DocIDForExternalID(externalID)
	if err != nil {
		return 0, false, err
	}

	var oldMeta VectorMeta
	if exists {
		oldMeta, _, err = s.GetMetaByDocID(docID)
		if err != nil {
			return 0, false, err
		}
	}

	vecPayload := encodeVectorPayload(rec.VectorFP32)
	metaPayload, err := encodeMetaPayload(toMeta(rec))
	if err != nil {
		return 0, false, err
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	if !exists {
		docID, err = s.nextDocID(batch)
		if err != nil {
			return 0, false, err
		}
		if err := s.deltaCounter(batch, keyVectorCount, 1); err != nil {
			return 0, false, err
		}
	}

	docIDPayload := make([]byte, 8)
	binary.BigEndian.PutUint64(docIDPayload, docID)
	if err := batch.Set(encodeExtToDocKey(externalID), docIDPayload, nil); err != nil {
		return 0, false, err
	}
	if err := batch.Set(encodeDocToExtKey(docID), append([]byte(nil), externalID...), nil); err != nil {
		return 0, false, err
	}
	if err := batch.Set(encodeVectorDocKey(docID), vecPayload, nil); err != nil {
		return 0, false, err
	}
	if err := batch.Set(encodeMetaDocKey(docID), metaPayload, nil); err != nil {
		return 0, false, err
	}
	if err := s.updatePostings(batch, docID, oldMeta, toMeta(rec)); err != nil {
		return 0, false, err
	}
	if err := s.markAppliedInBatch(batch, token); err != nil {
		return 0, false, err
	}
	if err := batch.Commit(wo); err != nil {
		return 0, false, err
	}
	return docID, true, nil
}

// ApplyDelete performs idempotent delete + token apply in one atomic batch.
func (s *IndexStore) ApplyDelete(token api.ApplyToken, externalID []byte, wo *pebble.WriteOptions) (docID uint64, existed bool, applied bool, err error) {
	appliedAlready, err := s.IsApplied(token)
	if err != nil {
		return 0, false, false, err
	}
	if appliedAlready {
		docID, existed, err = s.DocIDForExternalID(externalID)
		return docID, existed, false, err
	}

	docID, exists, err := s.DocIDForExternalID(externalID)
	if err != nil {
		return 0, false, false, err
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	if exists {
		meta, ok, err := s.GetMetaByDocID(docID)
		if err != nil {
			return 0, false, false, err
		}
		if ok {
			if err := s.updatePostings(batch, docID, meta, VectorMeta{}); err != nil {
				return 0, false, false, err
			}
		}
		if err := batch.Delete(encodeVectorDocKey(docID), nil); err != nil {
			return 0, false, false, err
		}
		if err := batch.Delete(encodeMetaDocKey(docID), nil); err != nil {
			return 0, false, false, err
		}
		if err := batch.Delete(encodeExtToDocKey(externalID), nil); err != nil {
			return 0, false, false, err
		}
		if err := batch.Delete(encodeDocToExtKey(docID), nil); err != nil {
			return 0, false, false, err
		}
		if err := s.deltaCounter(batch, keyVectorCount, -1); err != nil {
			return 0, false, false, err
		}
	}
	if err := s.markAppliedInBatch(batch, token); err != nil {
		return 0, false, false, err
	}
	if err := batch.Commit(wo); err != nil {
		return 0, false, false, err
	}
	return docID, exists, true, nil
}

func (s *IndexStore) GetVectorFP32ByDocID(docID uint64) ([]float32, bool, error) {
	v, closer, err := s.db.Get(encodeVectorDocKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	vec, err := decodeVectorPayload(v)
	if err != nil {
		return nil, false, err
	}
	return vec, true, nil
}

func (s *IndexStore) GetMetaByDocID(docID uint64) (VectorMeta, bool, error) {
	v, closer, err := s.db.Get(encodeMetaDocKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return VectorMeta{}, false, nil
	}
	if err != nil {
		return VectorMeta{}, false, err
	}
	defer closer.Close()
	meta, err := decodeMetaPayload(v)
	if err != nil {
		return VectorMeta{}, false, err
	}
	return meta, true, nil
}

func (s *IndexStore) GetVectorByDocID(docID uint64) (VectorRecord, bool, error) {
	vec, ok, err := s.GetVectorFP32ByDocID(docID)
	if err != nil || !ok {
		return VectorRecord{}, ok, err
	}
	meta, mok, err := s.GetMetaByDocID(docID)
	if err != nil {
		return VectorRecord{}, false, err
	}
	if !mok {
		meta = VectorMeta{}
	}
	return VectorRecord{PartitionKey: meta.PartitionKey, Tags: cloneTags(meta.Tags), VectorFP32: vec}, true, nil
}

func (s *IndexStore) NewVectorLookup() (*VectorLookup, error) {
	lower, upper := prefixBounds([]byte(prefixVectorV2))
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	return &VectorLookup{iter: iter, db: s.db}, nil
}

func (l *VectorLookup) Close() error {
	if l == nil || l.iter == nil {
		return nil
	}
	return l.iter.Close()
}

func (l *VectorLookup) GetVectorFP32ByDocID(docID uint64) ([]float32, bool, error) {
	if l == nil || l.iter == nil {
		return nil, false, fmt.Errorf("vector lookup is closed")
	}
	key := encodeVectorDocKey(docID)
	if !l.iter.SeekGE(key) {
		if err := l.iter.Error(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if !bytes.Equal(l.iter.Key(), key) {
		return nil, false, nil
	}
	vec, err := decodeVectorPayload(l.iter.Value())
	if err != nil {
		return nil, false, err
	}
	return vec, true, nil
}

func (l *VectorLookup) GetMetaByDocID(docID uint64) (VectorMeta, bool, error) {
	if l == nil || l.db == nil {
		return VectorMeta{}, false, fmt.Errorf("vector lookup is closed")
	}
	v, closer, err := l.db.Get(encodeMetaDocKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return VectorMeta{}, false, nil
	}
	if err != nil {
		return VectorMeta{}, false, err
	}
	defer closer.Close()
	meta, err := decodeMetaPayload(v)
	if err != nil {
		return VectorMeta{}, false, err
	}
	return meta, true, nil
}

func (l *VectorLookup) GetVectorByDocID(docID uint64) (VectorRecord, bool, error) {
	vec, ok, err := l.GetVectorFP32ByDocID(docID)
	if err != nil || !ok {
		return VectorRecord{}, ok, err
	}
	meta, mok, err := l.GetMetaByDocID(docID)
	if err != nil {
		return VectorRecord{}, false, err
	}
	if !mok {
		meta = VectorMeta{}
	}
	return VectorRecord{PartitionKey: meta.PartitionKey, Tags: cloneTags(meta.Tags), VectorFP32: vec}, true, nil
}

func (s *IndexStore) IterateVectorsByDoc(fn func(docID uint64, externalID []byte, rec VectorRecord) error) error {
	lower, upper := prefixBounds([]byte(prefixVectorV2))
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		docID := decodeDocIDFromKey(prefixVectorV2, iter.Key())
		vec, err := decodeVectorPayload(iter.Value())
		if err != nil {
			return err
		}
		externalID, ok, err := s.ExternalIDForDocID(docID)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		meta, _, err := s.GetMetaByDocID(docID)
		if err != nil {
			return err
		}
		if err := fn(docID, externalID, VectorRecord{PartitionKey: meta.PartitionKey, Tags: cloneTags(meta.Tags), VectorFP32: vec}); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *IndexStore) CandidateDocIDs(partition string, tags map[string]string) ([]uint64, error) {
	if partition == "" && len(tags) == 0 {
		return nil, nil
	}

	var bm *roaring64.Bitmap
	if partition != "" {
		pbm, err := s.postingBitmap(prefixPostPart, partition)
		if err != nil {
			return nil, err
		}
		bm = pbm
	}
	for _, k := range sortedTagKeys(tags) {
		tbm, err := s.postingBitmap(prefixPostTag, k+"="+tags[k])
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
	out := make([]uint64, 0, bm.GetCardinality())
	it := bm.Iterator()
	for it.HasNext() {
		out = append(out, it.Next())
	}
	return out, nil
}

func (s *IndexStore) CountVectors() (uint64, error) { return s.counter(keyVectorCount) }
func (s *IndexStore) CountApplied() (uint64, error) { return s.counter(keyAppliedCnt) }

func encodeGraphHead(head graphHead) ([]byte, error) { return msgpack.Marshal(head) }
func decodeGraphHead(payload []byte) (graphHead, error) {
	var h graphHead
	err := msgpack.Unmarshal(payload, &h)
	return h, err
}

func encodeGraphPage(entries []graphPageEntry) []byte {
	sort.Slice(entries, func(i, j int) bool { return entries[i].DocID < entries[j].DocID })
	size := 4
	for _, e := range entries {
		size += 8 + 4 + len(e.Neighbors)*8
	}
	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(entries)))
	off += 4
	for _, e := range entries {
		binary.BigEndian.PutUint64(buf[off:off+8], e.DocID)
		off += 8
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(e.Neighbors)))
		off += 4
		for _, n := range e.Neighbors {
			binary.BigEndian.PutUint64(buf[off:off+8], n)
			off += 8
		}
	}
	return buf
}

func decodeGraphPage(payload []byte) ([]graphPageEntry, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("invalid graph page payload")
	}
	off := 0
	count := int(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	out := make([]graphPageEntry, 0, count)
	for i := 0; i < count; i++ {
		if len(payload) < off+12 {
			return nil, fmt.Errorf("invalid graph page entry")
		}
		docID := binary.BigEndian.Uint64(payload[off : off+8])
		off += 8
		nbCount := int(binary.BigEndian.Uint32(payload[off : off+4]))
		off += 4
		if len(payload) < off+nbCount*8 {
			return nil, fmt.Errorf("invalid graph neighbors payload")
		}
		neighbors := make([]uint64, nbCount)
		for n := 0; n < nbCount; n++ {
			neighbors[n] = binary.BigEndian.Uint64(payload[off : off+8])
			off += 8
		}
		out = append(out, graphPageEntry{DocID: docID, Neighbors: neighbors})
	}
	return out, nil
}

func (s *IndexStore) SaveGraphState(state graphv2.State, wo *pebble.WriteOptions) error {
	generation := uint64(time.Now().UnixNano())
	if generation == 0 {
		generation = 1
	}
	if s.graphPageSize == 0 {
		s.graphPageSize = 64
	}

	pages := make(map[uint64][]graphPageEntry)
	for docID, neighbors := range state.Adj {
		pageID := docID / s.graphPageSize
		pages[pageID] = append(pages[pageID], graphPageEntry{DocID: docID, Neighbors: append([]uint64(nil), neighbors...)})
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	for pageID, entries := range pages {
		if err := batch.Set(encodeGraphPageKey(generation, pageID), encodeGraphPage(entries), nil); err != nil {
			return err
		}
	}
	headPayload, err := encodeGraphHead(graphHead{
		Generation: generation,
		PageSize:   s.graphPageSize,
		Metric:     state.Metric,
		R:          state.R,
		Start:      append([]uint64(nil), state.Start...),
		UpdatedAt:  time.Now().UTC().UnixNano(),
	})
	if err != nil {
		return err
	}
	if err := batch.Set(keyGraphHead, headPayload, nil); err != nil {
		return err
	}
	return batch.Commit(wo)
}

func (s *IndexStore) LoadGraphState() (graphv2.State, bool, error) {
	var out graphv2.State
	v, closer, err := s.db.Get(keyGraphHead)
	if errors.Is(err, pebble.ErrNotFound) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	head, err := decodeGraphHead(v)
	closer.Close()
	if err != nil {
		return out, false, err
	}

	out = graphv2.State{
		Metric: head.Metric,
		R:      head.R,
		Start:  append([]uint64(nil), head.Start...),
		Adj:    make(map[uint64][]uint64),
	}
	pagePrefix := make([]byte, len(prefixGraphPg)+8)
	copy(pagePrefix, []byte(prefixGraphPg))
	binary.BigEndian.PutUint64(pagePrefix[len(prefixGraphPg):], head.Generation)
	lower, upper := prefixBounds(pagePrefix)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return graphv2.State{}, false, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		entries, err := decodeGraphPage(iter.Value())
		if err != nil {
			return graphv2.State{}, false, err
		}
		for _, e := range entries {
			out.Adj[e.DocID] = append([]uint64(nil), e.Neighbors...)
		}
	}
	if err := iter.Error(); err != nil {
		return graphv2.State{}, false, err
	}
	return out, true, nil
}

func PebblePath(indexDir string) string {
	return filepath.Join(indexDir, "meta.pebble")
}
