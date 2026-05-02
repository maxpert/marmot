package vecindex

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
)

const (
	segmentBlockMetaMagic       = "MVSBLK01"
	segmentBlockMetaVersion     = 1
	segmentBlockMetaHeaderSize  = 96
	segmentBlockMetaRefSize     = 16
	segmentBlockMetaRecordFixed = 64
	segmentBlockPQMaskWords     = (quantize.PQ8CodebookSize + 63) / 64

	segmentBlockStatsNone uint8 = iota
	segmentBlockStatsPQ8
	segmentBlockStatsResidualInt8

	segmentBlockMetaReadBatch = 512 << 10
	segmentBlockMetaReadGap   = 32 << 10
)

const (
	blockHeaderMagicEnd       = 8
	blockHeaderVersionOffset  = 8
	blockHeaderMetricOffset   = 12
	blockHeaderEncodingOffset = 13
	blockHeaderStatsKind      = 14
	blockHeaderDimOffset      = 16
	blockHeaderInternalDim    = 20
	blockHeaderEpochOffset    = 24
	blockHeaderGeneration     = 32
	blockHeaderMaxCluster     = 40
	blockHeaderBlockRows      = 44
	blockHeaderStatsBytes     = 48
	blockHeaderRecordSize     = 52
	blockHeaderRecordCount    = 56

	blockRefFirstOffset = 0
	blockRefCountOffset = 8

	blockRecordClusterID       = 0
	blockRecordFirstRowOrdinal = 8
	blockRecordRowCount        = 16
	blockRecordDataOffset      = 24
	blockRecordDataBytes       = 32
	blockRecordMinRowID        = 40
	blockRecordMaxRowID        = 48
	blockRecordMinNorm2        = 56
	blockRecordMaxNorm2        = 60
)

type segmentBlockRef struct {
	first uint64
	count uint64
}

type SegmentBlockScanStats struct {
	MetaReadBytes uint64
	MetaReads     uint64
	Considered    uint64
	WouldSkip     uint64
	Skipped       uint64
	Scored        uint64
	RowsScored    uint64
}

type SegmentBlockRecord struct {
	ClusterID       int64
	FirstRowOrdinal uint64
	RowCount        uint64
	DataOffset      int64
	DataBytes       int64
	MinRowID        int64
	MaxRowID        int64
	MinNorm2        float32
	MaxNorm2        float32
	Stats           []byte
}

type segmentBlockReadSpan struct {
	offset int64
	bytes  int64
}

type SegmentBlockMetaStore struct {
	path          string
	file          *os.File
	metric        Metric
	encoding      int64
	dim           int
	internalDim   int
	epoch         uint64
	generation    uint64
	maxCluster    int
	blockRows     int
	statsKind     uint8
	statsBytes    int
	recordSize    int
	recordCount   uint64
	recordsOffset int64
	refs          []segmentBlockRef
	metaReadBytes atomic.Uint64
	metaReads     atomic.Uint64
	considered    atomic.Uint64
	wouldSkip     atomic.Uint64
	skipped       atomic.Uint64
	scored        atomic.Uint64
	rowsScored    atomic.Uint64
}

type SegmentBlockMetaWriter struct {
	path        string
	tmpPath     string
	file        *os.File
	spec        IVFSpec
	codec       *StableMemberCodec
	metric      Metric
	encoding    int64
	dim         int
	internalDim int
	epoch       uint64
	generation  uint64
	maxCluster  int
	blockRows   int
	statsKind   uint8
	statsBytes  int
	recordSize  int
	offset      uint64
	recordCount uint64
	refs        []segmentBlockRef
	lastCID     int64
	current     *segmentBlockAccumulator
	closed      bool
}

type segmentBlockAccumulator struct {
	clusterID       int64
	firstRowOrdinal uint64
	rowCount        uint64
	dataOffset      uint64
	dataBytes       uint64
	minRowID        int64
	maxRowID        int64
	minNorm2        float32
	maxNorm2        float32
	hasNorm         bool
	pqMasks         []uint64
	resStats        *quantize.ResidualInt8StatsAccumulator
}

func DefaultSegmentBlockRows(encoding int64) int {
	if encoding == MemberEncodingResidualPQ8 {
		return 128
	}
	return 256
}

func SegmentBlockMetaPath(dir string, generation uint64) string {
	return filepath.Join(dir, "blocks", fmt.Sprintf("gen-%020d.blk", generation))
}

func CreateSegmentBlockMetaWriter(path string, spec IVFSpec, codec *StableMemberCodec, blockRows, maxCluster int, epoch, generation uint64) (*SegmentBlockMetaWriter, error) {
	if codec == nil {
		return nil, fmt.Errorf("vecindex: block meta codec is nil")
	}
	if err := codec.Validate(); err != nil {
		return nil, err
	}
	if blockRows <= 0 {
		blockRows = DefaultSegmentBlockRows(codec.Encoding())
	}
	statsKind, statsBytes, err := segmentBlockStatsLayout(spec, codec)
	if err != nil {
		return nil, err
	}
	tmpPath := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	recordSize := segmentBlockMetaRecordFixed + statsBytes
	refs := make([]segmentBlockRef, maxCluster+1)
	dataOffset := segmentBlockMetaHeaderSize + len(refs)*segmentBlockMetaRefSize
	if _, err := file.Seek(int64(dataOffset), io.SeekStart); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	return &SegmentBlockMetaWriter{
		path:        path,
		tmpPath:     tmpPath,
		file:        file,
		spec:        spec,
		codec:       codec,
		metric:      spec.InternalMetric(),
		encoding:    codec.Encoding(),
		dim:         spec.Dim,
		internalDim: spec.InternalDim(),
		epoch:       epoch,
		generation:  generation,
		maxCluster:  maxCluster,
		blockRows:   blockRows,
		statsKind:   statsKind,
		statsBytes:  statsBytes,
		recordSize:  recordSize,
		offset:      uint64(dataOffset),
		refs:        refs,
	}, nil
}

func OpenSegmentBlockMetaStore(path string) (*SegmentBlockMetaStore, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	header := make([]byte, segmentBlockMetaHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		_ = file.Close()
		return nil, err
	}
	if string(header[:blockHeaderMagicEnd]) != segmentBlockMetaMagic {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid block meta magic")
	}
	if got := binary.LittleEndian.Uint32(header[blockHeaderVersionOffset:]); got != segmentBlockMetaVersion {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: unsupported block meta version %d", got)
	}
	metricKind := Metric(header[blockHeaderMetricOffset])
	encoding := int64(header[blockHeaderEncodingOffset])
	statsKind := header[blockHeaderStatsKind]
	dim := int(binary.LittleEndian.Uint32(header[blockHeaderDimOffset:]))
	internalDim := int(binary.LittleEndian.Uint32(header[blockHeaderInternalDim:]))
	epoch := binary.LittleEndian.Uint64(header[blockHeaderEpochOffset:])
	generation := binary.LittleEndian.Uint64(header[blockHeaderGeneration:])
	maxCluster := int(binary.LittleEndian.Uint32(header[blockHeaderMaxCluster:]))
	blockRows := int(binary.LittleEndian.Uint32(header[blockHeaderBlockRows:]))
	statsBytes := int(binary.LittleEndian.Uint32(header[blockHeaderStatsBytes:]))
	recordSize := int(binary.LittleEndian.Uint32(header[blockHeaderRecordSize:]))
	recordCount := binary.LittleEndian.Uint64(header[blockHeaderRecordCount:])
	if dim <= 0 || internalDim <= 0 || maxCluster < 0 || blockRows <= 0 || recordSize < segmentBlockMetaRecordFixed {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid block meta header")
	}
	if recordSize != segmentBlockMetaRecordFixed+statsBytes {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid block meta record size")
	}
	switch encoding {
	case MemberEncodingResidualPQ8:
		if statsKind != segmentBlockStatsPQ8 {
			_ = file.Close()
			return nil, fmt.Errorf("vecindex: invalid PQ block stats kind")
		}
	case MemberEncodingResidualInt8:
		if statsKind != segmentBlockStatsResidualInt8 {
			_ = file.Close()
			return nil, fmt.Errorf("vecindex: invalid residual-int8 block stats kind")
		}
	default:
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: unsupported block encoding %d", encoding)
	}
	recordsOffset := int64(segmentBlockMetaHeaderSize + (maxCluster+1)*segmentBlockMetaRefSize)
	wantSize := recordsOffset + int64(recordCount)*int64(recordSize)
	if info, statErr := file.Stat(); statErr != nil {
		_ = file.Close()
		return nil, statErr
	} else if info.Size() != wantSize {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid block meta file size")
	}
	refs := make([]segmentBlockRef, maxCluster+1)
	refBytes := len(refs) * segmentBlockMetaRefSize
	refBuf := make([]byte, refBytes)
	if _, err := file.ReadAt(refBuf, segmentBlockMetaHeaderSize); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: read block refs: %w", err)
	}
	var counted uint64
	ranges := make([]segmentBlockRef, 0, maxCluster)
	for i := range refs {
		cursor := i * segmentBlockMetaRefSize
		ref := segmentBlockRef{
			first: binary.LittleEndian.Uint64(refBuf[cursor+blockRefFirstOffset:]),
			count: binary.LittleEndian.Uint64(refBuf[cursor+blockRefCountOffset:]),
		}
		if ref.count > 0 && (ref.first >= recordCount || ref.count > recordCount-ref.first) {
			_ = file.Close()
			return nil, fmt.Errorf("vecindex: block refs out of range")
		}
		if ref.count > 0 {
			ranges = append(ranges, ref)
		}
		counted += ref.count
		refs[i] = ref
	}
	slices.SortFunc(ranges, func(a, b segmentBlockRef) int {
		switch {
		case a.first < b.first:
			return -1
		case a.first > b.first:
			return 1
		default:
			return 0
		}
	})
	var prevEnd uint64
	for _, ref := range ranges {
		if ref.first < prevEnd {
			_ = file.Close()
			return nil, fmt.Errorf("vecindex: block refs overlap")
		}
		prevEnd = ref.first + ref.count
	}
	if counted != recordCount {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: block ref count mismatch")
	}
	return &SegmentBlockMetaStore{
		path:          path,
		file:          file,
		metric:        metricKind,
		encoding:      encoding,
		dim:           dim,
		internalDim:   internalDim,
		epoch:         epoch,
		generation:    generation,
		maxCluster:    maxCluster,
		blockRows:     blockRows,
		statsKind:     statsKind,
		statsBytes:    statsBytes,
		recordSize:    recordSize,
		recordCount:   recordCount,
		recordsOffset: recordsOffset,
		refs:          refs,
	}, nil
}

func (w *SegmentBlockMetaWriter) Append(clusterID, rowID int64, dataOffset uint64, dataBytes int, encoded []byte) error {
	if w == nil || w.file == nil {
		return fmt.Errorf("vecindex: block meta writer is closed")
	}
	if clusterID <= 0 || int(clusterID) > w.maxCluster {
		return fmt.Errorf("vecindex: block meta cluster %d out of range", clusterID)
	}
	if w.current != nil && clusterID != w.current.clusterID {
		if err := w.flushCurrent(); err != nil {
			return err
		}
	}
	ref := &w.refs[clusterID]
	if ref.count > 0 && clusterID != w.lastCID {
		return fmt.Errorf("vecindex: block meta cluster %d cannot be reopened after another cluster", clusterID)
	}
	if w.current == nil {
		if ref.count == 0 {
			ref.first = w.recordCount
		}
		acc, err := w.newAccumulator(clusterID, dataOffset)
		if err != nil {
			return err
		}
		w.current = acc
	}
	if w.current.rowCount >= uint64(w.blockRows) {
		if err := w.flushCurrent(); err != nil {
			return err
		}
		acc, err := w.newAccumulator(clusterID, dataOffset)
		if err != nil {
			return err
		}
		w.current = acc
	}
	if err := w.current.add(w, rowID, dataOffset, dataBytes, encoded); err != nil {
		return err
	}
	w.lastCID = clusterID
	return nil
}

func (w *SegmentBlockMetaWriter) AppendRawBlocks(clusterID int64, blocks []SegmentBlockRecord, offsetDelta int64) error {
	if w == nil || w.file == nil {
		return fmt.Errorf("vecindex: block meta writer is closed")
	}
	if clusterID <= 0 || int(clusterID) > w.maxCluster {
		return fmt.Errorf("vecindex: block meta cluster %d out of range", clusterID)
	}
	if w.current != nil {
		if err := w.flushCurrent(); err != nil {
			return err
		}
	}
	if len(blocks) == 0 {
		return nil
	}
	ref := &w.refs[clusterID]
	if ref.count > 0 && clusterID != w.lastCID {
		return fmt.Errorf("vecindex: block meta cluster %d cannot be reopened after another cluster", clusterID)
	}
	if ref.count == 0 {
		ref.first = w.recordCount
	}
	var expectedOffset int64
	for _, block := range blocks {
		if block.ClusterID != clusterID {
			return fmt.Errorf("vecindex: raw block cluster mismatch")
		}
		if block.RowCount == 0 || block.DataBytes <= 0 {
			return fmt.Errorf("vecindex: raw block has invalid extent")
		}
		block.DataOffset += offsetDelta
		if expectedOffset != 0 && block.DataOffset != expectedOffset {
			return fmt.Errorf("vecindex: raw block offsets are not contiguous")
		}
		expectedOffset = block.DataOffset + block.DataBytes
		if err := w.writeRecord(block); err != nil {
			return err
		}
		ref.count++
		w.recordCount++
	}
	w.lastCID = clusterID
	return nil
}

func (w *SegmentBlockMetaWriter) Close() (*SegmentBlockMetaStore, error) {
	if w == nil || w.closed {
		return nil, fmt.Errorf("vecindex: block meta writer already closed")
	}
	w.closed = true
	if err := w.flushCurrent(); err != nil {
		w.Abort()
		return nil, err
	}
	header := make([]byte, segmentBlockMetaHeaderSize+len(w.refs)*segmentBlockMetaRefSize)
	copy(header[:blockHeaderMagicEnd], []byte(segmentBlockMetaMagic))
	binary.LittleEndian.PutUint32(header[blockHeaderVersionOffset:], segmentBlockMetaVersion)
	header[blockHeaderMetricOffset] = byte(w.metric)
	header[blockHeaderEncodingOffset] = byte(w.encoding)
	header[blockHeaderStatsKind] = w.statsKind
	binary.LittleEndian.PutUint32(header[blockHeaderDimOffset:], uint32(w.dim))
	binary.LittleEndian.PutUint32(header[blockHeaderInternalDim:], uint32(w.internalDim))
	binary.LittleEndian.PutUint64(header[blockHeaderEpochOffset:], w.epoch)
	binary.LittleEndian.PutUint64(header[blockHeaderGeneration:], w.generation)
	binary.LittleEndian.PutUint32(header[blockHeaderMaxCluster:], uint32(w.maxCluster))
	binary.LittleEndian.PutUint32(header[blockHeaderBlockRows:], uint32(w.blockRows))
	binary.LittleEndian.PutUint32(header[blockHeaderStatsBytes:], uint32(w.statsBytes))
	binary.LittleEndian.PutUint32(header[blockHeaderRecordSize:], uint32(w.recordSize))
	binary.LittleEndian.PutUint64(header[blockHeaderRecordCount:], w.recordCount)
	cursor := segmentBlockMetaHeaderSize
	for _, ref := range w.refs {
		binary.LittleEndian.PutUint64(header[cursor+blockRefFirstOffset:], ref.first)
		binary.LittleEndian.PutUint64(header[cursor+blockRefCountOffset:], ref.count)
		cursor += segmentBlockMetaRefSize
	}
	if _, err := w.file.WriteAt(header, 0); err != nil {
		w.Abort()
		return nil, err
	}
	if err := w.file.Sync(); err != nil {
		w.Abort()
		return nil, err
	}
	if err := w.file.Close(); err != nil {
		w.Abort()
		return nil, err
	}
	w.file = nil
	if err := os.Rename(w.tmpPath, w.path); err != nil {
		w.Abort()
		return nil, err
	}
	return OpenSegmentBlockMetaStore(w.path)
}

func (w *SegmentBlockMetaWriter) Abort() {
	if w == nil {
		return
	}
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if w.tmpPath != "" {
		_ = os.Remove(w.tmpPath)
	}
}

func (s *SegmentBlockMetaStore) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *SegmentBlockMetaStore) Path() string       { return s.path }
func (s *SegmentBlockMetaStore) Epoch() uint64      { return s.epoch }
func (s *SegmentBlockMetaStore) Generation() uint64 { return s.generation }
func (s *SegmentBlockMetaStore) Metric() Metric     { return s.metric }
func (s *SegmentBlockMetaStore) Encoding() int64    { return s.encoding }
func (s *SegmentBlockMetaStore) Dim() int           { return s.dim }
func (s *SegmentBlockMetaStore) InternalDim() int   { return s.internalDim }
func (s *SegmentBlockMetaStore) MaxCluster() int    { return s.maxCluster }
func (s *SegmentBlockMetaStore) BlockRows() int     { return s.blockRows }
func (s *SegmentBlockMetaStore) RecordCount() uint64 {
	if s == nil {
		return 0
	}
	return s.recordCount
}

func (s *SegmentBlockMetaStore) ResetScanStats() {
	if s == nil {
		return
	}
	s.metaReadBytes.Store(0)
	s.metaReads.Store(0)
	s.considered.Store(0)
	s.wouldSkip.Store(0)
	s.skipped.Store(0)
	s.scored.Store(0)
	s.rowsScored.Store(0)
}

func (s *SegmentBlockMetaStore) SnapshotScanStats() SegmentBlockScanStats {
	if s == nil {
		return SegmentBlockScanStats{}
	}
	return SegmentBlockScanStats{
		MetaReadBytes: s.metaReadBytes.Load(),
		MetaReads:     s.metaReads.Load(),
		Considered:    s.considered.Load(),
		WouldSkip:     s.wouldSkip.Load(),
		Skipped:       s.skipped.Load(),
		Scored:        s.scored.Load(),
		RowsScored:    s.rowsScored.Load(),
	}
}

func (s *SegmentBlockMetaStore) RecordQueryStats(considered, wouldSkip, skipped, scored, rowsScored uint64) {
	if s == nil {
		return
	}
	s.considered.Add(considered)
	s.wouldSkip.Add(wouldSkip)
	s.skipped.Add(skipped)
	s.scored.Add(scored)
	s.rowsScored.Add(rowsScored)
}

func (s *SegmentBlockMetaStore) ReadClusterBlocks(clusterIDs []int64) ([]SegmentBlockRecord, error) {
	if s == nil || s.file == nil || len(clusterIDs) == 0 {
		return nil, nil
	}
	spans := make([]segmentBlockReadSpan, 0, len(clusterIDs))
	var recordCap uint64
	seen := make([]bool, s.maxCluster+1)
	for _, cid := range clusterIDs {
		if cid <= 0 || int(cid) > s.maxCluster {
			continue
		}
		if seen[cid] {
			continue
		}
		seen[cid] = true
		ref := s.refs[cid]
		if ref.count == 0 {
			continue
		}
		bytes := int64(ref.count) * int64(s.recordSize)
		spans = append(spans, segmentBlockReadSpan{
			offset: s.recordsOffset + int64(ref.first)*int64(s.recordSize),
			bytes:  bytes,
		})
		recordCap += ref.count
	}
	if len(spans) == 0 {
		return nil, nil
	}
	slices.SortFunc(spans, func(a, b segmentBlockReadSpan) int {
		switch {
		case a.offset < b.offset:
			return -1
		case a.offset > b.offset:
			return 1
		default:
			return 0
		}
	})
	out := make([]SegmentBlockRecord, 0, int(recordCap))
	for start := 0; start < len(spans); {
		batchStart := spans[start].offset
		batchEnd := spans[start].offset + spans[start].bytes
		end := start + 1
		for end < len(spans) {
			nextStart := spans[end].offset
			nextEnd := nextStart + spans[end].bytes
			if nextStart-batchEnd > segmentBlockMetaReadGap || nextEnd-batchStart > segmentBlockMetaReadBatch {
				break
			}
			if nextEnd > batchEnd {
				batchEnd = nextEnd
			}
			end++
		}
		need := int(batchEnd - batchStart)
		buf := make([]byte, need)
		if _, err := s.file.ReadAt(buf, batchStart); err != nil {
			return nil, err
		}
		s.metaReadBytes.Add(uint64(need))
		s.metaReads.Add(1)
		for _, span := range spans[start:end] {
			spanStart := int(span.offset - batchStart)
			spanEnd := spanStart + int(span.bytes)
			for cursor := spanStart; cursor < spanEnd; cursor += s.recordSize {
				rec, err := s.decodeRecord(buf[cursor:cursor+s.recordSize], true)
				if err != nil {
					return nil, err
				}
				out = append(out, rec)
			}
		}
		start = end
	}
	return out, nil
}

func (s *SegmentBlockMetaStore) ValidateCoverage(data *SegmentDataStore) error {
	if s == nil || data == nil {
		return fmt.Errorf("vecindex: block coverage requires data and block stores")
	}
	if s.maxCluster != data.maxCluster || s.generation != data.generation || s.epoch != data.epoch {
		return fmt.Errorf("vecindex: block coverage header mismatch")
	}
	if s.encoding != data.encoding || s.metric != data.metric || s.dim != data.dim || s.internalDim != data.internalDim {
		return fmt.Errorf("vecindex: block coverage encoding mismatch")
	}
	var totalRows uint64
	for clusterID := 1; clusterID <= data.maxCluster; clusterID++ {
		dataRef := data.refs[clusterID]
		blockRef := s.refs[clusterID]
		if dataRef.count == 0 {
			if blockRef.count != 0 {
				return fmt.Errorf("vecindex: block coverage has records for empty cluster %d", clusterID)
			}
			continue
		}
		if blockRef.count == 0 {
			return fmt.Errorf("vecindex: block coverage missing cluster %d", clusterID)
		}
		expectedOffset := int64(dataRef.offset)
		expectedOrdinal := uint64(0)
		bytes := int(blockRef.count) * s.recordSize
		buf := make([]byte, bytes)
		offset := s.recordsOffset + int64(blockRef.first)*int64(s.recordSize)
		if _, err := s.file.ReadAt(buf, offset); err != nil {
			return fmt.Errorf("vecindex: read block coverage cluster %d: %w", clusterID, err)
		}
		for cursor := 0; cursor < bytes; cursor += s.recordSize {
			rec, err := s.decodeRecord(buf[cursor:cursor+s.recordSize], false)
			if err != nil {
				return err
			}
			if rec.ClusterID != int64(clusterID) {
				return fmt.Errorf("vecindex: block coverage cluster mismatch: got=%d want=%d", rec.ClusterID, clusterID)
			}
			if rec.RowCount == 0 || rec.RowCount > uint64(s.blockRows) {
				return fmt.Errorf("vecindex: block coverage invalid row count in cluster %d", clusterID)
			}
			if rec.FirstRowOrdinal != expectedOrdinal {
				return fmt.Errorf("vecindex: block coverage ordinal gap in cluster %d", clusterID)
			}
			if rec.DataOffset != expectedOffset {
				return fmt.Errorf("vecindex: block coverage data gap in cluster %d", clusterID)
			}
			wantBytes := int64(rec.RowCount) * int64(data.entrySize)
			if rec.DataBytes != wantBytes {
				return fmt.Errorf("vecindex: block coverage data bytes mismatch in cluster %d", clusterID)
			}
			if rec.MinRowID > rec.MaxRowID {
				return fmt.Errorf("vecindex: block coverage rowid bounds invalid in cluster %d", clusterID)
			}
			expectedOrdinal += rec.RowCount
			expectedOffset += rec.DataBytes
		}
		if expectedOrdinal != dataRef.count {
			return fmt.Errorf("vecindex: block coverage row count mismatch in cluster %d", clusterID)
		}
		totalRows += expectedOrdinal
	}
	if totalRows != data.rowCount {
		return fmt.Errorf("vecindex: block coverage total row count mismatch")
	}
	return nil
}

func (s *SegmentBlockMetaStore) decodeRecord(raw []byte, includeStats bool) (SegmentBlockRecord, error) {
	if len(raw) != s.recordSize {
		return SegmentBlockRecord{}, fmt.Errorf("vecindex: invalid block record size")
	}
	rec := SegmentBlockRecord{
		ClusterID:       int64(binary.LittleEndian.Uint32(raw[blockRecordClusterID:])),
		FirstRowOrdinal: binary.LittleEndian.Uint64(raw[blockRecordFirstRowOrdinal:]),
		RowCount:        uint64(binary.LittleEndian.Uint32(raw[blockRecordRowCount:])),
		DataOffset:      int64(binary.LittleEndian.Uint64(raw[blockRecordDataOffset:])),
		DataBytes:       int64(binary.LittleEndian.Uint64(raw[blockRecordDataBytes:])),
		MinRowID:        int64(binary.LittleEndian.Uint64(raw[blockRecordMinRowID:])),
		MaxRowID:        int64(binary.LittleEndian.Uint64(raw[blockRecordMaxRowID:])),
		MinNorm2:        math.Float32frombits(binary.LittleEndian.Uint32(raw[blockRecordMinNorm2:])),
		MaxNorm2:        math.Float32frombits(binary.LittleEndian.Uint32(raw[blockRecordMaxNorm2:])),
	}
	if includeStats && s.statsBytes > 0 {
		rec.Stats = raw[segmentBlockMetaRecordFixed:]
	}
	return rec, nil
}

func segmentBlockStatsLayout(spec IVFSpec, codec *StableMemberCodec) (uint8, int, error) {
	if codec == nil {
		return segmentBlockStatsNone, 0, fmt.Errorf("vecindex: block codec is nil")
	}
	switch codec.Encoding() {
	case MemberEncodingResidualPQ8:
		if codec.pq == nil {
			return segmentBlockStatsNone, 0, fmt.Errorf("vecindex: PQ block codec is nil")
		}
		return segmentBlockStatsPQ8, codec.pq.M * segmentBlockPQMaskWords * 8, nil
	case MemberEncodingResidualInt8:
		return segmentBlockStatsResidualInt8, spec.InternalDim() * 8, nil
	default:
		return segmentBlockStatsNone, 0, fmt.Errorf("vecindex: unsupported block encoding %d", codec.Encoding())
	}
}

func (w *SegmentBlockMetaWriter) newAccumulator(clusterID int64, dataOffset uint64) (*segmentBlockAccumulator, error) {
	acc := &segmentBlockAccumulator{
		clusterID:       clusterID,
		firstRowOrdinal: w.refs[clusterID].count * uint64(w.blockRows),
		dataOffset:      dataOffset,
		minNorm2:        float32(math.MaxFloat32),
		maxNorm2:        -float32(math.MaxFloat32),
	}
	switch w.statsKind {
	case segmentBlockStatsPQ8:
		acc.pqMasks = make([]uint64, w.codec.pq.M*segmentBlockPQMaskWords)
	case segmentBlockStatsResidualInt8:
		resStats, err := quantize.NewResidualInt8StatsAccumulator(w.metric, w.internalDim, MemberResidualBlockSize)
		if err != nil {
			return nil, err
		}
		acc.resStats = resStats
	}
	return acc, nil
}

func (a *segmentBlockAccumulator) add(w *SegmentBlockMetaWriter, rowID int64, dataOffset uint64, dataBytes int, encoded []byte) error {
	if dataBytes <= 0 {
		return fmt.Errorf("vecindex: block meta row data bytes must be > 0")
	}
	if a.rowCount == 0 {
		a.minRowID = rowID
		a.maxRowID = rowID
	} else {
		if rowID < a.minRowID {
			a.minRowID = rowID
		}
		if rowID > a.maxRowID {
			a.maxRowID = rowID
		}
	}
	if a.rowCount == 0 {
		a.dataOffset = dataOffset
	} else if dataOffset != a.dataOffset+a.dataBytes {
		return fmt.Errorf("vecindex: block meta row offset is not contiguous")
	}
	a.dataBytes += uint64(dataBytes)
	var norm2 float32
	var hasNorm bool
	switch w.statsKind {
	case segmentBlockStatsPQ8:
		var err error
		norm2, hasNorm, err = a.addPQ(w, encoded)
		if err != nil {
			return err
		}
	case segmentBlockStatsResidualInt8:
		var err error
		norm2, hasNorm, err = a.resStats.Accumulate(encoded)
		if err != nil {
			return err
		}
	}
	if hasNorm {
		if norm2 < a.minNorm2 {
			a.minNorm2 = norm2
		}
		if norm2 > a.maxNorm2 {
			a.maxNorm2 = norm2
		}
		a.hasNorm = true
	}
	a.rowCount++
	return nil
}

func (a *segmentBlockAccumulator) addPQ(w *SegmentBlockMetaWriter, encoded []byte) (float32, bool, error) {
	pq := w.codec.pq
	want := pq.EncodedSize(w.metric)
	if len(encoded) != want {
		return 0, false, fmt.Errorf("vecindex: PQ block encoded length %d want %d", len(encoded), want)
	}
	offset := 0
	norm2 := float32(0)
	hasNorm := false
	if w.metric == metric.MetricL2 || pq.StoreNorm {
		norm2 = math.Float32frombits(binary.LittleEndian.Uint32(encoded[:4]))
		hasNorm = true
		offset = 4
	}
	for sub := 0; sub < pq.M; sub++ {
		code := int(encoded[offset+sub])
		word := code / 64
		bit := uint(code % 64)
		a.pqMasks[sub*segmentBlockPQMaskWords+word] |= uint64(1) << bit
	}
	return norm2, hasNorm, nil
}

func (w *SegmentBlockMetaWriter) flushCurrent() error {
	if w == nil || w.current == nil {
		return nil
	}
	rec := w.current.record(w)
	if err := w.writeRecord(rec); err != nil {
		return err
	}
	w.refs[w.current.clusterID].count++
	w.recordCount++
	w.current = nil
	return nil
}

func (a *segmentBlockAccumulator) record(w *SegmentBlockMetaWriter) SegmentBlockRecord {
	minNorm, maxNorm := float32(0), float32(0)
	if a.hasNorm {
		minNorm, maxNorm = a.minNorm2, a.maxNorm2
	}
	rec := SegmentBlockRecord{
		ClusterID:       a.clusterID,
		FirstRowOrdinal: a.firstRowOrdinal,
		RowCount:        a.rowCount,
		DataOffset:      int64(a.dataOffset),
		DataBytes:       int64(a.dataBytes),
		MinRowID:        a.minRowID,
		MaxRowID:        a.maxRowID,
		MinNorm2:        minNorm,
		MaxNorm2:        maxNorm,
	}
	switch w.statsKind {
	case segmentBlockStatsPQ8:
		rec.Stats = encodeUint64s(a.pqMasks)
	case segmentBlockStatsResidualInt8:
		minResidual := a.resStats.MinResidual()
		maxResidual := a.resStats.MaxResidual()
		stats := make([]byte, len(minResidual)*8)
		for i, value := range minResidual {
			binary.LittleEndian.PutUint32(stats[i*4:], math.Float32bits(value))
		}
		base := len(minResidual) * 4
		for i, value := range maxResidual {
			binary.LittleEndian.PutUint32(stats[base+i*4:], math.Float32bits(value))
		}
		rec.Stats = stats
	}
	return rec
}

func (w *SegmentBlockMetaWriter) writeRecord(rec SegmentBlockRecord) error {
	if len(rec.Stats) != w.statsBytes {
		return fmt.Errorf("vecindex: block stats length %d want %d", len(rec.Stats), w.statsBytes)
	}
	buf := make([]byte, w.recordSize)
	binary.LittleEndian.PutUint32(buf[blockRecordClusterID:], uint32(rec.ClusterID))
	binary.LittleEndian.PutUint64(buf[blockRecordFirstRowOrdinal:], rec.FirstRowOrdinal)
	binary.LittleEndian.PutUint32(buf[blockRecordRowCount:], uint32(rec.RowCount))
	binary.LittleEndian.PutUint64(buf[blockRecordDataOffset:], uint64(rec.DataOffset))
	binary.LittleEndian.PutUint64(buf[blockRecordDataBytes:], uint64(rec.DataBytes))
	binary.LittleEndian.PutUint64(buf[blockRecordMinRowID:], uint64(rec.MinRowID))
	binary.LittleEndian.PutUint64(buf[blockRecordMaxRowID:], uint64(rec.MaxRowID))
	binary.LittleEndian.PutUint32(buf[blockRecordMinNorm2:], math.Float32bits(rec.MinNorm2))
	binary.LittleEndian.PutUint32(buf[blockRecordMaxNorm2:], math.Float32bits(rec.MaxNorm2))
	copy(buf[segmentBlockMetaRecordFixed:], rec.Stats)
	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	w.offset += uint64(w.recordSize)
	return nil
}

func encodeUint64s(values []uint64) []byte {
	out := make([]byte, len(values)*8)
	for i, value := range values {
		binary.LittleEndian.PutUint64(out[i*8:], value)
	}
	return out
}
