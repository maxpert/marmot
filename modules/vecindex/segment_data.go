package vecindex

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
)

const (
	segmentDataMagic   = "MVSDAT01"
	segmentDataVersion = 1
	segmentDataHdrSize = 56
	segmentDataRefSize = 16
	segmentReadChunk   = 1 << 20
	segmentReadBatch   = 8 << 20
	segmentPQReadBatch = 512 << 10
	segmentReadGap     = 1 << 20
	segmentPQReadGap   = 0
)

type segmentClusterRef struct {
	offset uint64
	count  uint64
}

type segmentClusterSpan struct {
	clusterID int64
	offset    int64
	bytes     int64
	count     uint64
}

type segmentReadBatchSpec struct {
	start int64
	end   int64
	spans []segmentClusterSpan
}

type SegmentScanStats struct {
	ReadBytes          uint64
	LogicalBytes       uint64
	ReadBatches        uint64
	BlockMetaReadBytes uint64
	BlockMetaReads     uint64
	BlocksConsidered   uint64
	BlocksWouldSkip    uint64
	BlocksSkipped      uint64
	BlocksScored       uint64
	BlockRowsScored    uint64
}

// SegmentDataStore is a read-only stable-vector generation file opened with
// explicit ReadAt-based scans. Stable rows are laid out as
// [rowid uint64][vec bytes] in cluster_id,rowid order.
//
// vecBytes yielded during ScanCluster/ScanClusters is only valid for the
// callback duration; callers that retain it must copy it.
type SegmentDataStore struct {
	path          string
	file          *os.File
	metric        Metric
	encoding      int64
	dim           int
	internalDim   int
	vecBytes      int
	epoch         uint64
	generation    uint64
	maxCluster    int
	rowCount      uint64
	entrySize     int
	fileSize      int64
	refs          []segmentClusterRef
	bufPool       sync.Pool
	batchBufPool  sync.Pool
	seenPool      sync.Pool
	spanByCluster []segmentClusterSpan
	fileOrderRank []int
	readBytes     atomic.Uint64
	logicalBytes  atomic.Uint64
	readBatches   atomic.Uint64
}

type SegmentDataWriter struct {
	path        string
	tmpPath     string
	file        *os.File
	metric      Metric
	encoding    int64
	dim         int
	internalDim int
	vecBytes    int
	epoch       uint64
	generation  uint64
	maxCluster  int
	rowCount    uint64
	entrySize   int
	offset      uint64
	lastCID     int64
	refs        []segmentClusterRef
	closed      bool
}

func (w *SegmentDataWriter) NextOffset() uint64 {
	if w == nil {
		return 0
	}
	return w.offset
}

func (w *SegmentDataWriter) EntrySize() int {
	if w == nil {
		return 0
	}
	return w.entrySize
}

func OpenSegmentDataStore(path string) (*SegmentDataStore, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() < segmentDataHdrSize {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: segment data %q is too small", path)
	}

	header := make([]byte, segmentDataHdrSize)
	if _, err := io.ReadFull(file, header); err != nil {
		_ = file.Close()
		return nil, err
	}
	if string(header[:8]) != segmentDataMagic {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid segment data magic %q", path)
	}
	if got := int(binary.LittleEndian.Uint32(header[8:12])); got != segmentDataVersion {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: unsupported segment data version %d", got)
	}

	metric := Metric(header[12])
	switch metric {
	case MetricL2, MetricDot, MetricCosine:
	default:
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid segment data metric %d", metric)
	}
	encoding := int64(header[13])
	dim := int(binary.LittleEndian.Uint32(header[16:20]))
	internalDim := int(binary.LittleEndian.Uint32(header[20:24]))
	epoch := binary.LittleEndian.Uint64(header[24:32])
	generation := binary.LittleEndian.Uint64(header[32:40])
	maxCluster := int(binary.LittleEndian.Uint32(header[40:44]))
	vecBytes := int(binary.LittleEndian.Uint32(header[44:48]))
	rowCount := binary.LittleEndian.Uint64(header[48:56])
	if dim <= 0 || internalDim <= 0 || vecBytes <= 0 {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid segment data header dim=%d internal=%d vecBytes=%d", dim, internalDim, vecBytes)
	}
	if maxCluster < 0 {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid segment data maxCluster=%d", maxCluster)
	}
	if err := validateSegmentStableEncoding(metric, encoding, internalDim, vecBytes); err != nil {
		_ = file.Close()
		return nil, err
	}

	refBytes := int64((maxCluster + 1) * segmentDataRefSize)
	dataOffset := int64(segmentDataHdrSize) + refBytes
	if info.Size() < dataOffset {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: truncated segment data refs")
	}

	refBuf := make([]byte, refBytes)
	if _, err := file.ReadAt(refBuf, int64(segmentDataHdrSize)); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: read segment refs: %w", err)
	}
	entrySize := 8 + vecBytes
	refs := make([]segmentClusterRef, maxCluster+1)
	var counted uint64
	type nonEmptyRef struct {
		offset uint64
		end    int64
	}
	nonEmpty := make([]nonEmptyRef, 0, maxCluster)
	for i := range refs {
		cursor := i * segmentDataRefSize
		ref := segmentClusterRef{
			offset: binary.LittleEndian.Uint64(refBuf[cursor : cursor+8]),
			count:  binary.LittleEndian.Uint64(refBuf[cursor+8 : cursor+16]),
		}
		if ref.count == 0 {
			refs[i] = ref
			continue
		}
		if int64(ref.offset) < dataOffset {
			_ = file.Close()
			return nil, fmt.Errorf("vecindex: segment cluster %d offset out of bounds", i)
		}
		end := int64(ref.offset + ref.count*uint64(entrySize))
		if end > info.Size() {
			_ = file.Close()
			return nil, fmt.Errorf("vecindex: segment cluster %d extends past EOF", i)
		}
		counted += ref.count
		nonEmpty = append(nonEmpty, nonEmptyRef{offset: ref.offset, end: end})
		refs[i] = ref
	}
	slices.SortFunc(nonEmpty, func(a, b nonEmptyRef) int {
		switch {
		case a.offset < b.offset:
			return -1
		case a.offset > b.offset:
			return 1
		default:
			return 0
		}
	})
	prevEnd := dataOffset
	for _, ref := range nonEmpty {
		if int64(ref.offset) < prevEnd {
			_ = file.Close()
			return nil, fmt.Errorf("vecindex: segment cluster extents overlap")
		}
		prevEnd = ref.end
	}
	if counted != rowCount {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: segment data row count mismatch: header=%d refs=%d", rowCount, counted)
	}

	s := &SegmentDataStore{
		path:        path,
		file:        file,
		metric:      metric,
		encoding:    encoding,
		dim:         dim,
		internalDim: internalDim,
		vecBytes:    vecBytes,
		epoch:       epoch,
		generation:  generation,
		maxCluster:  maxCluster,
		rowCount:    rowCount,
		entrySize:   entrySize,
		fileSize:    info.Size(),
		refs:        refs,
	}
	spans := make([]segmentClusterSpan, 0, maxCluster)
	fileOrderRank := make([]int, maxCluster+1)
	for i := range fileOrderRank {
		fileOrderRank[i] = -1
	}
	for cid := 1; cid <= maxCluster; cid++ {
		ref := refs[cid]
		if ref.count == 0 {
			continue
		}
		spans = append(spans, segmentClusterSpan{
			clusterID: int64(cid),
			offset:    int64(ref.offset),
			bytes:     int64(ref.count) * int64(entrySize),
			count:     ref.count,
		})
	}
	slices.SortFunc(spans, func(a, b segmentClusterSpan) int {
		switch {
		case a.offset < b.offset:
			return -1
		case a.offset > b.offset:
			return 1
		default:
			return 0
		}
	})
	for i, span := range spans {
		fileOrderRank[span.clusterID] = i
	}
	spanByCluster := make([]segmentClusterSpan, maxCluster+1)
	for _, span := range spans {
		spanByCluster[span.clusterID] = span
	}
	s.spanByCluster = spanByCluster
	s.fileOrderRank = fileOrderRank
	s.bufPool.New = func() any {
		buf := make([]byte, segmentReadChunk)
		return &buf
	}
	s.batchBufPool.New = func() any {
		buf := make([]byte, s.batchBufferSize())
		return &buf
	}
	seenWords := (maxCluster + 64) / 64
	s.seenPool.New = func() any {
		bits := make([]uint64, seenWords)
		return &bits
	}
	return s, nil
}

func (s *SegmentDataStore) batchBufferSize() int {
	if s != nil && s.encoding == MemberEncodingResidualPQ8 {
		return segmentPQReadBatch
	}
	return segmentReadBatch
}

func CreateSegmentDataWriter(path string, metric Metric, encoding int64, dim, internalDim, vecBytes, maxCluster int, epoch, generation uint64) (*SegmentDataWriter, error) {
	if dim <= 0 || internalDim <= 0 {
		return nil, fmt.Errorf("vecindex: segment data dims must be > 0")
	}
	if vecBytes <= 0 {
		return nil, fmt.Errorf("vecindex: segment data vec bytes must be > 0")
	}
	if maxCluster < 0 {
		return nil, fmt.Errorf("vecindex: segment data maxCluster must be >= 0")
	}
	if err := validateSegmentStableEncoding(metric, encoding, internalDim, vecBytes); err != nil {
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
	dataOffset := segmentDataHdrSize + (maxCluster+1)*segmentDataRefSize
	if _, err := file.Seek(int64(dataOffset), io.SeekStart); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	return &SegmentDataWriter{
		path:        path,
		tmpPath:     tmpPath,
		file:        file,
		metric:      metric,
		encoding:    encoding,
		dim:         dim,
		internalDim: internalDim,
		vecBytes:    vecBytes,
		epoch:       epoch,
		generation:  generation,
		maxCluster:  maxCluster,
		entrySize:   8 + vecBytes,
		offset:      uint64(dataOffset),
		refs:        make([]segmentClusterRef, maxCluster+1),
	}, nil
}

func (w *SegmentDataWriter) Append(clusterID, rowid int64, vec []byte) error {
	if w == nil || w.file == nil {
		return fmt.Errorf("vecindex: segment data writer is closed")
	}
	if clusterID <= 0 || int(clusterID) > w.maxCluster {
		return fmt.Errorf("vecindex: segment data cluster %d out of range", clusterID)
	}
	if len(vec) != w.vecBytes {
		return fmt.Errorf("vecindex: segment data vec length %d, want %d", len(vec), w.vecBytes)
	}
	ref := &w.refs[clusterID]
	if ref.count > 0 && clusterID != w.lastCID {
		return fmt.Errorf("vecindex: segment data cluster %d cannot be reopened after another cluster", clusterID)
	}
	if ref.count == 0 {
		ref.offset = w.offset
	}
	var rowidBuf [8]byte
	binary.LittleEndian.PutUint64(rowidBuf[:], uint64(rowid))
	if _, err := w.file.Write(rowidBuf[:]); err != nil {
		return err
	}
	if _, err := w.file.Write(vec); err != nil {
		return err
	}
	ref.count++
	w.rowCount++
	w.offset += uint64(w.entrySize)
	w.lastCID = clusterID
	return nil
}

func (w *SegmentDataWriter) AppendRawCluster(clusterID int64, count uint64, rows io.Reader) error {
	if w == nil || w.file == nil {
		return fmt.Errorf("vecindex: segment data writer is closed")
	}
	if clusterID <= 0 || int(clusterID) > w.maxCluster {
		return fmt.Errorf("vecindex: segment data cluster %d out of range", clusterID)
	}
	ref := &w.refs[clusterID]
	if ref.count > 0 && clusterID != w.lastCID {
		return fmt.Errorf("vecindex: segment data cluster %d cannot be reopened after another cluster", clusterID)
	}
	if ref.count == 0 {
		ref.offset = w.offset
	}
	bytesToCopy := int64(count) * int64(w.entrySize)
	if _, err := io.CopyN(w.file, rows, bytesToCopy); err != nil {
		return err
	}
	ref.count += count
	w.rowCount += count
	w.offset += uint64(bytesToCopy)
	w.lastCID = clusterID
	return nil
}

func (w *SegmentDataWriter) Close() (*SegmentDataStore, error) {
	if w == nil || w.closed {
		return nil, fmt.Errorf("vecindex: segment data writer already closed")
	}
	w.closed = true

	header := make([]byte, segmentDataHdrSize+(w.maxCluster+1)*segmentDataRefSize)
	copy(header[:8], []byte(segmentDataMagic))
	binary.LittleEndian.PutUint32(header[8:12], segmentDataVersion)
	header[12] = byte(w.metric)
	header[13] = byte(w.encoding)
	binary.LittleEndian.PutUint32(header[16:20], uint32(w.dim))
	binary.LittleEndian.PutUint32(header[20:24], uint32(w.internalDim))
	binary.LittleEndian.PutUint64(header[24:32], w.epoch)
	binary.LittleEndian.PutUint64(header[32:40], w.generation)
	binary.LittleEndian.PutUint32(header[40:44], uint32(w.maxCluster))
	binary.LittleEndian.PutUint32(header[44:48], uint32(w.vecBytes))
	binary.LittleEndian.PutUint64(header[48:56], w.rowCount)
	cursor := segmentDataHdrSize
	for _, ref := range w.refs {
		binary.LittleEndian.PutUint64(header[cursor:cursor+8], ref.offset)
		binary.LittleEndian.PutUint64(header[cursor+8:cursor+16], ref.count)
		cursor += segmentDataRefSize
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
	return OpenSegmentDataStore(w.path)
}

func (w *SegmentDataWriter) Abort() {
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

func validateSegmentStableEncoding(rankMetric Metric, encoding int64, internalDim int, vecBytes int) error {
	if internalDim <= 0 || vecBytes <= 0 {
		return fmt.Errorf("vecindex: invalid stable encoding dimensions")
	}
	switch encoding {
	case MemberEncodingRawPreparedF32:
		return fmt.Errorf("vecindex: raw stable encoding is retired")
	case MemberEncodingResidualInt8:
		if internalDim >= StablePQMinInternalDim {
			return fmt.Errorf("vecindex: residual-int8 stable encoding is only allowed for internal dim < %d", StablePQMinInternalDim)
		}
		want := quantize.EncodedResidualSize(rankMetric, internalDim, MemberResidualBlockSize)
		if vecBytes != want {
			return fmt.Errorf("vecindex: residual-int8 stable vec bytes=%d want=%d", vecBytes, want)
		}
		return nil
	case MemberEncodingResidualPQ8:
		if internalDim < StablePQMinInternalDim {
			return fmt.Errorf("vecindex: PQ stable encoding is only allowed for internal dim >= %d", StablePQMinInternalDim)
		}
		if vecBytes >= internalDim*4 {
			return fmt.Errorf("vecindex: PQ stable vec bytes=%d must be smaller than raw bytes=%d", vecBytes, internalDim*4)
		}
		return nil
	default:
		return fmt.Errorf("vecindex: invalid segment data encoding %d", encoding)
	}
}

func (s *SegmentDataStore) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *SegmentDataStore) Metric() Metric     { return s.metric }
func (s *SegmentDataStore) Encoding() int64    { return s.encoding }
func (s *SegmentDataStore) Dim() int           { return s.dim }
func (s *SegmentDataStore) InternalDim() int   { return s.internalDim }
func (s *SegmentDataStore) VecBytes() int      { return s.vecBytes }
func (s *SegmentDataStore) Epoch() uint64      { return s.epoch }
func (s *SegmentDataStore) Generation() uint64 { return s.generation }
func (s *SegmentDataStore) Path() string       { return s.path }
func (s *SegmentDataStore) MaxCluster() int    { return s.maxCluster }
func (s *SegmentDataStore) FileSize() int64    { return s.fileSize }
func (s *SegmentDataStore) RowCount() uint64   { return s.rowCount }

func (s *SegmentDataStore) ResetScanStats() {
	if s == nil {
		return
	}
	s.readBytes.Store(0)
	s.logicalBytes.Store(0)
	s.readBatches.Store(0)
}

func (s *SegmentDataStore) SnapshotScanStats() SegmentScanStats {
	if s == nil {
		return SegmentScanStats{}
	}
	return SegmentScanStats{
		ReadBytes:    s.readBytes.Load(),
		LogicalBytes: s.logicalBytes.Load(),
		ReadBatches:  s.readBatches.Load(),
	}
}

func (s *SegmentDataStore) ClusterCount(clusterID int64) uint64 {
	if s == nil || clusterID <= 0 || int(clusterID) > s.maxCluster {
		return 0
	}
	return s.refs[clusterID].count
}

func (s *SegmentDataStore) ClusterRowCounts() []uint64 {
	if s == nil || s.maxCluster == 0 {
		return nil
	}
	counts := make([]uint64, len(s.refs))
	for clusterID := 1; clusterID <= s.maxCluster; clusterID++ {
		counts[clusterID] = s.refs[clusterID].count
	}
	return counts
}

func (s *SegmentDataStore) FileOrderedClusters() []int64 {
	if s == nil || len(s.fileOrderRank) == 0 {
		return nil
	}
	out := make([]int64, 0, s.maxCluster)
	for _, span := range s.fileOrderedClusterSpans(allClustersUpTo(s.maxCluster)) {
		out = append(out, span.clusterID)
	}
	return out
}

func allClustersUpTo(maxCluster int) []int64 {
	if maxCluster <= 0 {
		return nil
	}
	out := make([]int64, maxCluster)
	for i := 1; i <= maxCluster; i++ {
		out[i-1] = int64(i)
	}
	return out
}

func (s *SegmentDataStore) ClusterSpan(clusterID int64) (offset int64, bytes int64, count uint64, ok bool) {
	if s == nil || clusterID <= 0 || int(clusterID) >= len(s.spanByCluster) {
		return 0, 0, 0, false
	}
	span := s.spanByCluster[clusterID]
	if span.count == 0 {
		return 0, 0, 0, false
	}
	return span.offset, span.bytes, span.count, true
}

func (s *SegmentDataStore) ReadEntryAt(offset uint64) (int64, []byte, error) {
	if s == nil || s.file == nil {
		return 0, nil, fmt.Errorf("vecindex: segment store is not open")
	}
	buf := make([]byte, s.entrySize)
	if _, err := s.file.ReadAt(buf, int64(offset)); err != nil {
		return 0, nil, err
	}
	rowID := int64(binary.LittleEndian.Uint64(buf[:8]))
	return rowID, append([]byte(nil), buf[8:s.entrySize]...), nil
}

func (s *SegmentDataStore) CopyClusterTo(clusterID int64, w io.Writer) (uint64, error) {
	if s == nil || s.file == nil {
		return 0, nil
	}
	offset, bytes, count, ok := s.ClusterSpan(clusterID)
	if !ok {
		return 0, nil
	}
	if _, err := io.CopyN(w, io.NewSectionReader(s.file, offset, bytes), bytes); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *SegmentDataStore) ScanCluster(clusterID int64, yield func(rowid int64, vecBytes []byte) bool) error {
	if s == nil || s.file == nil || clusterID <= 0 || int(clusterID) > s.maxCluster {
		return nil
	}
	ref := s.refs[clusterID]
	if ref.count == 0 {
		return nil
	}
	bufPtr := s.bufPool.Get().(*[]byte)
	defer s.bufPool.Put(bufPtr)
	buf := *bufPtr
	chunkRows := len(buf) / s.entrySize
	if chunkRows < 1 {
		chunkRows = 1
	}

	offset := int64(ref.offset)
	remaining := ref.count
	for remaining > 0 {
		rowsThisChunk := int(remaining)
		if rowsThisChunk > chunkRows {
			rowsThisChunk = chunkRows
		}
		chunkBytes := rowsThisChunk * s.entrySize
		if chunkBytes > len(buf) {
			buf = make([]byte, chunkBytes)
			*bufPtr = buf
		}
		if _, err := s.file.ReadAt(buf[:chunkBytes], offset); err != nil {
			return err
		}
		cursor := 0
		for i := 0; i < rowsThisChunk; i++ {
			rowid := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+8]))
			vec := buf[cursor+8 : cursor+s.entrySize]
			if !yield(rowid, vec) {
				return nil
			}
			cursor += s.entrySize
		}
		offset += int64(chunkBytes)
		remaining -= uint64(rowsThisChunk)
	}
	return nil
}

func (s *SegmentDataStore) ScanClusters(clusterIDs []int64, yield func(clusterID, rowid int64, vecBytes []byte) bool) error {
	for _, cid := range clusterIDs {
		currentCluster := cid
		if err := s.ScanCluster(cid, func(rowid int64, vecBytes []byte) bool {
			return yield(currentCluster, rowid, vecBytes)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *SegmentDataStore) ScanClustersFileOrder(clusterIDs []int64, yield func(clusterID, rowid int64, vecBytes []byte) bool) error {
	if s == nil || len(clusterIDs) == 0 {
		return nil
	}
	batches := s.clusterReadBatches(clusterIDs)
	if len(batches) == 0 {
		return nil
	}
	bufPtr := s.batchBufPool.Get().(*[]byte)
	maxBuf := s.batchBufferSize()
	defer func() {
		if cap(*bufPtr) > maxBuf {
			buf := make([]byte, maxBuf)
			*bufPtr = buf
		}
		s.batchBufPool.Put(bufPtr)
	}()
	buf := *bufPtr
	for _, batch := range batches {
		need := int(batch.end - batch.start)
		if need > len(buf) {
			buf = make([]byte, need)
			*bufPtr = buf
		}
		if _, err := s.file.ReadAt(buf[:need], batch.start); err != nil {
			return err
		}
		s.readBytes.Add(uint64(need))
		s.readBatches.Add(1)
		for _, span := range batch.spans {
			cursor := int(span.offset - batch.start)
			for i := uint64(0); i < span.count; i++ {
				rowid := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+8]))
				vec := buf[cursor+8 : cursor+s.entrySize]
				if !yield(span.clusterID, rowid, vec) {
					return nil
				}
				cursor += s.entrySize
			}
		}
	}
	return nil
}

func (s *SegmentDataStore) ScanClustersFileOrderSpans(clusterIDs []int64, yield func(clusterID int64, rows []byte, count uint64, entrySize int) bool) error {
	if s == nil || len(clusterIDs) == 0 {
		return nil
	}
	batches := s.clusterReadBatches(clusterIDs)
	if len(batches) == 0 {
		return nil
	}
	bufPtr := s.batchBufPool.Get().(*[]byte)
	maxBuf := s.batchBufferSize()
	defer func() {
		if cap(*bufPtr) > maxBuf {
			buf := make([]byte, maxBuf)
			*bufPtr = buf
		}
		s.batchBufPool.Put(bufPtr)
	}()
	buf := *bufPtr
	for _, batch := range batches {
		need := int(batch.end - batch.start)
		if need > len(buf) {
			buf = make([]byte, need)
			*bufPtr = buf
		}
		if _, err := s.file.ReadAt(buf[:need], batch.start); err != nil {
			return err
		}
		s.readBytes.Add(uint64(need))
		s.readBatches.Add(1)
		for _, span := range batch.spans {
			start := int(span.offset - batch.start)
			end := start + int(span.bytes)
			s.logicalBytes.Add(uint64(span.bytes))
			if !yield(span.clusterID, buf[start:end], span.count, s.entrySize) {
				return nil
			}
		}
	}
	return nil
}

func (s *SegmentDataStore) ScanBlockRecordsFileOrder(blocks []SegmentBlockRecord, yield func(clusterID int64, rows []byte, count uint64, entrySize int) bool) error {
	if s == nil || len(blocks) == 0 {
		return nil
	}
	spans := make([]segmentClusterSpan, 0, len(blocks))
	for _, block := range blocks {
		if block.RowCount == 0 || block.DataBytes <= 0 {
			continue
		}
		spans = append(spans, segmentClusterSpan{
			clusterID: block.ClusterID,
			offset:    block.DataOffset,
			bytes:     block.DataBytes,
			count:     block.RowCount,
		})
	}
	if len(spans) == 0 {
		return nil
	}
	slices.SortFunc(spans, func(a, b segmentClusterSpan) int {
		switch {
		case a.offset < b.offset:
			return -1
		case a.offset > b.offset:
			return 1
		default:
			return 0
		}
	})
	gap := int64(0)
	if s.encoding != MemberEncodingResidualPQ8 {
		gap = segmentReadGap
	}
	batches := planSegmentReadBatches(spans, int64(s.batchBufferSize()), gap)
	bufPtr := s.batchBufPool.Get().(*[]byte)
	maxBuf := s.batchBufferSize()
	defer func() {
		if cap(*bufPtr) > maxBuf {
			buf := make([]byte, maxBuf)
			*bufPtr = buf
		}
		s.batchBufPool.Put(bufPtr)
	}()
	buf := *bufPtr
	for _, batch := range batches {
		need := int(batch.end - batch.start)
		if need > len(buf) {
			buf = make([]byte, need)
			*bufPtr = buf
		}
		if _, err := s.file.ReadAt(buf[:need], batch.start); err != nil {
			return err
		}
		s.readBytes.Add(uint64(need))
		s.readBatches.Add(1)
		for _, span := range batch.spans {
			start := int(span.offset - batch.start)
			end := start + int(span.bytes)
			s.logicalBytes.Add(uint64(span.bytes))
			if !yield(span.clusterID, buf[start:end], span.count, s.entrySize) {
				return nil
			}
		}
	}
	return nil
}

func (s *SegmentDataStore) WarmClusters(clusterIDs []int64, maxBytes int64) error {
	if s == nil || s.file == nil || len(clusterIDs) == 0 {
		return nil
	}
	batches := s.clusterReadBatches(clusterIDs)
	if len(batches) == 0 {
		return nil
	}
	bufPtr := s.batchBufPool.Get().(*[]byte)
	maxBuf := s.batchBufferSize()
	defer func() {
		if cap(*bufPtr) > maxBuf {
			buf := make([]byte, maxBuf)
			*bufPtr = buf
		}
		s.batchBufPool.Put(bufPtr)
	}()
	buf := *bufPtr
	var warmed int64
	for _, batch := range batches {
		need64 := batch.end - batch.start
		if maxBytes > 0 {
			remaining := maxBytes - warmed
			if remaining <= 0 {
				break
			}
			if need64 > remaining {
				need64 = remaining
			}
		}
		if need64 <= 0 {
			continue
		}
		segmentFileReadAhead(s.file, batch.start, batch.end-batch.start)
		need := int(need64)
		if need > len(buf) {
			buf = make([]byte, need)
			*bufPtr = buf
		}
		if _, err := s.file.ReadAt(buf[:need], batch.start); err != nil && err != io.EOF {
			return err
		}
		warmed += need64
	}
	return nil
}

func (s *SegmentDataStore) fileOrderedClusterSpans(clusterIDs []int64) []segmentClusterSpan {
	if s == nil || len(clusterIDs) == 0 {
		return nil
	}
	ordered := make([]segmentClusterSpan, 0, len(clusterIDs))
	seenPtr := s.seenPool.Get().(*[]uint64)
	seen := *seenPtr
	clear(seen)
	defer s.seenPool.Put(seenPtr)
	for _, cid := range clusterIDs {
		if cid <= 0 || int(cid) > s.maxCluster {
			continue
		}
		word := cid / 64
		mask := uint64(1) << (cid % 64)
		if seen[word]&mask != 0 {
			continue
		}
		seen[word] |= mask
		span := s.spanByCluster[cid]
		if span.count == 0 {
			continue
		}
		ordered = append(ordered, span)
	}
	slices.SortFunc(ordered, func(a, b segmentClusterSpan) int {
		switch {
		case s.fileOrderRank[a.clusterID] < s.fileOrderRank[b.clusterID]:
			return -1
		case s.fileOrderRank[a.clusterID] > s.fileOrderRank[b.clusterID]:
			return 1
		default:
			return 0
		}
	})
	return ordered
}

func (s *SegmentDataStore) clusterReadBatches(clusterIDs []int64) []segmentReadBatchSpec {
	spans := s.fileOrderedClusterSpans(clusterIDs)
	if len(spans) == 0 {
		return nil
	}
	if s.encoding == MemberEncodingResidualPQ8 {
		return planSegmentReadBatches(spans, segmentPQReadBatch, segmentPQReadGap)
	}
	return planSegmentReadBatches(spans, segmentReadBatch, segmentReadGap)
}

func planSegmentReadBatches(spans []segmentClusterSpan, maxBatchBytes, maxGapBytes int64) []segmentReadBatchSpec {
	if len(spans) == 0 {
		return nil
	}
	if maxBatchBytes <= 0 {
		maxBatchBytes = segmentReadBatch
	}
	if maxGapBytes < 0 {
		maxGapBytes = 0
	}
	batches := make([]segmentReadBatchSpec, 0, len(spans))
	current := segmentReadBatchSpec{
		start: spans[0].offset,
		end:   spans[0].offset + spans[0].bytes,
		spans: []segmentClusterSpan{spans[0]},
	}
	for _, span := range spans[1:] {
		spanEnd := span.offset + span.bytes
		gap := span.offset - current.end
		if gap < 0 {
			gap = 0
		}
		if gap <= maxGapBytes && spanEnd-current.start <= maxBatchBytes {
			current.end = spanEnd
			current.spans = append(current.spans, span)
			continue
		}
		batches = append(batches, current)
		current = segmentReadBatchSpec{
			start: span.offset,
			end:   spanEnd,
			spans: []segmentClusterSpan{span},
		}
	}
	batches = append(batches, current)
	return batches
}
