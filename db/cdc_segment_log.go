package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	cdcSegmentDirName        = "cdcseg"
	cdcSegmentFileSize       = 64 << 20
	cdcSegmentInlineMaxBytes = 1 << 20
	cdcSegmentSyncBytes      = 16 << 20
	cdcSegmentSyncTxns       = 4096
	cdcSegmentSyncMaxWait    = 25 * time.Millisecond
	cdcSegmentRecordMagic    = uint32(0x4d434443) // "MCDC"
	cdcSegmentRecordVersion  = uint16(1)
	cdcSegmentRecordRow      = uint16(1)
	cdcSegmentHeaderSize     = 32
	cdcSegmentMaxPayloadSize = cdcSegmentFileSize - cdcSegmentHeaderSize
)

var errCDCSegmentTxnNotFound = errors.New("cdc segment transaction not found")

type cdcSegmentChunk struct {
	SegmentID uint64 `msgpack:"s"`
	Offset    uint64 `msgpack:"o"`
	Length    uint64 `msgpack:"l"`
}

type cdcSegmentTxnManifest struct {
	TxnID    uint64            `msgpack:"t"`
	RowCount uint64            `msgpack:"r"`
	FirstSeq uint64            `msgpack:"f"`
	LastSeq  uint64            `msgpack:"z"`
	Chunks   []cdcSegmentChunk `msgpack:"c"`
	CRC32    uint32            `msgpack:"x"`

	inlineRows     []cdcSegmentInlineRow
	inlineBytes    uint64
	inlineDisabled bool
}

type cdcSegmentInlineRow struct {
	seq  uint64
	data []byte
}

type cdcSegmentLog struct {
	dir string

	mu        sync.Mutex
	file      *os.File
	segmentID uint64
	offset    uint64
	pending   map[uint64]*cdcSegmentTxnManifest

	syncer   *cdcSegmentSyncer
	syncFile func(*os.File) error
}

func openCDCSegmentLog(basePath string) (*cdcSegmentLog, error) {
	dir := filepath.Join(basePath, cdcSegmentDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segmentID, offset, err := recoverCDCSegmentTail(dir)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(cdcSegmentPath(dir, segmentID), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	log := &cdcSegmentLog{
		dir:       dir,
		file:      f,
		segmentID: segmentID,
		offset:    offset,
		pending:   make(map[uint64]*cdcSegmentTxnManifest),
		syncFile:  func(f *os.File) error { return f.Sync() },
	}
	log.syncer = newCDCSegmentSyncer(log)
	return log, nil
}

func recoverCDCSegmentTail(dir string) (uint64, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1, 0, err
	}
	var ids []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if id, ok := parseCDCSegmentID(entry.Name()); ok {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return 1, 0, nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	id := ids[len(ids)-1]
	path := cdcSegmentPath(dir, id)
	offset, err := validateCDCSegment(path)
	if err != nil {
		return 0, 0, err
	}
	if err := os.Truncate(path, int64(offset)); err != nil {
		return 0, 0, err
	}
	return id, offset, nil
}

func validateCDCSegment(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	var offset uint64
	header := make([]byte, cdcSegmentHeaderSize)
	for {
		n, err := io.ReadFull(f, header)
		if err == io.EOF {
			return offset, nil
		}
		if err == io.ErrUnexpectedEOF {
			return offset, nil
		}
		if err != nil {
			return 0, err
		}
		if n != cdcSegmentHeaderSize {
			return offset, nil
		}
		magic := binary.LittleEndian.Uint32(header[0:4])
		version := binary.LittleEndian.Uint16(header[4:6])
		payloadLen := binary.LittleEndian.Uint32(header[24:28])
		payloadCRC := binary.LittleEndian.Uint32(header[28:32])
		if magic != cdcSegmentRecordMagic || version != cdcSegmentRecordVersion {
			return offset, nil
		}
		if uint64(payloadLen) > cdcSegmentMaxPayloadSize {
			return offset, nil
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			return offset, nil
		}
		if crc32.ChecksumIEEE(payload) != payloadCRC {
			return offset, nil
		}
		offset += cdcSegmentHeaderSize + uint64(payloadLen)
	}
}

func cdcSegmentPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("seg-%018d.log", id))
}

func parseCDCSegmentID(name string) (uint64, bool) {
	var id uint64
	if _, err := fmt.Sscanf(name, "seg-%018d.log", &id); err != nil || id == 0 {
		return 0, false
	}
	if name != fmt.Sprintf("seg-%018d.log", id) {
		return 0, false
	}
	return id, true
}

func (l *cdcSegmentLog) appendRow(txnID, seq uint64, payload []byte) error {
	_, err := l.appendRecord(cdcSegmentRecordRow, txnID, seq, payload)
	return err
}

func (l *cdcSegmentLog) appendRecord(recordType uint16, txnID, seq uint64, payload []byte) (cdcSegmentChunk, error) {
	if len(payload) > int(^uint32(0)) {
		return cdcSegmentChunk{}, fmt.Errorf("cdc row payload too large: %d bytes", len(payload))
	}
	recordLen := cdcSegmentHeaderSize + uint64(len(payload))
	if recordLen > cdcSegmentFileSize {
		return cdcSegmentChunk{}, fmt.Errorf("cdc segment record too large: payload=%d max=%d", len(payload), cdcSegmentMaxPayloadSize)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.offset > 0 && l.offset+recordLen > cdcSegmentFileSize {
		if err := l.file.Close(); err != nil {
			return cdcSegmentChunk{}, err
		}
		l.segmentID++
		l.offset = 0
		f, err := os.OpenFile(cdcSegmentPath(l.dir, l.segmentID), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return cdcSegmentChunk{}, err
		}
		l.file = f
	}

	offset := l.offset
	record := make([]byte, cdcSegmentHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(record[0:4], cdcSegmentRecordMagic)
	binary.LittleEndian.PutUint16(record[4:6], cdcSegmentRecordVersion)
	binary.LittleEndian.PutUint16(record[6:8], recordType)
	binary.LittleEndian.PutUint64(record[8:16], txnID)
	binary.LittleEndian.PutUint64(record[16:24], seq)
	binary.LittleEndian.PutUint32(record[24:28], uint32(len(payload)))
	binary.LittleEndian.PutUint32(record[28:32], crc32.ChecksumIEEE(payload))
	copy(record[cdcSegmentHeaderSize:], payload)

	if _, err := l.file.Write(record); err != nil {
		return cdcSegmentChunk{}, err
	}
	l.offset += recordLen
	chunk := cdcSegmentChunk{SegmentID: l.segmentID, Offset: offset, Length: recordLen}

	if recordType != cdcSegmentRecordRow {
		return chunk, nil
	}

	manifest := l.pending[txnID]
	if manifest == nil {
		manifest = &cdcSegmentTxnManifest{TxnID: txnID, FirstSeq: seq, LastSeq: seq}
		l.pending[txnID] = manifest
	}
	manifest.RowCount++
	if seq < manifest.FirstSeq {
		manifest.FirstSeq = seq
	}
	if seq > manifest.LastSeq {
		manifest.LastSeq = seq
	}
	manifest.CRC32 = crc32.Update(manifest.CRC32, crc32.IEEETable, payload)
	if !manifest.inlineDisabled {
		if manifest.inlineBytes+uint64(len(payload)) <= cdcSegmentInlineMaxBytes {
			data := append([]byte(nil), payload...)
			manifest.inlineRows = append(manifest.inlineRows, cdcSegmentInlineRow{seq: seq, data: data})
			manifest.inlineBytes += uint64(len(data))
		} else {
			manifest.inlineRows = nil
			manifest.inlineBytes = 0
			manifest.inlineDisabled = true
		}
	}
	if n := len(manifest.Chunks); n > 0 {
		last := &manifest.Chunks[n-1]
		if last.SegmentID == chunk.SegmentID && last.Offset+last.Length == chunk.Offset {
			last.Length += chunk.Length
		} else {
			manifest.Chunks = append(manifest.Chunks, chunk)
		}
	} else {
		manifest.Chunks = append(manifest.Chunks, chunk)
	}
	return chunk, nil
}

func (l *cdcSegmentLog) sealTxn(txnID uint64) (*cdcSegmentTxnManifest, error) {
	l.mu.Lock()
	manifest := cloneCDCManifest(l.pending[txnID])
	l.mu.Unlock()
	if manifest == nil {
		return nil, errCDCSegmentTxnNotFound
	}
	l.syncer.queue(manifest)
	return manifest, nil
}

func (l *cdcSegmentLog) getPendingManifest(txnID uint64) *cdcSegmentTxnManifest {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := l.pending[txnID]
	if m == nil {
		return nil
	}
	return cloneCDCManifest(m)
}

func (l *cdcSegmentLog) discardTxn(txnID uint64) {
	l.mu.Lock()
	delete(l.pending, txnID)
	l.mu.Unlock()
}

func (l *cdcSegmentLog) addRetainedSegments(retained map[uint64]struct{}) {
	l.mu.Lock()
	currentID := l.segmentID
	if currentID > 0 {
		retained[currentID] = struct{}{}
	}
	for _, manifest := range l.pending {
		addCDCManifestSegments(retained, manifest)
	}
	l.mu.Unlock()

	l.syncer.addRetainedSegments(retained)
}

func (l *cdcSegmentLog) gcSegments(retained map[uint64]struct{}) (int, error) {
	if l == nil {
		return 0, nil
	}
	if retained == nil {
		retained = make(map[uint64]struct{})
	}
	l.addRetainedSegments(retained)

	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		segmentID, ok := parseCDCSegmentID(entry.Name())
		if !ok {
			continue
		}
		if _, ok := retained[segmentID]; ok {
			continue
		}
		err := os.Remove(cdcSegmentPath(l.dir, segmentID))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func addCDCManifestSegments(retained map[uint64]struct{}, manifest *cdcSegmentTxnManifest) {
	if manifest == nil {
		return
	}
	for _, chunk := range manifest.Chunks {
		if chunk.SegmentID > 0 {
			retained[chunk.SegmentID] = struct{}{}
		}
	}
}

func (l *cdcSegmentLog) pendingTxnIDs() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]uint64, 0, len(l.pending))
	for txnID := range l.pending {
		ids = append(ids, txnID)
	}
	return ids
}

func (l *cdcSegmentLog) close() error {
	if l == nil {
		return nil
	}
	l.syncer.close()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Sync()
	if closeErr := l.file.Close(); err == nil {
		err = closeErr
	}
	l.file = nil
	return err
}

func cloneCDCManifest(m *cdcSegmentTxnManifest) *cdcSegmentTxnManifest {
	if m == nil {
		return nil
	}
	cp := *m
	if len(m.Chunks) > 0 {
		cp.Chunks = append([]cdcSegmentChunk(nil), m.Chunks...)
	}
	if len(m.inlineRows) > 0 {
		cp.inlineRows = make([]cdcSegmentInlineRow, len(m.inlineRows))
		for i, row := range m.inlineRows {
			cp.inlineRows[i] = cdcSegmentInlineRow{
				seq:  row.seq,
				data: append([]byte(nil), row.data...),
			}
		}
	}
	return &cp
}

func cloneCDCManifestForSync(m *cdcSegmentTxnManifest) *cdcSegmentTxnManifest {
	if m == nil {
		return nil
	}
	return &cdcSegmentTxnManifest{
		TxnID:    m.TxnID,
		RowCount: m.RowCount,
		FirstSeq: m.FirstSeq,
		LastSeq:  m.LastSeq,
		Chunks:   append([]cdcSegmentChunk(nil), m.Chunks...),
		CRC32:    m.CRC32,
	}
}

type cdcSegmentSyncer struct {
	log *cdcSegmentLog
	ch  chan *cdcSegmentSyncRequest
	wg  sync.WaitGroup

	mu          sync.Mutex
	segmentRefs map[uint64]uint64
}

type cdcSegmentSyncRequest struct {
	manifest *cdcSegmentTxnManifest
}

func newCDCSegmentSyncer(log *cdcSegmentLog) *cdcSegmentSyncer {
	s := &cdcSegmentSyncer{
		log:         log,
		ch:          make(chan *cdcSegmentSyncRequest, 8192),
		segmentRefs: make(map[uint64]uint64),
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

func (s *cdcSegmentSyncer) queue(manifest *cdcSegmentTxnManifest) {
	req := &cdcSegmentSyncRequest{manifest: cloneCDCManifestForSync(manifest)}
	s.retainSegments(req.manifest)
	s.ch <- req
}

func (s *cdcSegmentSyncer) close() {
	close(s.ch)
	s.wg.Wait()
}

func (s *cdcSegmentSyncer) loop() {
	defer s.wg.Done()
	var batch []*cdcSegmentSyncRequest
	var bytes uint64
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		err := s.syncSegments(batch)
		if err != nil {
			log.Warn().Err(err).Msg("CDC segment async sync failed")
		}
		s.releaseSegments(batch)
		batch = nil
		bytes = 0
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	for {
		if len(batch) == 0 {
			req, ok := <-s.ch
			if !ok {
				return
			}
			batch = append(batch, req)
			bytes += manifestBytes(req.manifest)
			timer.Reset(cdcSegmentSyncMaxWait)
		}

		if bytes >= cdcSegmentSyncBytes || len(batch) >= cdcSegmentSyncTxns {
			flush()
			continue
		}

		select {
		case req, ok := <-s.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, req)
			bytes += manifestBytes(req.manifest)
		case <-timer.C:
			flush()
		}
	}
}

func (s *cdcSegmentSyncer) retainSegments(manifest *cdcSegmentTxnManifest) {
	if s == nil || manifest == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, chunk := range manifest.Chunks {
		if chunk.SegmentID > 0 {
			s.segmentRefs[chunk.SegmentID]++
		}
	}
}

func (s *cdcSegmentSyncer) releaseSegments(batch []*cdcSegmentSyncRequest) {
	if s == nil || len(batch) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range batch {
		if req == nil || req.manifest == nil {
			continue
		}
		for _, chunk := range req.manifest.Chunks {
			if chunk.SegmentID == 0 {
				continue
			}
			refs := s.segmentRefs[chunk.SegmentID]
			if refs <= 1 {
				delete(s.segmentRefs, chunk.SegmentID)
				continue
			}
			s.segmentRefs[chunk.SegmentID] = refs - 1
		}
	}
}

func (s *cdcSegmentSyncer) addRetainedSegments(retained map[uint64]struct{}) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for segmentID := range s.segmentRefs {
		retained[segmentID] = struct{}{}
	}
}

func (s *cdcSegmentSyncer) syncSegments(batch []*cdcSegmentSyncRequest) error {
	segments := make(map[uint64]struct{})
	for _, req := range batch {
		if req == nil || req.manifest == nil {
			continue
		}
		for _, chunk := range req.manifest.Chunks {
			segments[chunk.SegmentID] = struct{}{}
		}
	}
	for segmentID := range segments {
		if err := s.log.syncSegment(segmentID); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func (l *cdcSegmentLog) syncSegment(segmentID uint64) error {
	syncFile := l.syncFile
	if syncFile == nil {
		syncFile = func(f *os.File) error { return f.Sync() }
	}
	f, err := os.OpenFile(cdcSegmentPath(l.dir, segmentID), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	err = syncFile(f)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func manifestBytes(manifest *cdcSegmentTxnManifest) uint64 {
	var n uint64
	for _, chunk := range manifest.Chunks {
		n += chunk.Length
	}
	return n
}

type cdcSegmentCursor struct {
	log       *cdcSegmentLog
	manifest  *cdcSegmentTxnManifest
	chunkIdx  int
	pos       uint64
	file      *os.File
	fileSeg   uint64
	inlineIdx int
	seq       uint64
	data      []byte
	err       error
}

func newCDCSegmentCursor(log *cdcSegmentLog, manifest *cdcSegmentTxnManifest) *cdcSegmentCursor {
	return &cdcSegmentCursor{log: log, manifest: cloneCDCManifest(manifest)}
}

func (c *cdcSegmentCursor) Next() bool {
	if c.err != nil || c.manifest == nil {
		return false
	}
	if len(c.manifest.inlineRows) > 0 {
		if c.inlineIdx >= len(c.manifest.inlineRows) {
			return false
		}
		row := c.manifest.inlineRows[c.inlineIdx]
		c.inlineIdx++
		c.seq = row.seq
		c.data = row.data
		return true
	}
	header := make([]byte, cdcSegmentHeaderSize)
	for c.chunkIdx < len(c.manifest.Chunks) {
		chunk := c.manifest.Chunks[c.chunkIdx]
		if c.pos == 0 {
			c.pos = chunk.Offset
		}
		if c.pos >= chunk.Offset+chunk.Length {
			c.closeFile()
			c.chunkIdx++
			c.pos = 0
			continue
		}
		f, err := c.openChunkFile(chunk.SegmentID)
		if err != nil {
			c.err = err
			return false
		}
		_, err = f.ReadAt(header, int64(c.pos))
		if err != nil {
			c.err = err
			return false
		}
		magic := binary.LittleEndian.Uint32(header[0:4])
		version := binary.LittleEndian.Uint16(header[4:6])
		recordType := binary.LittleEndian.Uint16(header[6:8])
		txnID := binary.LittleEndian.Uint64(header[8:16])
		seq := binary.LittleEndian.Uint64(header[16:24])
		payloadLen := binary.LittleEndian.Uint32(header[24:28])
		payloadCRC := binary.LittleEndian.Uint32(header[28:32])
		if magic != cdcSegmentRecordMagic || version != cdcSegmentRecordVersion || recordType != cdcSegmentRecordRow {
			c.err = fmt.Errorf("invalid cdc segment record at segment=%d offset=%d", chunk.SegmentID, c.pos)
			return false
		}
		if uint64(payloadLen) > cdcSegmentMaxPayloadSize {
			c.err = fmt.Errorf("cdc segment payload too large at segment=%d offset=%d", chunk.SegmentID, c.pos)
			return false
		}
		payload := make([]byte, payloadLen)
		_, err = f.ReadAt(payload, int64(c.pos+cdcSegmentHeaderSize))
		if err != nil {
			c.err = err
			return false
		}
		c.pos += cdcSegmentHeaderSize + uint64(payloadLen)
		if crc32.ChecksumIEEE(payload) != payloadCRC {
			c.err = fmt.Errorf("cdc segment crc mismatch at segment=%d offset=%d", chunk.SegmentID, c.pos)
			return false
		}
		if txnID != c.manifest.TxnID {
			continue
		}
		c.seq = seq
		c.data = payload
		return true
	}
	return false
}

func (c *cdcSegmentCursor) Row() (uint64, []byte) {
	return c.seq, c.data
}

func (c *cdcSegmentCursor) Err() error {
	return c.err
}

func (c *cdcSegmentCursor) Close() error {
	return c.closeFile()
}

func (c *cdcSegmentCursor) openChunkFile(segmentID uint64) (*os.File, error) {
	if c.file != nil && c.fileSeg == segmentID {
		return c.file, nil
	}
	if err := c.closeFile(); err != nil {
		return nil, err
	}
	f, err := os.Open(cdcSegmentPath(c.log.dir, segmentID))
	if err != nil {
		return nil, err
	}
	c.file = f
	c.fileSeg = segmentID
	return f, nil
}

func (c *cdcSegmentCursor) closeFile() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	c.fileSeg = 0
	return err
}
