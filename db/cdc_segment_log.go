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

	"github.com/maxpert/marmot/encoding"
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
	cdcSegmentRecordPrepare  = uint16(2)
	cdcSegmentHeaderSize     = 32
)

var errCDCSegmentTxnNotFound = errors.New("cdc segment transaction not found")

type cdcSegmentChunk struct {
	SegmentID uint64 `msgpack:"s"`
	Offset    uint64 `msgpack:"o"`
	Length    uint64 `msgpack:"l"`
}

type cdcSegmentTxnManifest struct {
	TxnID          uint64            `msgpack:"t"`
	NodeID         uint64            `msgpack:"n"`
	StartTSWall    int64             `msgpack:"w"`
	StartTSLogical int32             `msgpack:"l"`
	RowCount       uint64            `msgpack:"r"`
	FirstSeq       uint64            `msgpack:"f"`
	LastSeq        uint64            `msgpack:"z"`
	Chunks         []cdcSegmentChunk `msgpack:"c"`
	CRC32          uint32            `msgpack:"x"`

	inlineRows     []cdcSegmentInlineRow
	inlineBytes    uint64
	inlineDisabled bool
	sealed         bool
	published      bool
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

	syncer *cdcSegmentSyncer
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
	}
	prepared, err := recoverCDCSegmentPreparedManifests(dir)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	for txnID, manifest := range prepared {
		log.pending[txnID] = manifest
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
		var id uint64
		if _, err := fmt.Sscanf(entry.Name(), "seg-%018d.log", &id); err == nil && id > 0 {
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

func recoverCDCSegmentPreparedManifests(dir string) (map[uint64]*cdcSegmentTxnManifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id uint64
		if _, err := fmt.Sscanf(entry.Name(), "seg-%018d.log", &id); err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	prepared := make(map[uint64]*cdcSegmentTxnManifest)
	header := make([]byte, cdcSegmentHeaderSize)
	for _, id := range ids {
		path := cdcSegmentPath(dir, id)
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		var offset uint64
		for {
			n, err := io.ReadFull(f, header)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			if err != nil {
				_ = f.Close()
				return nil, err
			}
			if n != cdcSegmentHeaderSize {
				break
			}
			magic := binary.LittleEndian.Uint32(header[0:4])
			version := binary.LittleEndian.Uint16(header[4:6])
			recordType := binary.LittleEndian.Uint16(header[6:8])
			txnID := binary.LittleEndian.Uint64(header[8:16])
			payloadLen := binary.LittleEndian.Uint32(header[24:28])
			payloadCRC := binary.LittleEndian.Uint32(header[28:32])
			if magic != cdcSegmentRecordMagic || version != cdcSegmentRecordVersion {
				break
			}
			if uint64(payloadLen) > cdcSegmentFileSize {
				break
			}
			payload := make([]byte, payloadLen)
			if _, err := io.ReadFull(f, payload); err != nil {
				break
			}
			if crc32.ChecksumIEEE(payload) != payloadCRC {
				break
			}
			if recordType == cdcSegmentRecordPrepare && payloadLen > 0 {
				var manifest cdcSegmentTxnManifest
				if err := encoding.Unmarshal(payload, &manifest); err != nil {
					_ = f.Close()
					return nil, fmt.Errorf("recover CDC prepare manifest txn %d at %s:%d: %w", txnID, path, offset, err)
				}
				if manifest.TxnID != txnID {
					_ = f.Close()
					return nil, fmt.Errorf("recover CDC prepare manifest txn mismatch: header=%d payload=%d", txnID, manifest.TxnID)
				}
				prepared[txnID] = cloneCDCManifest(&manifest)
			}
			offset += cdcSegmentHeaderSize + uint64(payloadLen)
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return prepared, nil
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
		if uint64(payloadLen) > cdcSegmentFileSize {
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
		return cdcSegmentChunk{}, fmt.Errorf("cdc segment record too large: %d bytes", recordLen)
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

func (l *cdcSegmentLog) sealTxn(txnID uint64, immutable *TxnImmutableRecord, waitSync bool) (*cdcSegmentTxnManifest, error) {
	l.mu.Lock()
	pending := l.pending[txnID]
	if pending != nil && immutable != nil {
		pending.NodeID = immutable.NodeID
		pending.StartTSWall = immutable.StartTSWall
		pending.StartTSLogical = immutable.StartTSLogical
	}
	if pending != nil && pending.sealed {
		manifest := cloneCDCManifest(pending)
		l.mu.Unlock()
		return manifest, nil
	}
	manifest := cloneCDCManifest(pending)
	l.mu.Unlock()
	if manifest == nil {
		return nil, errCDCSegmentTxnNotFound
	}
	syncManifest := cloneCDCManifestForSync(manifest)
	if waitSync {
		payload, err := encoding.Marshal(manifest)
		if err != nil {
			return nil, err
		}
		prepareChunk, err := l.appendRecord(cdcSegmentRecordPrepare, txnID, 0, payload)
		if err != nil {
			return nil, err
		}
		syncManifest.Chunks = append(syncManifest.Chunks, prepareChunk)
		if err := l.syncer.sync(syncManifest); err != nil {
			return nil, err
		}
	} else {
		l.syncer.queue(syncManifest)
	}
	l.mu.Lock()
	if pending := l.pending[txnID]; pending != nil {
		pending.sealed = true
	}
	l.mu.Unlock()
	return manifest, nil
}

func (l *cdcSegmentLog) markTxnManifestPublished(txnID uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if pending := l.pending[txnID]; pending != nil {
		pending.published = true
	}
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
		TxnID:          m.TxnID,
		NodeID:         m.NodeID,
		StartTSWall:    m.StartTSWall,
		StartTSLogical: m.StartTSLogical,
		RowCount:       m.RowCount,
		FirstSeq:       m.FirstSeq,
		LastSeq:        m.LastSeq,
		Chunks:         append([]cdcSegmentChunk(nil), m.Chunks...),
		CRC32:          m.CRC32,
	}
}

type cdcSegmentSyncer struct {
	log *cdcSegmentLog
	ch  chan *cdcSegmentSyncRequest
	wg  sync.WaitGroup
}

type cdcSegmentSyncRequest struct {
	manifest *cdcSegmentTxnManifest
	done     chan error
}

func newCDCSegmentSyncer(log *cdcSegmentLog) *cdcSegmentSyncer {
	s := &cdcSegmentSyncer{
		log: log,
		ch:  make(chan *cdcSegmentSyncRequest, 8192),
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

func (s *cdcSegmentSyncer) sync(manifest *cdcSegmentTxnManifest) error {
	if manifest == nil || len(manifest.Chunks) == 0 {
		return nil
	}
	done := make(chan error, 1)
	s.ch <- &cdcSegmentSyncRequest{manifest: cloneCDCManifestForSync(manifest), done: done}
	return <-done
}

func (s *cdcSegmentSyncer) queue(manifest *cdcSegmentTxnManifest) {
	if manifest == nil || len(manifest.Chunks) == 0 {
		return
	}
	s.ch <- &cdcSegmentSyncRequest{manifest: cloneCDCManifestForSync(manifest)}
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
		for _, req := range batch {
			if req != nil && req.done != nil {
				req.done <- err
			}
		}
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
			return err
		}
	}
	return nil
}

func (l *cdcSegmentLog) syncSegment(segmentID uint64) error {
	l.mu.Lock()
	currentID := l.segmentID
	currentFile := l.file
	l.mu.Unlock()
	if segmentID == currentID && currentFile != nil {
		if err := currentFile.Sync(); err == nil {
			return nil
		}
	}

	f, err := os.OpenFile(cdcSegmentPath(l.dir, segmentID), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	err = f.Sync()
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
			c.chunkIdx++
			c.pos = 0
			continue
		}
		_, err := c.log.readAt(chunk.SegmentID, header, int64(c.pos))
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
		if uint64(payloadLen) > cdcSegmentFileSize {
			c.err = fmt.Errorf("cdc segment payload too large at segment=%d offset=%d", chunk.SegmentID, c.pos)
			return false
		}
		payload := make([]byte, payloadLen)
		_, err = c.log.readAt(chunk.SegmentID, payload, int64(c.pos+cdcSegmentHeaderSize))
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
	return nil
}

func (l *cdcSegmentLog) readAt(segmentID uint64, p []byte, offset int64) (int, error) {
	l.mu.Lock()
	currentID := l.segmentID
	currentFile := l.file
	l.mu.Unlock()
	if segmentID == currentID && currentFile != nil {
		return currentFile.ReadAt(p, offset)
	}
	f, err := os.Open(cdcSegmentPath(l.dir, segmentID))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.ReadAt(p, offset)
}
