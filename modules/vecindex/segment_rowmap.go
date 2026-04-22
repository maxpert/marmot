package vecindex

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	segmentRowMapMagic     = "MVSRMAP1"
	segmentRowMapVersion   = 1
	segmentRowMapHdrSize   = 40
	segmentRowMapEntrySize = 24
)

type SegmentRowLocation struct {
	RowID     int64
	ClusterID int64
	Offset    uint64
}

type SegmentRowMap struct {
	path       string
	file       *os.File
	epoch      uint64
	generation uint64
	entryCount uint64
	fileSize   int64
}

type SegmentRowMapWriter struct {
	path       string
	tmpPath    string
	file       *os.File
	epoch      uint64
	generation uint64
	entryCount uint64
	lastRowID  int64
	closed     bool
}

func OpenSegmentRowMap(path string) (*SegmentRowMap, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() < segmentRowMapHdrSize {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: segment rowmap %q is too small", path)
	}

	header := make([]byte, segmentRowMapHdrSize)
	if _, err := io.ReadFull(file, header); err != nil {
		_ = file.Close()
		return nil, err
	}
	if string(header[:8]) != segmentRowMapMagic {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: invalid segment rowmap magic %q", path)
	}
	if got := int(binary.LittleEndian.Uint32(header[8:12])); got != segmentRowMapVersion {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: unsupported segment rowmap version %d", got)
	}
	epoch := binary.LittleEndian.Uint64(header[16:24])
	generation := binary.LittleEndian.Uint64(header[24:32])
	entryCount := binary.LittleEndian.Uint64(header[32:40])
	wantSize := int64(segmentRowMapHdrSize) + int64(entryCount)*segmentRowMapEntrySize
	if info.Size() != wantSize {
		_ = file.Close()
		return nil, fmt.Errorf("vecindex: segment rowmap size mismatch: got=%d want=%d", info.Size(), wantSize)
	}
	return &SegmentRowMap{
		path:       path,
		file:       file,
		epoch:      epoch,
		generation: generation,
		entryCount: entryCount,
		fileSize:   info.Size(),
	}, nil
}

func CreateSegmentRowMapWriter(path string, epoch, generation uint64) (*SegmentRowMapWriter, error) {
	tmpPath := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(segmentRowMapHdrSize, io.SeekStart); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	return &SegmentRowMapWriter{
		path:       path,
		tmpPath:    tmpPath,
		file:       file,
		epoch:      epoch,
		generation: generation,
		lastRowID:  -1,
	}, nil
}

func (w *SegmentRowMapWriter) Append(rowID, clusterID int64, offset uint64) error {
	if w == nil || w.file == nil {
		return fmt.Errorf("vecindex: segment rowmap writer is closed")
	}
	if rowID <= 0 {
		return fmt.Errorf("vecindex: segment rowmap rowid %d invalid", rowID)
	}
	if clusterID <= 0 {
		return fmt.Errorf("vecindex: segment rowmap cluster %d invalid", clusterID)
	}
	if w.lastRowID >= 0 && rowID < w.lastRowID {
		return fmt.Errorf("vecindex: segment rowmap rows must be appended in rowid order")
	}
	var entry [segmentRowMapEntrySize]byte
	binary.LittleEndian.PutUint64(entry[0:8], uint64(rowID))
	binary.LittleEndian.PutUint32(entry[8:12], uint32(clusterID))
	binary.LittleEndian.PutUint64(entry[16:24], offset)
	if _, err := w.file.Write(entry[:]); err != nil {
		return err
	}
	w.entryCount++
	w.lastRowID = rowID
	return nil
}

func (w *SegmentRowMapWriter) Close() (*SegmentRowMap, error) {
	if w == nil || w.closed {
		return nil, fmt.Errorf("vecindex: segment rowmap writer already closed")
	}
	w.closed = true
	header := make([]byte, segmentRowMapHdrSize)
	copy(header[:8], []byte(segmentRowMapMagic))
	binary.LittleEndian.PutUint32(header[8:12], segmentRowMapVersion)
	binary.LittleEndian.PutUint64(header[16:24], w.epoch)
	binary.LittleEndian.PutUint64(header[24:32], w.generation)
	binary.LittleEndian.PutUint64(header[32:40], w.entryCount)
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
	return OpenSegmentRowMap(w.path)
}

func (w *SegmentRowMapWriter) Abort() {
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

func (m *SegmentRowMap) Close() error {
	if m == nil || m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

func (m *SegmentRowMap) Epoch() uint64      { return m.epoch }
func (m *SegmentRowMap) Generation() uint64 { return m.generation }
func (m *SegmentRowMap) EntryCount() uint64 { return m.entryCount }
func (m *SegmentRowMap) FileSize() int64    { return m.fileSize }
func (m *SegmentRowMap) Path() string       { return m.path }

func (m *SegmentRowMap) Lookup(rowID int64) (SegmentRowLocation, bool, error) {
	if m == nil || m.file == nil || rowID <= 0 || m.entryCount == 0 {
		return SegmentRowLocation{}, false, nil
	}
	var entry [segmentRowMapEntrySize]byte
	lo, hi := uint64(0), m.entryCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		offset := int64(segmentRowMapHdrSize) + int64(mid)*segmentRowMapEntrySize
		if _, err := m.file.ReadAt(entry[:], offset); err != nil {
			return SegmentRowLocation{}, false, err
		}
		gotRowID := int64(binary.LittleEndian.Uint64(entry[0:8]))
		switch {
		case gotRowID < rowID:
			lo = mid + 1
		case gotRowID > rowID:
			hi = mid
		default:
			return SegmentRowLocation{
				RowID:     gotRowID,
				ClusterID: int64(binary.LittleEndian.Uint32(entry[8:12])),
				Offset:    binary.LittleEndian.Uint64(entry[16:24]),
			}, true, nil
		}
	}
	return SegmentRowLocation{}, false, nil
}

func (m *SegmentRowMap) Scan(visit func(SegmentRowLocation) bool) error {
	if m == nil || m.file == nil || visit == nil || m.entryCount == 0 {
		return nil
	}
	buf := make([]byte, segmentRowMapEntrySize*1024)
	offset := int64(segmentRowMapHdrSize)
	remaining := m.entryCount
	for remaining > 0 {
		entriesThisChunk := int(remaining)
		if entriesThisChunk > 1024 {
			entriesThisChunk = 1024
		}
		chunkBytes := entriesThisChunk * segmentRowMapEntrySize
		if _, err := m.file.ReadAt(buf[:chunkBytes], offset); err != nil {
			return err
		}
		cursor := 0
		for i := 0; i < entriesThisChunk; i++ {
			loc := SegmentRowLocation{
				RowID:     int64(binary.LittleEndian.Uint64(buf[cursor : cursor+8])),
				ClusterID: int64(binary.LittleEndian.Uint32(buf[cursor+8 : cursor+12])),
				Offset:    binary.LittleEndian.Uint64(buf[cursor+16 : cursor+24]),
			}
			if !visit(loc) {
				return nil
			}
			cursor += segmentRowMapEntrySize
		}
		offset += int64(chunkBytes)
		remaining -= uint64(entriesThisChunk)
	}
	return nil
}
