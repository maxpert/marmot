package intentlog

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// Magic bytes for file identification
	Magic = "MLOG"
	// Current format version
	Version uint16 = 1
	// Header size in bytes
	HeaderSize = 24

	// Operation types
	OpInsert uint8 = 0
	OpUpdate uint8 = 1
	OpDelete uint8 = 2
)

var (
	ErrInvalidMagic    = errors.New("invalid magic bytes")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrChecksumFailed  = errors.New("checksum verification failed")
	ErrLogClosed       = errors.New("log is closed")
)

// Entry represents a single CDC entry captured during preupdate_hook
type Entry struct {
	Seq       uint64
	Operation uint8
	Table     string
	RowKey    string            // Serialized primary key
	OldValues map[string][]byte // nil for INSERT
	NewValues map[string][]byte // nil for DELETE
}

// Log is a write-ahead log for capturing CDC entries during transaction execution
type Log struct {
	mu        sync.Mutex
	file      *os.File
	writer    *bufio.Writer
	txnID     uint64
	createdAt time.Time
	seq       uint64
	closed    bool
	path      string
}

// New creates a new intent log for the given transaction
func New(txnID uint64, dataDir string) (*Log, error) {
	logDir := filepath.Join(dataDir, "intent_logs")
	if err := os.MkdirAll(logDir, 0750); err != nil {
		return nil, fmt.Errorf("create intent_logs dir: %w", err)
	}

	path := filepath.Join(logDir, fmt.Sprintf("%d.log", txnID))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0640)
	if err != nil {
		return nil, fmt.Errorf("create log file: %w", err)
	}

	l := &Log{
		file:      file,
		writer:    bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		txnID:     txnID,
		createdAt: time.Now(),
		path:      path,
	}

	if err := l.writeHeader(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write header: %w", err)
	}

	return l, nil
}

// writeHeader writes the file header
func (l *Log) writeHeader() error {
	header := make([]byte, HeaderSize)
	copy(header[0:4], Magic)
	binary.LittleEndian.PutUint16(header[4:6], Version)
	// reserved at 6:8
	binary.LittleEndian.PutUint64(header[8:16], l.txnID)
	binary.LittleEndian.PutUint64(header[16:24], uint64(l.createdAt.UnixNano()))

	_, err := l.writer.Write(header)
	return err
}

// Append writes an entry to the log
func (l *Log) Append(e *Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogClosed
	}

	l.seq++
	e.Seq = l.seq

	data, err := l.encodeEntry(e)
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	_, err = l.writer.Write(data)
	return err
}

// encodeEntry serializes an entry to bytes
func (l *Log) encodeEntry(e *Entry) ([]byte, error) {
	// Serialize old/new values as JSON
	var oldData, newData []byte
	var err error

	if e.OldValues != nil {
		oldData, err = json.Marshal(e.OldValues)
		if err != nil {
			return nil, fmt.Errorf("marshal old values: %w", err)
		}
	}

	if e.NewValues != nil {
		newData, err = json.Marshal(e.NewValues)
		if err != nil {
			return nil, fmt.Errorf("marshal new values: %w", err)
		}
	}

	tableBytes := []byte(e.Table)
	keyBytes := []byte(e.RowKey)

	// Calculate total size
	// entry_len(4) + seq(8) + op(1) + table_len(2) + table + key_len(4) + key
	// + old_len(4) + old + new_len(4) + new + checksum(4)
	size := 4 + 8 + 1 + 2 + len(tableBytes) + 4 + len(keyBytes) +
		4 + len(oldData) + 4 + len(newData) + 4

	buf := make([]byte, size)
	offset := 0

	// Entry length (excluding this field)
	binary.LittleEndian.PutUint32(buf[offset:], uint32(size-4))
	offset += 4

	binary.LittleEndian.PutUint64(buf[offset:], e.Seq)
	offset += 8

	buf[offset] = e.Operation
	offset++

	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(tableBytes)))
	offset += 2

	copy(buf[offset:], tableBytes)
	offset += len(tableBytes)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(keyBytes)))
	offset += 4

	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(oldData)))
	offset += 4

	if len(oldData) > 0 {
		copy(buf[offset:], oldData)
		offset += len(oldData)
	}

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(newData)))
	offset += 4

	if len(newData) > 0 {
		copy(buf[offset:], newData)
		offset += len(newData)
	}

	// CRC32 checksum of everything except entry_len and checksum fields
	// When reading, we read entry_len separately, so checksum must be computed on data[4:]
	checksum := crc32.ChecksumIEEE(buf[4:offset])
	binary.LittleEndian.PutUint32(buf[offset:], checksum)

	return buf, nil
}

// Flush flushes buffered data to disk
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogClosed
	}

	if err := l.writer.Flush(); err != nil {
		return err
	}
	return l.file.Sync()
}

// Close closes the log file
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true
	if err := l.writer.Flush(); err != nil {
		l.file.Close()
		return err
	}
	return l.file.Close()
}

// Delete closes and removes the log file
func (l *Log) Delete() error {
	l.Close()
	return os.Remove(l.path)
}

// TxnID returns the transaction ID
func (l *Log) TxnID() uint64 {
	return l.txnID
}

// Path returns the log file path
func (l *Log) Path() string {
	return l.path
}

// EntryCount returns the number of entries written
func (l *Log) EntryCount() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// Reader provides sequential access to log entries
type Reader struct {
	file   *os.File
	reader *bufio.Reader
	txnID  uint64
	closed bool
}

// Open opens an existing intent log for reading
func Open(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	r := &Reader{
		file:   file,
		reader: bufio.NewReaderSize(file, 64*1024),
	}

	if err := r.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return r, nil
}

// readHeader validates and reads the file header
func (r *Reader) readHeader() error {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r.reader, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if string(header[0:4]) != Magic {
		return ErrInvalidMagic
	}

	version := binary.LittleEndian.Uint16(header[4:6])
	if version != Version {
		return fmt.Errorf("%w: got %d, want %d", ErrVersionMismatch, version, Version)
	}

	r.txnID = binary.LittleEndian.Uint64(header[8:16])
	return nil
}

// TxnID returns the transaction ID from the log
func (r *Reader) TxnID() uint64 {
	return r.txnID
}

// Next reads the next entry from the log
func (r *Reader) Next() (*Entry, error) {
	if r.closed {
		return nil, ErrLogClosed
	}

	// Read entry length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, lenBuf); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read entry length: %w", err)
	}

	entryLen := binary.LittleEndian.Uint32(lenBuf)
	if entryLen == 0 || entryLen > 100*1024*1024 { // 100MB sanity check
		return nil, errors.New("invalid entry length")
	}

	// Read rest of entry
	data := make([]byte, entryLen)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		return nil, fmt.Errorf("read entry data: %w", err)
	}

	return r.decodeEntry(data)
}

// decodeEntry deserializes an entry from bytes
func (r *Reader) decodeEntry(data []byte) (*Entry, error) {
	if len(data) < 4 { // minimum: checksum
		return nil, errors.New("entry too short")
	}

	// Verify checksum
	checksumOffset := len(data) - 4
	storedChecksum := binary.LittleEndian.Uint32(data[checksumOffset:])
	computedChecksum := crc32.ChecksumIEEE(data[:checksumOffset])
	if storedChecksum != computedChecksum {
		return nil, ErrChecksumFailed
	}

	offset := 0
	e := &Entry{}

	// seq
	if offset+8 > checksumOffset {
		return nil, errors.New("truncated entry: seq")
	}
	e.Seq = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// operation
	if offset+1 > checksumOffset {
		return nil, errors.New("truncated entry: operation")
	}
	e.Operation = data[offset]
	offset++

	// table_len + table
	if offset+2 > checksumOffset {
		return nil, errors.New("truncated entry: table_len")
	}
	tableLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	if offset+tableLen > checksumOffset {
		return nil, errors.New("truncated entry: table")
	}
	e.Table = string(data[offset : offset+tableLen])
	offset += tableLen

	// key_len + key
	if offset+4 > checksumOffset {
		return nil, errors.New("truncated entry: key_len")
	}
	keyLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	if offset+keyLen > checksumOffset {
		return nil, errors.New("truncated entry: key")
	}
	e.RowKey = string(data[offset : offset+keyLen])
	offset += keyLen

	// old_len + old_values
	if offset+4 > checksumOffset {
		return nil, errors.New("truncated entry: old_len")
	}
	oldLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	if oldLen > 0 {
		if offset+oldLen > checksumOffset {
			return nil, errors.New("truncated entry: old_values")
		}
		e.OldValues = make(map[string][]byte)
		if err := json.Unmarshal(data[offset:offset+oldLen], &e.OldValues); err != nil {
			return nil, fmt.Errorf("unmarshal old values: %w", err)
		}
		offset += oldLen
	}

	// new_len + new_values
	if offset+4 > checksumOffset {
		return nil, errors.New("truncated entry: new_len")
	}
	newLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	if newLen > 0 {
		if offset+newLen > checksumOffset {
			return nil, errors.New("truncated entry: new_values")
		}
		e.NewValues = make(map[string][]byte)
		if err := json.Unmarshal(data[offset:offset+newLen], &e.NewValues); err != nil {
			return nil, fmt.Errorf("unmarshal new values: %w", err)
		}
	}

	return e, nil
}

// Close closes the reader
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.file.Close()
}

// Entries reads all entries from the log
func (r *Reader) Entries() ([]*Entry, error) {
	var entries []*Entry
	for {
		e, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// ListLogs returns all intent log files in the data directory
func ListLogs(dataDir string) ([]string, error) {
	logDir := filepath.Join(dataDir, "intent_logs")
	matches, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	if err != nil {
		return nil, err
	}
	return matches, nil
}
