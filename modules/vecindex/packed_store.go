package vecindex

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const (
	packedStoreMagic   = "MVTOR001"
	packedStoreVersion = 1
	packedStoreHdrSize = 24
	packedStoreRefSize = 16
)

type packedClusterRef struct {
	offset uint64
	count  uint64
}

// PackedPartitionStore is a read-only mmap-backed snapshot of stable
// (cluster_id > 0) sidecar partitions. Entries are laid out as
// [rowid uint64][vec bytes] in cluster_id,rowid order.
type PackedPartitionStore struct {
	path       string
	file       *os.File
	data       []byte
	dim        int
	maxCluster int
	entrySize  int
	refs       []packedClusterRef
}

// OpenPackedPartitionStore mmaps an existing packed partition snapshot.
func OpenPackedPartitionStore(path string) (*PackedPartitionStore, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() < packedStoreHdrSize {
		file.Close()
		return nil, fmt.Errorf("vecindex: packed store %q is too small", path)
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("vecindex: mmap packed store %q: %w", path, err)
	}
	if string(data[:8]) != packedStoreMagic {
		syscall.Munmap(data)
		file.Close()
		return nil, fmt.Errorf("vecindex: invalid packed store magic %q", path)
	}
	if got := int(binary.LittleEndian.Uint32(data[8:12])); got != packedStoreVersion {
		syscall.Munmap(data)
		file.Close()
		return nil, fmt.Errorf("vecindex: unsupported packed store version %d", got)
	}

	dim := int(binary.LittleEndian.Uint32(data[12:16]))
	maxCluster := int(binary.LittleEndian.Uint32(data[16:20]))
	if dim <= 0 || maxCluster < 0 {
		syscall.Munmap(data)
		file.Close()
		return nil, fmt.Errorf("vecindex: invalid packed store header dim=%d maxCluster=%d", dim, maxCluster)
	}
	entrySize := 8 + dim*4
	refBytes := (maxCluster + 1) * packedStoreRefSize
	dataOffset := packedStoreHdrSize + refBytes
	if len(data) < dataOffset {
		syscall.Munmap(data)
		file.Close()
		return nil, fmt.Errorf("vecindex: truncated packed store refs")
	}

	refs := make([]packedClusterRef, maxCluster+1)
	cursor := packedStoreHdrSize
	for i := range refs {
		refs[i] = packedClusterRef{
			offset: binary.LittleEndian.Uint64(data[cursor : cursor+8]),
			count:  binary.LittleEndian.Uint64(data[cursor+8 : cursor+16]),
		}
		cursor += packedStoreRefSize
		if refs[i].count == 0 {
			continue
		}
		end := int(refs[i].offset + refs[i].count*uint64(entrySize))
		if int(refs[i].offset) < dataOffset || end > len(data) {
			syscall.Munmap(data)
			file.Close()
			return nil, fmt.Errorf("vecindex: packed store cluster %d out of bounds", i)
		}
	}

	return &PackedPartitionStore{
		path:       path,
		file:       file,
		data:       data,
		dim:        dim,
		maxCluster: maxCluster,
		entrySize:  entrySize,
		refs:       refs,
	}, nil
}

// Close releases the mapping and file descriptor.
func (s *PackedPartitionStore) Close() error {
	if s == nil {
		return nil
	}
	var firstErr error
	if s.data != nil {
		if err := syscall.Munmap(s.data); err != nil && firstErr == nil {
			firstErr = err
		}
		s.data = nil
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.file = nil
	}
	return firstErr
}

// Path returns the backing snapshot path.
func (s *PackedPartitionStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Dim returns the vector dimensionality encoded in the snapshot.
func (s *PackedPartitionStore) Dim() int {
	if s == nil {
		return 0
	}
	return s.dim
}

// ScanCluster visits every entry in the given stable cluster.
func (s *PackedPartitionStore) ScanCluster(clusterID int64, yield func(rowid int64, vecBytes []byte) bool) {
	if s == nil || clusterID <= 0 || int(clusterID) > s.maxCluster {
		return
	}
	ref := s.refs[clusterID]
	if ref.count == 0 {
		return
	}
	offset := int(ref.offset)
	for i := uint64(0); i < ref.count; i++ {
		rowid := int64(binary.LittleEndian.Uint64(s.data[offset : offset+8]))
		vec := s.data[offset+8 : offset+s.entrySize]
		if !yield(rowid, vec) {
			return
		}
		offset += s.entrySize
	}
}

// ScanClusters visits each requested cluster in caller order.
func (s *PackedPartitionStore) ScanClusters(clusterIDs []int64, yield func(rowid int64, vecBytes []byte) bool) {
	for _, cid := range clusterIDs {
		s.ScanCluster(cid, yield)
	}
}

// PackedPartitionStoreWriter builds a packed partition snapshot in
// cluster_id,rowid order and atomically renames it into place on Close.
type PackedPartitionStoreWriter struct {
	path       string
	tmpPath    string
	file       *os.File
	dim        int
	maxCluster int
	entrySize  int
	offset     uint64
	lastCID    int64
	refs       []packedClusterRef
	closed     bool
}

// CreatePackedPartitionStoreWriter starts a new packed snapshot writer.
func CreatePackedPartitionStoreWriter(path string, dim, maxCluster int) (*PackedPartitionStoreWriter, error) {
	if dim <= 0 {
		return nil, fmt.Errorf("vecindex: packed store dim must be > 0")
	}
	if maxCluster < 0 {
		return nil, fmt.Errorf("vecindex: packed store maxCluster must be >= 0")
	}
	tmpPath := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	dataOffset := packedStoreHdrSize + (maxCluster+1)*packedStoreRefSize
	if _, err := file.Seek(int64(dataOffset), 0); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return nil, err
	}
	return &PackedPartitionStoreWriter{
		path:       path,
		tmpPath:    tmpPath,
		file:       file,
		dim:        dim,
		maxCluster: maxCluster,
		entrySize:  8 + dim*4,
		offset:     uint64(dataOffset),
		refs:       make([]packedClusterRef, maxCluster+1),
	}, nil
}

// Append appends one cluster_id,rowid,vec triple. Calls must be ordered by
// non-decreasing cluster_id.
func (w *PackedPartitionStoreWriter) Append(clusterID, rowid int64, vec []byte) error {
	if w == nil || w.file == nil {
		return fmt.Errorf("vecindex: packed store writer is closed")
	}
	if clusterID <= 0 || int(clusterID) > w.maxCluster {
		return fmt.Errorf("vecindex: packed store cluster %d out of range", clusterID)
	}
	if len(vec) != w.dim*4 {
		return fmt.Errorf("vecindex: packed store vec length %d, want %d", len(vec), w.dim*4)
	}
	if clusterID < w.lastCID {
		return fmt.Errorf("vecindex: packed store rows must be appended in cluster order")
	}
	ref := &w.refs[clusterID]
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
	w.offset += uint64(w.entrySize)
	w.lastCID = clusterID
	return nil
}

// Close finalizes the snapshot, renames it into place, and returns an mmap
// reader for the finished file.
func (w *PackedPartitionStoreWriter) Close() (*PackedPartitionStore, error) {
	if w == nil || w.closed {
		return nil, fmt.Errorf("vecindex: packed store writer already closed")
	}
	w.closed = true

	header := make([]byte, packedStoreHdrSize+(w.maxCluster+1)*packedStoreRefSize)
	copy(header[:8], []byte(packedStoreMagic))
	binary.LittleEndian.PutUint32(header[8:12], packedStoreVersion)
	binary.LittleEndian.PutUint32(header[12:16], uint32(w.dim))
	binary.LittleEndian.PutUint32(header[16:20], uint32(w.maxCluster))
	cursor := packedStoreHdrSize
	for _, ref := range w.refs {
		binary.LittleEndian.PutUint64(header[cursor:cursor+8], ref.offset)
		binary.LittleEndian.PutUint64(header[cursor+8:cursor+16], ref.count)
		cursor += packedStoreRefSize
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
	return OpenPackedPartitionStore(w.path)
}

// Abort closes and removes the temporary snapshot.
func (w *PackedPartitionStoreWriter) Abort() {
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
