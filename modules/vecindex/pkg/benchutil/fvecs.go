package benchutil

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// DatasetMetadata describes an ANN benchmark dataset.
type DatasetMetadata struct {
	NTrain int    `json:"n_train"`
	NTest  int    `json:"n_test"`
	Dim    int    `json:"dim"`
	Metric string `json:"metric"`
	K      int    `json:"k"`
}

// MMapFvecs is a memory-mapped .fvecs dataset. Vectors are exposed as slices
// backed directly by the mapped file so the OS can page them on demand.
type MMapFvecs struct {
	file    *os.File
	data    []byte
	dim     int
	n       int
	recSize int
}

// OpenMMapFvecs opens and memory-maps an .fvecs file.
func OpenMMapFvecs(path string) (*MMapFvecs, error) {
	file, data, dim, n, recSize, err := mmapVecFile(path)
	if err != nil {
		return nil, err
	}
	return &MMapFvecs{
		file:    file,
		data:    data,
		dim:     dim,
		n:       n,
		recSize: recSize,
	}, nil
}

func (m *MMapFvecs) Len() int { return m.n }
func (m *MMapFvecs) Dim() int { return m.dim }

// VectorBytes returns the raw little-endian float32 payload for vector i.
func (m *MMapFvecs) VectorBytes(i int) []byte {
	start := i*m.recSize + 4
	end := start + m.dim*4
	return m.data[start:end:end]
}

// Vector returns vector i as a []float32 backed by the mapped file.
func (m *MMapFvecs) Vector(i int) []float32 {
	b := m.VectorBytes(i)
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4)
}

// Close unmaps the file and closes the underlying descriptor.
func (m *MMapFvecs) Close() error {
	return closeMappedFile(m.file, m.data)
}

// MMapIvecs is a memory-mapped .ivecs dataset.
type MMapIvecs struct {
	file    *os.File
	data    []byte
	dim     int
	n       int
	recSize int
}

// OpenMMapIvecs opens and memory-maps an .ivecs file.
func OpenMMapIvecs(path string) (*MMapIvecs, error) {
	file, data, dim, n, recSize, err := mmapVecFile(path)
	if err != nil {
		return nil, err
	}
	return &MMapIvecs{
		file:    file,
		data:    data,
		dim:     dim,
		n:       n,
		recSize: recSize,
	}, nil
}

func (m *MMapIvecs) Len() int { return m.n }
func (m *MMapIvecs) Dim() int { return m.dim }

// Vector returns vector i as a []int32 backed by the mapped file.
func (m *MMapIvecs) Vector(i int) []int32 {
	start := i*m.recSize + 4
	end := start + m.dim*4
	b := m.data[start:end:end]
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(&b[0])), len(b)/4)
}

// Close unmaps the file and closes the underlying descriptor.
func (m *MMapIvecs) Close() error {
	return closeMappedFile(m.file, m.data)
}

func mmapVecFile(path string) (*os.File, []byte, int, int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, 0, 0, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, 0, 0, 0, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return f, nil, 0, 0, 0, nil
	}

	var dim32 int32
	if err := binary.Read(f, binary.LittleEndian, &dim32); err != nil {
		f.Close()
		return nil, nil, 0, 0, 0, err
	}
	if dim32 <= 0 {
		f.Close()
		return nil, nil, 0, 0, 0, fmt.Errorf("vecs: invalid dimension %d", dim32)
	}

	recSize := int64(4 + dim32*4)
	if fileSize%recSize != 0 {
		f.Close()
		return nil, nil, 0, 0, 0, fmt.Errorf("vecs: file size %d not divisible by record size %d", fileSize, recSize)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, nil, 0, 0, 0, err
	}

	return f, data, int(dim32), int(fileSize / recSize), int(recSize), nil
}

func closeMappedFile(file *os.File, data []byte) error {
	var firstErr error
	if data != nil {
		if err := syscall.Munmap(data); err != nil {
			firstErr = err
		}
	}
	if file != nil {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ReadFvecs reads an .fvecs file and returns a slice of float32 vectors.
// Format per vector: [dim: int32 LE][v1: float32 LE] ... [vN: float32 LE].
// Returns nil, nil for an empty file.
func ReadFvecs(path string) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return nil, nil
	}

	// Read dimension from first 4 bytes.
	var dim int32
	if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
		return nil, fmt.Errorf("fvecs: reading dim: %w", err)
	}
	if dim <= 0 {
		return nil, fmt.Errorf("fvecs: invalid dimension %d", dim)
	}

	recSize := int64(4 + dim*4)
	if fileSize%recSize != 0 {
		return nil, fmt.Errorf("fvecs: file size %d not divisible by record size %d", fileSize, recSize)
	}
	n := int(fileSize / recSize)

	// Seek back to start and read all records with a buffered reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(f, 1<<20)

	vecs := make([][]float32, n)
	for i := range n {
		var d int32
		if err := binary.Read(br, binary.LittleEndian, &d); err != nil {
			return nil, fmt.Errorf("fvecs: record %d: reading dim: %w", i, err)
		}
		if d != dim {
			return nil, fmt.Errorf("fvecs: record %d: dimension mismatch (got %d, want %d)", i, d, dim)
		}
		vec := make([]float32, dim)
		for j := range int(dim) {
			var bits uint32
			if err := binary.Read(br, binary.LittleEndian, &bits); err != nil {
				return nil, fmt.Errorf("fvecs: record %d component %d: %w", i, j, err)
			}
			vec[j] = math.Float32frombits(bits)
		}
		vecs[i] = vec
	}
	return vecs, nil
}

// ReadIvecs reads an .ivecs file and returns a slice of int32 index vectors.
// Format per vector: [dim: int32 LE][v1: int32 LE] ... [vN: int32 LE].
// Returns nil, nil for an empty file.
func ReadIvecs(path string) ([][]int32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return nil, nil
	}

	// Read dimension from first 4 bytes.
	var dim int32
	if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
		return nil, fmt.Errorf("ivecs: reading dim: %w", err)
	}
	if dim <= 0 {
		return nil, fmt.Errorf("ivecs: invalid dimension %d", dim)
	}

	recSize := int64(4 + dim*4)
	if fileSize%recSize != 0 {
		return nil, fmt.Errorf("ivecs: file size %d not divisible by record size %d", fileSize, recSize)
	}
	n := int(fileSize / recSize)

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(f, 1<<20)

	vecs := make([][]int32, n)
	for i := range n {
		var d int32
		if err := binary.Read(br, binary.LittleEndian, &d); err != nil {
			return nil, fmt.Errorf("ivecs: record %d: reading dim: %w", i, err)
		}
		if d != dim {
			return nil, fmt.Errorf("ivecs: record %d: dimension mismatch (got %d, want %d)", i, d, dim)
		}
		vec := make([]int32, dim)
		if err := binary.Read(br, binary.LittleEndian, vec); err != nil {
			return nil, fmt.Errorf("ivecs: record %d: reading values: %w", i, err)
		}
		vecs[i] = vec
	}
	return vecs, nil
}

// LoadMetadata reads metadata.json from dir and returns a DatasetMetadata.
func LoadMetadata(dir string) (DatasetMetadata, error) {
	path := filepath.Join(dir, "metadata.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return DatasetMetadata{}, fmt.Errorf("benchutil: reading metadata: %w", err)
	}
	var m DatasetMetadata
	if err := json.Unmarshal(data, &m); err != nil {
		return DatasetMetadata{}, fmt.Errorf("benchutil: parsing metadata: %w", err)
	}
	return m, nil
}
