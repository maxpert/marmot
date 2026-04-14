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
)

// DatasetMetadata describes an ANN benchmark dataset.
type DatasetMetadata struct {
	NTrain int    `json:"n_train"`
	NTest  int    `json:"n_test"`
	Dim    int    `json:"dim"`
	Metric string `json:"metric"`
	K      int    `json:"k"`
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
