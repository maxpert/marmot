package benchutil

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func writeFvecsFile(t *testing.T, path string, vecs [][]float32) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create fvecs file: %v", err)
	}
	defer f.Close()
	for _, vec := range vecs {
		if err := binary.Write(f, binary.LittleEndian, int32(len(vec))); err != nil {
			t.Fatalf("write dim: %v", err)
		}
		for _, v := range vec {
			if err := binary.Write(f, binary.LittleEndian, math.Float32bits(v)); err != nil {
				t.Fatalf("write component: %v", err)
			}
		}
	}
}

func writeIvecsFile(t *testing.T, path string, vecs [][]int32) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create ivecs file: %v", err)
	}
	defer f.Close()
	for _, vec := range vecs {
		if err := binary.Write(f, binary.LittleEndian, int32(len(vec))); err != nil {
			t.Fatalf("write dim: %v", err)
		}
		if err := binary.Write(f, binary.LittleEndian, vec); err != nil {
			t.Fatalf("write values: %v", err)
		}
	}
}

func TestReadFvecs_Roundtrip(t *testing.T) {
	t.Parallel()

	want := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{-1.5, 0.0, 1e10},
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.fvecs")
	writeFvecsFile(t, path, want)

	got, err := ReadFvecs(path)
	if err != nil {
		t.Fatalf("ReadFvecs: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("vector count: got %d, want %d", len(got), len(want))
	}
	for i, wvec := range want {
		if len(got[i]) != len(wvec) {
			t.Fatalf("vec[%d] len: got %d, want %d", i, len(got[i]), len(wvec))
		}
		for j, wval := range wvec {
			if got[i][j] != wval {
				t.Errorf("vec[%d][%d]: got %v, want %v", i, j, got[i][j], wval)
			}
		}
	}
}

func TestReadIvecs_Roundtrip(t *testing.T) {
	t.Parallel()

	want := [][]int32{
		{0, 1, 2, 3},
		{10, 20, 30, 40},
		{-1, 0, 100, 999},
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ivecs")
	writeIvecsFile(t, path, want)

	got, err := ReadIvecs(path)
	if err != nil {
		t.Fatalf("ReadIvecs: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("vector count: got %d, want %d", len(got), len(want))
	}
	for i, wvec := range want {
		if len(got[i]) != len(wvec) {
			t.Fatalf("vec[%d] len: got %d, want %d", i, len(got[i]), len(wvec))
		}
		for j, wval := range wvec {
			if got[i][j] != wval {
				t.Errorf("vec[%d][%d]: got %v, want %v", i, j, got[i][j], wval)
			}
		}
	}
}

func TestReadFvecs_EmptyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "empty.fvecs")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create empty file: %v", err)
	}

	vecs, err := ReadFvecs(path)
	if err != nil {
		t.Fatalf("expected nil error for empty file, got: %v", err)
	}
	if vecs != nil {
		t.Fatalf("expected nil slice for empty file, got %v", vecs)
	}
}

func TestReadFvecs_TruncatedFile(t *testing.T) {
	t.Parallel()

	// Write 2 full vectors of dim=3, then truncate mid-vector.
	want := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "truncated.fvecs")
	writeFvecsFile(t, path, want)

	// Truncate by removing the last 4 bytes (one float32 component).
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-4); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	_, err = ReadFvecs(path)
	if err == nil {
		t.Fatal("expected error for truncated file, got nil")
	}
}

func TestLoadMetadata(t *testing.T) {
	t.Parallel()

	want := DatasetMetadata{
		NTrain: 1000000,
		NTest:  10000,
		Dim:    128,
		Metric: "euclidean",
		K:      100,
	}
	dir := t.TempDir()
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0o600); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	got, err := LoadMetadata(dir)
	if err != nil {
		t.Fatalf("LoadMetadata: %v", err)
	}
	if got != want {
		t.Errorf("metadata mismatch: got %+v, want %+v", got, want)
	}
}
