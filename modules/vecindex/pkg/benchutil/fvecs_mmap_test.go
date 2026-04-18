package benchutil

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenMMapFvecs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.fvecs")
	writeFvecsFile(t, path, [][]float32{
		{1, 2, 3},
		{4, 5, 6},
	})

	ds, err := OpenMMapFvecs(path)
	if err != nil {
		t.Fatalf("OpenMMapFvecs: %v", err)
	}
	defer ds.Close()

	if ds.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", ds.Len())
	}
	if ds.Dim() != 3 {
		t.Fatalf("Dim() = %d, want 3", ds.Dim())
	}
	got := ds.Vector(1)
	want := []float32{4, 5, 6}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Vector(1)[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if len(ds.VectorBytes(0)) != 12 {
		t.Fatalf("VectorBytes(0) len = %d, want 12", len(ds.VectorBytes(0)))
	}
}

func TestOpenMMapIvecs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.ivecs")
	writeIvecsFile(t, path, [][]int32{
		{10, 11},
		{20, 21},
	})

	ds, err := OpenMMapIvecs(path)
	if err != nil {
		t.Fatalf("OpenMMapIvecs: %v", err)
	}
	defer ds.Close()

	if ds.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", ds.Len())
	}
	if ds.Dim() != 2 {
		t.Fatalf("Dim() = %d, want 2", ds.Dim())
	}
	got := ds.Vector(0)
	want := []int32{10, 11}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Vector(0)[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func writeFvecsFile(t *testing.T, path string, vecs [][]float32) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}
	defer f.Close()

	for _, vec := range vecs {
		if err := binary.Write(f, binary.LittleEndian, int32(len(vec))); err != nil {
			t.Fatalf("write dim: %v", err)
		}
		for _, x := range vec {
			if err := binary.Write(f, binary.LittleEndian, x); err != nil {
				t.Fatalf("write float: %v", err)
			}
		}
	}
}

func writeIvecsFile(t *testing.T, path string, vecs [][]int32) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}
	defer f.Close()

	for _, vec := range vecs {
		if err := binary.Write(f, binary.LittleEndian, int32(len(vec))); err != nil {
			t.Fatalf("write dim: %v", err)
		}
		if err := binary.Write(f, binary.LittleEndian, vec); err != nil {
			t.Fatalf("write ints: %v", err)
		}
	}
}
