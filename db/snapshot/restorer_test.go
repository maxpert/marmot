package snapshot

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// mockConnectionManager implements ConnectionManager for testing
type mockConnectionManager struct {
	closeCalled map[string]int
	openCalled  map[string]int
	closeErr    error
	openErr     error
}

func newMockConnectionManager() *mockConnectionManager {
	return &mockConnectionManager{
		closeCalled: make(map[string]int),
		openCalled:  make(map[string]int),
	}
}

func (m *mockConnectionManager) CloseDatabaseConnections(name string) error {
	m.closeCalled[name]++
	return m.closeErr
}

func (m *mockConnectionManager) OpenDatabaseConnections(name string) error {
	m.openCalled[name]++
	return m.openErr
}

// mockChunkStream implements ChunkReceiver for testing
type mockChunkStream struct {
	chunks []*Chunk
	index  int
}

func (m *mockChunkStream) Recv() (*Chunk, error) {
	if m.index >= len(m.chunks) {
		return nil, io.EOF
	}
	chunk := m.chunks[m.index]
	m.index++
	return chunk, nil
}

// createTestChunksWithMD5 creates test chunks for a file with given content
func createTestChunksWithMD5(filename string, content []byte) []*Chunk {
	checksum := md5.Sum(content)
	return []*Chunk{
		{
			Filename:      filename,
			ChunkIndex:    0,
			TotalChunks:   1,
			Data:          content,
			MD5Checksum:   fmt.Sprintf("%x", checksum),
			IsLastForFile: true,
		},
	}
}

func calculateSHA256(content []byte) string {
	h := sha256.Sum256(content)
	return hex.EncodeToString(h[:])
}

func calculateFileSHA256CreateTempFile(t *testing.T, content []byte) (string, string) {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "temp.db")
	if err := os.WriteFile(tmpFile, content, 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	hash, err := CalculateFileSHA256(tmpFile)
	if err != nil {
		t.Fatalf("failed to calculate hash: %v", err)
	}
	return hash, tmpFile
}

func TestRestorer_NewRestorer(t *testing.T) {
	r := NewRestorer("/tmp/test", nil)
	if r.dataDir != "/tmp/test" {
		t.Errorf("expected dataDir /tmp/test, got %s", r.dataDir)
	}
	if r.systemDB != "__marmot_system" {
		t.Errorf("expected systemDB __marmot_system, got %s", r.systemDB)
	}
	progress := r.GetProgress()
	if progress.Phase != PhaseIdle {
		t.Errorf("expected phase Idle, got %v", progress.Phase)
	}
}

func TestRestorer_GetProgress(t *testing.T) {
	r := NewRestorer("/tmp/test", nil)

	r.setPhase(PhaseDownloading)
	progress := r.GetProgress()
	if progress.Phase != PhaseDownloading {
		t.Errorf("expected phase Downloading, got %v", progress.Phase)
	}

	r.mu.Lock()
	r.progress.BytesDownloaded = 1000
	r.progress.BytesTotal = 5000
	r.mu.Unlock()

	progress = r.GetProgress()
	if progress.BytesDownloaded != 1000 {
		t.Errorf("expected BytesDownloaded 1000, got %d", progress.BytesDownloaded)
	}
}

func TestRestorer_RestoreFromStream_Success(t *testing.T) {
	tmpDir := t.TempDir()
	connMgr := newMockConnectionManager()
	r := NewRestorer(tmpDir, connMgr)

	// Create test content
	dbContent := []byte("SQLite format 3\x00test database content here")
	hash := calculateSHA256(dbContent)

	// Create mock stream
	stream := &mockChunkStream{
		chunks: createTestChunksWithMD5("databases/test.db", dbContent),
	}

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "databases/test.db",
			SizeBytes:      int64(len(dbContent)),
			SHA256Checksum: hash,
		},
	}

	// Restore
	err := r.RestoreFromStream(stream, files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify file was created
	finalPath := filepath.Join(tmpDir, "databases", "test.db")
	if _, err := os.Stat(finalPath); os.IsNotExist(err) {
		t.Error("expected database file to be created")
	}

	// Verify connections were managed
	if connMgr.closeCalled["test"] != 1 {
		t.Errorf("expected close called once for test, got %d", connMgr.closeCalled["test"])
	}
	if connMgr.openCalled["test"] != 1 {
		t.Errorf("expected open called once for test, got %d", connMgr.openCalled["test"])
	}

	// Verify progress
	progress := r.GetProgress()
	if progress.Phase != PhaseComplete {
		t.Errorf("expected phase Complete, got %v", progress.Phase)
	}
}

func TestRestorer_RestoreFromStream_MD5Mismatch(t *testing.T) {
	tmpDir := t.TempDir()
	r := NewRestorer(tmpDir, nil)

	// Create chunk with wrong MD5
	stream := &mockChunkStream{
		chunks: []*Chunk{
			{
				Filename:      "databases/test.db",
				ChunkIndex:    0,
				TotalChunks:   1,
				Data:          []byte("test content"),
				MD5Checksum:   "wrong_checksum",
				IsLastForFile: true,
			},
		},
	}

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "databases/test.db",
			SizeBytes:      12,
			SHA256Checksum: "abc",
		},
	}

	err := r.RestoreFromStream(stream, files)
	if err == nil {
		t.Error("expected error for MD5 mismatch")
	}

	progress := r.GetProgress()
	if progress.Phase != PhaseFailed {
		t.Errorf("expected phase Failed, got %v", progress.Phase)
	}
}

func TestRestorer_RestoreFromStream_InvalidFilename(t *testing.T) {
	tmpDir := t.TempDir()
	r := NewRestorer(tmpDir, nil)

	content := []byte("test")
	checksum := md5.Sum(content)
	stream := &mockChunkStream{
		chunks: []*Chunk{
			{
				Filename:      "../etc/passwd", // Invalid
				ChunkIndex:    0,
				TotalChunks:   1,
				Data:          content,
				MD5Checksum:   fmt.Sprintf("%x", checksum),
				IsLastForFile: true,
			},
		},
	}

	files := []DatabaseFileInfo{}

	err := r.RestoreFromStream(stream, files)
	if err == nil {
		t.Error("expected error for invalid filename")
	}
}

func TestRestorer_RestoreFromStream_SHA256Mismatch(t *testing.T) {
	tmpDir := t.TempDir()
	r := NewRestorer(tmpDir, nil)

	content := []byte("test database content")
	stream := &mockChunkStream{
		chunks: createTestChunksWithMD5("databases/test.db", content),
	}

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "databases/test.db",
			SizeBytes:      int64(len(content)),
			SHA256Checksum: "0000000000000000000000000000000000000000000000000000000000000000",
		},
	}

	err := r.RestoreFromStream(stream, files)
	if err == nil {
		t.Error("expected error for SHA256 mismatch")
	}

	progress := r.GetProgress()
	if progress.Phase != PhaseFailed {
		t.Errorf("expected phase Failed, got %v", progress.Phase)
	}
}

func TestRestorer_RestoreFromStream_SystemDBSkipped(t *testing.T) {
	tmpDir := t.TempDir()
	connMgr := newMockConnectionManager()
	r := NewRestorer(tmpDir, connMgr)

	// System DB should not have connections closed/opened
	content := []byte("system db content")
	hash := calculateSHA256(content)

	stream := &mockChunkStream{
		chunks: createTestChunksWithMD5("__marmot_system.db", content),
	}

	files := []DatabaseFileInfo{
		{
			Name:           "__marmot_system",
			Filename:       "__marmot_system.db",
			SizeBytes:      int64(len(content)),
			SHA256Checksum: hash,
		},
	}

	err := r.RestoreFromStream(stream, files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// System DB connections should NOT be managed
	if connMgr.closeCalled["__marmot_system"] != 0 {
		t.Error("system DB connections should not be closed")
	}
	if connMgr.openCalled["__marmot_system"] != 0 {
		t.Error("system DB connections should not be opened")
	}
}

func TestRestorer_RestoreFiles_Success(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source file
	if err := os.MkdirAll(filepath.Join(srcDir, "databases"), 0755); err != nil {
		t.Fatalf("failed to create src databases dir: %v", err)
	}
	content := []byte("test database content")
	srcFile := filepath.Join(srcDir, "databases", "test.db")
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	hash, _ := CalculateFileSHA256(srcFile)

	connMgr := newMockConnectionManager()
	r := NewRestorer(dstDir, connMgr)

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "databases/test.db",
			SizeBytes:      int64(len(content)),
			SHA256Checksum: hash,
		},
	}

	err := r.RestoreFiles(srcDir, files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify file was moved
	dstFile := filepath.Join(dstDir, "databases", "test.db")
	if _, err := os.Stat(dstFile); os.IsNotExist(err) {
		t.Error("expected database file to be created")
	}
}

func TestRestorer_ConnectionManagerErrors(t *testing.T) {
	tmpDir := t.TempDir()
	connMgr := newMockConnectionManager()
	connMgr.closeErr = errors.New("close failed")

	r := NewRestorer(tmpDir, connMgr)

	content := []byte("test database content")
	hash := calculateSHA256(content)

	stream := &mockChunkStream{
		chunks: createTestChunksWithMD5("databases/test.db", content),
	}

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "databases/test.db",
			SizeBytes:      int64(len(content)),
			SHA256Checksum: hash,
		},
	}

	// Should continue despite close error (logged as warning)
	err := r.RestoreFromStream(stream, files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestorer_NilConnectionManager(t *testing.T) {
	tmpDir := t.TempDir()
	r := NewRestorer(tmpDir, nil) // No connection manager

	content := []byte("test database content")
	hash := calculateSHA256(content)

	stream := &mockChunkStream{
		chunks: createTestChunksWithMD5("databases/test.db", content),
	}

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "databases/test.db",
			SizeBytes:      int64(len(content)),
			SHA256Checksum: hash,
		},
	}

	// Should work without connection manager (for cluster catch-up)
	err := r.RestoreFromStream(stream, files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPhase_String(t *testing.T) {
	tests := []struct {
		phase    Phase
		expected string
	}{
		{PhaseIdle, "idle"},
		{PhaseDownloading, "downloading"},
		{PhaseVerifying, "verifying"},
		{PhaseApplying, "applying"},
		{PhaseComplete, "complete"},
		{PhaseFailed, "failed"},
		{Phase(99), "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			if tc.phase.String() != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, tc.phase.String())
			}
		})
	}
}

func TestRestorer_MultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	connMgr := newMockConnectionManager()
	r := NewRestorer(tmpDir, connMgr)

	// Create test content for multiple databases
	db1Content := []byte("database one content")
	db2Content := []byte("database two content here")

	// Create chunks for both files
	chunks := append(
		createTestChunksWithMD5("databases/db1.db", db1Content),
		createTestChunksWithMD5("databases/db2.db", db2Content)...,
	)

	stream := &mockChunkStream{chunks: chunks}

	files := []DatabaseFileInfo{
		{
			Name:           "db1",
			Filename:       "databases/db1.db",
			SizeBytes:      int64(len(db1Content)),
			SHA256Checksum: calculateSHA256(db1Content),
		},
		{
			Name:           "db2",
			Filename:       "databases/db2.db",
			SizeBytes:      int64(len(db2Content)),
			SHA256Checksum: calculateSHA256(db2Content),
		},
	}

	err := r.RestoreFromStream(stream, files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify both files were created
	for _, name := range []string{"db1.db", "db2.db"} {
		path := filepath.Join(tmpDir, "databases", name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected %s to be created", name)
		}
	}

	// Verify connections were managed for both
	if connMgr.closeCalled["db1"] != 1 || connMgr.closeCalled["db2"] != 1 {
		t.Error("expected close called for both databases")
	}
	if connMgr.openCalled["db1"] != 1 || connMgr.openCalled["db2"] != 1 {
		t.Error("expected open called for both databases")
	}
}

func TestRestorer_RemovesOldWALAndSHM(t *testing.T) {
	tmpDir := t.TempDir()

	// Create existing db files with WAL and SHM
	dbDir := filepath.Join(tmpDir, "databases")
	os.MkdirAll(dbDir, 0755)

	dbFile := filepath.Join(dbDir, "test.db")
	walFile := filepath.Join(dbDir, "test.db-wal")
	shmFile := filepath.Join(dbDir, "test.db-shm")

	os.WriteFile(dbFile, []byte("old content"), 0644)
	os.WriteFile(walFile, []byte("old wal"), 0644)
	os.WriteFile(shmFile, []byte("old shm"), 0644)

	r := NewRestorer(tmpDir, nil)

	content := []byte("new database content")
	hash := calculateSHA256(content)

	stream := &mockChunkStream{
		chunks: createTestChunksWithMD5("databases/test.db", content),
	}

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "databases/test.db",
			SizeBytes:      int64(len(content)),
			SHA256Checksum: hash,
		},
	}

	err := r.RestoreFromStream(stream, files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify WAL and SHM were removed
	if _, err := os.Stat(walFile); !os.IsNotExist(err) {
		t.Error("expected WAL file to be removed")
	}
	if _, err := os.Stat(shmFile); !os.IsNotExist(err) {
		t.Error("expected SHM file to be removed")
	}

	// Verify new content
	data, _ := os.ReadFile(dbFile)
	if string(data) != string(content) {
		t.Error("expected new content in database file")
	}
}
