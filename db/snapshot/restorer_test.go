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
	_ = os.MkdirAll(dbDir, 0755)

	dbFile := filepath.Join(dbDir, "test.db")
	walFile := filepath.Join(dbDir, "test.db-wal")
	shmFile := filepath.Join(dbDir, "test.db-shm")

	_ = os.WriteFile(dbFile, []byte("old content"), 0644)
	_ = os.WriteFile(walFile, []byte("old wal"), 0644)
	_ = os.WriteFile(shmFile, []byte("old shm"), 0644)

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

// TestRestorer_SystemDBMergeRunsBeforeTheSwap: the merge sees the downloaded
// system database and the local one it is about to replace, and a failed
// merge leaves every local file untouched.
//
// Mutation: skip mergeSystemDB in atomicApply. "the system database was
// installed without the merge" fires.
func TestRestorer_SystemDBMergeRunsBeforeTheSwap(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	incoming := []byte("peer system db")
	local := []byte("local system db")
	userDB := []byte("peer user db")
	if err := os.MkdirAll(filepath.Join(srcDir, "databases"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dstDir, "databases"), 0755); err != nil {
		t.Fatal(err)
	}
	srcSys := filepath.Join(srcDir, "__marmot_system.db")
	srcUser := filepath.Join(srcDir, "databases", "app.db")
	dstSys := filepath.Join(dstDir, "__marmot_system.db")
	dstUser := filepath.Join(dstDir, "databases", "app.db")
	for path, content := range map[string][]byte{srcSys: incoming, srcUser: userDB, dstSys: local, dstUser: []byte("local user db")} {
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatal(err)
		}
	}
	files := []DatabaseFileInfo{
		{Name: "__marmot_system", Filename: "__marmot_system.db", SizeBytes: int64(len(incoming)), SHA256Checksum: calculateSHA256(incoming)},
		{Name: "app", Filename: "databases/app.db", SizeBytes: int64(len(userDB)), SHA256Checksum: calculateSHA256(userDB)},
	}

	var gotIncoming, gotLocal string
	failing := NewRestorer(dstDir, nil)
	failing.SetSystemDBMerge(func(in, loc string) error {
		gotIncoming, gotLocal = in, loc
		return os.ErrPermission
	})
	if err := failing.RestoreFiles(srcDir, files); err == nil {
		t.Fatal("a failed merge must abort the restore")
	}
	if gotIncoming != srcSys || gotLocal != dstSys {
		t.Fatalf("merge saw (%s, %s), want (%s, %s)", gotIncoming, gotLocal, srcSys, dstSys)
	}
	for path, want := range map[string]string{dstSys: "local system db", dstUser: "local user db"} {
		if got, _ := os.ReadFile(path); string(got) != want {
			t.Fatalf("%s was swapped although the merge failed: %q", path, got)
		}
	}

	r := NewRestorer(dstDir, nil)
	r.SetSystemDBMerge(func(in, _ string) error { return os.WriteFile(in, []byte("merged"), 0644) })
	if err := r.RestoreFiles(srcDir, files); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got, _ := os.ReadFile(dstSys); string(got) != "merged" {
		t.Fatalf("the system database was installed without the merge: %q", got)
	}
}

// TestRestorer_VerifiesAgainstTheStreamedManifest: GetSnapshotInfo's list
// describes an earlier snapshot than the one StreamSnapshot sends, so under
// write load its checksum and size are stale. A restore verifies each file
// against the checksum and size the stream reported with the file itself,
// and still refuses bytes that do not match those.
//
// Mutation: ignore the streamed manifest (withStreamedManifest returns files
// unchanged). "a file written after the info call was refused" fires.
func TestRestorer_VerifiesAgainstTheStreamedManifest(t *testing.T) {
	content := []byte("SQLite format 3\x00written after the info call")
	stale := []DatabaseFileInfo{{
		Name:           "test",
		Filename:       "databases/test.db",
		SizeBytes:      7,
		SHA256Checksum: calculateSHA256([]byte("earlier")),
	}}

	chunks := createTestChunksWithMD5("databases/test.db", content)
	chunks[0].FileSHA256 = calculateSHA256(content)
	chunks[0].FileSizeBytes = int64(len(content))
	if err := NewRestorer(t.TempDir(), nil).RestoreFromStream(&mockChunkStream{chunks: chunks}, stale); err != nil {
		t.Fatalf("a file written after the info call was refused: %v", err)
	}

	corrupt := createTestChunksWithMD5("databases/test.db", content)
	corrupt[0].FileSHA256 = calculateSHA256([]byte("other bytes"))
	corrupt[0].FileSizeBytes = int64(len(content))
	if err := NewRestorer(t.TempDir(), nil).RestoreFromStream(&mockChunkStream{chunks: corrupt}, stale); err == nil {
		t.Fatal("bytes that do not match the streamed checksum were accepted")
	}
}
