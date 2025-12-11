package snapshot

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog/log"
)

// ConnectionManager abstracts database connection operations for testability
type ConnectionManager interface {
	// CloseDatabaseConnections closes SQLite connections for a database
	CloseDatabaseConnections(name string) error
	// OpenDatabaseConnections opens SQLite connections for a database
	OpenDatabaseConnections(name string) error
}

// Restorer handles client-side snapshot restoration with atomic apply.
// It downloads snapshot files to a temp directory, verifies integrity,
// then atomically swaps them into place.
type Restorer struct {
	dataDir  string
	connMgr  ConnectionManager
	systemDB string // "__marmot_system" by default

	mu       sync.RWMutex
	progress Progress
}

// NewRestorer creates a restorer for the given data directory
func NewRestorer(dataDir string, connMgr ConnectionManager) *Restorer {
	return &Restorer{
		dataDir:  dataDir,
		connMgr:  connMgr,
		systemDB: "__marmot_system",
		progress: Progress{Phase: PhaseIdle},
	}
}

// GetProgress returns current restore progress (thread-safe)
func (r *Restorer) GetProgress() Progress {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.progress
}

// setPhase updates the restore phase
func (r *Restorer) setPhase(phase Phase) {
	r.mu.Lock()
	r.progress.Phase = phase
	r.mu.Unlock()
}

// setError sets phase to failed and records error
func (r *Restorer) setError(err error) {
	r.mu.Lock()
	r.progress.Phase = PhaseFailed
	r.progress.Error = err
	r.mu.Unlock()
}

// RestoreFromStream downloads and applies a snapshot atomically.
// This is the main entry point for both cluster catch-up and replica bootstrap.
//
// Steps:
// 1. Create temp directory for download
// 2. Download all chunks to temp (with MD5 verification per chunk)
// 3. Verify SHA256 integrity against file list
// 4. Acquire exclusive lock (implicit via connection close)
// 5. Close all SQLite connections
// 6. Remove old files, move new files into place
// 7. Open fresh SQLite connections
// 8. Clean up temp directory
func (r *Restorer) RestoreFromStream(
	stream ChunkReceiver,
	files []DatabaseFileInfo,
) error {
	r.setPhase(PhaseDownloading)

	// Calculate total bytes for progress tracking
	var totalBytes int64
	for _, f := range files {
		totalBytes += f.SizeBytes
	}
	r.mu.Lock()
	r.progress.BytesTotal = totalBytes
	r.progress.FilesTotal = len(files)
	r.mu.Unlock()

	// STEP 1: Create temp directory for download
	tempDir, err := os.MkdirTemp(r.dataDir, "snapshot-")
	if err != nil {
		r.setError(err)
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir) // Clean up temp dir on exit

	// Ensure temp databases subdirectory exists
	if err := os.MkdirAll(filepath.Join(tempDir, "databases"), 0755); err != nil {
		r.setError(err)
		return fmt.Errorf("failed to create temp databases directory: %w", err)
	}

	// STEP 2: Download all chunks to temp directory
	if err := r.downloadToTemp(stream, tempDir); err != nil {
		r.setError(err)
		return err
	}

	// STEP 3: Verify integrity
	r.setPhase(PhaseVerifying)
	if err := VerifyFileList(tempDir, files); err != nil {
		r.setError(err)
		return fmt.Errorf("snapshot integrity verification failed: %w", err)
	}
	log.Info().Msg("Snapshot integrity verified")

	// STEP 4-7: Atomic apply (close connections, swap files, reopen)
	r.setPhase(PhaseApplying)
	if err := r.atomicApply(tempDir, files); err != nil {
		r.setError(err)
		return err
	}

	r.setPhase(PhaseComplete)
	return nil
}

// downloadToTemp downloads all chunks from stream to temp directory
func (r *Restorer) downloadToTemp(stream ChunkReceiver, tempDir string) error {
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	var bytesDownloaded int64
	filesComplete := 0

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("snapshot stream error: %w", err)
		}

		// Process single chunk
		bytesWritten, fileCompleted, err := r.processChunk(chunk, tempDir, openFiles)
		if err != nil {
			return err
		}

		// Update progress
		bytesDownloaded += int64(bytesWritten)
		r.updateDownloadProgress(bytesDownloaded)

		if fileCompleted {
			filesComplete++
			r.updateFilesComplete(filesComplete)
		}
	}

	log.Info().
		Int64("bytes", bytesDownloaded).
		Int("files", filesComplete).
		Str("temp_dir", tempDir).
		Msg("Snapshot downloaded to temp directory")

	return nil
}

// processChunk handles a single chunk: verify, write, and optionally close file
func (r *Restorer) processChunk(chunk *Chunk, tempDir string, openFiles map[string]*os.File) (bytesWritten int, fileCompleted bool, err error) {
	// Verify MD5 checksum
	actualChecksum := fmt.Sprintf("%x", md5.Sum(chunk.Data))
	if actualChecksum != chunk.MD5Checksum {
		return 0, false, fmt.Errorf("MD5 checksum mismatch for chunk %d of %s: expected %s, got %s",
			chunk.ChunkIndex, chunk.Filename, chunk.MD5Checksum, actualChecksum)
	}

	// Sanitize filename (security check)
	sanitizedFilename, err := SanitizeFilename(chunk.Filename)
	if err != nil {
		return 0, false, fmt.Errorf("invalid snapshot filename: %w", err)
	}

	// Get or open file
	file, err := r.getOrCreateFile(sanitizedFilename, tempDir, openFiles)
	if err != nil {
		return 0, false, err
	}

	// Write chunk data
	n, err := file.Write(chunk.Data)
	if err != nil {
		return 0, false, fmt.Errorf("failed to write chunk: %w", err)
	}

	// Close file if this is the last chunk
	if chunk.IsLastForFile {
		if err := file.Sync(); err != nil {
			return 0, false, fmt.Errorf("failed to sync file: %w", err)
		}
		file.Close()
		delete(openFiles, sanitizedFilename)
		log.Debug().Str("file", sanitizedFilename).Msg("Finished downloading file")
		return n, true, nil
	}

	return n, false, nil
}

// getOrCreateFile gets an existing file or creates a new one
func (r *Restorer) getOrCreateFile(filename string, tempDir string, openFiles map[string]*os.File) (*os.File, error) {
	if file, exists := openFiles[filename]; exists {
		return file, nil
	}

	targetPath := filepath.Join(tempDir, filename)

	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for %s: %w", targetPath, err)
	}

	file, err := os.Create(targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", targetPath, err)
	}

	openFiles[filename] = file
	return file, nil
}

// updateDownloadProgress updates bytes downloaded (thread-safe)
func (r *Restorer) updateDownloadProgress(bytesDownloaded int64) {
	r.mu.Lock()
	r.progress.BytesDownloaded = bytesDownloaded
	r.mu.Unlock()
}

// updateFilesComplete updates files complete count (thread-safe)
func (r *Restorer) updateFilesComplete(filesComplete int) {
	r.mu.Lock()
	r.progress.FilesComplete = filesComplete
	r.mu.Unlock()
}

// atomicApply swaps snapshot files into place atomically
func (r *Restorer) atomicApply(tempDir string, files []DatabaseFileInfo) error {
	// STEP 5: Close all SQLite connections
	for _, dbInfo := range files {
		if dbInfo.Name == r.systemDB {
			continue // Don't close system DB connections
		}
		if r.connMgr != nil {
			if err := r.connMgr.CloseDatabaseConnections(dbInfo.Name); err != nil {
				log.Warn().Err(err).Str("database", dbInfo.Name).Msg("Failed to close connections before snapshot")
			}
		}
	}

	// STEP 6: Remove old files and move new files into place
	for _, dbInfo := range files {
		var relPath string
		if dbInfo.Name == r.systemDB {
			relPath = r.systemDB + ".db"
		} else {
			relPath = filepath.Join("databases", dbInfo.Name+".db")
		}

		srcPath := filepath.Join(tempDir, relPath)
		dstPath := filepath.Join(r.dataDir, relPath)

		// Skip if source doesn't exist
		if _, err := os.Stat(srcPath); os.IsNotExist(err) {
			continue
		}

		// Ensure destination directory exists
		if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
			r.reopenConnections(files)
			return fmt.Errorf("failed to create directory for %s: %w", dstPath, err)
		}

		// Remove old files completely (db + WAL + SHM)
		os.Remove(dstPath)
		os.Remove(dstPath + "-wal")
		os.Remove(dstPath + "-shm")

		// Move new file into place
		if err := os.Rename(srcPath, dstPath); err != nil {
			log.Error().Err(err).Str("src", srcPath).Str("dst", dstPath).Msg("Failed to move snapshot file")
			r.reopenConnections(files)
			return fmt.Errorf("failed to move snapshot file %s: %w", relPath, err)
		}

		log.Debug().Str("file", relPath).Msg("Moved snapshot file into place")
	}

	// STEP 7: Open fresh SQLite connections
	for _, dbInfo := range files {
		if dbInfo.Name == r.systemDB {
			continue
		}
		if r.connMgr != nil {
			if err := r.connMgr.OpenDatabaseConnections(dbInfo.Name); err != nil {
				log.Error().Err(err).Str("database", dbInfo.Name).Msg("Failed to open connections after snapshot")
			}
		}
	}

	log.Info().Msg("Snapshot applied successfully")
	return nil
}

// reopenConnections attempts to reopen connections after a failed apply
func (r *Restorer) reopenConnections(files []DatabaseFileInfo) {
	if r.connMgr == nil {
		return
	}
	for _, dbInfo := range files {
		if dbInfo.Name == r.systemDB {
			continue
		}
		if err := r.connMgr.OpenDatabaseConnections(dbInfo.Name); err != nil {
			log.Error().Err(err).Str("database", dbInfo.Name).Msg("Failed to reopen connections after snapshot failure")
		}
	}
}

// RestoreFiles applies pre-downloaded snapshot files to the data directory.
// Used when files are already downloaded (e.g., by a different mechanism).
func (r *Restorer) RestoreFiles(srcDir string, files []DatabaseFileInfo) error {
	r.setPhase(PhaseVerifying)

	// Verify integrity first
	if err := VerifyFileList(srcDir, files); err != nil {
		r.setError(err)
		return fmt.Errorf("snapshot integrity verification failed: %w", err)
	}

	r.setPhase(PhaseApplying)
	if err := r.atomicApply(srcDir, files); err != nil {
		r.setError(err)
		return err
	}

	r.setPhase(PhaseComplete)
	return nil
}
