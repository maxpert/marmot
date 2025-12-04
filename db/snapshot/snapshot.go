// Package snapshot provides unified snapshot creation and restoration for Marmot.
// It is used by both cluster catch-up and replica bootstrap mechanisms.
package snapshot

import (
	"time"
)

// Info contains metadata about a single database snapshot
type Info struct {
	Name     string // Database name (e.g., "marmot", "__marmot_system")
	TxnID    uint64 // Max committed txn_id at snapshot time
	Filename string // Relative path from data directory
	FullPath string // Absolute path to file
	Size     int64  // File size in bytes
	SHA256   string // SHA256 hex digest for integrity verification
}

// Manifest contains all database snapshots for a restore operation
type Manifest struct {
	Snapshots []Info
	MaxTxnID  uint64    // Highest txn_id across all snapshots
	CreatedAt time.Time // When snapshot was created
}

// GetDatabaseNames returns list of database names in the manifest
func (m *Manifest) GetDatabaseNames() []string {
	names := make([]string, 0, len(m.Snapshots))
	for _, s := range m.Snapshots {
		names = append(names, s.Name)
	}
	return names
}

// GetSnapshot returns snapshot info for a specific database, or nil if not found
func (m *Manifest) GetSnapshot(dbName string) *Info {
	for i := range m.Snapshots {
		if m.Snapshots[i].Name == dbName {
			return &m.Snapshots[i]
		}
	}
	return nil
}

// Phase indicates current phase of restore operation
type Phase int

const (
	PhaseIdle        Phase = iota // Not started
	PhaseDownloading              // Downloading snapshot chunks
	PhaseVerifying                // Verifying integrity
	PhaseApplying                 // Moving files, reopening connections
	PhaseComplete                 // Done
	PhaseFailed                   // Failed
)

// String returns human-readable phase name
func (p Phase) String() string {
	switch p {
	case PhaseIdle:
		return "idle"
	case PhaseDownloading:
		return "downloading"
	case PhaseVerifying:
		return "verifying"
	case PhaseApplying:
		return "applying"
	case PhaseComplete:
		return "complete"
	case PhaseFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Progress provides visibility into restore state
type Progress struct {
	Phase           Phase
	BytesDownloaded int64
	BytesTotal      int64
	FilesComplete   int
	FilesTotal      int
	Error           error
}

// Chunk represents a piece of snapshot data received from stream
type Chunk struct {
	Filename      string
	ChunkIndex    int32
	TotalChunks   int32
	Data          []byte
	MD5Checksum   string
	IsLastForFile bool
}

// ChunkReceiver abstracts the gRPC stream for testability
type ChunkReceiver interface {
	// Recv receives the next chunk from the stream
	// Returns io.EOF when stream is complete
	Recv() (*Chunk, error)
}

// DatabaseFileInfo matches the gRPC DatabaseFileInfo message
type DatabaseFileInfo struct {
	Name           string
	Filename       string
	SizeBytes      int64
	SHA256Checksum string
}
