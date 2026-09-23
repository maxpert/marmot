package grpc

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
)

// snapshotExportPrefix names the directories StreamSnapshot exports database
// files into, directly under the data directory. Each directory belongs to
// exactly one snapshotExport, so one found before any stream has started was
// left behind by a crash.
const snapshotExportPrefix = "snapshot-export-"

// snapshotExport is one exported copy of database files in its own directory.
// Every holder owns one reference: each stream reading the files, and the
// cache while the export is mapped. The last release removes the directory,
// so no directory outlives its readers and none is removed while one reads.
type snapshotExport struct {
	dir            string
	snapshots      []db.SnapshotInfo
	maxTxnID       uint64
	schemaVersions map[string]uint64
	expiresAt      time.Time
	refs           atomic.Int32
}

// newSnapshotExport creates an empty export directory under dataDir, with one
// reference held for the caller.
func newSnapshotExport(dataDir string) (*snapshotExport, error) {
	dir, err := os.MkdirTemp(dataDir, snapshotExportPrefix)
	if err != nil {
		return nil, err
	}
	e := &snapshotExport{dir: dir}
	e.refs.Store(1)
	return e, nil
}

// retain adds a reference. The caller must already hold one, directly or
// through the cache's lock while the export is mapped.
func (e *snapshotExport) retain() {
	e.refs.Add(1)
}

// release drops a reference and removes the directory with the last one.
func (e *snapshotExport) release() {
	if e.refs.Add(-1) != 0 {
		return
	}
	if err := os.RemoveAll(e.dir); err != nil {
		log.Warn().Err(err).Str("dir", e.dir).Msg("Failed to remove snapshot export")
	}
}

// snapshotExportCache maps a database name to its most recent export. The
// zero value is ready to use.
type snapshotExportCache struct {
	mu      sync.Mutex
	entries map[string]*snapshotExport
}

// acquire returns the export cached for database with a reference held for
// the caller, or nil when there is none or it expired before now.
func (c *snapshotExportCache) acquire(database string, now time.Time) *snapshotExport {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[database]
	if e == nil || !now.Before(e.expiresAt) {
		return nil
	}
	e.retain()
	return e
}

// publish maps e for database with its own reference and releases the
// cache's reference on the export it replaces. The caller keeps its reference.
func (c *snapshotExportCache) publish(database string, e *snapshotExport) {
	e.retain()
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]*snapshotExport)
	}
	replaced := c.entries[database]
	c.entries[database] = e
	c.mu.Unlock()
	if replaced != nil {
		replaced.release()
	}
}

// evictExpired unmaps every export that expired before now and releases the
// cache's reference on each. It returns how many were evicted.
func (c *snapshotExportCache) evictExpired(now time.Time) int {
	var expired []*snapshotExport
	c.mu.Lock()
	for database, e := range c.entries {
		if !now.Before(e.expiresAt) {
			expired = append(expired, e)
			delete(c.entries, database)
		}
	}
	c.mu.Unlock()
	for _, e := range expired {
		e.release()
	}
	return len(expired)
}

// removeOrphanedSnapshotExports removes every export directory under dataDir.
// Call it only before any stream can export into dataDir.
func removeOrphanedSnapshotExports(dataDir string) {
	dirs, err := filepath.Glob(filepath.Join(dataDir, snapshotExportPrefix+"*"))
	if err != nil {
		log.Warn().Err(err).Str("data_dir", dataDir).Msg("Failed to list orphaned snapshot exports")
		return
	}
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			log.Warn().Err(err).Str("dir", dir).Msg("Failed to remove orphaned snapshot export")
			continue
		}
		log.Info().Str("dir", dir).Msg("Removed orphaned snapshot export")
	}
}
