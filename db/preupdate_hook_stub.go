//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/maxpert/marmot/protocol/filter"
	"github.com/maxpert/marmot/protocol/intentlog"
)

var ErrPreupdateHookNotEnabled = errors.New("preupdate hook requires build tag: sqlite_preupdate_hook")

// SchemaCache is a stub when preupdate hook is not enabled
type SchemaCache struct {
	mu    sync.RWMutex
	cache map[string]*tableSchema
}

type tableSchema struct {
	columns   []string
	pkColumns []string
	pkIndices []int
}

// NewSchemaCache creates a stub schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		cache: make(map[string]*tableSchema),
	}
}

// Get returns nil (stub)
func (c *SchemaCache) Get(tableName string) *tableSchema {
	return nil
}

// Set is a no-op (stub)
func (c *SchemaCache) Set(tableName string, schema *tableSchema) {}

// Invalidate is a no-op (stub)
func (c *SchemaCache) Invalidate(tableName string) {}

// InvalidateAll is a no-op (stub)
func (c *SchemaCache) InvalidateAll() {}

// EphemeralHookSession is a stub when preupdate hook is not enabled
type EphemeralHookSession struct{}

// StartEphemeralSession returns an error when preupdate hook is not enabled
func StartEphemeralSession(ctx context.Context, db *sql.DB, schemaCache *SchemaCache, txnID uint64, dataDir string, tables []string) (*EphemeralHookSession, error) {
	return nil, ErrPreupdateHookNotEnabled
}

// BeginTx returns an error (stub)
func (s *EphemeralHookSession) BeginTx(ctx context.Context) error {
	return ErrPreupdateHookNotEnabled
}

// ExecContext returns an error (stub)
func (s *EphemeralHookSession) ExecContext(ctx context.Context, query string) error {
	return ErrPreupdateHookNotEnabled
}

// Commit returns an error (stub)
func (s *EphemeralHookSession) Commit() error {
	return ErrPreupdateHookNotEnabled
}

// Rollback returns an error (stub)
func (s *EphemeralHookSession) Rollback() error {
	return ErrPreupdateHookNotEnabled
}

// FlushIntentLog returns nil (stub - no intent log without preupdate hooks)
func (s *EphemeralHookSession) FlushIntentLog() error {
	return nil
}

// BuildFilters returns nil (stub)
func (s *EphemeralHookSession) BuildFilters() map[string]*filter.BloomFilter {
	return nil
}

// GetRowCounts returns nil (stub)
func (s *EphemeralHookSession) GetRowCounts() map[string]int64 {
	return nil
}

// GetIntentLog returns nil (stub)
func (s *EphemeralHookSession) GetIntentLog() *intentlog.Log {
	return nil
}
