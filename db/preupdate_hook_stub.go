//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"errors"
	"sync"
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
func StartEphemeralSession(ctx context.Context, userDB *sql.DB, metaStore MetaStore, schemaCache *SchemaCache, txnID uint64, tables []string) (*EphemeralHookSession, error) {
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

// GetRowCounts returns nil (stub)
func (s *EphemeralHookSession) GetRowCounts() map[string]int64 {
	return nil
}

// GetKeyHashes returns nil (stub)
func (s *EphemeralHookSession) GetKeyHashes(maxRows int) map[string][]uint64 {
	return nil
}

// IntentEntry represents a CDC entry (stub)
type IntentEntry struct {
	TxnID     uint64
	Seq       uint64
	Operation uint8
	Table     string
	RowKey    string
	OldValues map[string][]byte
	NewValues map[string][]byte
	CreatedAt int64
}

// GetIntentEntries returns nil (stub)
func (s *EphemeralHookSession) GetIntentEntries() ([]*IntentEntry, error) {
	return nil, ErrPreupdateHookNotEnabled
}

// GetTxnID returns 0 (stub)
func (s *EphemeralHookSession) GetTxnID() uint64 {
	return 0
}
