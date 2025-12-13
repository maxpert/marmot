//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

var ErrPreupdateHookNotEnabled = errors.New("preupdate hook requires build tag: sqlite_preupdate_hook")

// TableSchema represents a lightweight schema for CDC hooks (stub).
type TableSchema struct {
	Columns     []string
	PrimaryKeys []string
	PKIndices   []int
}

// SchemaCache is a stub when preupdate hook is not enabled
type SchemaCache struct {
	mu    sync.RWMutex
	cache map[string]*TableSchema
}

// NewSchemaCache creates a stub schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		cache: make(map[string]*TableSchema),
	}
}

// GetSchemaFor returns an error indicating preupdate hooks are not enabled
func (c *SchemaCache) GetSchemaFor(tableName string) (*TableSchema, error) {
	return nil, ErrPreupdateHookNotEnabled
}

// Reload returns an error indicating preupdate hooks are not enabled
func (c *SchemaCache) Reload(conn interface{}) error {
	return ErrPreupdateHookNotEnabled
}

// Clear is a no-op (stub)
func (c *SchemaCache) Clear() {}

// LoadTable returns an error indicating preupdate hooks are not enabled
func (c *SchemaCache) LoadTable(conn interface{}, tableName string) error {
	return fmt.Errorf("cannot load table %s: %w", tableName, ErrPreupdateHookNotEnabled)
}

// EphemeralHookSession is a stub when preupdate hook is not enabled
type EphemeralHookSession struct{}

// StartEphemeralSession returns an error when preupdate hook is not enabled
func StartEphemeralSession(ctx context.Context, userDB *sql.DB, metaStore MetaStore, schemaCache *SchemaCache, txnID uint64) (*EphemeralHookSession, error) {
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
	IntentKey string
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

// GetLastInsertId returns 0 (stub)
func (s *EphemeralHookSession) GetLastInsertId() int64 {
	return 0
}

// GetConflictError returns nil (stub - no CDC in stub mode)
func (s *EphemeralHookSession) GetConflictError() error {
	return nil
}

// ClearConflictError is a no-op (stub)
func (s *EphemeralHookSession) ClearConflictError() {
}
