//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

// reloadSchemaCache is a no-op stub when preupdate hooks are not enabled.
// Schema reloading is only needed for CDC-enabled builds.
func (tm *TransactionManager) reloadSchemaCache() error {
	return nil
}
