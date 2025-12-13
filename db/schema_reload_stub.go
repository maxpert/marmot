//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

// ReloadSchema is a no-op stub when preupdate hooks are not enabled.
// Schema reloading is only needed for CDC-enabled builds.
func (mdb *ReplicatedDatabase) ReloadSchema() error {
	return nil
}
