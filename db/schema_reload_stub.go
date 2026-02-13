//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

import (
	"context"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

// ReloadSchema reloads schema cache for non-preupdate builds as well.
// CDC replay paths still require PK metadata from the schema cache.
func (mdb *ReplicatedDatabase) ReloadSchema() error {
	conn, err := mdb.writeDB.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		return mdb.schemaCache.Reload(sqliteConn)
	})
}
