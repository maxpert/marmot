//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

// reloadSchemaCache reloads the schema cache after DDL operations.
// Similar to ReplicatedDatabase.ReloadSchema() but works with TransactionManager.
func (tm *TransactionManager) reloadSchemaCache() error {
	conn, err := tm.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		return tm.schemaCache.Reload(sqliteConn)
	})
}
