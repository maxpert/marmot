package db

import (
	"database/sql"
	"regexp"

	"github.com/mattn/go-sqlite3"
)

// SQLiteDriverName is the custom driver name with REGEXP support
const SQLiteDriverName = "sqlite3_marmot"

func init() {
	// Register custom SQLite driver with MySQL compatibility functions
	sql.Register(SQLiteDriverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			// Register REGEXP function for MySQL compatibility
			if err := conn.RegisterFunc("regexp", regexpMatch, true); err != nil {
				return err
			}

			// Register all MySQL-compatible functions for WordPress support
			if err := RegisterAllMySQLCompatFuncs(conn); err != nil {
				return err
			}

			// Load all registered extensions (always_loaded and dynamically loaded)
			if extMgr := GetExtensionManager(); extMgr != nil {
				if err := extMgr.LoadAllExtensions(conn); err != nil {
					return err
				}
			}

			return nil
		},
	})
}

// regexpMatch implements MySQL-compatible REGEXP behavior
// Returns 1 if text matches pattern, 0 otherwise
func regexpMatch(pattern, text string) (bool, error) {
	return regexp.MatchString(pattern, text)
}
