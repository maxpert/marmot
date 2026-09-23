package db

import (
	"database/sql"
	"regexp"
	"runtime"

	"github.com/mattn/go-sqlite3"
)

// SQLiteDriverName is the custom driver name with REGEXP support
const SQLiteDriverName = "sqlite3_marmot"

// SQLiteDurableDriverName is SQLiteDriverName for a database whose commits
// must survive a power loss (WithDurableCommits). On darwin its connections
// also set fullfsync: without it, synchronous=FULL syncs with fsync(), which
// there does not flush the drive's write cache; with it, commits and
// checkpoints sync with F_FULLFSYNC.
const SQLiteDurableDriverName = "sqlite3_marmot_durable"

// sqliteDrivers holds the registered drivers by name, so a database can open
// its pools through a connector of its own (see writeGate) with the same
// per-connection setup as sql.Open.
var sqliteDrivers = map[string]*sqlite3.SQLiteDriver{
	// Custom SQLite driver with MySQL compatibility functions
	SQLiteDriverName: {ConnectHook: registerConnFunctions},
	SQLiteDurableDriverName: {
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := registerConnFunctions(conn); err != nil {
				return err
			}
			if runtime.GOOS != "darwin" {
				return nil
			}
			_, err := conn.Exec("PRAGMA fullfsync=ON", nil)
			return err
		},
	},
}

func init() {
	for name, drv := range sqliteDrivers {
		sql.Register(name, drv)
	}
}

// registerConnFunctions registers Marmot's SQL functions and extensions on a
// new connection.
func registerConnFunctions(conn *sqlite3.SQLiteConn) error {
	// Register REGEXP function for MySQL compatibility
	if err := conn.RegisterFunc("regexp", regexpMatch, true); err != nil {
		return err
	}

	// Register all MySQL-compatible functions for WordPress support
	if err := RegisterAllMySQLCompatFuncs(conn); err != nil {
		return err
	}

	// Register v5.4 vector-search UDFs (vec_distance_*, __marmot_vec_*).
	if err := RegisterVectorUDFs(conn); err != nil {
		return err
	}

	// Load all registered extensions (always_loaded and dynamically loaded)
	if extMgr := GetExtensionManager(); extMgr != nil {
		if err := extMgr.LoadAllExtensions(conn); err != nil {
			return err
		}
	}

	return nil
}

// regexpMatch implements MySQL-compatible REGEXP behavior
// Returns 1 if text matches pattern, 0 otherwise
func regexpMatch(pattern, text string) (bool, error) {
	return regexp.MatchString(pattern, text)
}
