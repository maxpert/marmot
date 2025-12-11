package db

import (
	"database/sql"
	"regexp"

	"github.com/mattn/go-sqlite3"
)

// SQLiteDriverName is the custom driver name with REGEXP support
const SQLiteDriverName = "sqlite3_marmot"

func init() {
	// Register custom SQLite driver with REGEXP function
	sql.Register(SQLiteDriverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			// Register REGEXP function for MySQL compatibility
			// Usage: column REGEXP 'pattern'
			return conn.RegisterFunc("regexp", regexpMatch, true)
		},
	})
}

// regexpMatch implements MySQL-compatible REGEXP behavior
// Returns 1 if text matches pattern, 0 otherwise
func regexpMatch(pattern, text string) (bool, error) {
	return regexp.MatchString(pattern, text)
}
