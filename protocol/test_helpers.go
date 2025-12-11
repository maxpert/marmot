package protocol

import (
	"database/sql"
	"testing"
)

// openTestDB opens a SQLite database for testing.
// It tries to use the custom driver (sqlite3_marmot) first,
// but falls back to the standard sqlite3 driver if not available.
func openTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()

	// Try custom driver first
	db, err := sql.Open(SQLiteDriverName, dsn)
	if err != nil {
		// Fallback to standard sqlite3 driver
		db, err = sql.Open("sqlite3", dsn)
		if err != nil {
			t.Fatalf("Failed to open SQLite database: %v", err)
		}
	}
	return db
}
