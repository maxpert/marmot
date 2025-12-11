package db

import (
	"github.com/mattn/go-sqlite3"
)

// RegisterAllMySQLCompatFuncs registers all MySQL-compatible functions for SQLite.
// This is called from the SQLite driver's ConnectHook to enable WordPress compatibility.
func RegisterAllMySQLCompatFuncs(conn *sqlite3.SQLiteConn) error {
	// Register date/time functions (YEAR, MONTH, DAY, NOW, etc.)
	if err := RegisterMySQLDateTimeFuncs(conn); err != nil {
		return err
	}

	// Register string functions (CONCAT_WS, LEFT, RIGHT, etc.)
	if err := RegisterMySQLStringFuncs(conn); err != nil {
		return err
	}

	// Register math and hash functions (RAND, MD5, SHA1, etc.)
	if err := RegisterMySQLMathFuncs(conn); err != nil {
		return err
	}

	return nil
}
