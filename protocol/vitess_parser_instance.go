package protocol

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// SQLiteDriverName is the custom SQLite driver name with REGEXP support.
// This must match the driver name registered in db/sqlite_driver.go.
// The driver will be registered when the db package is imported elsewhere in the application.
const SQLiteDriverName = "sqlite3_marmot"

// Global parser instance (reused for efficiency)
var vitessParser *sqlparser.Parser

func init() {
	var err error
	vitessParser, err = sqlparser.New(sqlparser.Options{})
	if err != nil {
		panic("failed to initialize Vitess parser: " + err.Error())
	}
}
