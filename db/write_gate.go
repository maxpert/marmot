package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
)

// writeGate refuses every commit on one ReplicatedDatabase's SQLite file once
// the database is taken out of service. Each connection the database opens
// (its three pools and its batch committer's) carries a SQLite commit hook
// that consults the gate, so the refusal holds whatever path the write takes:
// an explicit transaction, an autocommit statement, a prepared statement, or
// a caller that still holds a *sql.Tx begun before the detach. SQLite turns a
// refused commit into a rollback and the caller's COMMIT fails with
// SQLITE_CONSTRAINT_COMMITHOOK, so nothing it wrote is ACKed. A read-only
// transaction commits without calling the hook, so reads pay nothing; a write
// commit pays one atomic load.
type writeGate struct {
	closed atomic.Bool
}

// close refuses every commit from now on. A commit already past its hook
// completes; draining it is the caller's job (ReplicatedDatabase.closeSQLite).
func (g *writeGate) close() {
	g.closed.Store(true)
}

// commitHook is the SQLite commit hook: non-zero turns the commit into a
// rollback.
func (g *writeGate) commitHook() int {
	if g.closed.Load() {
		return 1
	}
	return 0
}

// openDB is sql.Open for one of the registered SQLite drivers, with every
// connection it opens gated by g.
func (g *writeGate) openDB(driverName, dsn string) (*sql.DB, error) {
	drv, ok := sqliteDrivers[driverName]
	if !ok {
		return nil, fmt.Errorf("unknown SQLite driver %q", driverName)
	}
	return sql.OpenDB(&gatedConnector{driver: drv, dsn: dsn, gate: g}), nil
}

// gatedConnector opens connections through a registered SQLite driver and
// installs the gate's commit hook on each. It hands database/sql the driver's
// own *sqlite3.SQLiteConn, so code that reaches the raw connection through
// sql.Conn.Raw keeps working.
type gatedConnector struct {
	driver *sqlite3.SQLiteDriver
	dsn    string
	gate   *writeGate
}

func (c *gatedConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn, ok := conn.(*sqlite3.SQLiteConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected SQLite driver connection type: %T", conn)
	}
	sqliteConn.RegisterCommitHook(c.gate.commitHook)
	return sqliteConn, nil
}

func (c *gatedConnector) Driver() driver.Driver {
	return c.driver
}
