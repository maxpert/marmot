package db

import (
	"database/sql"
	"fmt"
)

// schemaVersionTableDDL creates __marmot_schema_version, the durable home of
// a user database's schema version. The
// single row (id=1) is bumped inside the same SQLite transaction as the DDL
// that changed the schema, so the version and the DDL it counts can never
// diverge across a crash - unlike the old pebble-stored counter, which lived
// in a different file from the DDL it counted.
const schemaVersionTableDDL = `
CREATE TABLE IF NOT EXISTS __marmot_schema_version (
	id INTEGER PRIMARY KEY CHECK(id = 1),
	version INTEGER NOT NULL
)`

// sqliteTableExists reports whether table exists in db's schema.
func sqliteTableExists(db interface {
	QueryRow(query string, args ...any) *sql.Row
}, table string) (bool, error) {
	var name string
	err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", table).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check table %s: %w", table, err)
	}
	return true, nil
}

// readSchemaVersionRow reads the single seed row of an existing
// __marmot_schema_version table, reporting ok=false (no error) when the table
// exists but the row does not - the row-less state a crash between CREATE and
// the seeding INSERT can leave behind (see ensureSchemaVersionTable).
func readSchemaVersionRow(q rowQuerier) (uint64, bool, error) {
	var v uint64
	err := q.QueryRow("SELECT version FROM __marmot_schema_version WHERE id = 1").Scan(&v)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read schema version: %w", err)
	}
	return v, true, nil
}

// computeInitialSchemaVersion returns the value to seed
// __marmot_schema_version's single row with, either on first creation or when
// repairing a row-less table:
// max(0, legacyRead(dbName)) from the retiring pebble-stored counter, but
// ONLY when filePreexisted is true - i.e. this SQLite file already carried
// __marmot_applied_txn before this open, so it predates this feature and may
// carry DDL the pebble store already counted. A database file that did not
// exist before this open (a fresh CREATE DATABASE) always starts at 0, even
// if a stale pebble entry exists under the same name from a prior, dropped
// incarnation.
func computeInitialSchemaVersion(dbName string, filePreexisted bool, legacyRead func(dbName string) (int64, error)) (uint64, error) {
	if !filePreexisted || legacyRead == nil || dbName == "" {
		return 0, nil
	}
	legacy, err := legacyRead(dbName)
	if err != nil {
		return 0, fmt.Errorf("read legacy schema version for %s: %w", dbName, err)
	}
	if legacy > 0 {
		return uint64(legacy), nil
	}
	return 0, nil
}

// ensureSchemaVersionTable creates __marmot_schema_version in a database file
// if it is not already there, repairs it if a prior open left it without its
// seed row, and returns the version to cache.
//
// Atomicity: the CREATE TABLE and the seeding INSERT run
// together inside one SQLite transaction, and the INSERT is OR IGNORE, so
// this function is idempotent whether the table is missing entirely, already
// exists with its row, or - the state a crash between the two statements used
// to leave a pre-fix binary in - exists without a row. Every open path
// (NewReplicatedDatabase) calls this before any query that reads the cached
// version, so a row-less table can never make a later open fail with
// "read schema version: sql: no rows in result set".
//
// Migration rule: see computeInitialSchemaVersion. Note for a rolling
// upgrade: restoring a database from a snapshot taken on an older release
// leaves the
// restored file with no __marmot_schema_version table (the source predates
// it), so this function's migration reads THIS node's own legacy pebble
// value, never the source's - the source node is a different pebble store.
// The documented upgrade procedure is to upgrade every node (so every node's
// legacy value is populated and consistent) before issuing the DDL that would
// make the versions matter; that keeps every node's migrated value equal.
func ensureSchemaVersionTable(db *sql.DB, dbName string, filePreexisted bool, legacyRead func(dbName string) (int64, error)) (uint64, error) {
	existed, err := sqliteTableExists(db, "__marmot_schema_version")
	if err != nil {
		return 0, err
	}
	if existed {
		if v, ok, err := readSchemaVersionRow(db); err != nil {
			return 0, err
		} else if ok {
			return v, nil
		}
		// Row-less table: repair it below with the same seed rule as a fresh
		// creation.
	}

	initial, err := computeInitialSchemaVersion(dbName, filePreexisted, legacyRead)
	if err != nil {
		return 0, err
	}

	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin schema version setup: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(schemaVersionTableDDL); err != nil {
		return 0, fmt.Errorf("create schema version table: %w", err)
	}
	if _, err := tx.Exec("INSERT OR IGNORE INTO __marmot_schema_version (id, version) VALUES (1, ?)", initial); err != nil {
		return 0, fmt.Errorf("seed schema version: %w", err)
	}
	v, ok, err := readSchemaVersionRow(tx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("seed schema version: row missing immediately after insert")
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit schema version setup: %w", err)
	}
	return v, nil
}

// bumpSchemaVersionInTx increments the user database's schema version by one
// inside tx and returns the new value. The caller commits tx together with
// the DDL that earned the bump, so the two can never diverge.
func bumpSchemaVersionInTx(tx *sql.Tx) (uint64, error) {
	if _, err := tx.Exec("UPDATE __marmot_schema_version SET version = version + 1 WHERE id = 1"); err != nil {
		return 0, fmt.Errorf("bump schema version: %w", err)
	}
	var v uint64
	if err := tx.QueryRow("SELECT version FROM __marmot_schema_version WHERE id = 1").Scan(&v); err != nil {
		return 0, fmt.Errorf("read bumped schema version: %w", err)
	}
	return v, nil
}
