//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSchemaReloadFaultDB opens a *sql.DB through a private driver registration
// whose ConnectHook fails every connection after the first. Combined with
// SetMaxIdleConns(0) (which forces the pool to open a brand-new connection -
// and so re-run the hook - for every operation instead of reusing one), this
// deterministically fails the SECOND connection a caller requests while
// letting the first succeed, with no timing/race dependency: both connection
// attempts happen strictly sequentially in the calling goroutine's own code.
func newSchemaReloadFaultDB(t *testing.T) (faultyDB *sql.DB, dbPath string) {
	t.Helper()
	dbPath = filepath.Join(t.TempDir(), "test.db")

	var connects atomic.Int32
	driverName := fmt.Sprintf("sqlite3_reload_fault_%p", t)
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if connects.Add(1) > 1 {
				return fmt.Errorf("simulated connection failure (connect #%d)", connects.Load())
			}
			return nil
		},
	})

	db, err := sql.Open(driverName, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxIdleConns(0) // force a fresh connection (and ConnectHook call) per operation
	return db, dbPath
}

// TestApplyNonDMLIntents_PropagatesSchemaReloadFailure is a regression test
// for db/transaction.go's applyNonDMLIntents: previously, a failure to reload
// the schema cache after a successful DDL exec was only logged
// (log.Warn "Failed to reload schema cache after DDL") and the function
// returned nil, leaving the cache stale so subsequent preupdate hooks would
// silently drop CDC data for any column the DDL touched. It must now
// propagate the error and fail the DDL apply instead.
func TestApplyNonDMLIntents_PropagatesSchemaReloadFailure(t *testing.T) {
	db, dbPath := newSchemaReloadFaultDB(t)
	schemaCache := NewSchemaCache()
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), schemaCache)

	intents := []*WriteIntentRecord{
		{
			IntentType:   IntentTypeDDL,
			SQLStatement: `CREATE TABLE t (id INTEGER PRIMARY KEY)`,
		},
	}

	// The DDL exec itself is the first connection (succeeds); the schema
	// reload that follows needs a second, fresh connection (fails). Every
	// connection through this faulty driver after the first fails, so this
	// is the last operation this test can perform against `db` itself.
	err := tm.applyNonDMLIntents(1, intents)
	require.Error(t, err, "a failed post-DDL schema reload must fail the DDL apply, not be silently logged and swallowed")
	assert.Contains(t, err.Error(), "reload schema cache")

	// Sanity: the DDL itself really did succeed against the underlying file -
	// this proves the test isolates the reload failure and isn't just
	// failing because the connection was broken from the start. Checked via
	// a plain, unfaulty connection since every further connection on `db`
	// deliberately fails from here on.
	plainDB, err := sql.Open(SQLiteDriverName, dbPath)
	require.NoError(t, err)
	defer plainDB.Close()
	var name string
	require.NoError(t, plainDB.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name='t'`).Scan(&name))
	assert.Equal(t, "t", name)
}

// TestApplyNonDMLIntents_NoDDLNeverReloads verifies a purely non-DDL intent
// list (e.g. only LOAD DATA) does not attempt a schema reload at all, so it
// is unaffected by this fix.
func TestApplyNonDMLIntents_NoDDLNeverReloads(t *testing.T) {
	db, _ := newSchemaReloadFaultDB(t)
	schemaCache := NewSchemaCache()
	tm := NewTransactionManager(db, nil, hlc.NewClock(1), schemaCache)

	// No intents at all: hasDDL stays false, so reloadSchemaCache must never
	// be called, and the second (failing) connection must never be needed.
	err := tm.applyNonDMLIntents(1, nil)
	require.NoError(t, err)
}
