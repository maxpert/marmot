package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// newDetachTestManager opens a DatabaseManager with a user database "app"
// holding table t.
func newDetachTestManager(t *testing.T) *DatabaseManager {
	t.Helper()
	dm, err := NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dm.Close() })
	require.NoError(t, dm.CreateDatabase("app"))
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	_, err = app.GetWriteDB().Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	require.NoError(t, err)
	return dm
}

// holdGCPassBeforeTheManagerLock restarts tm's GC with a short interval and
// a GC-safe-position step that blocks the first pass until release is
// closed. When the returned channel closes, a pass is inside that step; a
// real GCSafePositionFunc implementation is expected to take
// DatabaseManager.mu to aggregate consumed positions across databases, the
// same way the GetMinAppliedTxnID callback it replaced did.
func holdGCPassBeforeTheManagerLock(tm *TransactionManager, release <-chan struct{}) <-chan struct{} {
	entered := make(chan struct{})
	var once sync.Once
	tm.StopGarbageCollection()
	tm.SetGCSafePositionFunc(func() (LogPosition, bool) {
		once.Do(func() { close(entered) })
		<-release
		return LogPosition{}, false
	})
	tm.gcInterval = 5 * time.Millisecond
	tm.StartGarbageCollection()
	return entered
}

// gcStopRequested reports whether StopGarbageCollection has begun on tm.
func gcStopRequested(tm *TransactionManager) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return !tm.gcRunning
}

// TestClosingADatabaseDoesNotDeadlockWithAGCPassWaitingOnTheManager: detach,
// drop and shutdown each stop a database's GC and wait for its pass to end,
// while that pass may be about to take DatabaseManager.mu (a
// GCSafePositionFunc implementation aggregating consumed positions across
// databases). None of them may hold mu while it waits. The pass is held in
// its GC-safe-position step until the operation is waiting for it, then let
// go towards the lock.
//
// Mutation: hold dm.mu across DetachDatabase's drain (or DropDatabase's
// db.Close, or Close's database loop). The pass then waits on the lock the
// waiter holds, and "deadlocked with a GC pass waiting on the manager's lock"
// fires for that operation.
func TestClosingADatabaseDoesNotDeadlockWithAGCPassWaitingOnTheManager(t *testing.T) {
	operations := map[string]func(dm *DatabaseManager) error{
		"detach": func(dm *DatabaseManager) error { return dm.DetachDatabase(context.Background(), "app") },
		"drop":   func(dm *DatabaseManager) error { return dm.DropDatabase("app") },
		"close":  func(dm *DatabaseManager) error { return dm.Close() },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			dm := newDetachTestManager(t)
			app, err := dm.GetDatabase("app")
			require.NoError(t, err)
			tm := app.GetTransactionManager()
			release := make(chan struct{})
			entered := holdGCPassBeforeTheManagerLock(tm, release)
			<-entered

			done := make(chan error, 1)
			go func() { done <- operation(dm) }()
			require.Eventually(t, func() bool { return gcStopRequested(tm) }, 5*time.Second, time.Millisecond,
				"%s never began stopping the GC", name)
			close(release)

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatalf("%s deadlocked with a GC pass waiting on the manager's lock", name)
			}
		})
	}
}

// TestDetachRefusesAWriteTransactionInFlight: a caller that began a write
// transaction on the database before the detach still holds it. The detach
// waits for it rather than returning under it, and its commit is refused, so
// the write is never ACKed into a file a restore then replaces.
//
// A MySQL client sees the refusal as retryable (1213, SQLSTATE 40001).
//
// Mutation: drop the commit hook's refusal (writeGate.commitHook returns 0).
// "an in-flight transaction committed after its database was detached" fires.
// Mutation: drop the mapper's ErrConstraintCommitHook case. "a commit refused
// at the detach does not reach a MySQL client as retryable" fires.
// Mutation: skip the drain (DetachDatabase calls closeSQLite instead of
// drainSQLite). "the detach returned while a write transaction was in flight"
// fires.
func TestDetachRefusesAWriteTransactionInFlight(t *testing.T) {
	dm := newDetachTestManager(t)
	held, err := dm.GetDatabase("app")
	require.NoError(t, err)
	tx, err := held.GetWriteDB().Begin()
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO t (id, v) VALUES (500, 'in flight')")
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- dm.DetachDatabase(context.Background(), "app") }()
	require.Eventually(t, held.gate.closed.Load, 5*time.Second, time.Millisecond, "the detach never closed the write gate")
	require.Never(t, func() bool { return len(done) > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"the detach returned while a write transaction was in flight")

	commitErr := tx.Commit()
	require.Error(t, commitErr, "an in-flight transaction committed after its database was detached")
	refused := protocol.ConvertToMySQLError(commitErr)
	require.Equal(t, [2]any{protocol.ErrCodeDeadlock, protocol.SQLStateDeadlock}, [2]any{refused.Code, refused.SQLState},
		"a commit refused at the detach does not reach a MySQL client as retryable: %v", commitErr)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the detach did not finish once the transaction in flight ended")
	}

	require.NoError(t, dm.AttachDatabase("app"))
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	var n int
	require.NoError(t, app.GetReadDB().QueryRow("SELECT COUNT(*) FROM t WHERE id = 500").Scan(&n))
	require.Zero(t, n, "a refused commit left its row behind")
}

// TestDetachThatCannotDrainKeepsTheFile: a write transaction that outlasts the
// drain's deadline makes DetachDatabase return ErrDrainIncomplete with the
// database still detached. The transaction's commit is refused all the same,
// and the database reattaches over its own file, in service.
//
// Mutation: drop the drain's error (drainSQLite returns nil after a failed
// Conn). "a detach that did not drain reported success" fires.
func TestDetachThatCannotDrainKeepsTheFile(t *testing.T) {
	dm := newDetachTestManager(t)
	held, err := dm.GetDatabase("app")
	require.NoError(t, err)
	_, err = held.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (1, 'kept')")
	require.NoError(t, err)
	tx, err := held.GetWriteDB().Begin()
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO t (id, v) VALUES (500, 'in flight')")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = dm.DetachDatabase(ctx, "app")
	require.ErrorIs(t, err, ErrDrainIncomplete, "a detach that did not drain reported success")
	_, err = dm.GetDatabase("app")
	require.ErrorIs(t, err, ErrDatabaseDetached)
	require.Error(t, tx.Commit(), "an in-flight transaction committed after its database was detached")

	require.NoError(t, dm.AttachDatabase("app"))
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	var v string
	require.NoError(t, app.GetReadDB().QueryRow("SELECT v FROM t WHERE id = 1").Scan(&v))
	require.Equal(t, "kept", v)
	_, err = app.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (2, 'after')")
	require.NoError(t, err, "the reattached database does not accept writes")
}

// TestSnapshotsRefuseWhileADatabaseIsDetached: a snapshot of every database
// ships the registry, which still names a detached database. Rather than
// silently leave its file out, both snapshot producers refuse with
// ErrDatabaseDetached (the gRPC layer turns that into Unavailable, which the
// caller retries), and include it again once it is back.
//
// Mutation: drop the check (errIfDetachedLocked returns nil). "a snapshot
// omitted a detached database" fires for TakeSnapshot and TakeSnapshotToDir.
func TestSnapshotsRefuseWhileADatabaseIsDetached(t *testing.T) {
	dm := newDetachTestManager(t)
	require.NoError(t, dm.DetachDatabase(context.Background(), "app"))

	_, _, err := dm.TakeSnapshot()
	require.ErrorIs(t, err, ErrDatabaseDetached, "a snapshot omitted a detached database (TakeSnapshot)")
	_, _, err = dm.TakeSnapshotToDir(t.TempDir())
	require.ErrorIs(t, err, ErrDatabaseDetached, "a snapshot omitted a detached database (TakeSnapshotToDir)")

	require.NoError(t, dm.AttachDatabase("app"))
	infos, _, err := dm.TakeSnapshot()
	require.NoError(t, err)
	require.Contains(t, snapshotNames(infos), "app")
	infos, _, err = dm.TakeSnapshotToDir(t.TempDir())
	require.NoError(t, err)
	require.Contains(t, snapshotNames(infos), "app")
}

// TestPerDatabaseSnapshotsRefuseOnlyTheirOwnDetach: a snapshot of one
// database ships no other database and no registry, so another database
// being out of service does not refuse it; its own detach does, with
// ErrDatabaseDetached.
//
// Mutation: make TakeDatabaseSnapshotInfo (or TakeSnapshotForDatabase) call
// errIfDetachedLocked first. "a per-database snapshot was refused because
// another database was detached" fires. Mutation: make getDatabaseLocked
// report a detached database as not existing. "a per-database snapshot of a
// detached database was not refused as detached" fires.
func TestPerDatabaseSnapshotsRefuseOnlyTheirOwnDetach(t *testing.T) {
	dm := newDetachTestManager(t)
	require.NoError(t, dm.CreateDatabase("other"))
	require.NoError(t, dm.DetachDatabase(context.Background(), "other"))

	info, _, err := dm.TakeDatabaseSnapshotInfo("app")
	require.NoError(t, err, "a per-database snapshot was refused because another database was detached (TakeDatabaseSnapshotInfo)")
	require.Equal(t, "app", info.Name)
	copied, _, err := dm.TakeSnapshotForDatabase(t.TempDir(), "app")
	require.NoError(t, err, "a per-database snapshot was refused because another database was detached (TakeSnapshotForDatabase)")
	require.Equal(t, info.SHA256, copied.SHA256, "the per-database info does not describe the file the snapshot copies")

	_, _, err = dm.TakeDatabaseSnapshotInfo("other")
	require.ErrorIs(t, err, ErrDatabaseDetached, "a per-database snapshot of a detached database was not refused as detached (TakeDatabaseSnapshotInfo)")
	_, _, err = dm.TakeSnapshotForDatabase(t.TempDir(), "other")
	require.ErrorIs(t, err, ErrDatabaseDetached, "a per-database snapshot of a detached database was not refused as detached (TakeSnapshotForDatabase)")
}

func snapshotNames(infos []SnapshotInfo) []string {
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name)
	}
	return names
}

// strandDatabase detaches "app" and reattaches it over a file that is not a
// SQLite database, as a restore that installed an unreadable file would. It
// returns the path of the database's file and its original content.
func strandDatabase(t *testing.T, dm *DatabaseManager) (string, []byte) {
	t.Helper()
	path, err := dm.GetDatabasePath("app")
	require.NoError(t, err)
	require.NoError(t, dm.DetachDatabase(context.Background(), "app"))
	good, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, suffix := range []string{"-wal", "-shm"} {
		require.NoError(t, os.RemoveAll(path+suffix))
	}
	require.NoError(t, os.WriteFile(path, []byte("not a SQLite database, not at all"), 0o644))
	require.Error(t, dm.AttachDatabase("app"))
	return path, good
}

// TestFailedReattachAwaitsAnotherRestore: a reattach that cannot open the
// restored file leaves the database out of service but not lost. It still
// exists, lookups get ErrDatabaseDetached, it is listed for another restore,
// and the next restore of it (a detach that takes it over, a good file, a
// reattach) puts it back in service.
//
// Mutation: make DetachDatabase refuse a database whose reattach failed.
// "a database whose reattach failed cannot be restored again" fires.
func TestFailedReattachAwaitsAnotherRestore(t *testing.T) {
	dm := newDetachTestManager(t)
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	_, err = app.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (1, 'restored')")
	require.NoError(t, err)
	path, good := strandDatabase(t, dm)

	require.True(t, dm.DatabaseExists("app"))
	_, err = dm.GetDatabase("app")
	require.ErrorIs(t, err, ErrDatabaseDetached)
	require.Equal(t, []string{"app"}, dm.DatabasesAwaitingRestore(), "a database whose reattach failed is not listed for another restore")
	require.NoError(t, dm.CreateDatabase("app"), "CREATE of an existing database must stay idempotent")

	require.NoError(t, dm.DetachDatabase(context.Background(), "app"), "a database whose reattach failed cannot be restored again")
	require.Empty(t, dm.DatabasesAwaitingRestore(), "a database under restore is listed as awaiting one")
	require.ErrorIs(t, dm.DetachDatabase(context.Background(), "app"), ErrDatabaseDetached, "two restores took the same database")
	require.NoError(t, os.WriteFile(path, good, 0o644))
	require.NoError(t, dm.AttachDatabase("app"))

	app, err = dm.GetDatabase("app")
	require.NoError(t, err)
	var v string
	require.NoError(t, app.GetReadDB().QueryRow("SELECT v FROM t WHERE id = 1").Scan(&v))
	require.Equal(t, "restored", v)
}

// TestDropDatabaseDuringARestore: a replicated DROP DATABASE that arrives
// while the database is detached for a restore takes effect at once - the
// registry forgets it and it no longer exists for lookups - and the end of
// the restore removes its files instead of reattaching it. A CREATE of the
// same name is refused until then (retryable) and succeeds afterwards.
//
// Mutation: refuse DROP DATABASE while the database is detached, as fix
// round 2 did. "a DROP DATABASE during a restore was refused" fires.
func TestDropDatabaseDuringARestore(t *testing.T) {
	dm := newDetachTestManager(t)
	path, err := dm.GetDatabasePath("app")
	require.NoError(t, err)
	require.NoError(t, dm.DetachDatabase(context.Background(), "app"))

	require.NoError(t, dm.DropDatabase("app"), "a DROP DATABASE during a restore was refused")
	require.False(t, dm.DatabaseExists("app"), "a dropped database still exists")
	_, err = dm.GetDatabase("app")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrDatabaseDetached)
	// The registry keeps the row as a tombstone (dropped=1) rather than
	// deleting it (DatabaseRegistryKey), so a later
	// CREATE can fence a stale peer with a strictly higher generation.
	require.Contains(t, registryNames(t, dm), "app", "the registry must keep a dropped database's row as a tombstone")
	key, err := dm.RegistryKey("app")
	require.NoError(t, err)
	require.True(t, key.Dropped, "a dropped database's registry row must be tombstoned")
	require.ErrorIs(t, dm.CreateDatabase("app"), ErrDatabaseDetached, "a CREATE raced the files of a drop in progress")
	require.NoError(t, dm.DropDatabase("app"), "a repeated DROP must stay idempotent")

	require.NoError(t, dm.AttachDatabase("app"))
	require.False(t, dm.DatabaseExists("app"), "a database dropped during its restore came back")
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist, "a database dropped during its restore kept its file")
	_, err = os.Stat(filepath.Join(filepath.Dir(path), "app_meta.pebble"))
	require.ErrorIs(t, err, os.ErrNotExist, "a database dropped during its restore kept its meta store")

	require.NoError(t, dm.CreateDatabase("app"))
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	var n int
	require.NoError(t, app.GetReadDB().QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = 't'").Scan(&n))
	require.Zero(t, n, "the re-created database kept the dropped one's tables")
}

// TestDropDatabaseAwaitingARestore: a database whose reattach failed is
// dropped outright.
//
// Mutation: treat it as a restore in progress (leave the drop to
// AttachDatabase). "a database awaiting a restore was not dropped" fires.
func TestDropDatabaseAwaitingARestore(t *testing.T) {
	dm := newDetachTestManager(t)
	path, _ := strandDatabase(t, dm)

	require.NoError(t, dm.DropDatabase("app"))
	require.False(t, dm.DatabaseExists("app"))
	require.Empty(t, dm.DatabasesAwaitingRestore())
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist, "a database awaiting a restore was not dropped")
	require.NoError(t, dm.CreateDatabase("app"))
}

func registryNames(t *testing.T, dm *DatabaseManager) []string {
	t.Helper()
	rows, err := dm.GetSystemDatabase().GetReadDB().Query("SELECT name FROM __marmot_databases")
	require.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// TestQueuedBatchCommitsAtCloseAndAtDetach: a commit queued on the batch
// committer and not yet flushed is committed when the database closes (drop,
// shutdown), as before the write gate existed, but refused when the database
// is detached for a restore, whose file is about to be replaced.
//
// Mutation: close the gate before stopping the batch committer in
// closeSQLite. "a commit queued before Close was refused" fires. Mutation:
// stop the batch committer before the gate closes on detach (in
// takeOutOfService and drainSQLite). "a commit queued before the detach was
// written into the file being replaced" fires.
func TestQueuedBatchCommitsAtCloseAndAtDetach(t *testing.T) {
	restore := cfg.Config.BatchCommit
	t.Cleanup(func() { cfg.Config.BatchCommit = restore })
	cfg.Config.BatchCommit.Enabled = true
	cfg.Config.BatchCommit.MaxWaitMS = int(time.Minute / time.Millisecond) // only Stop flushes
	cfg.Config.BatchCommit.MaxBatchSize = 1024
	clock := hlc.NewClock(1)

	t.Run("close", func(t *testing.T) {
		dm := newDetachTestManager(t)
		app, err := dm.GetDatabase("app")
		require.NoError(t, err)
		queued := app.batchCommitter.Enqueue(9801, clock.Now(), nil, nil)
		require.NoError(t, app.Close())
		_, err = queued.Get()
		require.NoError(t, err, "a commit queued before Close was refused")
	})
	t.Run("detach", func(t *testing.T) {
		dm := newDetachTestManager(t)
		app, err := dm.GetDatabase("app")
		require.NoError(t, err)
		queued := app.batchCommitter.Enqueue(9802, clock.Now(), nil, nil)
		require.NoError(t, dm.DetachDatabase(context.Background(), "app"))
		_, err = queued.Get()
		require.Error(t, err, "a commit queued before the detach was written into the file being replaced")
	})
}

// TestReplicaReopenedPoolsAreGated: a replica restore closes a database's
// pools and opens new ones over the installed file (CloseDatabaseConnections,
// OpenDatabaseConnections). The new pools carry the same write gate, so once
// the database leaves service a commit on any of them is refused, as on the
// pools it was opened with.
//
// Mutation: open any one of the pools in OpenSQLiteConnections with sql.Open.
// "a database's reopened pool committed after it left service" fires for that
// pool.
func TestReplicaReopenedPoolsAreGated(t *testing.T) {
	dm := newDetachTestManager(t)
	require.NoError(t, dm.CloseDatabaseConnections("app"))
	require.NoError(t, dm.OpenDatabaseConnections("app"))
	app, err := dm.GetDatabase("app")
	require.NoError(t, err)
	_, err = app.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (1, 'reopened')")
	require.NoError(t, err, "a database's reopened pools do not accept writes")

	app.gate.close()
	pools := map[string]*sql.DB{"write": app.writeDB, "hook": app.hookDB, "read": app.readDB}
	for name, pool := range pools {
		_, err := pool.Exec("INSERT INTO t (id, v) VALUES (2, ?)", name)
		require.Error(t, err, "a database's reopened pool committed after it left service (%s)", name)
	}
}
