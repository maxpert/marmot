//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// setupConflictTestDBs creates user DB and meta store for conflict testing
func setupConflictTestDBs(t *testing.T) (userDB *sql.DB, metaStore MetaStore, cleanup func()) {
	tmpDir, err := os.MkdirTemp("", "hook_conflict_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	userDBPath := filepath.Join(tmpDir, "test.db")
	// Enable WAL mode for better concurrency, set busy timeout, and use READ UNCOMMITTED
	// isolation to allow preupdate hooks to fire before SQLite's write lock blocks
	userDB, err = sql.Open("sqlite3", userDBPath+"?_journal_mode=WAL&_timeout=100&_txlock=immediate")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to open user DB: %v", err)
	}

	// Set connection pool to allow multiple concurrent connections
	userDB.SetMaxOpenConns(10)

	// Enable read uncommitted mode for testing
	_, err = userDB.Exec("PRAGMA read_uncommitted = 1")
	if err != nil {
		userDB.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to set read_uncommitted: %v", err)
	}

	metaStore, err = NewMetaStore(userDBPath)
	if err != nil {
		userDB.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create meta store: %v", err)
	}

	cleanup = func() {
		metaStore.Close()
		userDB.Close()
		os.RemoveAll(tmpDir)
	}

	return userDB, metaStore, cleanup
}

// TestHookSession_RowConflict verifies that the CDC row lock mechanism works correctly
// by testing the lock acquisition and conflict detection using direct MetaStore API.
func TestHookSession_RowConflict(t *testing.T) {
	userDB, metaStore, cleanup := setupConflictTestDBs(t)
	defer cleanup()

	// Create test table
	_, err := userDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert initial data
	_, err = userDB.Exec("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	ctx := context.Background()
	schemaCache := NewSchemaCache()

	// Session 1: Start transaction and modify row 1
	session1, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 1001, []string{"users"})
	if err != nil {
		t.Fatalf("failed to start session1: %v", err)
	}
	defer session1.Rollback()

	if err := session1.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session1: %v", err)
	}

	// Session1 updates row 1 - this should acquire the lock
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'alice2' WHERE id = 1"); err != nil {
		t.Fatalf("session1 failed to update: %v", err)
	}

	// Check no conflict error
	if conflictErr := session1.GetConflictError(); conflictErr != nil {
		t.Fatalf("session1 should not have conflict: %v", conflictErr)
	}

	// Verify the lock is held by session1
	lockHolder, err := metaStore.GetCDCRowLock("users", "users:1")
	if err != nil {
		t.Fatalf("failed to check lock: %v", err)
	}
	if lockHolder != 1001 {
		t.Fatalf("expected lock held by txn 1001, got %d", lockHolder)
	}

	// Test conflict detection by trying to acquire the same lock from a different txn
	err = metaStore.AcquireCDCRowLock(1002, "users", "users:1")
	if err == nil {
		t.Fatal("expected error when acquiring lock held by another transaction")
	}

	// Verify it's the correct error type
	if _, ok := err.(ErrCDCRowLocked); !ok {
		t.Fatalf("expected ErrCDCRowLocked, got %T: %v", err, err)
	}

	// Verify the lock is still held by session1
	lockHolder, _ = metaStore.GetCDCRowLock("users", "users:1")
	if lockHolder != 1001 {
		t.Fatalf("lock should still be held by txn 1001, got %d", lockHolder)
	}
}

// TestHookSession_DifferentRowsNoConflict verifies that modifying different rows
// in the same table doesn't cause conflicts within a single session.
func TestHookSession_DifferentRowsNoConflict(t *testing.T) {
	userDB, metaStore, cleanup := setupConflictTestDBs(t)
	defer cleanup()

	// Create test table
	_, err := userDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert initial data
	_, err = userDB.Exec("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	ctx := context.Background()
	schemaCache := NewSchemaCache()

	// Single session: Modify different rows - should not conflict with itself
	session1, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 2001, []string{"users"})
	if err != nil {
		t.Fatalf("failed to start session1: %v", err)
	}
	defer session1.Rollback()

	if err := session1.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session1: %v", err)
	}

	// Update row 1
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'alice2' WHERE id = 1"); err != nil {
		t.Fatalf("session1 failed to update row 1: %v", err)
	}

	// Update row 2 - different row, no conflict
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'bob2' WHERE id = 2"); err != nil {
		t.Fatalf("session1 failed to update row 2: %v", err)
	}

	// Update row 3 - different row, no conflict
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'charlie2' WHERE id = 3"); err != nil {
		t.Fatalf("session1 failed to update row 3: %v", err)
	}

	// Should have no conflicts
	if conflictErr := session1.GetConflictError(); conflictErr != nil {
		t.Fatalf("session1 should not have conflict: %v", conflictErr)
	}

	// Verify all three rows were locked
	lock1, _ := metaStore.GetCDCRowLock("users", "users:1")
	lock2, _ := metaStore.GetCDCRowLock("users", "users:2")
	lock3, _ := metaStore.GetCDCRowLock("users", "users:3")

	if lock1 != 2001 || lock2 != 2001 || lock3 != 2001 {
		t.Fatalf("expected all locks held by txn 2001, got: row1=%d, row2=%d, row3=%d", lock1, lock2, lock3)
	}
}

// TestHookSession_DifferentTablesNoConflict verifies that modifying rows in different
// tables doesn't cause conflicts within a single session.
func TestHookSession_DifferentTablesNoConflict(t *testing.T) {
	userDB, metaStore, cleanup := setupConflictTestDBs(t)
	defer cleanup()

	// Create two tables
	_, err := userDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	_, err = userDB.Exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)")
	if err != nil {
		t.Fatalf("failed to create posts table: %v", err)
	}

	// Insert initial data
	_, err = userDB.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("failed to insert into users: %v", err)
	}

	_, err = userDB.Exec("INSERT INTO posts (id, title) VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("failed to insert into posts: %v", err)
	}

	ctx := context.Background()
	schemaCache := NewSchemaCache()

	// Single session: Modify both tables - should not conflict
	session1, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 3001, []string{"users", "posts"})
	if err != nil {
		t.Fatalf("failed to start session1: %v", err)
	}
	defer session1.Rollback()

	if err := session1.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session1: %v", err)
	}

	// Update users table
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'alice2' WHERE id = 1"); err != nil {
		t.Fatalf("session1 failed to update users: %v", err)
	}

	// Update posts table - different table, no conflict
	if err := session1.ExecContext(ctx, "UPDATE posts SET title = 'world' WHERE id = 1"); err != nil {
		t.Fatalf("session1 failed to update posts: %v", err)
	}

	// Should have no conflicts
	if conflictErr := session1.GetConflictError(); conflictErr != nil {
		t.Fatalf("session1 should not have conflict: %v", conflictErr)
	}

	// Verify both tables have locks
	usersLock, _ := metaStore.GetCDCRowLock("users", "users:1")
	postsLock, _ := metaStore.GetCDCRowLock("posts", "posts:1")

	if usersLock != 3001 || postsLock != 3001 {
		t.Fatalf("expected both locks held by txn 3001, got: users=%d, posts=%d", usersLock, postsLock)
	}
}

// TestHookSession_CleanupReleasesLocks verifies that cleanup properly releases locks
// so subsequent transactions can acquire them.
func TestHookSession_CleanupReleasesLocks(t *testing.T) {
	userDB, metaStore, cleanup := setupConflictTestDBs(t)
	defer cleanup()

	// Create test table
	_, err := userDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert initial data
	_, err = userDB.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	ctx := context.Background()
	schemaCache := NewSchemaCache()

	// Session 1: Acquire lock on row 1
	session1, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 4001, []string{"users"})
	if err != nil {
		t.Fatalf("failed to start session1: %v", err)
	}

	if err := session1.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session1: %v", err)
	}

	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'alice2' WHERE id = 1"); err != nil {
		t.Fatalf("session1 failed to update: %v", err)
	}

	// Commit session1 (cleanup happens here)
	if err := session1.Commit(); err != nil {
		t.Fatalf("session1 failed to commit: %v", err)
	}

	// Session 2: Should be able to acquire lock on same row now
	session2, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 4002, []string{"users"})
	if err != nil {
		t.Fatalf("failed to start session2: %v", err)
	}
	defer session2.Rollback()

	if err := session2.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session2: %v", err)
	}

	if err := session2.ExecContext(ctx, "UPDATE users SET name = 'alice3' WHERE id = 1"); err != nil {
		t.Fatalf("session2 failed to update: %v", err)
	}

	// Session2 should NOT have conflict (lock was released)
	if conflictErr := session2.GetConflictError(); conflictErr != nil {
		t.Fatalf("session2 should not have conflict after session1 cleanup: %v", conflictErr)
	}
}

// TestHookSession_DDLBlocksDML verifies that DML operations fail when a DDL lock
// is held on the table.
func TestHookSession_DDLBlocksDML(t *testing.T) {
	userDB, metaStore, cleanup := setupConflictTestDBs(t)
	defer cleanup()

	// Create test table
	_, err := userDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert initial data
	_, err = userDB.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	ctx := context.Background()
	schemaCache := NewSchemaCache()

	// Acquire DDL lock on users table using transaction 5001
	if err := metaStore.AcquireCDCTableDDLLock(5001, "users"); err != nil {
		t.Fatalf("failed to acquire DDL lock: %v", err)
	}
	defer metaStore.ReleaseCDCTableDDLLock("users", 5001)

	// Session 1: Try to modify row in locked table (different txn)
	session1, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 5002, []string{"users"})
	if err != nil {
		t.Fatalf("failed to start session1: %v", err)
	}
	defer session1.Rollback()

	if err := session1.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session1: %v", err)
	}

	// Try to update - the SQL might succeed, but hook should detect DDL conflict
	session1.ExecContext(ctx, "UPDATE users SET name = 'alice2' WHERE id = 1")

	// Check for DDL conflict error
	conflictErr := session1.GetConflictError()
	if conflictErr == nil {
		t.Fatal("session1 should have conflict error when table has DDL lock")
	}

	// Verify it's the correct error type
	if _, ok := conflictErr.(ErrCDCTableDDLInProgress); !ok {
		t.Fatalf("expected ErrCDCTableDDLInProgress, got %T: %v", conflictErr, conflictErr)
	}
}

// TestHookSession_SameTxnNoConflict verifies that the same transaction can
// re-acquire locks (idempotent).
func TestHookSession_SameTxnNoConflict(t *testing.T) {
	userDB, metaStore, cleanup := setupConflictTestDBs(t)
	defer cleanup()

	// Create test table
	_, err := userDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert initial data
	_, err = userDB.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	ctx := context.Background()
	schemaCache := NewSchemaCache()

	// Session 1: Update same row multiple times in same transaction
	session1, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 6001, []string{"users"})
	if err != nil {
		t.Fatalf("failed to start session1: %v", err)
	}
	defer session1.Rollback()

	if err := session1.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session1: %v", err)
	}

	// First update
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'alice2' WHERE id = 1"); err != nil {
		t.Fatalf("session1 first update failed: %v", err)
	}

	if conflictErr := session1.GetConflictError(); conflictErr != nil {
		t.Fatalf("session1 should not have conflict on first update: %v", conflictErr)
	}

	// Second update of same row - should be idempotent
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'alice3' WHERE id = 1"); err != nil {
		t.Fatalf("session1 second update failed: %v", err)
	}

	if conflictErr := session1.GetConflictError(); conflictErr != nil {
		t.Fatalf("session1 should not have conflict on second update (idempotent): %v", conflictErr)
	}
}

// TestHookSession_InsertDeleteSameSession verifies INSERT and DELETE operations
// within the same session work without conflicts.
func TestHookSession_InsertDeleteSameSession(t *testing.T) {
	userDB, metaStore, cleanup := setupConflictTestDBs(t)
	defer cleanup()

	// Create test table
	_, err := userDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert some initial data
	_, err = userDB.Exec("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("failed to insert initial data: %v", err)
	}

	ctx := context.Background()
	schemaCache := NewSchemaCache()

	// Session 1: Perform multiple operations
	session1, err := StartEphemeralSession(ctx, userDB, metaStore, schemaCache, 7001, []string{"users"})
	if err != nil {
		t.Fatalf("failed to start session1: %v", err)
	}
	defer session1.Rollback()

	if err := session1.BeginTx(ctx); err != nil {
		t.Fatalf("failed to begin tx in session1: %v", err)
	}

	// Insert a new row
	if err := session1.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (3, 'charlie')"); err != nil {
		t.Fatalf("session1 insert failed: %v", err)
	}

	// Delete an existing row
	if err := session1.ExecContext(ctx, "DELETE FROM users WHERE id = 1"); err != nil {
		t.Fatalf("session1 delete failed: %v", err)
	}

	// Update another row
	if err := session1.ExecContext(ctx, "UPDATE users SET name = 'bob2' WHERE id = 2"); err != nil {
		t.Fatalf("session1 update failed: %v", err)
	}

	// No conflicts expected
	if conflictErr := session1.GetConflictError(); conflictErr != nil {
		t.Fatalf("session1 should not have conflict: %v", conflictErr)
	}

	// Verify locks are held for all affected rows
	lock1, _ := metaStore.GetCDCRowLock("users", "users:1") // deleted
	lock2, _ := metaStore.GetCDCRowLock("users", "users:2") // updated
	lock3, _ := metaStore.GetCDCRowLock("users", "users:3") // inserted

	if lock1 != 7001 || lock2 != 7001 || lock3 != 7001 {
		t.Fatalf("expected all locks held by txn 7001, got: row1=%d, row2=%d, row3=%d", lock1, lock2, lock3)
	}
}
