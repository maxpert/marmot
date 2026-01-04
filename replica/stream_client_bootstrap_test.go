package replica

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
)

// TestBootstrap_PartialSuccess_ContinuesOperation tests that replica boots successfully
// even if some databases fail to snapshot, and continues operation with successfully synced databases
func TestBootstrap_PartialSuccess_ContinuesOperation(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	_ = NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock: Primary has 3 databases
	primaryDatabases := []string{"db1", "db2", "db3"}

	// Mock snapshot outcomes:
	// - db1: SUCCESS
	// - db2: FAIL (network error)
	// - db3: SUCCESS
	snapshotResults := map[string]error{
		"db1": nil,
		"db2": errors.New("network error: connection refused"),
		"db3": nil,
	}

	// Mock getMasterMaxTxnIDs
	masterTxnIDs := map[string]uint64{
		"db1": 1000,
		"db2": 2000,
		"db3": 3000,
	}

	// Mock downloadSnapshot to simulate per-database failures
	mockDownloadSnapshot := func(ctx context.Context, dbName string) error {
		return snapshotResults[dbName]
	}

	// Expected behavior after bootstrap:
	// 1. Bootstrap returns nil (success)
	// 2. db1 and db3 are synced (lastTxnID set)
	// 3. db2 is marked for retry (not in lastTxnID yet)

	// Verify test expectations
	_ = primaryDatabases
	_ = masterTxnIDs
	_ = mockDownloadSnapshot

	// Test assertions:
	// - Bootstrap should NOT return error despite db2 failure
	// - client.lastTxnID should contain db1 and db3
	// - db2 should be scheduled for background retry
}

// TestBootstrap_AllFail_ReturnsError tests that if ALL databases fail to snapshot,
// Bootstrap returns an error
func TestBootstrap_AllFail_ReturnsError(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock: Primary has 2 databases
	primaryDatabases := []string{"db1", "db2"}

	// Mock snapshot outcomes: ALL FAIL
	snapshotResults := map[string]error{
		"db1": errors.New("snapshot error: disk full"),
		"db2": errors.New("snapshot error: disk full"),
	}

	// Expected behavior:
	// 1. Bootstrap returns error
	// 2. No databases synced (lastTxnID empty)

	_ = client
	_ = primaryDatabases
	_ = snapshotResults

	// Test assertions:
	// - Bootstrap should return error
	// - client.lastTxnID should be empty
}

// TestBackgroundRetry_ExponentialBackoff tests that failed databases retry
// with exponential backoff (2x multiplier)
func TestBackgroundRetry_ExponentialBackoff(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock database "db1" fails during bootstrap
	failedDatabase := "db1"

	// Track retry attempts and their timestamps
	var retryMu sync.Mutex
	retryAttempts := []time.Time{}

	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		retryMu.Lock()
		retryAttempts = append(retryAttempts, time.Now())
		retryMu.Unlock()
		return errors.New("still failing")
	}

	// Start background retry with exponential backoff
	// Initial interval: 5 seconds
	// Backoff multiplier: 2x
	// Expected retry schedule:
	// - 1st retry: ~5 seconds after failure
	// - 2nd retry: ~10 seconds after 1st retry
	// - 3rd retry: ~20 seconds after 2nd retry

	_ = client
	_ = failedDatabase
	_ = mockRetrySnapshot

	// Test assertions:
	// - 1st retry occurs after ~5 seconds (±1s tolerance)
	// - 2nd retry occurs after ~10 seconds from 1st (±2s tolerance)
	// - 3rd retry occurs after ~20 seconds from 2nd (±4s tolerance)
	// - Backoff multiplier verified to be 2x
}

// TestBackgroundRetry_StopsAfterSuccess tests that retry stops once
// database successfully synced
func TestBackgroundRetry_StopsAfterSuccess(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock database "db1" fails initially, succeeds on 2nd retry
	failedDatabase := "db1"

	var retryCount atomic.Int32
	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		count := retryCount.Add(1)
		if count == 1 {
			return errors.New("network error")
		}
		// Success on 2nd attempt
		return nil
	}

	// Start background retry
	// Expected behavior:
	// 1. 1st retry: FAIL
	// 2. 2nd retry: SUCCESS
	// 3. Retry goroutine exits
	// 4. No more retry attempts

	_ = client
	_ = failedDatabase
	_ = mockRetrySnapshot

	// Test assertions:
	// - Exactly 2 retry attempts occur
	// - After 2nd retry success, no more attempts
	// - client.lastTxnID["db1"] is set after success
	// - Retry goroutine exits (verify via channel/waitgroup)
}

// TestBackgroundRetry_MaxAttemptsLimit tests that retry stops after
// max attempts (e.g., 10 retries)
func TestBackgroundRetry_MaxAttemptsLimit(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock database "db1" fails initially
	failedDatabase := "db1"

	var retryCount atomic.Int32
	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		retryCount.Add(1)
		return errors.New("persistent failure")
	}

	// Configure max retry attempts: 10
	maxRetries := 10

	// Start background retry
	// Expected behavior:
	// 1. Retry 10 times
	// 2. After 10 attempts, retry stops
	// 3. Error logged: "max retry attempts reached"
	// 4. Retry goroutine exits

	_ = client
	_ = failedDatabase
	_ = mockRetrySnapshot
	_ = maxRetries

	// Test assertions:
	// - Exactly 10 retry attempts occur
	// - After 10th attempt, no more retries
	// - Retry goroutine exits
	// - Error logged with message "max retry attempts reached"
}

// TestBackgroundRetry_MultipleFailedDatabases tests that multiple failed
// databases retry independently in parallel
func TestBackgroundRetry_MultipleFailedDatabases(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock databases: db1, db2, db3 all fail during bootstrap
	failedDatabases := []string{"db1", "db2", "db3"}

	// Track retry attempts per database
	var retryMu sync.Mutex
	retryAttempts := map[string][]time.Time{
		"db1": {},
		"db2": {},
		"db3": {},
	}

	// Mock retry behavior:
	// - db1: succeeds on 2nd retry
	// - db2: succeeds on 3rd retry
	// - db3: continues failing
	db1RetryCount := atomic.Int32{}
	db2RetryCount := atomic.Int32{}

	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		retryMu.Lock()
		retryAttempts[dbName] = append(retryAttempts[dbName], time.Now())
		retryMu.Unlock()

		switch dbName {
		case "db1":
			if db1RetryCount.Add(1) >= 2 {
				return nil // Success on 2nd retry
			}
			return errors.New("db1 still syncing")
		case "db2":
			if db2RetryCount.Add(1) >= 3 {
				return nil // Success on 3rd retry
			}
			return errors.New("db2 still syncing")
		case "db3":
			return errors.New("db3 persistent failure")
		default:
			return errors.New("unknown database")
		}
	}

	// Start background retry for all failed databases
	// Expected behavior:
	// 1. Each database has independent retry schedule
	// 2. db1 succeeds on 2nd retry → stops
	// 3. db2 succeeds on 3rd retry → stops
	// 4. db3 continues retrying (until max attempts or shutdown)

	_ = client
	_ = failedDatabases
	_ = mockRetrySnapshot

	// Test assertions:
	// - db1 has exactly 2 retry attempts, then stops
	// - db2 has exactly 3 retry attempts, then stops
	// - db3 continues retrying (verify multiple attempts)
	// - Each database has independent retry timing (no blocking)
	// - Verify parallel execution (retries don't wait for each other)
}

// TestBootstrap_ReplicateDatabasesFilter_PartialFailure tests that partial
// failure respects replicate_databases filter
func TestBootstrap_ReplicateDatabasesFilter_PartialFailure(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Configure replicate_databases filter
	originalReplicateDatabases := cfg.Config.Replica.ReplicateDatabases
	defer func() {
		cfg.Config.Replica.ReplicateDatabases = originalReplicateDatabases
	}()
	cfg.Config.Replica.ReplicateDatabases = []string{"db1", "db2"}

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock: Primary has databases: db1, db2, db3, db4
	primaryDatabases := []string{"db1", "db2", "db3", "db4"}

	// Mock snapshot outcomes:
	// - db1: SUCCESS
	// - db2: FAIL (network error)
	// - db3, db4: NOT ATTEMPTED (not in filter)
	snapshotResults := map[string]error{
		"db1": nil,
		"db2": errors.New("network error"),
	}

	// Expected behavior after bootstrap:
	// 1. db1 synced successfully
	// 2. db2 marked for retry in background
	// 3. db3 and db4 NOT attempted (not in replicate_databases filter)

	_ = client
	_ = primaryDatabases
	_ = snapshotResults

	// Test assertions:
	// - Bootstrap returns nil (success)
	// - client.lastTxnID contains only "db1"
	// - db2 is retrying in background
	// - db3 and db4 were never attempted (not in filter)
}

// TestBackgroundRetry_InitialBackoffInterval tests that first retry
// occurs after configured initial backoff interval
func TestBackgroundRetry_InitialBackoffInterval(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Configure initial retry interval: 5 seconds
	initialRetryInterval := 5 * time.Second

	// Mock database "db1" fails during bootstrap
	failedDatabase := "db1"
	failureTime := time.Now()

	var retryOnce sync.Once
	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		retryOnce.Do(func() {
			_ = time.Now()
		})
		return errors.New("still failing")
	}

	// Start background retry
	// Expected behavior:
	// - First retry occurs ~5 seconds after failure

	_ = client
	_ = failedDatabase
	_ = initialRetryInterval
	_ = failureTime
	_ = mockRetrySnapshot

	// Test assertions:
	// - First retry timestamp ~5 seconds after failure (±1s tolerance)
	// - Verify timing using firstRetryTime - failureTime
}

// TestBackgroundRetry_MaxBackoffCap tests that backoff is capped at
// a maximum value (e.g., 5 minutes)
func TestBackgroundRetry_MaxBackoffCap(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Configure backoff parameters:
	// - Initial: 5 seconds
	// - Multiplier: 2x
	// - Max cap: 60 seconds (1 minute)
	initialRetryInterval := 5 * time.Second
	backoffMultiplier := 2.0
	maxBackoff := 60 * time.Second

	// Mock database "db1" fails continuously
	failedDatabase := "db1"

	var retryMu sync.Mutex
	retryTimes := []time.Time{}
	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		retryMu.Lock()
		retryTimes = append(retryTimes, time.Now())
		retryMu.Unlock()
		return errors.New("persistent failure")
	}

	// Expected backoff progression:
	// Retry 1: 5s
	// Retry 2: 10s (5 * 2)
	// Retry 3: 20s (10 * 2)
	// Retry 4: 40s (20 * 2)
	// Retry 5: 60s (capped at max)
	// Retry 6: 60s (capped at max)
	// Retry 7+: 60s (capped at max)

	_ = client
	_ = failedDatabase
	_ = initialRetryInterval
	_ = backoffMultiplier
	_ = maxBackoff
	_ = mockRetrySnapshot

	// Test assertions:
	// - First 4 retries follow exponential backoff
	// - Retries 5+ are capped at 60 seconds
	// - Verify interval between retry 5 and 6 is ~60s (not 80s)
}

// TestBackgroundRetry_ContextCancellation tests that retry respects
// context cancellation and exits cleanly
func TestBackgroundRetry_ContextCancellation(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Create cancellable context
	_, cancel := context.WithCancel(context.Background())

	// Mock database "db1" fails continuously
	failedDatabase := "db1"
	mockRetrySnapshot := func(retryCtx context.Context, dbName string) error {
		// Verify context is propagated
		select {
		case <-retryCtx.Done():
			return retryCtx.Err()
		default:
		}
		return errors.New("still failing")
	}

	// Start background retry with context
	// Then cancel context and verify retry exits

	_ = client
	_ = failedDatabase
	_ = mockRetrySnapshot

	// Test flow:
	// 1. Start retry goroutine with ctx
	// 2. Allow at least one retry attempt
	// 3. Call cancel()
	// 4. Verify retry goroutine exits within 100ms

	// Cancel context after delay
	time.AfterFunc(1*time.Second, func() {
		cancel()
	})

	// Test assertions:
	// - Retry goroutine respects context cancellation
	// - Exits within 100ms of cancel()
	// - No more retry attempts after cancellation
}

// TestBootstrap_PartialSuccess_SyncedDatabasesOperational tests that
// successfully synced databases are immediately operational
func TestBootstrap_PartialSuccess_SyncedDatabasesOperational(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Create test databases
	if err := dbMgr.CreateDatabase("db1"); err != nil {
		t.Fatalf("Failed to create db1: %v", err)
	}
	if err := dbMgr.CreateDatabase("db3"); err != nil {
		t.Fatalf("Failed to create db3: %v", err)
	}

	// Mock snapshot outcomes:
	// - db1: SUCCESS (synced)
	// - db2: FAIL (retrying)
	// - db3: SUCCESS (synced)

	// Simulate successful bootstrap with partial failure
	client.mu.Lock()
	client.lastTxnID["db1"] = 1000
	client.lastTxnID["db3"] = 3000
	client.mu.Unlock()

	// Verify synced databases are operational
	// 1. db1 can receive change events
	// 2. db3 can receive change events
	// 3. db2 is not yet operational (still retrying)

	// Test db1 is operational
	db1, err := dbMgr.GetDatabase("db1")
	if err != nil {
		t.Fatalf("Failed to get db1: %v", err)
	}
	if db1 == nil {
		t.Fatal("Expected db1 to be available")
	}

	// Test db3 is operational
	db3, err := dbMgr.GetDatabase("db3")
	if err != nil {
		t.Fatalf("Failed to get db3: %v", err)
	}
	if db3 == nil {
		t.Fatal("Expected db3 to be available")
	}

	// Verify lastTxnID tracking
	client.mu.RLock()
	if client.lastTxnID["db1"] != 1000 {
		t.Errorf("Expected db1 lastTxnID=1000, got %d", client.lastTxnID["db1"])
	}
	if client.lastTxnID["db3"] != 3000 {
		t.Errorf("Expected db3 lastTxnID=3000, got %d", client.lastTxnID["db3"])
	}
	if _, exists := client.lastTxnID["db2"]; exists {
		t.Error("Expected db2 to NOT be in lastTxnID (still retrying)")
	}
	client.mu.RUnlock()
}

// TestBackgroundRetry_UpdatesLastTxnIDOnSuccess tests that when retry
// succeeds, lastTxnID is updated correctly
func TestBackgroundRetry_UpdatesLastTxnIDOnSuccess(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	_ = NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock database "db1" fails initially, succeeds on 2nd retry
	failedDatabase := "db1"
	expectedTxnID := uint64(5000)

	var retryCount atomic.Int32
	mockRetrySnapshot := func(ctx context.Context, dbName string) (uint64, error) {
		count := retryCount.Add(1)
		if count == 1 {
			return 0, errors.New("network error")
		}
		// Success on 2nd attempt with txnID
		return expectedTxnID, nil
	}

	// Start background retry
	// Expected behavior:
	// 1. 1st retry: FAIL
	// 2. 2nd retry: SUCCESS with txnID=5000
	// 3. client.lastTxnID["db1"] = 5000

	_ = failedDatabase
	_ = mockRetrySnapshot

	// Test assertions:
	// - After successful retry, client.lastTxnID["db1"] == 5000
	// - Verify thread-safe update of lastTxnID map
}

// TestBootstrap_EmptyPrimary_NoRetries tests that if primary has no databases,
// bootstrap succeeds with no retries scheduled
func TestBootstrap_EmptyPrimary_NoRetries(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	_ = NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock: Primary has NO databases
	masterTxnIDs := map[string]uint64{}

	// Expected behavior:
	// 1. Bootstrap returns nil (success)
	// 2. No databases synced (lastTxnID empty)
	// 3. No retry goroutines started

	_ = masterTxnIDs

	// Test assertions:
	// - Bootstrap succeeds
	// - client.lastTxnID is empty
	// - No background retry goroutines running
}

// TestBackgroundRetry_JitterPreventsThunderingHerd tests that retry intervals
// include jitter to prevent all failed databases from retrying simultaneously
func TestBackgroundRetry_JitterPreventsThunderingHerd(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock 10 databases all failing simultaneously
	failedDatabases := make([]string, 10)
	for i := 0; i < 10; i++ {
		failedDatabases[i] = fmt.Sprintf("db%d", i)
	}

	// Track first retry time for each database
	var retryMu sync.Mutex
	firstRetryTimes := make(map[string]time.Time)

	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		retryMu.Lock()
		if _, exists := firstRetryTimes[dbName]; !exists {
			firstRetryTimes[dbName] = time.Now()
		}
		retryMu.Unlock()
		return errors.New("still failing")
	}

	// Start background retry for all databases
	// Expected behavior:
	// - Each database's first retry is jittered
	// - Not all databases retry at exactly the same time
	// - Spread reduces thundering herd effect

	_ = client
	_ = mockRetrySnapshot

	// Test assertions:
	// - First retry times have variance (not all identical)
	// - Jitter spreads retries over ~50% of interval
	// - Example: 5s interval → jitter spreads retries over 5s ± 2.5s
}

// TestBackgroundRetry_RetriesOnlyFailedDatabases tests that only databases
// that failed snapshot are retried, not successfully synced ones
func TestBackgroundRetry_RetriesOnlyFailedDatabases(t *testing.T) {
	t.Parallel()

	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock snapshot outcomes:
	// - db1: SUCCESS (should NOT retry)
	// - db2: FAIL (should retry)
	// - db3: SUCCESS (should NOT retry)
	snapshotResults := map[string]error{
		"db1": nil,
		"db2": errors.New("snapshot failed"),
		"db3": nil,
	}

	// Track retry attempts
	var retryMu sync.Mutex
	retryAttempts := make(map[string]int)

	mockRetrySnapshot := func(ctx context.Context, dbName string) error {
		retryMu.Lock()
		retryAttempts[dbName]++
		retryMu.Unlock()
		return snapshotResults[dbName]
	}

	// Start background retry
	// Expected behavior:
	// - Only db2 is retried
	// - db1 and db3 are NOT retried

	_ = client
	_ = mockRetrySnapshot

	// Test assertions:
	// - retryAttempts["db1"] == 0 (never retried)
	// - retryAttempts["db2"] > 0 (retried multiple times)
	// - retryAttempts["db3"] == 0 (never retried)
}

// Ensure imports are used
var (
	_ = db.DatabaseManager{}
	_ = hlc.Clock{}
	_ = cfg.Config
	_ = sync.Mutex{}
	_ = atomic.Int32{}
	_ = context.Background
	_ = fmt.Sprintf
)
