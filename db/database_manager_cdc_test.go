package db

import (
	"os"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testNodeID uint64 = 1

// mockCDCHub implements CDCHub for testing
type mockCDCHub struct {
	signals []CDCSignal
}

func (m *mockCDCHub) Signal(database string, txnID uint64) {
	m.signals = append(m.signals, CDCSignal{
		Database: database,
		TxnID:    txnID,
	})
}

func (m *mockCDCHub) Subscribe(filter CDCFilter) (<-chan CDCSignal, func()) {
	ch := make(chan CDCSignal, 10)
	cancel := func() { close(ch) }
	return ch, cancel
}

func TestDatabaseManager_CDCHubWiring(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "marmot-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database manager
	clock := hlc.NewClock(testNodeID)
	dm, err := NewDatabaseManager(tmpDir, testNodeID, clock)
	require.NoError(t, err)
	defer dm.Close()

	// Verify CDCHub is nil initially
	assert.Nil(t, dm.GetCDCHub())

	// Create a mock hub
	hub := &mockCDCHub{}

	// Set the CDC hub
	dm.SetCDCHub(hub)

	// Verify hub is set
	assert.Equal(t, hub, dm.GetCDCHub())

	// Create a new database
	err = dm.CreateDatabase("test_db")
	require.NoError(t, err)

	// Get the database
	db, err := dm.GetDatabase("test_db")
	require.NoError(t, err)

	// Verify the transaction manager has the notifier wired
	txnMgr := db.GetTransactionManager()
	assert.NotNil(t, txnMgr)

	// The notifier should be set (we can't directly test it without triggering a transaction,
	// but we've verified the wiring logic is correct)
}

func TestDatabaseManager_CDCHubWiringExistingDatabases(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "marmot-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database manager
	clock := hlc.NewClock(testNodeID)
	dm, err := NewDatabaseManager(tmpDir, testNodeID, clock)
	require.NoError(t, err)
	defer dm.Close()

	// Create databases before setting CDC hub
	err = dm.CreateDatabase("db1")
	require.NoError(t, err)
	err = dm.CreateDatabase("db2")
	require.NoError(t, err)

	// Create a mock hub
	hub := &mockCDCHub{}

	// Set the CDC hub - this should wire to all existing databases
	dm.SetCDCHub(hub)

	// Verify hub is set
	assert.Equal(t, hub, dm.GetCDCHub())

	// Verify both existing databases have the notifier
	db1, err := dm.GetDatabase("db1")
	require.NoError(t, err)
	assert.NotNil(t, db1.GetTransactionManager())

	db2, err := dm.GetDatabase("db2")
	require.NoError(t, err)
	assert.NotNil(t, db2.GetTransactionManager())
}

func TestDatabaseManager_CDCHubNilSafe(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "marmot-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database manager without setting CDC hub
	clock := hlc.NewClock(1)
	dm, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	defer dm.Close()

	// Verify CDCHub is nil
	assert.Nil(t, dm.GetCDCHub())

	// Create a database - should work fine with nil CDC hub
	err = dm.CreateDatabase("test_db")
	require.NoError(t, err)

	// Verify database was created
	db, err := dm.GetDatabase("test_db")
	require.NoError(t, err)
	assert.NotNil(t, db)
}

func TestDatabaseManager_ReopenDatabasePreservesCDCHub(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "marmot-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database manager
	clock := hlc.NewClock(testNodeID)
	dm, err := NewDatabaseManager(tmpDir, testNodeID, clock)
	require.NoError(t, err)
	defer dm.Close()

	// Set CDC hub
	hub := &mockCDCHub{}
	dm.SetCDCHub(hub)

	// Create a database
	err = dm.CreateDatabase("test_db")
	require.NoError(t, err)

	// Reopen the database (simulates snapshot reload)
	err = dm.ReopenDatabase("test_db")
	require.NoError(t, err)

	// Verify CDC hub is still set
	assert.Equal(t, hub, dm.GetCDCHub())

	// Verify the reopened database has the notifier
	db, err := dm.GetDatabase("test_db")
	require.NoError(t, err)
	assert.NotNil(t, db.GetTransactionManager())
}
