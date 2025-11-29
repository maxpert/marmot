package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockNodeProvider implements coordinator.NodeProvider
type MockNodeProvider struct {
	nodes []uint64
}

func (m *MockNodeProvider) GetAliveNodes() ([]uint64, error) {
	return m.nodes, nil
}

func (m *MockNodeProvider) GetClusterSize() int {
	return len(m.nodes)
}

func (m *MockNodeProvider) GetTotalMembershipSize() int {
	return len(m.nodes)
}

// MockReplicator implements coordinator.Replicator
type MockReplicator struct{}

func (m *MockReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	return &coordinator.ReplicationResponse{Success: true}, nil
}

// TestDatabaseManager wraps a single MVCC database for testing
type TestDatabaseManager struct {
	db *db.MVCCDatabase
}

func NewTestDatabaseManager(mvccDB *db.MVCCDatabase) *TestDatabaseManager {
	return &TestDatabaseManager{db: mvccDB}
}

func (tdm *TestDatabaseManager) GetDatabase(name string) (*db.MVCCDatabase, error) {
	// For testing, always return the single database regardless of name
	return tdm.db, nil
}

func (tdm *TestDatabaseManager) ListDatabases() []string {
	return []string{"marmot"}
}

func (tdm *TestDatabaseManager) DatabaseExists(name string) bool {
	return true
}

func (tdm *TestDatabaseManager) CreateDatabase(name string) error {
	return nil
}

func (tdm *TestDatabaseManager) DropDatabase(name string) error {
	return nil
}

func (tdm *TestDatabaseManager) GetDatabaseConnection(name string) (*sql.DB, error) {
	return tdm.db.GetDB(), nil
}

func (tdm *TestDatabaseManager) GetMVCCDatabase(name string) (coordinator.MVCCDatabaseProvider, error) {
	return tdm.db, nil
}

func TestMySQLServerIntegration(t *testing.T) {
	// Setup temporary DB with MetaStore
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"
	metaPath := tmpDir + "/test_meta.db"

	// Create MetaStore
	metaStore, err := db.NewSQLiteMetaStore(metaPath, 5000)
	require.NoError(t, err)
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	mvccDB, err := db.NewMVCCDatabase(dbPath, 1, clock, metaStore)
	require.NoError(t, err)
	defer mvccDB.Close()

	// Setup Coordinators
	nodeProvider := &MockNodeProvider{nodes: []uint64{1}}
	replicator := &MockReplicator{}

	// Wrap in test database manager
	dbMgr := NewTestDatabaseManager(mvccDB)

	localReplicator := db.NewLocalReplicator(1, dbMgr, clock)
	writeCoord := coordinator.NewWriteCoordinator(1, nodeProvider, replicator, localReplicator, time.Second, clock)

	localReader := db.NewLocalReader(dbMgr)
	readCoord := coordinator.NewReadCoordinator(1, nodeProvider, localReader, time.Second)

	// Setup Handler
	handler := coordinator.NewCoordinatorHandler(1, writeCoord, readCoord, clock, dbMgr, nil, nil, nil)

	// Setup Server
	port := 3307 // Use non-standard port
	server := protocol.NewMySQLServer(fmt.Sprintf("127.0.0.1:%d", port), handler)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Connect with MySQL client
	cfg := mysql.Config{
		User:                 "root",
		Passwd:               "",
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("127.0.0.1:%d", port),
		DBName:               "test",
		AllowNativePasswords: true,
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()

	// Test Connection
	err = db.Ping()
	require.NoError(t, err, "Failed to ping server")

	// Test Create Table
	_, err = db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Test Insert (CDC requires explicit column list)
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	// Test Select
	rows, err := db.Query("SELECT * FROM users WHERE id = 1")
	require.NoError(t, err)
	defer rows.Close()

	var id int
	var name string

	found := false
	for rows.Next() {
		err := rows.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "alice", name)
		found = true
	}
	assert.True(t, found, "Expected to find row")

	// Test MVCC Read (Implicit via ExecuteMVCCRead)
	// We can't easily verify internal MVCC state here without exposing it,
	// but success means the read path is working.
}
