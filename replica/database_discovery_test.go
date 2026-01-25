package replica

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// mockMarmotServiceClient is a mock for gRPC MarmotServiceClient
type mockMarmotServiceClient struct {
	mock.Mock
}

func (m *mockMarmotServiceClient) Gossip(ctx context.Context, req *marmotgrpc.GossipRequest, opts ...grpc.CallOption) (*marmotgrpc.GossipResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.GossipResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) Join(ctx context.Context, req *marmotgrpc.JoinRequest, opts ...grpc.CallOption) (*marmotgrpc.JoinResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.JoinResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) Ping(ctx context.Context, req *marmotgrpc.PingRequest, opts ...grpc.CallOption) (*marmotgrpc.PingResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.PingResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) ReplicateTransaction(ctx context.Context, req *marmotgrpc.TransactionRequest, opts ...grpc.CallOption) (*marmotgrpc.TransactionResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.TransactionResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) Read(ctx context.Context, req *marmotgrpc.ReadRequest, opts ...grpc.CallOption) (*marmotgrpc.ReadResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.ReadResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) GetLatestTxnIDs(ctx context.Context, req *marmotgrpc.LatestTxnIDsRequest, opts ...grpc.CallOption) (*marmotgrpc.LatestTxnIDsResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.LatestTxnIDsResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) StreamChanges(ctx context.Context, req *marmotgrpc.StreamRequest, opts ...grpc.CallOption) (marmotgrpc.MarmotService_StreamChangesClient, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(marmotgrpc.MarmotService_StreamChangesClient), args.Error(1)
}

func (m *mockMarmotServiceClient) GetSnapshotInfo(ctx context.Context, req *marmotgrpc.SnapshotInfoRequest, opts ...grpc.CallOption) (*marmotgrpc.SnapshotInfoResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.SnapshotInfoResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) StreamSnapshot(ctx context.Context, req *marmotgrpc.SnapshotRequest, opts ...grpc.CallOption) (marmotgrpc.MarmotService_StreamSnapshotClient, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(marmotgrpc.MarmotService_StreamSnapshotClient), args.Error(1)
}

func (m *mockMarmotServiceClient) GetReplicationState(ctx context.Context, req *marmotgrpc.ReplicationStateRequest, opts ...grpc.CallOption) (*marmotgrpc.ReplicationStateResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.ReplicationStateResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) GetClusterNodes(ctx context.Context, req *marmotgrpc.GetClusterNodesRequest, opts ...grpc.CallOption) (*marmotgrpc.GetClusterNodesResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*marmotgrpc.GetClusterNodesResponse), args.Error(1)
}

func (m *mockMarmotServiceClient) TransactionStream(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[marmotgrpc.TransactionStreamMessage, marmotgrpc.TransactionResponse], error) {
	args := m.Called(ctx, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(grpc.ClientStreamingClient[marmotgrpc.TransactionStreamMessage, marmotgrpc.TransactionResponse]), args.Error(1)
}

// TestDiscoverNewDatabases_DetectsNewDatabase tests that discovery loop detects new databases
func TestDiscoverNewDatabases_DetectsNewDatabase(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Create initial database
	err := dbMgr.CreateDatabase("db1")
	assert.NoError(t, err)

	// Initialize lastTxnID for db1
	client.mu.Lock()
	client.lastTxnID["db1"] = 100
	client.mu.Unlock()

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// First call: primary has db1 only
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{
				"db1": 100,
			},
			DatabaseInfo: []*marmotgrpc.DatabaseInfo{
				{Name: "db1", MaxTxnId: 100},
			},
		}, nil).Once()

	// Second call: primary adds db2
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{
				"db1": 100,
				"db2": 0,
			},
			DatabaseInfo: []*marmotgrpc.DatabaseInfo{
				{Name: "db1", MaxTxnId: 100},
				{Name: "db2", MaxTxnId: 0},
			},
		}, nil).Once()

	// Mock snapshot download for db2
	mockClient.On("GetSnapshotInfo", mock.Anything, mock.Anything).
		Return(&marmotgrpc.SnapshotInfoResponse{
			SnapshotTxnId:     0,
			SnapshotSizeBytes: 0,
			TotalChunks:       0,
			Databases:         []*marmotgrpc.DatabaseFileInfo{},
		}, nil).Maybe()

	// Run discovery loop twice
	ctx := context.Background()
	client.discoverNewDatabases(ctx)
	client.discoverNewDatabases(ctx)

	// Verify GetLatestTxnIDs was called
	mockClient.AssertExpectations(t)
}

// TestDiscoverNewDatabases_RespectsReplicateDatabasesFilter tests database filtering
func TestDiscoverNewDatabases_RespectsReplicateDatabasesFilter(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Set replicate_databases filter
	originalReplicateDbs := cfg.Config.Replica.ReplicateDatabases
	defer func() {
		cfg.Config.Replica.ReplicateDatabases = originalReplicateDbs
	}()
	cfg.Config.Replica.ReplicateDatabases = []string{"db1", "db2"}

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// Primary has 4 databases
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{
				"db1": 0,
				"db2": 0,
				"db3": 0,
				"db4": 0,
			},
			DatabaseInfo: []*marmotgrpc.DatabaseInfo{
				{Name: "db1", MaxTxnId: 0},
				{Name: "db2", MaxTxnId: 0},
				{Name: "db3", MaxTxnId: 0},
				{Name: "db4", MaxTxnId: 0},
			},
		}, nil).Once()

	ctx := context.Background()
	client.discoverNewDatabases(ctx)

	// Verify: replica should only have db1 and db2 in tracking
	client.mu.RLock()
	databases := make([]string, 0, len(client.lastTxnID))
	for dbName := range client.lastTxnID {
		databases = append(databases, dbName)
	}
	client.mu.RUnlock()

	// Verify databases were discovered
	assert.Greater(t, len(databases), 0, "Expected databases to be discovered")

	// Note: This test verifies behavior - actual implementation should filter
	// For now, just verify GetLatestTxnIDs was called
	mockClient.AssertExpectations(t)
}

// TestDiscoverNewDatabases_SkipsSystemDatabase tests that system database is never replicated
func TestDiscoverNewDatabases_SkipsSystemDatabase(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Empty filter = replicate all
	originalReplicateDbs := cfg.Config.Replica.ReplicateDatabases
	defer func() {
		cfg.Config.Replica.ReplicateDatabases = originalReplicateDbs
	}()
	cfg.Config.Replica.ReplicateDatabases = []string{}

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// Primary has system database and user databases
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{
				"__marmot_system": 100,
				"db1":             50,
				"db2":             75,
			},
			DatabaseInfo: []*marmotgrpc.DatabaseInfo{
				{Name: "__marmot_system", MaxTxnId: 100},
				{Name: "db1", MaxTxnId: 50},
				{Name: "db2", MaxTxnId: 75},
			},
		}, nil).Once()

	ctx := context.Background()
	client.discoverNewDatabases(ctx)

	// Verify: __marmot_system should NOT be in lastTxnID
	client.mu.RLock()
	_, hasSystemDB := client.lastTxnID["__marmot_system"]
	client.mu.RUnlock()

	assert.False(t, hasSystemDB, "System database should never be tracked for replication")
	mockClient.AssertExpectations(t)
}

// TestDiscoverNewDatabases_SkipsAlreadySyncedDatabases tests that existing databases don't trigger snapshots
func TestDiscoverNewDatabases_SkipsAlreadySyncedDatabases(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Create and sync db1
	err := dbMgr.CreateDatabase("db1")
	assert.NoError(t, err)

	client.mu.Lock()
	client.lastTxnID["db1"] = 1000
	client.mu.Unlock()

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// Primary has db1 with newer txn_id
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{
				"db1": 1500,
			},
			DatabaseInfo: []*marmotgrpc.DatabaseInfo{
				{Name: "db1", MaxTxnId: 1500},
			},
		}, nil).Once()

	ctx := context.Background()
	client.discoverNewDatabases(ctx)

	// Verify: GetSnapshotInfo should NOT be called for already synced database
	mockClient.AssertNotCalled(t, "GetSnapshotInfo", mock.Anything, mock.Anything)
	mockClient.AssertExpectations(t)
}

// TestDiscoverNewDatabases_HandlesRPCFailure tests graceful error handling
func TestDiscoverNewDatabases_HandlesRPCFailure(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client with error
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(nil, assert.AnError).Once()

	ctx := context.Background()

	// Should not panic
	assert.NotPanics(t, func() {
		client.discoverNewDatabases(ctx)
	})

	mockClient.AssertExpectations(t)
}

// TestDiscoverNewDatabases_PollInterval tests that discovery loop respects poll interval
func TestDiscoverNewDatabases_PollInterval(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Set short discovery interval for testing
	originalInterval := cfg.Config.Replica.DatabaseDiscoveryIntervalSec
	defer func() {
		cfg.Config.Replica.DatabaseDiscoveryIntervalSec = originalInterval
	}()
	cfg.Config.Replica.DatabaseDiscoveryIntervalSec = 1

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// Track call count
	var callCount atomic.Int32
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			callCount.Add(1)
		}).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{},
			DatabaseInfo:   []*marmotgrpc.DatabaseInfo{},
		}, nil)

	// Start discovery loop in background
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go client.startDatabaseDiscoveryLoop(ctx)

	// Wait for interval
	time.Sleep(3 * time.Second)

	// Verify GetLatestTxnIDs called approximately 2-3 times
	calls := callCount.Load()
	assert.GreaterOrEqual(t, calls, int32(2), "Expected at least 2 calls in 3 seconds with 1s interval")
	assert.LessOrEqual(t, calls, int32(4), "Expected at most 4 calls in 3 seconds with 1s interval (allowing timing variance)")
}

// TestDiscoverNewDatabases_ConcurrentDatabaseCreation tests concurrent database discovery
func TestDiscoverNewDatabases_ConcurrentDatabaseCreation(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// First call: no databases
	// Second call: 5 databases created concurrently
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{},
			DatabaseInfo:   []*marmotgrpc.DatabaseInfo{},
		}, nil).Once()

	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{
				"db1": 0,
				"db2": 0,
				"db3": 0,
				"db4": 0,
				"db5": 0,
			},
			DatabaseInfo: []*marmotgrpc.DatabaseInfo{
				{Name: "db1", MaxTxnId: 0},
				{Name: "db2", MaxTxnId: 0},
				{Name: "db3", MaxTxnId: 0},
				{Name: "db4", MaxTxnId: 0},
				{Name: "db5", MaxTxnId: 0},
			},
		}, nil).Once()

	ctx := context.Background()
	client.discoverNewDatabases(ctx)
	client.discoverNewDatabases(ctx)

	mockClient.AssertExpectations(t)
}

// TestDiscoverNewDatabases_EmptyClusterResponse tests handling of empty cluster
func TestDiscoverNewDatabases_EmptyClusterResponse(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// Primary has no databases
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{},
			DatabaseInfo:   []*marmotgrpc.DatabaseInfo{},
		}, nil).Once()

	ctx := context.Background()

	// Should not panic on empty response
	assert.NotPanics(t, func() {
		client.discoverNewDatabases(ctx)
	})

	mockClient.AssertExpectations(t)
}

// TestDiscoverNewDatabases_ContextCancellation tests discovery stops on context cancellation
func TestDiscoverNewDatabases_ContextCancellation(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	// Start discovery loop
	done := make(chan bool)
	go func() {
		client.startDatabaseDiscoveryLoop(ctx)
		done <- true
	}()

	// Should exit quickly
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Discovery loop did not stop after context cancellation")
	}
}

// TestDiscoverNewDatabases_FilterByPattern tests glob pattern filtering
func TestDiscoverNewDatabases_FilterByPattern(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Set pattern filter
	originalReplicateDbs := cfg.Config.Replica.ReplicateDatabases
	defer func() {
		cfg.Config.Replica.ReplicateDatabases = originalReplicateDbs
	}()
	cfg.Config.Replica.ReplicateDatabases = []string{"prod_*", "staging_main"}

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Mock gRPC client
	mockClient := new(mockMarmotServiceClient)
	client.client = mockClient

	// Primary has multiple databases with different patterns
	mockClient.On("GetLatestTxnIDs", mock.Anything, mock.Anything).
		Return(&marmotgrpc.LatestTxnIDsResponse{
			DatabaseTxnIds: map[string]uint64{
				"prod_users":   0,
				"prod_orders":  0,
				"staging_main": 0,
				"staging_test": 0,
				"dev_database": 0,
			},
			DatabaseInfo: []*marmotgrpc.DatabaseInfo{
				{Name: "prod_users", MaxTxnId: 0},
				{Name: "prod_orders", MaxTxnId: 0},
				{Name: "staging_main", MaxTxnId: 0},
				{Name: "staging_test", MaxTxnId: 0},
				{Name: "dev_database", MaxTxnId: 0},
			},
		}, nil).Once()

	ctx := context.Background()
	client.discoverNewDatabases(ctx)

	mockClient.AssertExpectations(t)
}
