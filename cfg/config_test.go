package cfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidate_ValidConfig(t *testing.T) {
	// Save original config
	original := Config
	defer func() { Config = original }()

	// Set valid config with all required fields
	Config = &Configuration{
		NodeID:  1,
		DataDir: "./test-data",
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: true,
			Port:    3306,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
		Transaction: TransactionConfiguration{
			HeartbeatTimeoutSeconds: 10,
			ConflictWindowSeconds:   10,
		},
		ConnectionPool: ConnectionPoolConfiguration{
			PoolSize:           4,
			MaxIdleTimeSeconds: 10,
			MaxLifetimeSeconds: 300,
		},
		GRPCClient: GRPCClientConfiguration{
			KeepaliveTimeSeconds:    10,
			KeepaliveTimeoutSeconds: 3,
			MaxRetries:              3,
			RetryBackoffMS:          100,
		},
		Coordinator: CoordinatorConfiguration{
			PrepareTimeoutMS: 2000,
			CommitTimeoutMS:  2000,
			AbortTimeoutMS:   2000,
		},
	}

	err := Validate()
	if err != nil {
		t.Errorf("Expected no error for valid config, got: %v", err)
	}
}

func TestValidate_InvalidGRPCPort(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	tests := []int{-1, 0, 70000}

	for _, port := range tests {
		Config = &Configuration{
			Cluster: ClusterConfiguration{
				GRPCPort: port,
			},
			MySQL: MySQLConfiguration{
				Enabled: false,
			},
			Replication: ReplicationConfiguration{
				DefaultWriteConsist: "QUORUM",
				DefaultReadConsist:  "LOCAL_ONE",
			},
		}

		err := Validate()
		if err == nil {
			t.Errorf("Expected error for invalid gRPC port %d", port)
		}
	}
}

func TestValidate_InvalidMySQLPort(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: true,
			Port:    -1,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
	}

	err := Validate()
	if err == nil {
		t.Error("Expected error for invalid MySQL port")
	}
}

func TestValidate_InvalidAutoIDMode(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled:    true,
			Port:       3306,
			AutoIDMode: "invalid",
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
	}

	err := Validate()
	if err == nil {
		t.Error("Expected error for invalid auto_id_mode")
	}
}

func TestValidate_InvalidWriteConsistency(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: false,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "INVALID",
			DefaultReadConsist:  "LOCAL_ONE",
		},
	}

	err := Validate()
	if err == nil {
		t.Error("Expected error for invalid write consistency")
	}
}

func TestValidate_InvalidReadConsistency(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: false,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "INVALID",
		},
	}

	err := Validate()
	if err == nil {
		t.Error("Expected error for invalid read consistency")
	}
}

func TestLoad_NonExistentFile(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	// Use a temporary directory for testing
	tempDir := filepath.Join(os.TempDir(), "marmot-test-load")
	defer os.RemoveAll(tempDir)

	// Reset config with valid defaults
	Config = &Configuration{
		DataDir: tempDir,
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Port: 3306,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
	}

	// Load non-existent file should use defaults
	err := Load("non-existent-file.toml")
	if err != nil {
		t.Errorf("Expected no error for non-existent file, got: %v", err)
	}

	// Node ID should be auto-generated
	if Config.NodeID == 0 {
		t.Error("Expected node ID to be auto-generated")
	}
}

func TestLoad_CreateDataDir(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	// Use a temporary directory
	tempDir := filepath.Join(os.TempDir(), "marmot-test-data")
	defer os.RemoveAll(tempDir)

	Config = &Configuration{
		DataDir: tempDir,
	}

	err := Load("")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify directory was created
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Error("Data directory was not created")
	}
}

func TestGenerateNodeID(t *testing.T) {
	id1, err := generateNodeID()
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if id1 == 0 {
		t.Error("Generated node ID should not be 0")
	}

	// Generate another ID - should be the same (deterministic for machine)
	id2, err := generateNodeID()
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if id1 != id2 {
		t.Error("Node ID should be deterministic for same machine")
	}
}

func TestLoad_CLIOverrides(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	// Set up CLI flags
	tempDir := filepath.Join(os.TempDir(), "marmot-test-override")
	defer os.RemoveAll(tempDir)

	*DataDirFlag = tempDir
	*NodeIDFlag = 12345
	*GRPCPortFlag = 9999
	*MySQLPortFlag = 3307

	defer func() {
		*DataDirFlag = ""
		*NodeIDFlag = 0
		*GRPCPortFlag = 0
		*MySQLPortFlag = 0
	}()

	// Reset config
	Config = &Configuration{
		DataDir: "./default-data",
		NodeID:  0,
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Port: 3306,
		},
	}

	err := Load("")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify CLI overrides were applied
	if Config.DataDir != tempDir {
		t.Errorf("Expected data dir %s, got %s", tempDir, Config.DataDir)
	}

	if Config.NodeID != 12345 {
		t.Errorf("Expected node ID 12345, got %d", Config.NodeID)
	}

	if Config.Cluster.GRPCPort != 9999 {
		t.Errorf("Expected gRPC port 9999, got %d", Config.Cluster.GRPCPort)
	}

	if Config.MySQL.Port != 3307 {
		t.Errorf("Expected MySQL port 3307, got %d", Config.MySQL.Port)
	}
}

func BenchmarkGenerateNodeID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		generateNodeID()
	}
}

func BenchmarkValidate(b *testing.B) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: true,
			Port:    3306,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Validate()
	}
}

// Replica configuration tests

func TestReplicaConfigDefaults(t *testing.T) {
	// The global Config variable has defaults set at initialization
	// Verify replica defaults are correctly initialized
	if Config.Replica.Enabled {
		t.Error("Replica should be disabled by default")
	}

	if Config.Replica.ReconnectIntervalSec != 5 {
		t.Errorf("Expected reconnect_interval_seconds=5, got %d", Config.Replica.ReconnectIntervalSec)
	}

	if Config.Replica.ReconnectMaxBackoffSec != 30 {
		t.Errorf("Expected reconnect_max_backoff_seconds=30, got %d", Config.Replica.ReconnectMaxBackoffSec)
	}

	if Config.Replica.InitialSyncTimeoutMin != 30 {
		t.Errorf("Expected initial_sync_timeout_minutes=30, got %d", Config.Replica.InitialSyncTimeoutMin)
	}
}

func TestReplicaConfigValidation_MissingFollowAddresses(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: true,
			Port:    3306,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
		Replica: ReplicaConfiguration{
			Enabled:         true,
			FollowAddresses: []string{}, // Missing
			Secret:          "test-secret",
		},
	}

	err := Validate()
	if err == nil {
		t.Error("Expected error for missing follow_addresses when replica enabled")
	}
}

func TestReplicaConfigValidation_MutuallyExclusiveWithSeedNodes(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort:  8080,
			SeedNodes: []string{"node1:8080"},
		},
		MySQL: MySQLConfiguration{
			Enabled: true,
			Port:    3306,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
		Replica: ReplicaConfiguration{
			Enabled:         true,
			FollowAddresses: []string{"master:8080"},
			Secret:          "test-secret",
		},
	}

	err := Validate()
	if err == nil {
		t.Error("Expected error when both replica mode and seed_nodes are configured")
	}
}

func TestReplicaConfigValidation_ValidConfig(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	Config = &Configuration{
		NodeID:  1,
		DataDir: "./test-data",
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: true,
			Port:    3306,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
		Transaction: TransactionConfiguration{
			HeartbeatTimeoutSeconds: 10,
			ConflictWindowSeconds:   10,
		},
		ConnectionPool: ConnectionPoolConfiguration{
			PoolSize:           4,
			MaxIdleTimeSeconds: 10,
			MaxLifetimeSeconds: 300,
		},
		GRPCClient: GRPCClientConfiguration{
			KeepaliveTimeSeconds:    10,
			KeepaliveTimeoutSeconds: 3,
			MaxRetries:              3,
			RetryBackoffMS:          100,
		},
		Coordinator: CoordinatorConfiguration{
			PrepareTimeoutMS: 2000,
			CommitTimeoutMS:  2000,
			AbortTimeoutMS:   2000,
		},
		Replica: ReplicaConfiguration{
			Enabled:                true,
			FollowAddresses:        []string{"master1:8080", "master2:8080"},
			DiscoveryIntervalSec:   30,
			FailoverTimeoutSec:     60,
			ReconnectIntervalSec:   5,
			ReconnectMaxBackoffSec: 30,
			InitialSyncTimeoutMin:  30,
			Secret:                 "test-secret",
		},
	}

	err := Validate()
	if err != nil {
		t.Errorf("Expected no error for valid replica config, got: %v", err)
	}
}

func TestIsReplicaMode(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	// Test disabled
	Config = &Configuration{
		Replica: ReplicaConfiguration{
			Enabled: false,
		},
	}
	if IsReplicaMode() {
		t.Error("Expected IsReplicaMode()=false when disabled")
	}

	// Test enabled
	Config = &Configuration{
		Replica: ReplicaConfiguration{
			Enabled: true,
		},
	}
	if !IsReplicaMode() {
		t.Error("Expected IsReplicaMode()=true when enabled")
	}
}

func TestReplicaConfig_CLIOverridesFollowAddresses(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	// Set up CLI flag
	*FollowAddrsFlag = "node1:8080,node2:8080,node3:8080"
	defer func() {
		*FollowAddrsFlag = ""
	}()

	// Reset config
	Config = &Configuration{
		DataDir: "./test-data",
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		Replica: ReplicaConfiguration{
			FollowAddresses: []string{},
		},
	}

	err := Load("")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify CLI override was applied
	expectedAddrs := []string{"node1:8080", "node2:8080", "node3:8080"}
	if len(Config.Replica.FollowAddresses) != len(expectedAddrs) {
		t.Errorf("Expected %d addresses, got %d", len(expectedAddrs), len(Config.Replica.FollowAddresses))
	}

	for i, addr := range expectedAddrs {
		if i >= len(Config.Replica.FollowAddresses) || Config.Replica.FollowAddresses[i] != addr {
			t.Errorf("Expected address[%d] to be %s, got %s", i, addr, Config.Replica.FollowAddresses[i])
		}
	}
}

func TestReplicaConfig_ValidationRequiresFollowAddresses(t *testing.T) {
	original := Config
	defer func() { Config = original }()

	// Test 1: Empty follow_addresses should fail
	Config = &Configuration{
		Cluster: ClusterConfiguration{
			GRPCPort: 8080,
		},
		MySQL: MySQLConfiguration{
			Enabled: true,
			Port:    3306,
		},
		Replication: ReplicationConfiguration{
			DefaultWriteConsist: "QUORUM",
			DefaultReadConsist:  "LOCAL_ONE",
		},
		Transaction: TransactionConfiguration{
			HeartbeatTimeoutSeconds: 10,
		},
		ConnectionPool: ConnectionPoolConfiguration{
			PoolSize: 4,
		},
		GRPCClient: GRPCClientConfiguration{
			KeepaliveTimeSeconds:    10,
			KeepaliveTimeoutSeconds: 3,
		},
		Coordinator: CoordinatorConfiguration{
			PrepareTimeoutMS: 2000,
			CommitTimeoutMS:  2000,
			AbortTimeoutMS:   2000,
		},
		Replica: ReplicaConfiguration{
			Enabled:         true,
			FollowAddresses: []string{},
			Secret:          "test-secret",
		},
	}

	err := Validate()
	if err == nil {
		t.Error("Expected validation error for empty follow_addresses")
	}

	// Test 2: Valid follow_addresses should pass
	Config.Replica.FollowAddresses = []string{"node1:8080", "node2:8080"}
	Config.Replica.DiscoveryIntervalSec = 30
	Config.Replica.FailoverTimeoutSec = 60
	Config.Replica.ReconnectIntervalSec = 5
	Config.Replica.ReconnectMaxBackoffSec = 30
	Config.Replica.InitialSyncTimeoutMin = 30

	err = Validate()
	if err != nil {
		t.Errorf("Expected no validation error with valid follow_addresses, got: %v", err)
	}
}
