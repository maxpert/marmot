package cfg

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
	"github.com/rs/zerolog/log"
)

// PromotionConfiguration controls JOINING → ALIVE promotion
type PromotionConfiguration struct {
	CheckIntervalSeconds  int `toml:"check_interval_seconds"`   // How often to check for promotion
	MinHealthyDurationSec int `toml:"min_healthy_duration_sec"` // Must be healthy for this long before promotion
}

// ClusterConfiguration controls cluster membership and communication
type ClusterConfiguration struct {
	GRPCBindAddress      string                 `toml:"grpc_bind_address"`
	GRPCAdvertiseAddress string                 `toml:"grpc_advertise_address"` // Address other nodes use to connect (defaults to hostname:port)
	GRPCPort             int                    `toml:"grpc_port"`
	SeedNodes            []string               `toml:"seed_nodes"`
	ClusterSecret        string                 `toml:"cluster_secret"` // PSK for cluster authentication (env: MARMOT_CLUSTER_SECRET)
	GossipIntervalMS     int                    `toml:"gossip_interval_ms"`
	GossipFanout         int                    `toml:"gossip_fanout"`
	SuspectTimeoutMS     int                    `toml:"suspect_timeout_ms"`
	DeadTimeoutMS        int                    `toml:"dead_timeout_ms"`
	Promotion            PromotionConfiguration `toml:"promotion"`
}

// ReplicationConfiguration controls replication behavior
type ReplicationConfiguration struct {
	DefaultWriteConsist       string `toml:"default_write_consistency"`
	DefaultReadConsist        string `toml:"default_read_consistency"`
	WriteTimeoutMS            int    `toml:"write_timeout_ms"`
	ReadTimeoutMS             int    `toml:"read_timeout_ms"`
	EnableAntiEntropy         bool   `toml:"enable_anti_entropy"`
	AntiEntropyIntervalS      int    `toml:"anti_entropy_interval_seconds"`
	GCIntervalS               int    `toml:"gc_interval_seconds"` // GC interval - MUST be >= anti_entropy_interval_seconds
	DeltaSyncThresholdTxns    int    `toml:"delta_sync_threshold_transactions"`
	DeltaSyncThresholdSeconds int    `toml:"delta_sync_threshold_seconds"`
	GCMinRetentionHours       int    `toml:"gc_min_retention_hours"`
	GCMaxRetentionHours       int    `toml:"gc_max_retention_hours"`
	StreamChunkSizeKB         int    `toml:"stream_chunk_size_kb"` // Size in KB for streaming chunks (default: 1024 = 1MB)
}

// MySQLConfiguration for MySQL wire protocol server
type MySQLConfiguration struct {
	Enabled        bool   `toml:"enabled"`
	BindAddress    string `toml:"bind_address"`
	Port           int    `toml:"port"`
	MaxConnections int    `toml:"max_connections"`
	AutoIDMode     string `toml:"auto_id_mode"`     // "compact" (default, 53-bit) or "extended" (64-bit)
	UnixSocket     string `toml:"unix_socket"`      // Path to Unix socket (empty = disabled)
	UnixSocketPerm int    `toml:"unix_socket_perm"` // Permissions, default 0660
}

// LoggingConfiguration controls logging behavior
type LoggingConfiguration struct {
	Verbose bool   `toml:"verbose"`
	Format  string `toml:"format"` // "console" or "json"
}

// PrometheusConfiguration for metrics
// Metrics are served on the gRPC port at /metrics endpoint
type PrometheusConfiguration struct {
	Enabled bool `toml:"enabled"`
}

// TransactionConfiguration controls transaction manager behavior
type TransactionConfiguration struct {
	HeartbeatTimeoutSeconds int `toml:"heartbeat_timeout_seconds"` // Transaction timeout without heartbeat
	ConflictWindowSeconds   int `toml:"conflict_window_seconds"`   // LWW conflict resolution window
	LockWaitTimeoutSeconds  int `toml:"lock_wait_timeout_seconds"` // How long to wait for locks (MySQL: innodb_lock_wait_timeout)
}

// MetaStoreConfiguration controls PebbleDB metadata storage
type MetaStoreConfiguration struct {
	CacheSizeMB           int64 `toml:"cache_size_mb"`           // Block cache size in MB (default: 128)
	MemTableSizeMB        int64 `toml:"memtable_size_mb"`        // MemTable size in MB (default: 64)
	MemTableCount         int   `toml:"memtable_count"`          // Number of MemTables (default: 2)
	L0CompactionThreshold int   `toml:"l0_compaction_threshold"` // L0 compaction trigger (default: 500)
	L0StopWrites          int   `toml:"l0_stop_writes"`          // L0 stop writes trigger (default: 1000)
	WALBytesPerSyncKB     int   `toml:"wal_bytes_per_sync_kb"`   // Background WAL sync every N KB (default: 512, 0=disabled)
}

// ConnectionPoolConfiguration controls database connection pooling
type ConnectionPoolConfiguration struct {
	PoolSize           int `toml:"pool_size"`             // Number of connections in pool
	MaxIdleTimeSeconds int `toml:"max_idle_time_seconds"` // Max time connection can be idle
	MaxLifetimeSeconds int `toml:"max_lifetime_seconds"`  // Max lifetime of a connection
}

// BatchCommitConfiguration controls SQLite write batching for improved throughput
type BatchCommitConfiguration struct {
	Enabled      bool `toml:"enabled"`        // Enable batch committing (default: true)
	MaxBatchSize int  `toml:"max_batch_size"` // Max transactions per batch (default: 128)
	MaxWaitMS    int  `toml:"max_wait_ms"`    // Max wait before flush in ms (default: 10)

	// Adaptive checkpoint configuration for WAL management
	CheckpointEnabled         bool    `toml:"checkpoint_enabled"`           // Enable automatic checkpointing (default: true)
	CheckpointPassiveThreshMB float64 `toml:"checkpoint_passive_thresh_mb"` // PASSIVE checkpoint threshold in MB (default: 4.0)
	CheckpointRestartThreshMB float64 `toml:"checkpoint_restart_thresh_mb"` // RESTART checkpoint threshold in MB (default: 16.0)
	AllowDynamicBatchSize     bool    `toml:"allow_dynamic_batch_size"`     // Allow dynamic batch size adjustment (default: true)
}

// GRPCClientConfiguration controls gRPC client behavior
type GRPCClientConfiguration struct {
	KeepaliveTimeSeconds    int `toml:"keepalive_time_seconds"`    // Keepalive ping interval
	KeepaliveTimeoutSeconds int `toml:"keepalive_timeout_seconds"` // Keepalive ping timeout
	MaxRetries              int `toml:"max_retries"`               // Max retry attempts
	RetryBackoffMS          int `toml:"retry_backoff_ms"`          // Retry backoff duration
}

// CoordinatorConfiguration controls transaction coordinator behavior
type CoordinatorConfiguration struct {
	PrepareTimeoutMS int `toml:"prepare_timeout_ms"` // Timeout for prepare phase
	CommitTimeoutMS  int `toml:"commit_timeout_ms"`  // Timeout for commit phase
	AbortTimeoutMS   int `toml:"abort_timeout_ms"`   // Timeout for abort phase
}

// DDLConfiguration controls DDL replication behavior
type DDLConfiguration struct {
	LockLeaseSeconds int  `toml:"lock_lease_seconds"` // DDL lock lease duration in seconds
	EnableIdempotent bool `toml:"enable_idempotent"`  // Automatically rewrite DDL for idempotency
}

// QueryPipelineConfiguration controls query processing pipeline
type QueryPipelineConfiguration struct {
	TranspilerCacheSize int `toml:"transpiler_cache_size"` // LRU cache size for transpiled queries
	ValidatorPoolSize   int `toml:"validator_pool_size"`   // SQLite connection pool size for validation
}

// ReplicaConfiguration controls read-only replica mode
// When enabled, the node follows seed nodes for discovery without joining the cluster
type ReplicaConfiguration struct {
	Enabled                bool     `toml:"enabled"`                       // Enable read-only replica mode
	FollowAddresses        []string `toml:"follow_addresses"`              // Seed nodes for discovery (required when enabled)
	ReplicateDatabases     []string `toml:"replicate_databases"`           // Filter databases, empty = all
	Secret                 string   `toml:"secret"`                        // PSK for authenticating with cluster (env: MARMOT_REPLICA_SECRET)
	DiscoveryIntervalSec   int      `toml:"discovery_interval_seconds"`    // Discovery interval (default: 30)
	FailoverTimeoutSec     int      `toml:"failover_timeout_seconds"`      // Failover timeout (default: 60)
	ReconnectIntervalSec   int      `toml:"reconnect_interval_seconds"`    // Initial reconnect interval (default: 5)
	ReconnectMaxBackoffSec int      `toml:"reconnect_max_backoff_seconds"` // Max reconnect backoff (default: 30)
	InitialSyncTimeoutMin  int      `toml:"initial_sync_timeout_minutes"`  // Timeout for initial snapshot (default: 30)

	DatabaseDiscoveryIntervalSec int `toml:"database_discovery_interval_seconds"` // Database discovery interval (default: 10)
	SnapshotConcurrency          int `toml:"snapshot_concurrency"`                // Snapshot concurrency (default: 3)
	SnapshotCacheTTLSec          int `toml:"snapshot_cache_ttl_seconds"`          // Snapshot cache TTL (default: 30)
}

// PublisherConfiguration configures the CDC publishing system
type PublisherConfiguration struct {
	Enabled bool                `toml:"enabled"` // Enable CDC publishing
	Sinks   []SinkConfiguration `toml:"sinks"`   // List of sink configurations
}

// SinkConfiguration configures a single CDC sink
type SinkConfiguration struct {
	Name            string   `toml:"name"`             // Unique sink name
	Type            string   `toml:"type"`             // "kafka" or "nats"
	Format          string   `toml:"format"`           // "debezium"
	Brokers         []string `toml:"brokers"`          // Kafka broker addresses
	NatsURL         string   `toml:"nats_url"`         // NATS server URL (e.g., "nats://localhost:4222")
	TopicPrefix     string   `toml:"topic_prefix"`     // Topic prefix (e.g., "marmot.cdc")
	FilterTables    []string `toml:"filter_tables"`    // Glob patterns for tables
	FilterDatabases []string `toml:"filter_databases"` // Glob patterns for databases
	BatchSize       int      `toml:"batch_size"`       // Events per poll (default: 100)
	PollIntervalMS  int      `toml:"poll_interval_ms"` // Poll interval (default: 10)
	RetryInitialMS  int      `toml:"retry_initial_ms"` // Initial retry delay (default: 100)
	RetryMaxMS      int      `toml:"retry_max_ms"`     // Max retry delay (default: 30000)
	RetryMultiplier float64  `toml:"retry_multiplier"` // Backoff multiplier (default: 2.0)
}

// ExtensionConfiguration controls SQLite extension loading
type ExtensionConfiguration struct {
	Directory    string   `toml:"directory"`     // Search path for extensions (like SQLite CLI's .load)
	AlwaysLoaded []string `toml:"always_loaded"` // Extensions loaded into every connection via ConnectHook
}

// Configuration is the main configuration structure
type Configuration struct {
	NodeID  uint64 `toml:"node_id"`
	DataDir string `toml:"data_dir"`

	Cluster        ClusterConfiguration        `toml:"cluster"`
	Replication    ReplicationConfiguration    `toml:"replication"`
	Transaction    TransactionConfiguration    `toml:"transaction"`
	MetaStore      MetaStoreConfiguration      `toml:"metastore"`
	ConnectionPool ConnectionPoolConfiguration `toml:"connection_pool"`
	GRPCClient     GRPCClientConfiguration     `toml:"grpc_client"`
	Coordinator    CoordinatorConfiguration    `toml:"coordinator"`
	DDL            DDLConfiguration            `toml:"ddl"`
	QueryPipeline  QueryPipelineConfiguration  `toml:"query_pipeline"`
	MySQL          MySQLConfiguration          `toml:"mysql"`
	Logging        LoggingConfiguration        `toml:"logging"`
	Prometheus     PrometheusConfiguration     `toml:"prometheus"`
	Replica        ReplicaConfiguration        `toml:"replica"`
	Publisher      PublisherConfiguration      `toml:"publisher"`
	BatchCommit    BatchCommitConfiguration    `toml:"batch_commit"`
	Extensions     ExtensionConfiguration      `toml:"extensions"`
}

// Command line flags
var (
	ConfigPathFlag  = flag.String("config", "config.toml", "Path to configuration file")
	DataDirFlag     = flag.String("data-dir", "", "Data directory (overrides config)")
	NodeIDFlag      = flag.Uint64("node-id", 0, "Node ID (overrides config, 0=auto)")
	GRPCPortFlag    = flag.Int("grpc-port", 0, "gRPC port (overrides config)")
	MySQLPortFlag   = flag.Int("mysql-port", 0, "MySQL port (overrides config)")
	FollowAddrsFlag = flag.String("follow-addresses", "", "Comma-separated addresses for replica mode")
)

// Default configuration
var Config = &Configuration{
	NodeID:  0, // Auto-generate
	DataDir: "./marmot-data",

	Cluster: ClusterConfiguration{
		GRPCBindAddress:  "0.0.0.0",
		GRPCPort:         8080,
		SeedNodes:        []string{},
		GossipIntervalMS: 1000,
		GossipFanout:     3,
		SuspectTimeoutMS: 5000,
		DeadTimeoutMS:    10000,
		Promotion: PromotionConfiguration{
			CheckIntervalSeconds:  2, // Check every 2 seconds
			MinHealthyDurationSec: 3, // Must be healthy for 3 seconds
		},
	},

	Replication: ReplicationConfiguration{
		DefaultWriteConsist:       "QUORUM",
		DefaultReadConsist:        "LOCAL_ONE",
		WriteTimeoutMS:            5000,
		ReadTimeoutMS:             2000,
		EnableAntiEntropy:         true,
		AntiEntropyIntervalS:      30,    // 30s - frequent watermark updates (was 60s, flipped for GC safety)
		GCIntervalS:               60,    // 60s - MUST be >= AntiEntropyIntervalS for fresh watermarks before GC
		DeltaSyncThresholdTxns:    10000, // Trigger snapshot if lag > 10k transactions
		DeltaSyncThresholdSeconds: 3600,  // 1 hour - trigger snapshot after this (like Cassandra's daily repair)
		GCMinRetentionHours:       2,     // 2 hours - MUST be >= 2x delta threshold (safety margin)
		GCMaxRetentionHours:       24,    // 24 hours - 24x delta threshold (like Cassandra's 10-day gc_grace)
		StreamChunkSizeKB:         1024,  // 1MB - use streaming for large payloads >= this size
	},

	Transaction: TransactionConfiguration{
		HeartbeatTimeoutSeconds: 10, // Timeout transactions after 10s without heartbeat
		ConflictWindowSeconds:   10, // 10 second window for LWW conflict resolution
		LockWaitTimeoutSeconds:  50, // MySQL default: innodb_lock_wait_timeout
	},

	MetaStore: MetaStoreConfiguration{
		CacheSizeMB:           128,  // 128MB block cache (reduce Pebble disk reads)
		MemTableSizeMB:        64,   // 64MB memtable (CockroachDB-style)
		MemTableCount:         2,    // 2 memtables
		L0CompactionThreshold: 500,  // CockroachDB default
		L0StopWrites:          1000, // CockroachDB default
		WALBytesPerSyncKB:     512,  // 512KB (CockroachDB default)
	},

	ConnectionPool: ConnectionPoolConfiguration{
		PoolSize:           4,   // 4 connections per pool
		MaxIdleTimeSeconds: 10,  // Max 10s idle time
		MaxLifetimeSeconds: 300, // Max 5 minute connection lifetime
	},

	GRPCClient: GRPCClientConfiguration{
		KeepaliveTimeSeconds:    10,  // Send keepalive ping every 10s
		KeepaliveTimeoutSeconds: 3,   // Timeout keepalive after 3s
		MaxRetries:              3,   // Retry failed requests up to 3 times
		RetryBackoffMS:          100, // 100ms backoff between retries
	},

	Coordinator: CoordinatorConfiguration{
		PrepareTimeoutMS: 2000, // 2 second timeout for prepare phase
		CommitTimeoutMS:  2000, // 2 second timeout for commit phase
		AbortTimeoutMS:   2000, // 2 second timeout for abort phase
	},

	DDL: DDLConfiguration{
		LockLeaseSeconds: 30,   // 30 second DDL lock lease
		EnableIdempotent: true, // Auto-rewrite DDL for idempotency
	},

	MySQL: MySQLConfiguration{
		Enabled:        true,
		BindAddress:    "0.0.0.0",
		Port:           3306,
		MaxConnections: 1000,
		AutoIDMode:     "compact", // Default to compact 53-bit IDs
		UnixSocketPerm: 0660,
	},

	Logging: LoggingConfiguration{
		Verbose: false,
		Format:  "json", // Use "json" for production (21% faster than "console")
	},

	Prometheus: PrometheusConfiguration{
		Enabled: true, // Served on gRPC port at /metrics
	},

	Replica: ReplicaConfiguration{
		Enabled:                false,
		FollowAddresses:        []string{},
		DiscoveryIntervalSec:   30,
		FailoverTimeoutSec:     60,
		ReconnectIntervalSec:   5,
		ReconnectMaxBackoffSec: 30,
		InitialSyncTimeoutMin:  30,
	},

	Publisher: PublisherConfiguration{
		Enabled: false,
		Sinks:   []SinkConfiguration{},
	},

	BatchCommit: BatchCommitConfiguration{
		Enabled:                   true,
		MaxBatchSize:              128,
		MaxWaitMS:                 10,
		CheckpointEnabled:         true,
		CheckpointPassiveThreshMB: 4.0,
		CheckpointRestartThreshMB: 16.0,
		AllowDynamicBatchSize:     true,
	},
}

// Load loads configuration from file and applies CLI overrides
func Load(configPath string) error {
	// Load from file if it exists
	if configPath != "" {
		if _, err := os.Stat(configPath); err == nil {
			log.Info().Str("path", configPath).Msg("Loading configuration")
			if _, err := toml.DecodeFile(configPath, Config); err != nil {
				return fmt.Errorf("failed to decode config: %w", err)
			}
		} else {
			log.Warn().Str("path", configPath).Msg("Config file not found, using defaults")
		}
	}

	// Apply CLI overrides
	if *DataDirFlag != "" {
		Config.DataDir = *DataDirFlag
	}
	if *NodeIDFlag != 0 {
		Config.NodeID = *NodeIDFlag
	}
	if *GRPCPortFlag != 0 {
		Config.Cluster.GRPCPort = *GRPCPortFlag
	}
	if *MySQLPortFlag != 0 {
		Config.MySQL.Port = *MySQLPortFlag
	}
	if *FollowAddrsFlag != "" {
		Config.Replica.FollowAddresses = strings.Split(*FollowAddrsFlag, ",")
	}

	// Environment variable override for cluster secret (takes precedence over config)
	if envSecret := os.Getenv("MARMOT_CLUSTER_SECRET"); envSecret != "" {
		Config.Cluster.ClusterSecret = envSecret
	}

	// Environment variable override for replica secret (takes precedence over config)
	if envSecret := os.Getenv("MARMOT_REPLICA_SECRET"); envSecret != "" {
		Config.Replica.Secret = envSecret
	}

	// Auto-generate node ID if not set
	if Config.NodeID == 0 {
		var err error
		Config.NodeID, err = generateNodeID()
		if err != nil {
			return fmt.Errorf("failed to generate node ID: %w", err)
		}
		log.Info().Uint64("node_id", Config.NodeID).Msg("Auto-generated node ID")
	}

	// Set query pipeline defaults if not configured
	if Config.QueryPipeline.TranspilerCacheSize == 0 {
		Config.QueryPipeline.TranspilerCacheSize = 10000
	}
	if Config.QueryPipeline.ValidatorPoolSize == 0 {
		Config.QueryPipeline.ValidatorPoolSize = 8
	}

	// Set publisher defaults
	setPublisherDefaults()

	// Ensure data directory exists
	if err := os.MkdirAll(Config.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	return nil
}

// setPublisherDefaults applies default values to publisher configuration
func setPublisherDefaults() {
	if !Config.Publisher.Enabled {
		return
	}

	for i := range Config.Publisher.Sinks {
		sink := &Config.Publisher.Sinks[i]

		// Set BatchSize default
		if sink.BatchSize <= 0 {
			sink.BatchSize = 100
		}

		// Set PollIntervalMS default
		if sink.PollIntervalMS <= 0 {
			sink.PollIntervalMS = 10
		}

		// Set RetryInitialMS default
		if sink.RetryInitialMS <= 0 {
			sink.RetryInitialMS = 100
		}

		// Set RetryMaxMS default
		if sink.RetryMaxMS <= 0 {
			sink.RetryMaxMS = 30000
		}

		// Set RetryMultiplier default
		if sink.RetryMultiplier <= 0 {
			sink.RetryMultiplier = 2.0
		}

		// Set FilterTables default
		if len(sink.FilterTables) == 0 {
			sink.FilterTables = []string{"*"}
		}

		// Set FilterDatabases default
		if len(sink.FilterDatabases) == 0 {
			sink.FilterDatabases = []string{"*"}
		}
	}
}

// generateNodeID creates a unique node ID based on machine ID
func generateNodeID() (uint64, error) {
	id, err := machineid.ProtectedID("marmot")
	if err != nil {
		return 0, err
	}

	h := fnv.New64a()
	h.Write([]byte(id))
	return h.Sum64(), nil
}

// Validate checks configuration for errors
func Validate() error {
	if Config.Cluster.GRPCPort < 1 || Config.Cluster.GRPCPort > 65535 {
		return fmt.Errorf("invalid gRPC port: %d", Config.Cluster.GRPCPort)
	}

	// Auto-fill advertise address if not provided
	if Config.Cluster.GRPCAdvertiseAddress == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get hostname, using localhost")
			hostname = "localhost"
		}
		Config.Cluster.GRPCAdvertiseAddress = fmt.Sprintf("%s:%d", hostname, Config.Cluster.GRPCPort)
		log.Info().
			Str("advertise_address", Config.Cluster.GRPCAdvertiseAddress).
			Msg("Auto-configured gRPC advertise address")
	}

	if Config.MySQL.Enabled && (Config.MySQL.Port < 1 || Config.MySQL.Port > 65535) {
		return fmt.Errorf("invalid MySQL port: %d", Config.MySQL.Port)
	}

	// Validate auto_id_mode
	if Config.MySQL.AutoIDMode != "" && Config.MySQL.AutoIDMode != "compact" && Config.MySQL.AutoIDMode != "extended" {
		return fmt.Errorf("invalid auto_id_mode: %q (must be \"compact\" or \"extended\")", Config.MySQL.AutoIDMode)
	}

	// Validate consistency levels
	validConsistency := map[string]bool{
		"ONE": true, "TWO": true, "THREE": true,
		"QUORUM": true, "ALL": true, "LOCAL_ONE": true,
	}

	if !validConsistency[Config.Replication.DefaultWriteConsist] {
		return fmt.Errorf("invalid write consistency: %s", Config.Replication.DefaultWriteConsist)
	}

	if !validConsistency[Config.Replication.DefaultReadConsist] {
		return fmt.Errorf("invalid read consistency: %s", Config.Replication.DefaultReadConsist)
	}

	// Validate transaction configuration
	if Config.Transaction.HeartbeatTimeoutSeconds < 1 {
		return fmt.Errorf("transaction heartbeat timeout must be >= 1 second")
	}

	if Config.Transaction.ConflictWindowSeconds < 0 {
		return fmt.Errorf("transaction conflict window must be >= 0")
	}

	// Set LockWaitTimeoutSeconds default if not configured
	if Config.Transaction.LockWaitTimeoutSeconds == 0 {
		Config.Transaction.LockWaitTimeoutSeconds = 50 // MySQL default: innodb_lock_wait_timeout
	}

	// Set MetaStore (PebbleDB) defaults if not configured
	if Config.MetaStore.CacheSizeMB < 1 {
		Config.MetaStore.CacheSizeMB = 64
	}
	if Config.MetaStore.MemTableSizeMB < 1 {
		Config.MetaStore.MemTableSizeMB = 32
	}
	if Config.MetaStore.MemTableCount < 1 {
		Config.MetaStore.MemTableCount = 2
	}
	if Config.MetaStore.L0CompactionThreshold < 1 {
		Config.MetaStore.L0CompactionThreshold = 500 // CockroachDB default
	}
	if Config.MetaStore.L0StopWrites < 1 {
		Config.MetaStore.L0StopWrites = 1000 // CockroachDB default
	}
	if Config.MetaStore.WALBytesPerSyncKB == 0 {
		Config.MetaStore.WALBytesPerSyncKB = 512 // 512KB like CockroachDB
	}

	// Validate connection pool configuration
	if Config.ConnectionPool.PoolSize < 1 {
		return fmt.Errorf("connection pool size must be >= 1")
	}

	if Config.ConnectionPool.MaxIdleTimeSeconds < 0 {
		return fmt.Errorf("connection pool max idle time must be >= 0")
	}

	if Config.ConnectionPool.MaxLifetimeSeconds < 0 {
		return fmt.Errorf("connection pool max lifetime must be >= 0")
	}

	// Validate gRPC client configuration
	if Config.GRPCClient.KeepaliveTimeSeconds < 1 {
		return fmt.Errorf("gRPC keepalive time must be >= 1 second")
	}

	if Config.GRPCClient.KeepaliveTimeoutSeconds < 1 {
		return fmt.Errorf("gRPC keepalive timeout must be >= 1 second")
	}

	if Config.GRPCClient.MaxRetries < 0 {
		return fmt.Errorf("gRPC max retries must be >= 0")
	}

	if Config.GRPCClient.RetryBackoffMS < 0 {
		return fmt.Errorf("gRPC retry backoff must be >= 0")
	}

	// Validate coordinator configuration
	if Config.Coordinator.PrepareTimeoutMS < 1 {
		return fmt.Errorf("coordinator prepare timeout must be >= 1ms")
	}

	if Config.Coordinator.CommitTimeoutMS < 1 {
		return fmt.Errorf("coordinator commit timeout must be >= 1ms")
	}

	if Config.Coordinator.AbortTimeoutMS < 1 {
		return fmt.Errorf("coordinator abort timeout must be >= 1ms")
	}

	// Validate anti-entropy and GC parameter alignment (fail-fast)
	// Critical: GC must retain data longer than anti-entropy thresholds to enable snapshot recovery
	if Config.Replication.EnableAntiEntropy {
		deltaSyncThresholdHours := float64(Config.Replication.DeltaSyncThresholdSeconds) / 3600.0

		// Rule 1: GC min retention must be at least as long as delta sync threshold
		// This ensures nodes within the threshold window can use delta sync
		if float64(Config.Replication.GCMinRetentionHours) < deltaSyncThresholdHours {
			return fmt.Errorf(
				"gc_min_retention_hours (%d) must be >= delta_sync_threshold_seconds in hours (%.1f). "+
					"GC cannot delete data needed for delta sync. "+
					"Recommendation: set gc_min_retention_hours to %d",
				Config.Replication.GCMinRetentionHours,
				deltaSyncThresholdHours,
				int(deltaSyncThresholdHours)+1,
			)
		}

		// Rule 2: GC max retention should be at least 2x delta sync threshold
		// This provides safety margin for nodes that are slightly delayed
		// UNLESS gc_max_retention_hours is 0 (unlimited)
		if Config.Replication.GCMaxRetentionHours > 0 {
			minRecommendedMaxRetention := deltaSyncThresholdHours * 2.0
			if float64(Config.Replication.GCMaxRetentionHours) < minRecommendedMaxRetention {
				return fmt.Errorf(
					"gc_max_retention_hours (%d) should be at least 2x delta_sync_threshold_seconds in hours (%.1f). "+
						"Current ratio: %.1fx. This prevents snapshot recovery if nodes are down longer than expected. "+
						"Recommendation: set gc_max_retention_hours to at least %d (or disable force GC with value 0)",
					Config.Replication.GCMaxRetentionHours,
					minRecommendedMaxRetention,
					float64(Config.Replication.GCMaxRetentionHours)/deltaSyncThresholdHours,
					int(minRecommendedMaxRetention)+1,
				)
			}

			// Rule 3: GC min must be strictly less than GC max (when max > 0)
			if Config.Replication.GCMinRetentionHours >= Config.Replication.GCMaxRetentionHours {
				return fmt.Errorf(
					"gc_min_retention_hours (%d) must be < gc_max_retention_hours (%d). "+
						"Set gc_max_retention_hours to 0 for unlimited retention, or increase it above gc_min_retention_hours",
					Config.Replication.GCMinRetentionHours,
					Config.Replication.GCMaxRetentionHours,
				)
			}
		}

		// Rule 4: GC interval MUST be >= AE interval for watermark freshness
		// GC queries watermarks that are updated by AE - if GC runs more often than AE,
		// it makes decisions based on stale watermarks which can cause data loss
		if Config.Replication.GCIntervalS < Config.Replication.AntiEntropyIntervalS {
			return fmt.Errorf(
				"gc_interval_seconds (%d) must be >= anti_entropy_interval_seconds (%d). "+
					"GC must not run more frequently than anti-entropy to ensure fresh watermarks before GC decisions. "+
					"Recommendation: set gc_interval_seconds to at least %d",
				Config.Replication.GCIntervalS,
				Config.Replication.AntiEntropyIntervalS,
				Config.Replication.AntiEntropyIntervalS,
			)
		}
	}

	// Validate replica configuration
	if Config.Replica.Enabled {
		// Follow addresses are required
		if len(Config.Replica.FollowAddresses) == 0 {
			return fmt.Errorf("replica.follow_addresses is required when replica mode is enabled")
		}

		// Replica secret is required for authentication
		if Config.Replica.Secret == "" {
			return fmt.Errorf("replica.secret is required when replica mode is enabled (PSK authentication mandatory)")
		}

		// Replica mode and cluster mode are mutually exclusive
		if len(Config.Cluster.SeedNodes) > 0 {
			return fmt.Errorf("replica mode cannot be used with cluster seed_nodes - replicas do not join the cluster")
		}

		// Validate discovery interval
		if Config.Replica.DiscoveryIntervalSec < 1 {
			return fmt.Errorf("replica.discovery_interval_seconds must be >= 1")
		}

		// Validate failover timeout
		if Config.Replica.FailoverTimeoutSec < 1 {
			return fmt.Errorf("replica.failover_timeout_seconds must be >= 1")
		}

		// Validate reconnect intervals
		if Config.Replica.ReconnectIntervalSec < 1 {
			return fmt.Errorf("replica.reconnect_interval_seconds must be >= 1")
		}

		if Config.Replica.ReconnectMaxBackoffSec < Config.Replica.ReconnectIntervalSec {
			return fmt.Errorf("replica.reconnect_max_backoff_seconds must be >= reconnect_interval_seconds")
		}

		if Config.Replica.InitialSyncTimeoutMin < 1 {
			return fmt.Errorf("replica.initial_sync_timeout_minutes must be >= 1")
		}

		// Set defaults for new fields
		if Config.Replica.DatabaseDiscoveryIntervalSec <= 0 {
			Config.Replica.DatabaseDiscoveryIntervalSec = 10
		}

		if Config.Replica.SnapshotConcurrency <= 0 {
			Config.Replica.SnapshotConcurrency = 3
		}

		if Config.Replica.SnapshotCacheTTLSec <= 0 {
			Config.Replica.SnapshotCacheTTLSec = 30
		}

		// Validate replicate_databases (cannot include system DB)
		for _, dbName := range Config.Replica.ReplicateDatabases {
			if dbName == "__marmot_system" {
				return fmt.Errorf("cannot explicitly replicate system database in replicate_databases")
			}
		}
	}

	// Validate publisher configuration
	if Config.Publisher.Enabled {
		// At least one sink must be configured
		if len(Config.Publisher.Sinks) == 0 {
			return fmt.Errorf("publisher.enabled=true requires at least one sink to be configured")
		}

		// Track sink names for uniqueness validation
		sinkNames := make(map[string]bool)

		for i, sink := range Config.Publisher.Sinks {
			// Each sink must have unique non-empty Name
			if sink.Name == "" {
				return fmt.Errorf("publisher.sinks[%d].name cannot be empty", i)
			}

			if sinkNames[sink.Name] {
				return fmt.Errorf("publisher.sinks[%d].name=%q is not unique - sink names must be unique", i, sink.Name)
			}
			sinkNames[sink.Name] = true

			// Type must be "kafka" or "nats"
			if sink.Type != "kafka" && sink.Type != "nats" {
				return fmt.Errorf("publisher.sinks[%d].type=%q is invalid - must be \"kafka\" or \"nats\"", i, sink.Type)
			}

			// Format must be "debezium" for now
			if sink.Format != "debezium" {
				return fmt.Errorf("publisher.sinks[%d].format=%q is invalid - only \"debezium\" is supported", i, sink.Format)
			}

			// Brokers must not be empty for kafka type
			if sink.Type == "kafka" && len(sink.Brokers) == 0 {
				return fmt.Errorf("publisher.sinks[%d].brokers cannot be empty for type \"kafka\"", i)
			}

			// NatsURL must not be empty for nats type
			if sink.Type == "nats" && sink.NatsURL == "" {
				return fmt.Errorf("publisher.sinks[%d].nats_url cannot be empty for type \"nats\"", i)
			}

			// TopicPrefix must not be empty
			if sink.TopicPrefix == "" {
				return fmt.Errorf("publisher.sinks[%d].topic_prefix cannot be empty", i)
			}
		}
	}

	return nil
}

// IsClusterAuthEnabled returns true if cluster authentication is configured
func IsClusterAuthEnabled() bool {
	return Config.Cluster.ClusterSecret != ""
}

// GetClusterSecret returns the cluster secret for PSK authentication
func GetClusterSecret() string {
	return Config.Cluster.ClusterSecret
}

// IsReplicaMode returns true if read-only replica mode is enabled
func IsReplicaMode() bool {
	return Config.Replica.Enabled
}

// GetReplicaSecret returns the replica secret for PSK authentication with master
func GetReplicaSecret() string {
	return Config.Replica.Secret
}
