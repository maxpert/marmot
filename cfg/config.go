package cfg

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"path"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
	"github.com/rs/zerolog/log"
)

// SnapshotStoreType defines where snapshots are stored
type SnapshotStoreType string

const (
	SnapshotPeer   SnapshotStoreType = "peer"   // Transfer via gRPC from peers
	SnapshotS3     SnapshotStoreType = "s3"     // S3-compatible storage
	SnapshotWebDAV SnapshotStoreType = "webdav" // WebDAV storage
	SnapshotSFTP   SnapshotStoreType = "sftp"   // SFTP storage
	SnapshotLocal  SnapshotStoreType = "local"  // Local file system
)

// S3Configuration for S3-compatible storage backends
type S3Configuration struct {
	Endpoint     string `toml:"endpoint"`
	AccessKey    string `toml:"access_key"`
	SecretKey    string `toml:"secret"`
	SessionToken string `toml:"session_token"`
	Bucket       string `toml:"bucket"`
	Path         string `toml:"path"`
	UseSSL       bool   `toml:"use_ssl"`
}

// WebDAVConfiguration for WebDAV storage
type WebDAVConfiguration struct {
	URL string `toml:"url"`
}

// SFTPConfiguration for SFTP storage
type SFTPConfiguration struct {
	URL string `toml:"url"`
}

// SnapshotConfiguration controls snapshot behavior
type SnapshotConfiguration struct {
	Enabled         bool                `toml:"enabled"`
	IntervalSeconds int                 `toml:"interval_seconds"`
	StoreType       SnapshotStoreType   `toml:"store"`
	ChunkSizeMB     int                 `toml:"chunk_size_mb"`
	ParallelChunks  int                 `toml:"parallel_chunks"`
	IncThreshold    int                 `toml:"incremental_threshold"` // Changes before full snapshot
	S3              S3Configuration     `toml:"s3"`
	WebDAV          WebDAVConfiguration `toml:"webdav"`
	SFTP            SFTPConfiguration   `toml:"sftp"`
}

// ClusterConfiguration controls cluster membership and communication
type ClusterConfiguration struct {
	GRPCBindAddress      string   `toml:"grpc_bind_address"`
	GRPCAdvertiseAddress string   `toml:"grpc_advertise_address"` // Address other nodes use to connect (defaults to hostname:port)
	GRPCPort             int      `toml:"grpc_port"`
	SeedNodes            []string `toml:"seed_nodes"`
	GossipIntervalMS     int      `toml:"gossip_interval_ms"`
	GossipFanout         int      `toml:"gossip_fanout"`
	SuspectTimeoutMS     int      `toml:"suspect_timeout_ms"`
	DeadTimeoutMS        int      `toml:"dead_timeout_ms"`
}

// ReplicationConfiguration controls replication behavior
type ReplicationConfiguration struct {
	ReplicationFactor    int    `toml:"replication_factor"`
	VirtualNodes         int    `toml:"virtual_nodes"`
	DefaultWriteConsist  string `toml:"default_write_consistency"`
	DefaultReadConsist   string `toml:"default_read_consistency"`
	WriteTimeoutMS       int    `toml:"write_timeout_ms"`
	ReadTimeoutMS        int    `toml:"read_timeout_ms"`
	EnableAntiEntropy    bool   `toml:"enable_anti_entropy"`
	AntiEntropyIntervalS int    `toml:"anti_entropy_interval_seconds"`
}

// MySQLConfiguration for MySQL wire protocol server
type MySQLConfiguration struct {
	Enabled        bool   `toml:"enabled"`
	BindAddress    string `toml:"bind_address"`
	Port           int    `toml:"port"`
	MaxConnections int    `toml:"max_connections"`
}

// LoggingConfiguration controls logging behavior
type LoggingConfiguration struct {
	Verbose bool   `toml:"verbose"`
	Format  string `toml:"format"` // "console" or "json"
}

// PrometheusConfiguration for metrics
type PrometheusConfiguration struct {
	Enabled bool   `toml:"enabled"`
	Address string `toml:"address"`
	Port    int    `toml:"port"`
}

// MVCCConfiguration controls MVCC transaction manager behavior
type MVCCConfiguration struct {
	GCIntervalSeconds       int `toml:"gc_interval_seconds"`       // How often to run garbage collection
	GCRetentionHours        int `toml:"gc_retention_hours"`        // How long to keep old transaction records
	HeartbeatTimeoutSeconds int `toml:"heartbeat_timeout_seconds"` // Transaction timeout without heartbeat
	VersionRetentionCount   int `toml:"version_retention_count"`   // How many MVCC versions to keep per row
	ConflictWindowSeconds   int `toml:"conflict_window_seconds"`   // LWW conflict resolution window
}

// ConnectionPoolConfiguration controls database connection pooling
type ConnectionPoolConfiguration struct {
	PoolSize           int `toml:"pool_size"`             // Number of connections in pool
	MaxIdleTimeSeconds int `toml:"max_idle_time_seconds"` // Max time connection can be idle
	MaxLifetimeSeconds int `toml:"max_lifetime_seconds"`  // Max lifetime of a connection
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

// Configuration is the main configuration structure
type Configuration struct {
	NodeID  uint64 `toml:"node_id"`
	DataDir string `toml:"data_dir"`

	Snapshot       SnapshotConfiguration       `toml:"snapshot"`
	Cluster        ClusterConfiguration        `toml:"cluster"`
	Replication    ReplicationConfiguration    `toml:"replication"`
	MVCC           MVCCConfiguration           `toml:"mvcc"`
	ConnectionPool ConnectionPoolConfiguration `toml:"connection_pool"`
	GRPCClient     GRPCClientConfiguration     `toml:"grpc_client"`
	Coordinator    CoordinatorConfiguration    `toml:"coordinator"`
	MySQL          MySQLConfiguration          `toml:"mysql"`
	Logging        LoggingConfiguration        `toml:"logging"`
	Prometheus     PrometheusConfiguration     `toml:"prometheus"`
}

// Command line flags
var (
	ConfigPathFlag = flag.String("config", "config.toml", "Path to configuration file")
	DataDirFlag    = flag.String("data-dir", "", "Data directory (overrides config)")
	NodeIDFlag     = flag.Uint64("node-id", 0, "Node ID (overrides config, 0=auto)")
	GRPCPortFlag   = flag.Int("grpc-port", 0, "gRPC port (overrides config)")
	MySQLPortFlag  = flag.Int("mysql-port", 0, "MySQL port (overrides config)")
)

// Default configuration
var Config = &Configuration{
	NodeID:  0, // Auto-generate
	DataDir: "./marmot-data",

	Snapshot: SnapshotConfiguration{
		Enabled:         true,
		IntervalSeconds: 300, // 5 minutes
		StoreType:       SnapshotPeer,
		ChunkSizeMB:     5,
		ParallelChunks:  5,
		IncThreshold:    10000,
		S3:              S3Configuration{},
		WebDAV:          WebDAVConfiguration{},
		SFTP:            SFTPConfiguration{},
	},

	Cluster: ClusterConfiguration{
		GRPCBindAddress:  "0.0.0.0",
		GRPCPort:         8080,
		SeedNodes:        []string{},
		GossipIntervalMS: 1000,
		GossipFanout:     3,
		SuspectTimeoutMS: 5000,
		DeadTimeoutMS:    10000,
	},

	Replication: ReplicationConfiguration{
		ReplicationFactor:    3,
		VirtualNodes:         150,
		DefaultWriteConsist:  "QUORUM",
		DefaultReadConsist:   "LOCAL_ONE",
		WriteTimeoutMS:       5000,
		ReadTimeoutMS:        2000,
		EnableAntiEntropy:    true,
		AntiEntropyIntervalS: 600, // 10 minutes
	},

	MVCC: MVCCConfiguration{
		GCIntervalSeconds:       30, // Run GC every 30 seconds
		GCRetentionHours:        1,  // Keep transaction records for 1 hour
		HeartbeatTimeoutSeconds: 10, // Timeout transactions after 10s without heartbeat
		VersionRetentionCount:   10, // Keep last 10 MVCC versions per row
		ConflictWindowSeconds:   10, // 10 second window for LWW conflict resolution
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

	MySQL: MySQLConfiguration{
		Enabled:        true,
		BindAddress:    "0.0.0.0",
		Port:           3306,
		MaxConnections: 1000,
	},

	Logging: LoggingConfiguration{
		Verbose: false,
		Format:  "console",
	},

	Prometheus: PrometheusConfiguration{
		Enabled: true,
		Address: "0.0.0.0",
		Port:    9090,
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

	// Auto-generate node ID if not set
	if Config.NodeID == 0 {
		var err error
		Config.NodeID, err = generateNodeID()
		if err != nil {
			return fmt.Errorf("failed to generate node ID: %w", err)
		}
		log.Info().Uint64("node_id", Config.NodeID).Msg("Auto-generated node ID")
	}

	// Ensure data directory exists
	if err := os.MkdirAll(Config.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	return nil
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

	if Config.Replication.ReplicationFactor < 1 {
		return fmt.Errorf("replication factor must be >= 1")
	}

	if Config.Replication.VirtualNodes < 1 {
		return fmt.Errorf("virtual nodes must be >= 1")
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

	// Validate MVCC configuration
	if Config.MVCC.GCIntervalSeconds < 1 {
		return fmt.Errorf("MVCC GC interval must be >= 1 second")
	}

	if Config.MVCC.GCRetentionHours < 0 {
		return fmt.Errorf("MVCC GC retention hours must be >= 0")
	}

	if Config.MVCC.HeartbeatTimeoutSeconds < 1 {
		return fmt.Errorf("MVCC heartbeat timeout must be >= 1 second")
	}

	if Config.MVCC.VersionRetentionCount < 1 {
		return fmt.Errorf("MVCC version retention count must be >= 1")
	}

	if Config.MVCC.ConflictWindowSeconds < 0 {
		return fmt.Errorf("MVCC conflict window must be >= 0")
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

	return nil
}

// GetSeqMapPath returns the path to the sequence map file (legacy, may remove)
func GetSeqMapPath() string {
	return path.Join(Config.DataDir, "seq-map.cbor")
}
