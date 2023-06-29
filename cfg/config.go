package cfg

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"path"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
)

type SnapshotStoreType string

const NodeNamePrefix = "marmot-node"
const (
	Nats SnapshotStoreType = "nats"
	S3                     = "s3"
)

type ReplicationLogConfiguration struct {
	Shards     uint64 `toml:"shards"`
	MaxEntries int64  `toml:"max_entries"`
	Replicas   int    `toml:"replicas"`
	Compress   bool   `toml:"compress"`
}

type S3Configuration struct {
	DirPath      string `toml:"path"`
	Endpoint     string `toml:"endpoint"`
	AccessKey    string `toml:"access_key"`
	SecretKey    string `toml:"secret"`
	SessionToken string `toml:"session_token"`
	Bucket       string `toml:"bucket"`
	UseSSL       bool   `toml:"use_ssl"`
}

type ObjectStoreConfiguration struct {
	Replicas   int    `toml:"replicas"`
	BucketName string `toml:"bucket"`
}

type SnapshotConfiguration struct {
	Enable    bool                     `toml:"enabled"`
	StoreType SnapshotStoreType        `toml:"store"`
	Nats      ObjectStoreConfiguration `toml:"nats"`
	S3        S3Configuration          `toml:"s3"`
}

type NATSConfiguration struct {
	URLs             []string `toml:"urls"`
	SubjectPrefix    string   `toml:"subject_prefix"`
	StreamPrefix     string   `toml:"stream_prefix"`
	ServerConfigFile string   `toml:"server_config"`
	SeedFile         string   `toml:"seed_file"`
}

type LoggingConfiguration struct {
	Verbose bool   `toml:"verbose"`
	Format  string `toml:"format"`
}

type Configuration struct {
	SeqMapPath      string `toml:"seq_map_path"`
	DBPath          string `toml:"db_path"`
	NodeID          uint64 `toml:"node_id"`
	Publish         bool   `toml:"publish"`
	Replicate       bool   `toml:"replicate"`
	ScanMaxChanges  uint32 `toml:"scan_max_changes"`
	CleanupInterval uint32 `toml:"cleanup_interval"`
	SleepTimeout    uint32 `toml:"sleep_timeout"`
	PollingInterval uint32 `toml:"polling_interval"`

	Snapshot       SnapshotConfiguration       `toml:"snapshot"`
	ReplicationLog ReplicationLogConfiguration `toml:"replication_log"`
	NATS           NATSConfiguration           `toml:"nats"`
	Logging        LoggingConfiguration        `toml:"logging"`
}

var ConfigPath = flag.String("config", "marmot.toml", "Path to configuration file")
var Cleanup = flag.Bool("cleanup", false, "Only cleanup marmot triggers and changelogs")
var SaveSnapshot = flag.Bool("save-snapshot", false, "Only take snapshot and upload")
var ClusterListenAddr = flag.String("cluster-addr", "", "Cluster listening address")
var ClusterPeers = flag.String("cluster-peers", "", "Comma separated list of clusters")

var TmpDir = os.TempDir()

var Config = &Configuration{
	SeqMapPath:      path.Join(TmpDir, "seq-map.cbor"),
	DBPath:          path.Join(TmpDir, "marmot.db"),
	NodeID:          1,
	Publish:         true,
	Replicate:       true,
	ScanMaxChanges:  512,
	CleanupInterval: 5000,
	SleepTimeout:    0,
	PollingInterval: 0,

	Snapshot: SnapshotConfiguration{
		Enable:    true,
		StoreType: Nats,
		Nats: ObjectStoreConfiguration{
			Replicas: 1,
		},
		S3: S3Configuration{},
	},

	ReplicationLog: ReplicationLogConfiguration{
		Shards:     1,
		MaxEntries: 1024,
		Replicas:   1,
		Compress:   true,
	},

	NATS: NATSConfiguration{
		URLs:             []string{},
		SubjectPrefix:    "marmot-change-log",
		StreamPrefix:     "marmot-changes",
		ServerConfigFile: "",
	},

	Logging: LoggingConfiguration{
		Verbose: false,
		Format:  "console",
	},
}

func init() {
	id, err := machineid.ID()
	if err != nil {
		panic(err)
	}

	hasher := fnv.New64()
	_, err = hasher.Write([]byte(id))
	if err != nil {
		panic(err)
	}

	Config.NodeID = hasher.Sum64()
}

func Load(path string) error {
	_, err := toml.DecodeFile(path, Config)
	if os.IsNotExist(err) {
		return nil
	}

	return err
}

func (c *Configuration) SnapshotStorageType() SnapshotStoreType {
	return c.Snapshot.StoreType
}

func (c *Configuration) NodeName() string {
	return fmt.Sprintf("%s-%d", NodeNamePrefix, c.NodeID)
}
