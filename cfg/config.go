package cfg

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
	"github.com/nats-io/nats.go"
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
	URLs          []string `toml:"urls"`
	SubjectPrefix string   `toml:"subject_prefix"`
	StreamPrefix  string   `toml:"stream_prefix"`
}

type LoggingConfiguration struct {
	Verbose bool   `toml:"verbose"`
	Format  string `toml:"format"`
}

type Configuration struct {
	SeqMapPath string `toml:"seq_map_path"`
	DBPath     string `toml:"db_path"`
	NodeID     uint64 `toml:"node_id"`

	Snapshot       SnapshotConfiguration       `toml:"snapshot"`
	ReplicationLog ReplicationLogConfiguration `toml:"replication_log"`
	NATS           NATSConfiguration           `toml:"nats"`
	Logging        LoggingConfiguration        `toml:"logging"`
}

var ConfigPath = flag.String("config", "marmot.toml", "Path to configuration file")
var Cleanup = flag.Bool("cleanup", false, "Only cleanup marmot triggers and changelogs")
var SaveSnapshot = flag.Bool("save-snapshot", false, "Only take snapshot and upload")

var Config = &Configuration{
	SeqMapPath: "/tmp/seq-map.cbor",
	DBPath:     "/tmp/marmot.db",
	NodeID:     1,

	Snapshot: SnapshotConfiguration{
		Enable:    true,
		StoreType: Nats,
		Nats: ObjectStoreConfiguration{
			Replicas: 1,
		},
		S3: S3Configuration{},
	},

	ReplicationLog: ReplicationLogConfiguration{
		Shards:     8,
		MaxEntries: 1024,
		Replicas:   1,
		Compress:   true,
	},

	NATS: NATSConfiguration{
		URLs:          []string{nats.DefaultURL},
		SubjectPrefix: "marmot-change-log",
		StreamPrefix:  "marmot-changes",
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
