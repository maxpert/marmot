package cfg

import (
	"flag"
	"hash/fnv"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
	"github.com/nats-io/nats.go"
)

type ReplicationLogConfiguration struct {
	Shards     uint64 `toml:"shards"`
	MaxEntries int64  `toml:"max_entries"`
	Replicas   int    `toml:"replicas"`
	Compress   bool   `toml:"compress"`
}

type SnapshotConfiguration struct {
	Enable   bool `toml:"enabled"`
	Replicas int  `toml:"replicas"`
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
		Enable:   true,
		Replicas: 1,
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
