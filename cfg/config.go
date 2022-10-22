package cfg

import (
	"flag"
	"hash/fnv"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
	"github.com/nats-io/nats.go"
)

type Configuration struct {
	EnableSnapshot bool     `toml:"enable_snapshot"`
	SeqMapPath     string   `toml:"meta_path"`
	DBPath         string   `toml:"db_path"`
	NodeID         uint64   `toml:"node_id"`
	NatsAddr       []string `toml:"nats_urls"`
	Shards         uint64   `toml:"shards"`
	MaxLogEntries  int64    `toml:"max_log_entries"`
	LogReplicas    int      `toml:"log_replicas"`
	SubjectPrefix  string   `toml:"subject_prefix"`
	StreamPrefix   string   `toml:"stream_prefix"`
	EnableCompress bool     `toml:"compress_logs"`
	Verbose        bool     `toml:"verbose"`
	StdOutFormat   string   `toml:"stdout_format"`
}

var ConfigPath = flag.String("config", "marmot.toml", "Path to configuration file")
var Cleanup = flag.Bool("cleanup", false, "Only cleanup marmot triggers and changelogs")
var SaveSnapshot = flag.Bool("save-snapshot", false, "Only take snapshot and upload")

var Config = &Configuration{
	EnableSnapshot: true,
	SeqMapPath:     "/tmp/seq-map.cbor",
	DBPath:         "/tmp/marmot.db",
	NodeID:         1,
	NatsAddr:       []string{nats.DefaultURL},
	Shards:         8,
	MaxLogEntries:  1024,
	LogReplicas:    1,
	SubjectPrefix:  "marmot-change-log",
	StreamPrefix:   "marmot-changes",
	EnableCompress: true,
	Verbose:        false,
	StdOutFormat:   "console",
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
