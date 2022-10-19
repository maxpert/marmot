package cfg

import (
	"flag"
	"math/rand"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/nats-io/nats.go"
)

type Configuration struct {
	Cleanup bool `koanf:"cleanup"`
}

var Cleanup = flag.Bool("cleanup", false, "Only cleanup marmot triggers and changelogs")
var SaveSnapshot = flag.Bool("save-snapshot", false, "Only take snapshot and upload")

var EnableSnapshot = flag.Bool("enable-snapshot", true, "Restore snapshot at boot")
var SeqMapPath = flag.String("seq-map-path", "/tmp/seq-map.cbor", "Path to stream sequence map")
var DBPathString = flag.String("db-path", "/tmp/marmot.db", "Path to SQLite database")
var NodeID = flag.Uint64("node-id", rand.Uint64(), "Node ID")
var NatsAddr = flag.String("nats-url", nats.DefaultURL, "NATS server URL")
var Shards = flag.Uint64("shards", 8, "Number of stream shards to distribute change log on")
var MaxLogEntries = flag.Int64("max-log-entries", 1024, "Maximum number of change log entries to persist")
var LogReplicas = flag.Int("log-replicas", 1, "Number of copies to be committed for single change log")
var SubjectPrefix = flag.String("subject-prefix", "marmot-change-log", "Prefix for publish subjects")
var StreamPrefix = flag.String("stream-prefix", "marmot-changes", "Prefix for publish subjects")
var EnableCompress = flag.Bool("compress", false, "Enable message compression")
var Verbose = flag.Bool("verbose", false, "Log debug level")

func Init(path string) (*Configuration, error) {
	k := koanf.New(path)
	err := k.Load(file.Provider("marmot.yaml"), yaml.Parser())
	if err != nil {
		return nil, err
	}

	conf := &Configuration{}
	err = k.Unmarshal(".", conf)
	if err != nil {
		return nil, err
	}

	return conf, nil
}
