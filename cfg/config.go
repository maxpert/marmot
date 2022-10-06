package cfg

import (
	"flag"
	"math/rand"

	"github.com/nats-io/nats.go"
)

var Cleanup = flag.Bool("cleanup", false, "Cleanup all trigger hooks for marmot")
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
