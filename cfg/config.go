package cfg

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type SnapshotStoreType string

const NodeNamePrefix = "marmot-node"
const EmbeddedClusterName = "e-marmot"
const (
	Nats   SnapshotStoreType = "nats"
	S3     SnapshotStoreType = "s3"
	WebDAV SnapshotStoreType = "webdav"
	SFTP   SnapshotStoreType = "sftp"
)

type ReplicationLogConfiguration struct {
	Shards         uint64 `toml:"shards"`
	MaxEntries     int64  `toml:"max_entries"`
	Replicas       int    `toml:"replicas"`
	Compress       bool   `toml:"compress"`
	UpdateExisting bool   `toml:"update_existing"`
}

type WebDAVConfiguration struct {
	Url string `toml:"url"`
}

type SFTPConfiguration struct {
	Url string `toml:"url"`
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
	Interval  uint32                   `toml:"interval"`
	StoreType SnapshotStoreType        `toml:"store"`
	Nats      ObjectStoreConfiguration `toml:"nats"`
	S3        S3Configuration          `toml:"s3"`
	WebDAV    WebDAVConfiguration      `toml:"webdav"`
	SFTP      SFTPConfiguration        `toml:"sftp"`
}

type NATSConfiguration struct {
	URLs                 []string `toml:"urls"`
	SubjectPrefix        string   `toml:"subject_prefix"`
	StreamPrefix         string   `toml:"stream_prefix"`
	ServerConfigFile     string   `toml:"server_config"`
	SeedFile             string   `toml:"seed_file"`
	CredsUser            string   `toml:"user_name"`
	CredsPassword        string   `toml:"user_password"`
	CAFile               string   `toml:"ca_file"`
	CertFile             string   `toml:"cert_file"`
	KeyFile              string   `toml:"key_file"`
	BindAddress          string   `toml:"bind_address"`
	ConnectRetries       int      `toml:"connect_retries"`
	ReconnectWaitSeconds int      `toml:"reconnect_wait_seconds"`
}

type LoggingConfiguration struct {
	Verbose bool   `toml:"verbose"`
	Format  string `toml:"format"`
}

type PrometheusConfiguration struct {
	Bind      string `toml:"bind"`
	Enable    bool   `toml:"enable"`
	Namespace string `toml:"namespace"`
	Subsystem string `toml:"subsystem"`
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
	Prometheus     PrometheusConfiguration     `toml:"prometheus"`
}

var ConfigPathFlag = flag.String("config", "", "Path to configuration file")
var CleanupFlag = flag.Bool("cleanup", false, "Only cleanup marmot triggers and changelogs")
var SaveSnapshotFlag = flag.Bool("save-snapshot", false, "Only take snapshot and upload")
var ClusterAddrFlag = flag.String("cluster-addr", "", "Cluster listening address")
var ClusterPeersFlag = flag.String("cluster-peers", "", "Comma separated list of clusters")
var LeafServerFlag = flag.String("leaf-servers", "", "Comma separated list of leaf servers")
var ProfServer = flag.String("pprof", "", "PProf listening address")

var DataRootDir = os.TempDir()
var Config = &Configuration{
	SeqMapPath:      path.Join(DataRootDir, "seq-map.cbor"),
	DBPath:          path.Join(DataRootDir, "marmot.db"),
	NodeID:          0,
	Publish:         true,
	Replicate:       true,
	ScanMaxChanges:  512,
	CleanupInterval: 5000,
	SleepTimeout:    0,
	PollingInterval: 0,

	Snapshot: SnapshotConfiguration{
		Enable:    true,
		Interval:  0,
		StoreType: Nats,
		Nats: ObjectStoreConfiguration{
			Replicas: 1,
		},
		S3:     S3Configuration{},
		WebDAV: WebDAVConfiguration{},
		SFTP:   SFTPConfiguration{},
	},

	ReplicationLog: ReplicationLogConfiguration{
		Shards:         1,
		MaxEntries:     1024,
		Replicas:       1,
		Compress:       true,
		UpdateExisting: false,
	},

	NATS: NATSConfiguration{
		URLs:                 []string{},
		SubjectPrefix:        "marmot-change-log",
		StreamPrefix:         "marmot-changes",
		ServerConfigFile:     "",
		SeedFile:             "",
		CredsPassword:        "",
		CredsUser:            "",
		BindAddress:          ":-1",
		ConnectRetries:       5,
		ReconnectWaitSeconds: 2,
	},

	Logging: LoggingConfiguration{
		Verbose: false,
		Format:  "console",
	},

	Prometheus: PrometheusConfiguration{
		Bind:      ":3010",
		Enable:    false,
		Namespace: "marmot",
		Subsystem: "",
	},
}

func init() {
	id, err := machineid.ID()
	if err != nil {
		log.Warn().Err(err).Msg("⚠️⚠️⚠️ Unable to read machine ID from OS, generating random ID ⚠️⚠️⚠️")
		id = uuid.NewString()
	}

	hasher := fnv.New64()
	_, err = hasher.Write([]byte(id))
	if err != nil {
		panic(err)
	}

	Config.NodeID = hasher.Sum64()
}

func Load(filePath string) error {
	_, err := toml.DecodeFile(filePath, Config)
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	DataRootDir, err = filepath.Abs(path.Dir(Config.DBPath))
	if err != nil {
		return err
	}

	if Config.SeqMapPath == "" {
		Config.SeqMapPath = path.Join(DataRootDir, "seq-map.cbor")
	}

	return nil
}

func (c *Configuration) SnapshotStorageType() SnapshotStoreType {
	return c.Snapshot.StoreType
}

func (c *Configuration) NodeName() string {
	return fmt.Sprintf("%s-%d", NodeNamePrefix, c.NodeID)
}
