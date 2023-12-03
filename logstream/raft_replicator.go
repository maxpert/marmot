package logstream

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/rs/zerolog/log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type RaftReplicator struct {
	raft *raft.Raft
	sm   *raftFSM
}

func raftBoltStore(baseDir, name string) (*boltdb.BoltStore, error) {
	fullPath := filepath.Join(baseDir, name)
	db, err := boltdb.NewBoltStore(fullPath)
	if err != nil {
		return nil, fmt.Errorf(`unable to open store %q: %v`, fullPath, err)
	}

	return db, err
}

func prepareRaftCluster(nodes ...string) []raft.Server {
	var ret []raft.Server
	for _, n := range nodes {
		if !strings.HasPrefix(n, "raft:") {
			continue
		}
		n = strings.TrimPrefix(n, "raft:")
		u, err := url.Parse(n)
		if err != nil {
			log.Warn().Err(err).Msg("Unable to parse URL")
			continue
		}

		ret = append(ret, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(u.Scheme),
			Address:  raft.ServerAddress(u.Host),
		})
	}

	return ret
}

func NewRaftReplicator(
	nodeID string,
	rootDir string,
	bindAddress string,
	joinAddresses []string,
) (*RaftReplicator, error) {
	logWriter := log.With().Str("replicator", "marmot-raft").Logger()
	hcLogger := hclog.New(&hclog.LoggerOptions{
		Name:        "marmot-raft",
		Output:      logWriter,
		Level:       hclog.DefaultLevel,
		DisableTime: true,
	})

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(nodeID)
	raftConfig.LogOutput = logWriter
	raftConfig.LogLevel = "INFO"
	raftConfig.Logger = hcLogger

	baseDir := filepath.Join(rootDir, fmt.Sprintf("%v", nodeID))
	err := os.MkdirAll(baseDir, 0770)
	if err != nil {
		return nil, err
	}

	ldb, err := raftBoltStore(baseDir, "log.dat")
	if err != nil {
		return nil, err
	}

	sdb, err := raftBoltStore(baseDir, "store.dat")
	if err != nil {
		return nil, err
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, logWriter)
	tm, err := raft.NewTCPTransportWithConfig(bindAddress, nil, &raft.NetworkTransportConfig{
		MaxPool:         4,
		MaxRPCsInFlight: 2,
		Timeout:         2 * time.Second,
		Logger:          hcLogger,
	})

	if err != nil {
		return nil, err
	}

	sm := &raftFSM{}
	rft, err := raft.NewRaft(raftConfig, sm, ldb, sdb, fss, tm)

	selfUrl := fmt.Sprintf("raft:%s://%s", nodeID, bindAddress)
	f := rft.BootstrapCluster(raft.Configuration{
		Servers: prepareRaftCluster(append(joinAddresses, selfUrl)...),
	})

	if err = f.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return nil, err
	}

	return &RaftReplicator{
		raft: rft,
		sm:   sm,
	}, nil
}
