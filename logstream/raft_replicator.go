package logstream

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/maxpert/marmot/cfg"
	"github.com/rs/zerolog/log"
	"path/filepath"
	"time"
)

type RaftReplicator struct {
	raft *raft.Raft
}

func raftBoltStore(baseDir, name string) (*boltdb.BoltStore, error) {
	db, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, fmt.Errorf(`unable to open store %q: %v`, filepath.Join(baseDir, name), err)
	}

	return db, err
}

func NewRaftReplicator(
	nodeID string,
	rootDir string,
	bindAddress string,
	joinAddresses []string,
) (*RaftReplicator, error) {
	logWriter := log.With().Str("replicator", "marmot-raft").Logger()
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(nodeID)
	raftConfig.LogOutput = logWriter

	baseDir := filepath.Join(rootDir, fmt.Sprintf("%v", cfg.Config.NodeID))
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
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:   "marmot-raft",
			Output: logWriter,
			Level:  hclog.DefaultLevel,
		}),
	})

	if err != nil {
		return nil, err
	}

	rft, err := raft.NewRaft(raftConfig, nil, ldb, sdb, fss, tm)
	return &RaftReplicator{
		raft: rft,
	}, nil
}
