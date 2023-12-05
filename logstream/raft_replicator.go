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

func parseRaftNodeAddress(suff raft.ServerSuffrage, nodes ...string) []raft.Server {
	var ret []raft.Server
	for _, n := range nodes {
		if !strings.HasPrefix(n, "raft:") {
			log.Warn().Str("address", n).Msg("Invalid node address, must start with raft:")
			continue
		}

		n = strings.TrimPrefix(n, "raft:")
		u, err := url.Parse(n)
		if err != nil {
			log.Warn().Err(err).Msg("Unable to parse URL")
			continue
		}

		ret = append(ret, raft.Server{
			Suffrage: suff,
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
	logWriter := log.With().Str("node", nodeID).Logger()
	hcLogger := hclog.New(&hclog.LoggerOptions{
		Name:        nodeID,
		Level:       hclog.Info,
		DisableTime: true,
		JSONFormat:  true,
	})

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(nodeID)
	raftConfig.LogOutput = logWriter
	raftConfig.LogLevel = "INFO"
	raftConfig.Logger = hcLogger

	baseDir := filepath.Join(rootDir, fmt.Sprintf("data-%v", nodeID))
	err := os.MkdirAll(baseDir, 0o755)
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

	fss, err := raft.NewFileSnapshotStoreWithLogger(baseDir, 3, hcLogger)
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
		Servers: parseRaftNodeAddress(raft.Voter, append(joinAddresses, selfUrl)...),
	})

	if err = f.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return nil, err
	}

	return &RaftReplicator{
		raft: rft,
		sm:   sm,
	}, nil
}

func (r *RaftReplicator) Shutdown() {
	f := r.raft.Shutdown()
	if f.Error() != nil {
		log.Warn().Err(f.Error()).Msg("Unable to shutdown")
	}
}

func (r *RaftReplicator) AddMember(url string) error {
	mem := parseRaftNodeAddress(raft.Voter, url)
	f := r.raft.AddVoter(mem[0].ID, mem[0].Address, 0, 2*time.Second)
	return f.Error()
}

func (r *RaftReplicator) AddFollower(url string) error {
	mem := parseRaftNodeAddress(raft.Nonvoter, url)
	f := r.raft.AddNonvoter(mem[0].ID, mem[0].Address, 0, 2*time.Second)
	return f.Error()
}
