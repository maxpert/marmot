package logstream

import (
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var tmpPath = "/tmp"
var maxWait = 5 * time.Second

func setup(t *testing.T) {
	var err error
	tmpPath, err = os.MkdirTemp(os.TempDir(), "marmot-test-*")
	assert.Nil(t, err)
}

func cleanup(t *testing.T) {
	err := os.RemoveAll(tmpPath)
	assert.Nil(t, err)
}

func waitState(r *RaftReplicator, state raft.RaftState) {
	start := time.Now()
	for {
		if r.raft.State() == state {
			return
		}

		if time.Now().Sub(start) >= maxWait {
			log.Panic().Msg("Timeout")
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func TestRaftReplicatorCanBootStandalone(t *testing.T) {
	setup(t)
	defer cleanup(t)

	r1, err := NewRaftReplicator("node-1", tmpPath, "127.0.0.1:2001", []string{})
	assert.Nil(t, err)
	defer r1.Shutdown()
	waitState(r1, raft.Leader)
}

func TestRafReplicatorCanJoinExistingNodeAsMember(t *testing.T) {
	setup(t)
	defer cleanup(t)

	r1, err := NewRaftReplicator("node-1", tmpPath, "127.0.0.1:2001", []string{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to bind")
	}
	assert.Nil(t, err)
	defer r1.Shutdown()
	waitState(r1, raft.Leader)

	r2, err := NewRaftReplicator("node-2", tmpPath, "127.0.0.1:2002", []string{})
	assert.Nil(t, err)
	defer r2.Shutdown()
	waitState(r2, raft.Leader)

	err = r1.AddMember("raft:node-2://127.0.0.1:2002/")
	assert.Nil(t, err)
	waitState(r2, raft.Follower)
}

func TestRafReplicatorCanJoinExistingNodeAsFollower(t *testing.T) {
	setup(t)
	defer cleanup(t)

	r1, err := NewRaftReplicator("node-1", tmpPath, "127.0.0.1:2001", []string{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to bind")
	}
	assert.Nil(t, err)
	defer r1.Shutdown()
	waitState(r1, raft.Leader)

	r2, err := NewRaftReplicator("node-2", tmpPath, "127.0.0.1:2002", []string{})
	assert.Nil(t, err)
	defer r2.Shutdown()
	waitState(r2, raft.Leader)

	err = r1.AddFollower("raft:node-2://127.0.0.1:2002/")
	assert.Nil(t, err)
	waitState(r2, raft.Follower)
}

func TestRafReplicatorCanJoinFromBootstrap(t *testing.T) {
	setup(t)
	defer cleanup(t)

	r1, err := NewRaftReplicator("node-1", tmpPath, "127.0.0.1:2001", []string{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to bind")
	}
	assert.Nil(t, err)
	defer r1.Shutdown()
	waitState(r1, raft.Leader)

	r2, err := NewRaftReplicator("node-2", tmpPath, "127.0.0.1:2002", []string{
		"raft:node-1://127.0.0.1:2001",
	})
	assert.Nil(t, err)
	defer r2.Shutdown()
	waitState(r2, raft.Follower)
}
