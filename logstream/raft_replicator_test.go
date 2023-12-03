package logstream

import (
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"testing"
	"time"
)

func TestNatsReplicatorCanBootStandalone(t *testing.T) {
	tmpPath := "/tmp"
	r1, err := NewRaftReplicator("node-1", tmpPath, "127.0.0.1:2001", []string{})
	if err != nil {
		t.Failed()
		log.Error().Err(err).Msg("unable to boot cluster")
		return
	}

	for {
		if r1.raft.State() == raft.Leader {
			break
		}

		time.Sleep(1 * time.Second)
	}
}
