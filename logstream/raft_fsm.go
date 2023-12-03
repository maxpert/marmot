package logstream

import (
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"io"
)

type raftFSM struct {
}

func (r *raftFSM) Apply(cmd *raft.Log) interface{} {
	log.Info().Str("cmd", cmd.Type.String()).Msg("Apply")
	return nil
}

func (r *raftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (r *raftFSM) Restore(snapshot io.ReadCloser) error {
	return nil
}
