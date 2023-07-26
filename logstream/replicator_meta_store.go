package logstream

import (
	"context"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type replicatorMetaStore struct {
	nats.KeyValue
}

type replicatorLockInfo struct {
	NodeID    uint64
	Timestamp int64
}

func newReplicatorMetaStore(name string, nc *nats.Conn) (*replicatorMetaStore, error) {
	jsx, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	kv, err := jsx.KeyValue(name)
	if err == nats.ErrBucketNotFound {
		kv, err = jsx.CreateKeyValue(&nats.KeyValueConfig{
			Storage:  nats.FileStorage,
			Bucket:   name,
			Replicas: cfg.Config.ReplicationLog.Replicas,
		})
	}

	if err != nil {
		return nil, err
	}

	return &replicatorMetaStore{KeyValue: kv}, nil
}

func (m *replicatorMetaStore) AcquireLease(name string, duration time.Duration) (bool, error) {
	now := time.Now().UnixMilli()
	info := &replicatorLockInfo{
		NodeID:    cfg.Config.NodeID,
		Timestamp: now,
	}
	payload, err := info.Serialize()
	if err != nil {
		return false, err
	}

	entry, err := m.Get(name)
	if err == nats.ErrKeyNotFound {
		rev := uint64(0)
		rev, err = m.Create(name, payload)
		if rev != 0 && err == nil {
			return true, nil
		}
	}

	if err != nil {
		return false, err
	}

	err = info.DeserializeFrom(entry.Value())
	if err != nil {
		return false, err
	}

	if info.NodeID != cfg.Config.NodeID && info.Timestamp+duration.Milliseconds() > now {
		return false, err
	}

	_, err = m.Update(name, payload, entry.Revision())
	if err != nil {
		return false, err
	}

	return true, nil
}

func (m *replicatorMetaStore) ContextRefreshingLease(
	name string,
	duration time.Duration,
	ctx context.Context,
) (bool, error) {
	locked, err := m.AcquireLease(name, duration)
	go func(locked bool, err error) {
		if !locked || err != nil {
			return
		}

		refresh := time.NewTicker(duration / 2)
		for {
			locked, err = m.AcquireLease(name, duration)
			if err != nil {
				log.Warn().Err(err).Str("name", name).Msg("Error acquiring lease")
				return
			} else if !locked {
				log.Warn().Str("name", name).Msg("Unable to acquire lease")
				continue
			}

			refresh.Reset(duration / 2)
			select {
			case <-refresh.C:
				continue
			case <-ctx.Done():
				return
			}
		}
	}(locked, err)

	return locked, err
}

func (r *replicatorLockInfo) Serialize() ([]byte, error) {
	return cbor.Marshal(r)
}

func (r *replicatorLockInfo) DeserializeFrom(data []byte) error {
	return cbor.Unmarshal(data, r)
}
