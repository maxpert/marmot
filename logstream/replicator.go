package logstream

import (
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/snapshot"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const maxReplicateRetries = 7
const SnapshotShardID = uint64(1)

type Replicator struct {
	nodeID             uint64
	shards             uint64
	compressionEnabled bool

	client    *nats.Conn
	repState  *replicationState
	snapshot  snapshot.NatsSnapshot
	streamMap map[uint64]nats.JetStreamContext
}

func NewReplicator(
	nodeID uint64,
	natsServer string,
	shards uint64,
	compress bool,
	snapshot snapshot.NatsSnapshot,
) (*Replicator, error) {
	nc, err := nats.Connect(natsServer, nats.Name(cfg.Config.NodeName()))

	if err != nil {
		return nil, err
	}

	streamMap := map[uint64]nats.JetStreamContext{}
	for i := uint64(0); i < shards; i++ {
		shard := i + 1
		js, err := nc.JetStream()
		if err != nil {
			return nil, err
		}

		streamCfg := makeShardConfig(shard, shards, compress)
		info, err := js.StreamInfo(streamName(shard, compress))
		if err == nats.ErrStreamNotFound {
			log.Debug().Uint64("shard", shard).Msg("Creating stream")
			info, err = js.AddStream(streamCfg)
		}

		if err != nil {
			return nil, err
		}

		leader := ""
		if info.Cluster != nil {
			leader = info.Cluster.Leader
		}

		log.Debug().
			Uint64("shard", shard).
			Str("name", info.Config.Name).
			Int("replicas", info.Config.Replicas).
			Str("leader", leader).
			Msg("Stream ready...")

		if err != nil {
			return nil, err
		}

		streamMap[shard] = js
	}

	repState := &replicationState{}
	err = repState.init()
	if err != nil {
		return nil, err
	}

	return &Replicator{
		client:             nc,
		nodeID:             nodeID,
		compressionEnabled: compress,

		shards:    shards,
		streamMap: streamMap,
		snapshot:  snapshot,
		repState:  repState,
	}, nil
}

func (r *Replicator) Publish(hash uint64, payload []byte) error {
	shardID := (hash % r.shards) + 1
	js, ok := r.streamMap[shardID]
	if !ok {
		log.Panic().
			Uint64("shard", shardID).
			Msg("Invalid shard")
	}

	if r.compressionEnabled {
		compPayload, err := payloadCompress(payload)
		if err != nil {
			return err
		}

		payload = compPayload
	}

	ack, err := js.Publish(subjectName(shardID), payload)
	if err != nil {
		return err
	}

	if cfg.Config.Snapshot.Enable {
		seq, err := r.repState.save(ack.Stream, ack.Sequence)
		if err != nil {
			return err
		}

		snapshotEntries := uint64(cfg.Config.ReplicationLog.MaxEntries) / r.shards
		if snapshotEntries != 0 && seq%snapshotEntries == 0 && shardID == SnapshotShardID {
			log.Debug().
				Uint64("seq", seq).
				Str("stream", ack.Stream).
				Msg("Initiating save snapshot")
			go r.SaveSnapshot()
		}
	}

	return nil
}

func (r *Replicator) Listen(shardID uint64, callback func(payload []byte) error) error {
	js := r.streamMap[shardID]

	sub, err := js.SubscribeSync(subjectName(shardID))
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	savedSeq := r.repState.get(streamName(shardID, r.compressionEnabled))
	for sub.IsValid() {
		msg, err := sub.NextMsg(5 * time.Second)

		if err == nats.ErrTimeout {
			continue
		}

		if err != nil {
			return err
		}

		meta, err := msg.Metadata()
		if err != nil {
			return err
		}

		if meta.Sequence.Stream <= savedSeq {
			continue
		}

		err = r.invokeListener(callback, msg)
		if err != nil {
			msg.Nak()
			log.Error().Err(err).Msg("Replication failed, terminating...")
			return err
		}

		savedSeq, err = r.repState.save(meta.Stream, meta.Sequence.Stream)
		if err != nil {
			return err
		}

		err = msg.Ack()
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Replicator) RestoreSnapshot() error {
	if r.snapshot == nil {
		return nil
	}

	for shardID, js := range r.streamMap {
		strName := streamName(shardID, r.compressionEnabled)
		info, err := js.StreamInfo(strName)
		if err != nil {
			return err
		}

		savedSeq := r.repState.get(strName)
		if savedSeq < info.State.FirstSeq {
			return r.snapshot.RestoreSnapshot()
		}
	}

	return nil
}

func (r *Replicator) SaveSnapshot() {
	if r.snapshot == nil {
		return
	}

	err := r.snapshot.SaveSnapshot()
	if err != nil {
		log.Error().
			Err(err).
			Msg("Unable snapshot database")
	}
}

func (r *Replicator) invokeListener(callback func(payload []byte) error, msg *nats.Msg) error {
	var err error
	payload := msg.Data

	if r.compressionEnabled {
		payload, err = payloadDecompress(msg.Data)
		if err != nil {
			return err
		}
	}

	for repRetry := 0; repRetry < maxReplicateRetries; repRetry++ {
		// Don't invoke for first iteration
		if repRetry != 0 {
			err = msg.InProgress()
			if err != nil {
				return err
			}
		}

		err = callback(payload)
		if err == nil {
			return nil
		}

		log.Error().
			Err(err).
			Int("attempt", repRetry).
			Msg("Unable to process message retrying")
	}

	return err
}

func makeShardConfig(shardID uint64, totalShards uint64, compressed bool) *nats.StreamConfig {
	streamName := streamName(shardID, compressed)
	replicas := cfg.Config.ReplicationLog.Replicas
	if replicas < 1 {
		replicas = int(totalShards>>1) + 1
	}

	if replicas > 5 {
		replicas = 5
	}

	return &nats.StreamConfig{
		Name:              streamName,
		Subjects:          []string{subjectName(shardID)},
		Discard:           nats.DiscardOld,
		MaxMsgs:           cfg.Config.ReplicationLog.MaxEntries,
		Storage:           nats.FileStorage,
		Retention:         nats.LimitsPolicy,
		AllowDirect:       true,
		MaxConsumers:      -1,
		MaxMsgsPerSubject: -1,
		Duplicates:        0,
		DenyDelete:        true,
		Replicas:          replicas,
	}
}

func streamName(shardID uint64, compressed bool) string {
	compPostfix := ""
	if compressed {
		compPostfix = "-c"
	}

	return fmt.Sprintf("%s%s-%d", cfg.Config.NATS.StreamPrefix, compPostfix, shardID)
}

func subjectName(shardID uint64) string {
	return fmt.Sprintf("%s-%d", cfg.Config.NATS.SubjectPrefix, shardID)
}

func payloadCompress(payload []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}

	return enc.EncodeAll(payload, nil), nil
}

func payloadDecompress(payload []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	return dec.DecodeAll(payload, nil)
}
