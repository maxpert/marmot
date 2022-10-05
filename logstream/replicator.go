package logstream

import (
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/maxpert/marmot/snapshot"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const maxReplicateRetries = 7
const DefaultUrl = nats.DefaultURL
const NodeNamePrefix = "marmot-node"

var MaxLogEntries = int64(1024)
var EntryReplicas = 1
var SnapshotShardID = uint64(1)
var StreamNamePrefix = "marmot-changes"
var SubjectPrefix = "marmot-change-log"

type Replicator struct {
	nodeID             uint64
	shards             uint64
	compressionEnabled bool

	client    *nats.Conn
	streamMap map[uint64]nats.JetStream
	snapshot  snapshot.NatsSnapshot
}

func NewReplicator(
	nodeID uint64,
	natsServer string,
	shards uint64,
	compress bool,
	snapshot snapshot.NatsSnapshot,
) (*Replicator, error) {
	nc, err := nats.Connect(natsServer, nats.Name(nodeName(nodeID)))

	if err != nil {
		return nil, err
	}

	streamMap := map[uint64]nats.JetStream{}
	for i := uint64(0); i < shards; i++ {
		shard := i + 1
		js, err := nc.JetStream()
		if err != nil {
			return nil, err
		}

		streamCfg := makeShardConfig(shard, shards, compress)
		info, err := js.StreamInfo(streamCfg.Name)
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

	return &Replicator{
		client:             nc,
		nodeID:             nodeID,
		compressionEnabled: compress,

		shards:    shards,
		streamMap: streamMap,
		snapshot:  snapshot,
	}, nil
}

func (r *Replicator) Publish(hash uint64, payload []byte) error {
	shardID := (hash % r.shards) + 1
	js, ok := r.streamMap[shardID]
	if !ok {
		log.Panic().Uint64("shard", shardID).Msg("Invalid shard")
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

	snapshotEntries := uint64(MaxLogEntries) / r.shards
	if snapshotEntries != 0 && ack.Sequence%snapshotEntries == 0 && shardID == SnapshotShardID {
		go r.SaveSnapshot()
	}

	log.Debug().Uint64("seq", ack.Sequence).Msg(ack.Stream)
	return nil
}

func (r *Replicator) Listen(shardID uint64, callback func(payload []byte) error) error {
	js := r.streamMap[shardID]

	sub, err := js.SubscribeSync(
		subjectName(shardID),
		nats.Durable(nodeName(r.nodeID)),
	)

	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	replRetry := 0
	for sub.IsValid() {
		msg, err := sub.NextMsg(1 * time.Second)

		if err == nats.ErrTimeout {
			continue
		}

		if err != nil {
			return err
		}

		payload := msg.Data
		if r.compressionEnabled {
			payload, err = payloadDecompress(msg.Data)
			if err != nil {
				return err
			}
		}

		log.Debug().Str("sub", msg.Subject).Uint64("shard", shardID).Send()
		err = callback(payload)
		if err != nil {
			if replRetry > maxReplicateRetries {
				return err
			}

			log.Error().Err(err).Msg("Unable to process message retrying")
			msg.Nak()
			replRetry++
			continue
		}

		replRetry = 0
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

	return r.snapshot.RestoreSnapshot(r.client)
}

func (r *Replicator) SaveSnapshot() {
	if r.snapshot == nil {
		return
	}

	err := r.snapshot.SaveSnapshot(r.client)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Unable snapshot database")
	}
}

func makeShardConfig(shardID uint64, totalShards uint64, compressed bool) *nats.StreamConfig {
	compPostfix := ""
	if compressed {
		compPostfix = "-c"
	}

	streamName := fmt.Sprintf("%s%s-%d", StreamNamePrefix, compPostfix, shardID)
	replicas := EntryReplicas
	if replicas < 1 {
		replicas = int(totalShards>>1) + 1
	}

	return &nats.StreamConfig{
		Name:              streamName,
		Subjects:          []string{subjectName(shardID)},
		Discard:           nats.DiscardOld,
		MaxMsgs:           MaxLogEntries,
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

func nodeName(nodeID uint64) string {
	return fmt.Sprintf("%s-%d", NodeNamePrefix, nodeID)
}

func subjectName(shardID uint64) string {
	return fmt.Sprintf("%s-%d", SubjectPrefix, shardID)
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
