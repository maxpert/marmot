package logstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/marmot/stream"

	"github.com/klauspost/compress/zstd"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/snapshot"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const maxReplicateRetries = 7
const SnapshotShardID = uint64(1)

var SnapshotLeaseTTL = 10 * time.Second

type Replicator struct {
	nodeID             uint64
	shards             uint64
	compressionEnabled bool
	lastSnapshot       time.Time

	client    *nats.Conn
	repState  *replicationState
	metaStore *replicatorMetaStore
	snapshot  snapshot.NatsSnapshot
	streamMap map[uint64]nats.JetStreamContext
}

func NewReplicator(
	snapshot snapshot.NatsSnapshot,
) (*Replicator, error) {
	nodeID := cfg.Config.NodeID
	shards := cfg.Config.ReplicationLog.Shards
	compress := cfg.Config.ReplicationLog.Compress
	updateExisting := cfg.Config.ReplicationLog.UpdateExisting

	nc, err := stream.Connect()
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

		streamCfg := makeShardStreamConfig(shard, shards, compress)
		info, err := js.StreamInfo(streamName(shard, compress), nats.MaxWait(10*time.Second))
		if err == nats.ErrStreamNotFound {
			log.Debug().Uint64("shard", shard).Msg("Creating stream")
			info, err = js.AddStream(streamCfg)
		}

		if err != nil {
			log.Error().
				Err(err).
				Str("name", streamName(shard, compress)).
				Msg("Unable to get stream info...")
			return nil, err
		}

		if updateExisting && !eqShardStreamConfig(&info.Config, streamCfg) {
			log.Warn().Msgf("Stream configuration not same for %s, updating...", streamName(shard, compress))
			info, err = js.UpdateStream(streamCfg)
			if err != nil {
				log.Error().
					Err(err).
					Str("name", streamName(shard, compress)).
					Msg("Unable update stream info...")
				return nil, err
			}
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

	metaStore, err := newReplicatorMetaStore(cfg.EmbeddedClusterName, nc)
	if err != nil {
		return nil, err
	}

	return &Replicator{
		client:             nc,
		nodeID:             nodeID,
		compressionEnabled: compress,
		lastSnapshot:       time.Time{},

		shards:    shards,
		streamMap: streamMap,
		snapshot:  snapshot,
		repState:  repState,
		metaStore: metaStore,
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
		if errors.Is(err, nats.ErrTimeout) {
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
			if errors.Is(err, context.Canceled) {
				return nil
			}

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

func (r *Replicator) LastSaveSnapshotTime() time.Time {
	return r.lastSnapshot
}

func (r *Replicator) SaveSnapshot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	locked, err := r.metaStore.ContextRefreshingLease("snapshot", SnapshotLeaseTTL, ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Error acquiring snapshot lock")
		return
	}

	if !locked {
		log.Info().Msg("Snapshot saving already locked, skipping")
		return
	}

	r.ForceSaveSnapshot()
}

func (r *Replicator) ForceSaveSnapshot() {
	if r.snapshot == nil {
		return
	}

	err := r.snapshot.SaveSnapshot()
	if err != nil {
		log.Error().
			Err(err).
			Msg("Unable snapshot database")
		return
	}

	r.lastSnapshot = time.Now()
}

func (r *Replicator) ReloadCertificates() error {
	if cfg.Config.NATS.CAFile != "" {
		err := nats.RootCAs(cfg.Config.NATS.CAFile)(&r.client.Opts)
		if err != nil {
			return err
		}
	}

	if cfg.Config.NATS.CertFile != "" && cfg.Config.NATS.KeyFile != "" {
		err := nats.ClientCert(cfg.Config.NATS.CertFile, cfg.Config.NATS.KeyFile)(&r.client.Opts)
		if err != nil {
			return err
		}
	}

	return nil
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
		if err == context.Canceled {
			return err
		}

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

func makeShardStreamConfig(shardID uint64, totalShards uint64, compressed bool) *nats.StreamConfig {
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

func eqShardStreamConfig(a *nats.StreamConfig, b *nats.StreamConfig) bool {
	return a.Name == b.Name &&
		len(a.Subjects) == 1 &&
		len(b.Subjects) == 1 &&
		a.Subjects[0] == b.Subjects[0] &&
		a.Discard == b.Discard &&
		a.MaxMsgs == b.MaxMsgs &&
		a.Storage == b.Storage &&
		a.Retention == b.Retention &&
		a.AllowDirect == b.AllowDirect &&
		a.MaxConsumers == b.MaxConsumers &&
		a.MaxMsgsPerSubject == b.MaxMsgsPerSubject &&
		a.Duplicates == b.Duplicates &&
		a.DenyDelete == b.DenyDelete &&
		a.Replicas == b.Replicas
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
