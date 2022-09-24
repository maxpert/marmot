package lib

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const DefaultUrl = nats.DefaultURL
const NodeNamePrefix = "marmot-node"

var MaxLogEntries = int64(1024)
var StreamNamePrefix = "marmot-changes"
var SubjectPrefix = "change-event"

type Replicator struct {
	nodeID uint64
	shards uint64

	client    *nats.Conn
	streamMap map[uint64]nats.JetStream
}

func NewReplicator(nodeID uint64, natsServer string, shards uint64) (*Replicator, error) {
	nc, err := nats.Connect(natsServer, nats.Name(nodeName(nodeID)))

	if err != nil {
		return nil, err
	}

	streamMap := map[uint64]nats.JetStream{}
	for i := uint64(0); i < shards; i++ {
		js, err := nc.JetStream()
		if err != nil {
			return nil, err
		}

		streamCfg := makeConfig(i+1, shards)
		_, err = js.StreamInfo(streamCfg.Name)
		if err != nil {
			_, err = js.AddStream(streamCfg)
		} else {
			_, err = js.UpdateStream(streamCfg)
		}

		if err != nil {
			return nil, err
		}

		log.Info().
			Uint64("id", i).
			Msg("Created stream")

		streamMap[i+1] = js
	}

	return &Replicator{
		client: nc,
		nodeID: nodeID,

		shards:    shards,
		streamMap: streamMap,
	}, nil
}

func (r *Replicator) Publish(hash uint64, payload []byte) error {
	shardID := (hash % r.shards) + 1
	js, ok := r.streamMap[shardID]
	if !ok {
		log.Panic().Uint64("shard", shardID).Msg("Invalid shard")
	}

	ack, err := js.Publish(topicName(shardID), payload)
	if err != nil {
		return err
	}

	log.Debug().Uint64("seq", ack.Sequence).Msg(ack.Stream)
	return nil
}

func (r *Replicator) Listen(shardID uint64, callback func(payload []byte) error) error {
	js := r.streamMap[shardID]

	sub, err := js.SubscribeSync(
		topicName(shardID),
		nats.Durable(nodeName(r.nodeID)),
		nats.ManualAck(),
	)

	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	for sub.IsValid() {
		msg, err := sub.NextMsg(1 * time.Second)

		if err == nats.ErrTimeout {
			continue
		}

		if err != nil {
			return err
		}

		log.Debug().Str("sub", msg.Subject).Uint64("shard", shardID).Send()
		err = callback(msg.Data)
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

func nodeName(nodeID uint64) string {
	return fmt.Sprintf("%s-%d", NodeNamePrefix, nodeID)
}

func topicName(shardID uint64) string {
	return fmt.Sprintf("%s.%d", SubjectPrefix, shardID)
}

func makeConfig(shardID uint64, totalShards uint64) *nats.StreamConfig {
	streamName := fmt.Sprintf("%s-%d-%d", StreamNamePrefix, totalShards, shardID)
	return &nats.StreamConfig{
		Name:              streamName,
		Subjects:          []string{topicName(shardID)},
		Discard:           nats.DiscardOld,
		MaxMsgs:           MaxLogEntries,
		Storage:           nats.FileStorage,
		Retention:         nats.LimitsPolicy,
		AllowDirect:       true,
		MaxConsumers:      -1,
		MaxMsgsPerSubject: -1,
		Duplicates:        0,
		DenyDelete:        true,
		Replicas:          int(totalShards>>1 + 1),
	}
}
