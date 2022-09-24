package lib

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const DefaultUrl = nats.DefaultURL
const MaxLogEntries = int64(1024)
const clusterPrefix = "marmot.cluster."

type Replicator struct {
	nodeID uint64
	shards uint64

	client *nats.Conn
	js     nats.JetStream
}

func NewReplicator(nodeID uint64, natsServer string, shards uint64) (*Replicator, error) {
	nc, err := nats.Connect(natsServer, nats.Name(fmt.Sprintf("node-%d", nodeID)))
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	streamCfg := &nats.StreamConfig{
		Name:         "marmot",
		Subjects:     []string{clusterPrefix + "*"},
		Discard:      nats.DiscardOld,
		MaxMsgs:      MaxLogEntries,
		Storage:      nats.FileStorage,
		Retention:    nats.InterestPolicy,
		AllowDirect:  true,
		MaxConsumers: -1,
		Duplicates:   0,
		DenyDelete:   true,
		Replicas:     2,
	}

	info, err := js.StreamInfo("marmot")
	if err != nil {
		info, err = js.AddStream(streamCfg)
	} else {
		info, err = js.UpdateStream(streamCfg)
	}

	log.Info().Msg(fmt.Sprintf("Stream info %v", info))
	return &Replicator{
		client: nc,
		nodeID: nodeID,
		js:     js,
		shards: shards,
	}, nil
}

func (r *Replicator) Publish(hash uint64, payload []byte) error {
	shard := hash % r.shards
	ack, err := r.js.Publish(fmt.Sprintf("%s%d", clusterPrefix, shard), payload)
	if err != nil {
		return err
	}

	log.Debug().Uint64("seq", ack.Sequence).Msg(ack.Stream)
	return nil
}

func (r *Replicator) Listen(callback func(payload []byte) error) error {
	sub, err := r.js.SubscribeSync(clusterPrefix + "*")
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

		log.Debug().Str("sub", msg.Subject).Msg("Msg...")

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
