package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/publisher"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func init() {
	publisher.RegisterSink("nats", func(config cfg.SinkConfiguration) (publisher.Sink, error) {
		if config.NatsURL == "" {
			return nil, fmt.Errorf("nats sink requires nats_url")
		}
		return NewNatsSink(config.NatsURL)
	})
}

// NatsSink implements the Sink interface for NATS JetStream publishing
type NatsSink struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// NewNatsSink creates a new NATS JetStream sink
func NewNatsSink(url string) (*NatsSink, error) {
	nc, err := nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &NatsSink{nc: nc, js: js}, nil
}

// Publish sends a message to NATS JetStream
// topic: JetStream subject (e.g., "cdc.app.todos")
// key: Message key (stored as header for routing)
// value: Message payload (nil for tombstones)
func (n *NatsSink) Publish(topic, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ensure stream exists for this topic prefix
	streamName := sanitizeStreamName(topic)
	_, err := n.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{topic},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("failed to ensure stream %s: %w", streamName, err)
	}

	// Publish message with key as header
	msg := &nats.Msg{
		Subject: topic,
		Data:    value,
		Header:  nats.Header{"key": []string{key}},
	}

	_, err = n.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", topic, err)
	}

	return nil
}

// Close releases resources held by the NatsSink
func (n *NatsSink) Close() error {
	if n.nc != nil {
		n.nc.Close()
	}
	return nil
}

// sanitizeStreamName converts a topic to a valid JetStream stream name
// JetStream stream names can't contain "." so we replace with "_"
func sanitizeStreamName(topic string) string {
	result := make([]byte, len(topic))
	for i, c := range topic {
		if c == '.' {
			result[i] = '_'
		} else {
			result[i] = byte(c)
		}
	}
	return string(result)
}
