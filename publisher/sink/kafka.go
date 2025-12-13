package sink

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/publisher"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultKafkaBatchSize  = 100
	DefaultKafkaBatchBytes = 1 << 20 // 1MB
)

func init() {
	// Register kafka sink factory
	publisher.RegisterSink("kafka", func(config cfg.SinkConfiguration) (publisher.Sink, error) {
		kafkaConfig := KafkaConfig{
			Brokers:          config.Brokers,
			BatchSize:        config.BatchSize,
			BatchBytes:       DefaultKafkaBatchBytes,
			RequiredAcks:     -1,   // RequireAll for durability
			AutoCreateTopics: true, // Auto-create topics by default
		}
		return NewKafkaSink(kafkaConfig)
	})
}

// KafkaSink implements the Sink interface for Kafka publishing
type KafkaSink struct {
	writer *kafka.Writer
}

// KafkaConfig holds configuration for KafkaSink
type KafkaConfig struct {
	Brokers          []string           // Kafka broker addresses
	BatchSize        int                // Batch size for async writes (default: 100)
	BatchBytes       int64              // Max batch bytes (default: 1MB)
	RequiredAcks     kafka.RequiredAcks // Ack requirement (default: RequireAll)
	AutoCreateTopics bool               // Auto-create topics if they don't exist (default: true)
}

// DefaultKafkaConfig returns a KafkaConfig with sensible defaults
func DefaultKafkaConfig(brokers []string) KafkaConfig {
	return KafkaConfig{
		Brokers:          brokers,
		BatchSize:        DefaultKafkaBatchSize,
		BatchBytes:       DefaultKafkaBatchBytes,
		RequiredAcks:     kafka.RequireAll,
		AutoCreateTopics: true,
	}
}

// NewKafkaSink creates a new KafkaSink with the given configuration
func NewKafkaSink(config KafkaConfig) (*KafkaSink, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink requires at least one broker address")
	}

	// Set defaults if not provided
	if config.BatchSize == 0 {
		config.BatchSize = DefaultKafkaBatchSize
	}
	if config.BatchBytes == 0 {
		config.BatchBytes = DefaultKafkaBatchBytes
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(config.Brokers...),
		Balancer:               &kafka.Hash{}, // Partition by key for consistent routing
		BatchSize:              config.BatchSize,
		BatchBytes:             config.BatchBytes,
		RequiredAcks:           config.RequiredAcks,
		Async:                  false, // Sync writes for durability
		AllowAutoTopicCreation: config.AutoCreateTopics,
	}

	return &KafkaSink{writer: writer}, nil
}

// Publish sends a message to Kafka
// topic: Kafka topic name
// key: Partition key (same key → same partition)
// value: Message payload (nil for tombstones)
//
// Note: Uses context.Background() because the publisher worker manages timeouts
// and retries at a higher level. The worker's retry logic ensures messages
// are eventually published or the worker shuts down gracefully.
func (k *KafkaSink) Publish(topic, key string, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value, // nil value = tombstone (DELETE marker)
	}

	return k.writer.WriteMessages(context.Background(), msg)
}

// Close releases resources held by the KafkaSink
func (k *KafkaSink) Close() error {
	if k.writer == nil {
		return nil
	}
	return k.writer.Close()
}
