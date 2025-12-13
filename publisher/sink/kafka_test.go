package sink

import (
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestDefaultKafkaConfig(t *testing.T) {
	brokers := []string{"localhost:9092", "localhost:9093"}
	config := DefaultKafkaConfig(brokers)

	if len(config.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(config.Brokers))
	}

	if config.Brokers[0] != "localhost:9092" {
		t.Errorf("expected first broker localhost:9092, got %s", config.Brokers[0])
	}

	if config.BatchSize != 100 {
		t.Errorf("expected batch size 100, got %d", config.BatchSize)
	}

	if config.BatchBytes != 1048576 {
		t.Errorf("expected batch bytes 1048576, got %d", config.BatchBytes)
	}

	if config.RequiredAcks != kafka.RequireAll {
		t.Errorf("expected RequireAll acks, got %v", config.RequiredAcks)
	}
}

func TestNewKafkaSink(t *testing.T) {
	config := KafkaConfig{
		Brokers:      []string{"localhost:9092"},
		BatchSize:    50,
		BatchBytes:   2048,
		RequiredAcks: kafka.RequireOne,
	}

	sink, err := NewKafkaSink(config)
	if err != nil {
		t.Fatalf("unexpected error creating sink: %v", err)
	}

	if sink == nil {
		t.Fatal("expected non-nil sink")
	}

	if sink.writer == nil {
		t.Fatal("expected non-nil writer")
	}

	// Verify writer configuration
	if sink.writer.BatchSize != 50 {
		t.Errorf("expected batch size 50, got %d", sink.writer.BatchSize)
	}

	if sink.writer.BatchBytes != 2048 {
		t.Errorf("expected batch bytes 2048, got %d", sink.writer.BatchBytes)
	}

	if sink.writer.RequiredAcks != kafka.RequireOne {
		t.Errorf("expected RequireOne acks, got %v", sink.writer.RequiredAcks)
	}

	if sink.writer.Async {
		t.Error("expected Async to be false for durability")
	}

	// Clean up
	sink.Close()
}

func TestNewKafkaSinkEmptyBrokers(t *testing.T) {
	config := KafkaConfig{
		Brokers: []string{},
	}
	_, err := NewKafkaSink(config)
	if err == nil {
		t.Error("expected error for empty brokers, got nil")
	}
}

func TestKafkaSink_Close(t *testing.T) {
	config := DefaultKafkaConfig([]string{"localhost:9092"})
	sink, err := NewKafkaSink(config)
	if err != nil {
		t.Fatalf("unexpected error creating sink: %v", err)
	}

	err = sink.Close()
	if err != nil {
		t.Errorf("unexpected error closing sink: %v", err)
	}
}

func TestMockSink_Publish(t *testing.T) {
	mock := &MockSink{}

	err := mock.Publish("test-topic", "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(mock.Messages))
	}

	msg := mock.Messages[0]
	if msg.Topic != "test-topic" {
		t.Errorf("expected topic test-topic, got %s", msg.Topic)
	}

	if msg.Key != "key1" {
		t.Errorf("expected key key1, got %s", msg.Key)
	}

	if string(msg.Value) != "value1" {
		t.Errorf("expected value value1, got %s", string(msg.Value))
	}
}

func TestMockSink_PublishTombstone(t *testing.T) {
	mock := &MockSink{}

	err := mock.Publish("test-topic", "key1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(mock.Messages))
	}

	msg := mock.Messages[0]
	if msg.Value != nil {
		t.Errorf("expected nil value for tombstone, got %v", msg.Value)
	}
}

func TestMockSink_PublishError(t *testing.T) {
	expectedErr := errors.New("publish failed")
	mock := &MockSink{
		PublishErr: expectedErr,
	}

	err := mock.Publish("test-topic", "key1", []byte("value1"))
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if len(mock.Messages) != 0 {
		t.Errorf("expected 0 messages on error, got %d", len(mock.Messages))
	}
}

func TestMockSink_Reset(t *testing.T) {
	mock := &MockSink{}

	mock.Publish("topic1", "key1", []byte("value1"))
	mock.Publish("topic2", "key2", []byte("value2"))

	if len(mock.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(mock.Messages))
	}

	mock.Reset()

	if len(mock.Messages) != 0 {
		t.Errorf("expected 0 messages after reset, got %d", len(mock.Messages))
	}
}

func TestMockSink_Close(t *testing.T) {
	mock := &MockSink{}

	err := mock.Close()
	if err != nil {
		t.Errorf("unexpected error closing mock: %v", err)
	}
}

func TestMockSink_Concurrent(t *testing.T) {
	mock := &MockSink{}

	// Test concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			mock.Publish("topic", "key", []byte("value"))
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if len(mock.Messages) != 10 {
		t.Errorf("expected 10 messages, got %d", len(mock.Messages))
	}
}
