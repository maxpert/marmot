package sink

import "sync"

// MockSink is a mock implementation of Sink for testing
type MockSink struct {
	Messages   []MockMessage
	PublishErr error
	mu         sync.Mutex
}

// MockMessage represents a published message for testing
type MockMessage struct {
	Topic string
	Key   string
	Value []byte
}

// Publish records a message for later inspection in tests
func (m *MockSink) Publish(topic, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.PublishErr != nil {
		return m.PublishErr
	}

	m.Messages = append(m.Messages, MockMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	})

	return nil
}

// Close is a no-op for MockSink
func (m *MockSink) Close() error {
	return nil
}

// Reset clears all recorded messages
func (m *MockSink) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Messages = nil
}
