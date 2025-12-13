package publisher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Mock implementations for testing

type mockSink struct {
	mu        sync.Mutex
	events    []mockPublishCall
	failCount atomic.Int32 // Number of times to fail before succeeding
}

type mockPublishCall struct {
	topic string
	key   string
	value []byte
}

func (m *mockSink) Publish(topic, key string, value []byte) error {
	// Check if we should fail
	if m.failCount.Load() > 0 {
		m.failCount.Add(-1)
		return fmt.Errorf("mock publish failure")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, mockPublishCall{
		topic: topic,
		key:   key,
		value: value,
	})
	return nil
}

func (m *mockSink) Close() error {
	return nil
}

func (m *mockSink) getEvents() []mockPublishCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]mockPublishCall, len(m.events))
	copy(result, m.events)
	return result
}

func (m *mockSink) eventCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

type mockTransformer struct{}

func (m *mockTransformer) Transform(event CDCEvent, schema TableSchema) ([]byte, error) {
	return []byte(fmt.Sprintf("transformed:%s:%s:%d", event.Database, event.Table, event.SeqNum)), nil
}

func (m *mockTransformer) Tombstone(key string) []byte {
	return []byte(fmt.Sprintf("tombstone:%s", key))
}

type mockFilter struct {
	allowedTables map[string]bool
}

func (m *mockFilter) Match(database, table string) bool {
	if m.allowedTables == nil {
		return true // Allow all by default
	}
	return m.allowedTables[database+"."+table]
}

type mockSchemaProvider struct{}

func (m *mockSchemaProvider) GetSchema(database, table string) (TableSchema, error) {
	return TableSchema{
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER", IsPK: true},
			{Name: "name", Type: "TEXT"},
		},
	}, nil
}

// Test NewWorker validation
func TestNewWorker_Validation(t *testing.T) {
	tests := []struct {
		name        string
		config      WorkerConfig
		expectError bool
	}{
		{
			name:        "missing name",
			config:      WorkerConfig{},
			expectError: true,
		},
		{
			name: "missing log",
			config: WorkerConfig{
				Name: "test",
			},
			expectError: true,
		},
		{
			name: "missing sink",
			config: WorkerConfig{
				Name: "test",
				Log:  &PublishLog{},
			},
			expectError: true,
		},
		{
			name: "missing transformer",
			config: WorkerConfig{
				Name: "test",
				Log:  &PublishLog{},
				Sink: &mockSink{},
			},
			expectError: true,
		},
		{
			name: "missing filter",
			config: WorkerConfig{
				Name:        "test",
				Log:         &PublishLog{},
				Sink:        &mockSink{},
				Transformer: &mockTransformer{},
			},
			expectError: true,
		},
		{
			name: "missing schema provider",
			config: WorkerConfig{
				Name:        "test",
				Log:         &PublishLog{},
				Sink:        &mockSink{},
				Transformer: &mockTransformer{},
				Filter:      &mockFilter{},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWorker(tt.config)
			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test normal event processing
func TestWorker_NormalProcessing(t *testing.T) {
	// Create publish log
	pubLog, cleanup := createTestPublishLog(t)
	defer cleanup()

	// Add test events
	events := []CDCEvent{
		{
			TxnID:     1,
			Database:  "testdb",
			Table:     "users",
			Operation: OpInsert,
			IntentKey: "user:1",
			After:     map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
			CommitTS:  time.Now().UnixMilli(),
			NodeID:    100,
		},
		{
			TxnID:     2,
			Database:  "testdb",
			Table:     "users",
			Operation: OpUpdate,
			IntentKey: "user:1",
			Before:    map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
			After:     map[string][]byte{"id": []byte("1"), "name": []byte("Bob")},
			CommitTS:  time.Now().UnixMilli(),
			NodeID:    100,
		},
	}

	if err := pubLog.Append(events); err != nil {
		t.Fatalf("failed to append events: %v", err)
	}

	// Create worker
	sink := &mockSink{}
	config := WorkerConfig{
		Name:            "test-worker",
		Log:             pubLog,
		Sink:            sink,
		Transformer:     &mockTransformer{},
		Filter:          &mockFilter{},
		SchemaProvider:  &mockSchemaProvider{},
		TopicPrefix:     "marmot.cdc",
		BatchSize:       10,
		PollInterval:    10 * time.Millisecond,
		RetryInitial:    10 * time.Millisecond,
		RetryMax:        100 * time.Millisecond,
		RetryMultiplier: 2.0,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	// Start worker
	worker.Start()
	defer worker.Stop()

	// Wait for processing
	waitForEvents(t, sink, 2, 2*time.Second)

	// Verify events were published
	published := sink.getEvents()
	if len(published) != 2 {
		t.Fatalf("expected 2 published events, got %d", len(published))
	}

	// Check first event
	if published[0].topic != "marmot.cdc.testdb.users" {
		t.Errorf("expected topic 'marmot.cdc.testdb.users', got '%s'", published[0].topic)
	}
	if published[0].key != "user:1" {
		t.Errorf("expected key 'user:1', got '%s'", published[0].key)
	}

	// Verify cursor was advanced
	cursor, err := pubLog.GetCursor("test-worker")
	if err != nil {
		t.Fatalf("failed to get cursor: %v", err)
	}
	if cursor != 2 {
		t.Errorf("expected cursor 2, got %d", cursor)
	}
}

// Test filter skipping
func TestWorker_FilterSkipping(t *testing.T) {
	// Create publish log
	pubLog, cleanup := createTestPublishLog(t)
	defer cleanup()

	// Add test events
	events := []CDCEvent{
		{
			TxnID:     1,
			Database:  "testdb",
			Table:     "users",
			Operation: OpInsert,
			IntentKey: "user:1",
			After:     map[string][]byte{"id": []byte("1")},
			CommitTS:  time.Now().UnixMilli(),
			NodeID:    100,
		},
		{
			TxnID:     2,
			Database:  "testdb",
			Table:     "products", // This will be filtered
			Operation: OpInsert,
			IntentKey: "product:1",
			After:     map[string][]byte{"id": []byte("1")},
			CommitTS:  time.Now().UnixMilli(),
			NodeID:    100,
		},
		{
			TxnID:     3,
			Database:  "testdb",
			Table:     "users",
			Operation: OpInsert,
			IntentKey: "user:2",
			After:     map[string][]byte{"id": []byte("2")},
			CommitTS:  time.Now().UnixMilli(),
			NodeID:    100,
		},
	}

	if err := pubLog.Append(events); err != nil {
		t.Fatalf("failed to append events: %v", err)
	}

	// Create worker with filter
	sink := &mockSink{}
	filter := &mockFilter{
		allowedTables: map[string]bool{
			"testdb.users": true,
			// products not in allowlist
		},
	}

	config := WorkerConfig{
		Name:            "test-worker",
		Log:             pubLog,
		Sink:            sink,
		Transformer:     &mockTransformer{},
		Filter:          filter,
		SchemaProvider:  &mockSchemaProvider{},
		BatchSize:       10,
		PollInterval:    10 * time.Millisecond,
		RetryInitial:    10 * time.Millisecond,
		RetryMax:        100 * time.Millisecond,
		RetryMultiplier: 2.0,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	// Start worker
	worker.Start()
	defer worker.Stop()

	// Wait for processing
	waitForEvents(t, sink, 2, 2*time.Second)

	// Verify only 2 events were published (products filtered)
	published := sink.getEvents()
	if len(published) != 2 {
		t.Fatalf("expected 2 published events, got %d", len(published))
	}

	// Verify cursor was advanced to 3 (all events processed, even filtered ones)
	cursor, err := pubLog.GetCursor("test-worker")
	if err != nil {
		t.Fatalf("failed to get cursor: %v", err)
	}
	if cursor != 3 {
		t.Errorf("expected cursor 3, got %d", cursor)
	}
}

// Test retry on failure
func TestWorker_RetryOnFailure(t *testing.T) {
	// Create publish log
	pubLog, cleanup := createTestPublishLog(t)
	defer cleanup()

	// Add test event
	events := []CDCEvent{
		{
			TxnID:     1,
			Database:  "testdb",
			Table:     "users",
			Operation: OpInsert,
			IntentKey: "user:1",
			After:     map[string][]byte{"id": []byte("1")},
			CommitTS:  time.Now().UnixMilli(),
			NodeID:    100,
		},
	}

	if err := pubLog.Append(events); err != nil {
		t.Fatalf("failed to append events: %v", err)
	}

	// Create worker with failing sink
	sink := &mockSink{}
	sink.failCount.Store(2) // Fail twice, then succeed

	config := WorkerConfig{
		Name:            "test-worker",
		Log:             pubLog,
		Sink:            sink,
		Transformer:     &mockTransformer{},
		Filter:          &mockFilter{},
		SchemaProvider:  &mockSchemaProvider{},
		BatchSize:       10,
		PollInterval:    10 * time.Millisecond,
		RetryInitial:    10 * time.Millisecond,
		RetryMax:        100 * time.Millisecond,
		RetryMultiplier: 2.0,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	// Start worker
	worker.Start()
	defer worker.Stop()

	// Wait for processing (should retry and eventually succeed)
	waitForEvents(t, sink, 1, 2*time.Second)

	// Verify event was published
	published := sink.getEvents()
	if len(published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(published))
	}
}

// Test graceful shutdown
func TestWorker_GracefulShutdown(t *testing.T) {
	// Create publish log
	pubLog, cleanup := createTestPublishLog(t)
	defer cleanup()

	// Create worker
	sink := &mockSink{}
	config := WorkerConfig{
		Name:            "test-worker",
		Log:             pubLog,
		Sink:            sink,
		Transformer:     &mockTransformer{},
		Filter:          &mockFilter{},
		SchemaProvider:  &mockSchemaProvider{},
		BatchSize:       10,
		PollInterval:    50 * time.Millisecond,
		RetryInitial:    10 * time.Millisecond,
		RetryMax:        100 * time.Millisecond,
		RetryMultiplier: 2.0,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	// Start worker
	worker.Start()

	// Verify running
	if !worker.running.Load() {
		t.Error("worker should be running")
	}

	// Stop worker
	done := make(chan struct{})
	go func() {
		worker.Stop()
		close(done)
	}()

	// Wait for shutdown with timeout
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop within timeout")
	}

	// Verify not running
	if worker.running.Load() {
		t.Error("worker should not be running")
	}
}

// Test DELETE with tombstone
func TestWorker_DeleteWithTombstone(t *testing.T) {
	// Create publish log
	pubLog, cleanup := createTestPublishLog(t)
	defer cleanup()

	// Add DELETE event
	events := []CDCEvent{
		{
			TxnID:     1,
			Database:  "testdb",
			Table:     "users",
			Operation: OpDelete,
			IntentKey: "user:1",
			Before:    map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
			CommitTS:  time.Now().UnixMilli(),
			NodeID:    100,
		},
	}

	if err := pubLog.Append(events); err != nil {
		t.Fatalf("failed to append events: %v", err)
	}

	// Create worker
	sink := &mockSink{}
	config := WorkerConfig{
		Name:            "test-worker",
		Log:             pubLog,
		Sink:            sink,
		Transformer:     &mockTransformer{},
		Filter:          &mockFilter{},
		SchemaProvider:  &mockSchemaProvider{},
		TopicPrefix:     "marmot.cdc",
		BatchSize:       10,
		PollInterval:    10 * time.Millisecond,
		RetryInitial:    10 * time.Millisecond,
		RetryMax:        100 * time.Millisecond,
		RetryMultiplier: 2.0,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	// Start worker
	worker.Start()
	defer worker.Stop()

	// Wait for processing (DELETE sends event + tombstone = 2 messages)
	waitForEvents(t, sink, 2, 2*time.Second)

	// Verify both event and tombstone were published
	published := sink.getEvents()
	if len(published) != 2 {
		t.Fatalf("expected 2 published events (event + tombstone), got %d", len(published))
	}

	// First should be the delete event
	if published[0].topic != "marmot.cdc.testdb.users" {
		t.Errorf("expected topic 'marmot.cdc.testdb.users', got '%s'", published[0].topic)
	}
	if published[0].key != "user:1" {
		t.Errorf("expected key 'user:1', got '%s'", published[0].key)
	}

	// Second should be tombstone
	if published[1].topic != "marmot.cdc.testdb.users" {
		t.Errorf("expected topic 'marmot.cdc.testdb.users', got '%s'", published[1].topic)
	}
	if published[1].key != "user:1" {
		t.Errorf("expected key 'user:1', got '%s'", published[1].key)
	}
	if string(published[1].value) != "tombstone:user:1" {
		t.Errorf("expected tombstone value, got '%s'", string(published[1].value))
	}
}

// Helper functions

func createTestPublishLog(t *testing.T) (*PublishLog, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	pubLog, err := NewPublishLog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create publish log: %v", err)
	}
	return pubLog, func() {
		pubLog.Close()
	}
}

func waitForEvents(t *testing.T, sink *mockSink, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sink.eventCount() >= expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d events, got %d", expected, sink.eventCount())
}
