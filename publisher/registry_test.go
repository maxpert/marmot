package publisher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Register mock kafka sink for tests
	// This avoids import cycle with sink package
	RegisterSink("kafka", func(config cfg.SinkConfiguration) (Sink, error) {
		return &mockRegistrySink{}, nil
	})

	// Register mock debezium transformer for tests
	// This avoids import cycle with transformer package
	RegisterTransformer("debezium", func() Transformer {
		return &mockDebeziumTransformer{}
	})
}

// mockRegistrySink is a simple mock for registry testing
type mockRegistrySink struct{}

func (m *mockRegistrySink) Publish(topic, key string, value []byte) error {
	return nil
}

func (m *mockRegistrySink) Close() error {
	return nil
}

// mockDebeziumTransformer is a simple mock for testing
type mockDebeziumTransformer struct{}

func (m *mockDebeziumTransformer) Transform(event CDCEvent, schema TableSchema) ([]byte, error) {
	return []byte(`{"mock":"data"}`), nil
}

func (m *mockDebeziumTransformer) Tombstone(key string) []byte {
	return nil
}

// mockDatabaseManager implements DatabaseManager for testing
type mockDatabaseManager struct {
	schemas map[string]TableSchema
}

func newMockDatabaseManager() *mockDatabaseManager {
	return &mockDatabaseManager{
		schemas: make(map[string]TableSchema),
	}
}

func (m *mockDatabaseManager) GetTableSchema(database, table string) (TableSchema, error) {
	key := database + "." + table
	if schema, exists := m.schemas[key]; exists {
		return schema, nil
	}
	// Return default schema
	return TableSchema{
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: true, IsPK: false},
		},
	}, nil
}

func (m *mockDatabaseManager) addSchema(database, table string, schema TableSchema) {
	key := database + "." + table
	m.schemas[key] = schema
}

func TestNewRegistry(t *testing.T) {
	tempDir := t.TempDir()

	dbManager := newMockDatabaseManager()

	t.Run("creates registry successfully", func(t *testing.T) {
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		require.NotNil(t, registry)
		assert.NotNil(t, registry.log)
		assert.Empty(t, registry.workers)

		registry.Stop()
	})

	t.Run("requires data directory", func(t *testing.T) {
		config := RegistryConfig{
			DataDir:     "",
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		assert.Error(t, err)
		assert.Nil(t, registry)
		assert.Contains(t, err.Error(), "data directory is required")
	})

	t.Run("requires database manager", func(t *testing.T) {
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   nil,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		assert.Error(t, err)
		assert.Nil(t, registry)
		assert.Contains(t, err.Error(), "database manager is required")
	})

	t.Run("creates publish log directory", func(t *testing.T) {
		dataDir := filepath.Join(tempDir, "test_registry")
		config := RegistryConfig{
			DataDir:     dataDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		pubLogPath := filepath.Join(dataDir, "publish_log")
		_, err = os.Stat(pubLogPath)
		assert.NoError(t, err, "publish log directory should exist")
	})
}

func TestRegistryAddSink(t *testing.T) {
	dbManager := newMockDatabaseManager()

	t.Run("adds kafka sink successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		sinkConfig := cfg.SinkConfiguration{
			Name:            "test-sink",
			Type:            "kafka",
			Format:          "debezium",
			Brokers:         []string{"localhost:9092"},
			TopicPrefix:     "marmot.cdc",
			FilterTables:    []string{"*"},
			FilterDatabases: []string{"*"},
			BatchSize:       100,
			PollIntervalMS:  10,
			RetryInitialMS:  100,
			RetryMaxMS:      30000,
			RetryMultiplier: 2.0,
		}

		err = registry.AddSink(sinkConfig)
		require.NoError(t, err)
		assert.Len(t, registry.workers, 1)
	})

	t.Run("rejects unknown sink type", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		sinkConfig := cfg.SinkConfiguration{
			Name:   "test-sink",
			Type:   "unknown",
			Format: "debezium",
		}

		err = registry.AddSink(sinkConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown sink type")
	})

	t.Run("rejects unknown format", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		sinkConfig := cfg.SinkConfiguration{
			Name:    "test-sink",
			Type:    "kafka",
			Format:  "unknown",
			Brokers: []string{"localhost:9092"},
		}

		err = registry.AddSink(sinkConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown format")
	})

	t.Run("adds multiple sinks", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		for i := 0; i < 3; i++ {
			sinkConfig := cfg.SinkConfiguration{
				Name:            "test-sink-" + string(rune('1'+i)),
				Type:            "kafka",
				Format:          "debezium",
				Brokers:         []string{"localhost:9092"},
				TopicPrefix:     "marmot.cdc",
				FilterTables:    []string{"*"},
				FilterDatabases: []string{"*"},
				BatchSize:       100,
			}

			err = registry.AddSink(sinkConfig)
			require.NoError(t, err)
		}

		assert.Len(t, registry.workers, 3)
	})
}

func TestRegistryWithSinkConfigs(t *testing.T) {
	tempDir := t.TempDir()
	dbManager := newMockDatabaseManager()

	t.Run("creates workers from initial config", func(t *testing.T) {
		sinkConfigs := []cfg.SinkConfiguration{
			{
				Name:            "sink-1",
				Type:            "kafka",
				Format:          "debezium",
				Brokers:         []string{"localhost:9092"},
				TopicPrefix:     "marmot.cdc",
				FilterTables:    []string{"*"},
				FilterDatabases: []string{"*"},
				BatchSize:       100,
			},
			{
				Name:            "sink-2",
				Type:            "kafka",
				Format:          "debezium",
				Brokers:         []string{"localhost:9092"},
				TopicPrefix:     "marmot.events",
				FilterTables:    []string{"users", "orders"},
				FilterDatabases: []string{"prod"},
				BatchSize:       50,
			},
		}

		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: sinkConfigs,
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		assert.Len(t, registry.workers, 2)
	})

	t.Run("cleanup on init error", func(t *testing.T) {
		sinkConfigs := []cfg.SinkConfiguration{
			{
				Name:    "sink-1",
				Type:    "kafka",
				Format:  "debezium",
				Brokers: []string{"localhost:9092"},
			},
			{
				Name:   "sink-2",
				Type:   "unknown", // This will fail
				Format: "debezium",
			},
		}

		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: sinkConfigs,
		}

		registry, err := NewRegistry(config)
		assert.Error(t, err)
		assert.Nil(t, registry)
	})
}

func TestRegistryLifecycle(t *testing.T) {
	dbManager := newMockDatabaseManager()

	t.Run("start and stop lifecycle", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:   tempDir,
			DBManager: dbManager,
			SinkConfigs: []cfg.SinkConfiguration{
				{
					Name:    "test-sink",
					Type:    "kafka",
					Format:  "debezium",
					Brokers: []string{"localhost:9092"},
				},
			},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)

		// Should not be running initially
		assert.False(t, registry.running.Load())

		// Start
		err = registry.Start()
		require.NoError(t, err)
		assert.True(t, registry.running.Load())

		// Allow workers to start
		time.Sleep(50 * time.Millisecond)

		// Stop
		registry.Stop()
		assert.False(t, registry.running.Load())
	})

	t.Run("prevents double start", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		err = registry.Start()
		require.NoError(t, err)

		err = registry.Start()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already running")
	})

	t.Run("handles stop when not running", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)

		// Should not panic
		assert.NotPanics(t, func() {
			registry.Stop()
		})
	})

	t.Run("stops all workers", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:   tempDir,
			DBManager: dbManager,
			SinkConfigs: []cfg.SinkConfiguration{
				{Name: "sink-1", Type: "kafka", Format: "debezium", Brokers: []string{"localhost:9092"}},
				{Name: "sink-2", Type: "kafka", Format: "debezium", Brokers: []string{"localhost:9092"}},
				{Name: "sink-3", Type: "kafka", Format: "debezium", Brokers: []string{"localhost:9092"}},
			},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)

		err = registry.Start()
		require.NoError(t, err)

		// Allow workers to start
		time.Sleep(50 * time.Millisecond)

		// All workers should be running
		for _, worker := range registry.workers {
			assert.True(t, worker.running.Load())
		}

		registry.Stop()

		// All workers should be stopped
		for _, worker := range registry.workers {
			assert.False(t, worker.running.Load())
		}
	})
}

func TestRegistryAppend(t *testing.T) {
	dbManager := newMockDatabaseManager()

	t.Run("appends events successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		err = registry.Start()
		require.NoError(t, err)

		events := []CDCEvent{
			{
				TxnID:     1,
				Database:  "test",
				Table:     "users",
				Operation: OpInsert,
				IntentKey: []byte("1"),
				After:     map[string][]byte{"id": []byte{1}},
				CommitTS:  time.Now().UnixMilli(),
				NodeID:    100,
			},
		}

		err = registry.Append(events)
		assert.NoError(t, err)
	})

	t.Run("rejects append when not running", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		// Don't start registry

		events := []CDCEvent{
			{TxnID: 1, Database: "test", Table: "users"},
		}

		err = registry.Append(events)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not running")
	})

	t.Run("appends empty event list", func(t *testing.T) {
		tempDir := t.TempDir()
		config := RegistryConfig{
			DataDir:     tempDir,
			DBManager:   dbManager,
			SinkConfigs: []cfg.SinkConfiguration{},
		}

		registry, err := NewRegistry(config)
		require.NoError(t, err)
		defer registry.Stop()

		err = registry.Start()
		require.NoError(t, err)

		err = registry.Append([]CDCEvent{})
		assert.NoError(t, err)
	})
}

func TestSchemaProviderAdapter(t *testing.T) {
	dbManager := newMockDatabaseManager()

	schema := TableSchema{
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "email", Type: "TEXT", Nullable: false, IsPK: false},
		},
	}
	dbManager.addSchema("prod", "users", schema)

	adapter := &schemaProviderAdapter{dbMgr: dbManager}

	t.Run("retrieves schema", func(t *testing.T) {
		result, err := adapter.GetSchema("prod", "users")
		require.NoError(t, err)
		assert.Equal(t, schema, result)
	})

	t.Run("returns default for unknown table", func(t *testing.T) {
		result, err := adapter.GetSchema("test", "unknown")
		require.NoError(t, err)
		assert.NotEmpty(t, result.Columns)
	})
}

func TestCreateSink(t *testing.T) {
	t.Run("creates kafka sink", func(t *testing.T) {
		config := cfg.SinkConfiguration{
			Type:      "kafka",
			Brokers:   []string{"localhost:9092"},
			BatchSize: 100,
		}

		snk, err := createSink(config)
		require.NoError(t, err)
		require.NotNil(t, snk)
		defer snk.Close()

		// Verify it implements Sink interface
		var _ Sink = snk
	})

	t.Run("rejects unknown type", func(t *testing.T) {
		config := cfg.SinkConfiguration{
			Type: "unknown",
		}

		snk, err := createSink(config)
		assert.Error(t, err)
		assert.Nil(t, snk)
		assert.Contains(t, err.Error(), "unknown sink type")
	})
}

func TestCreateTransformer(t *testing.T) {
	t.Run("creates debezium transformer", func(t *testing.T) {
		trans, err := createTransformer("debezium")
		require.NoError(t, err)
		require.NotNil(t, trans)

		// Verify it's the right type
		_, ok := trans.(Transformer)
		assert.True(t, ok)
	})

	t.Run("rejects unknown format", func(t *testing.T) {
		trans, err := createTransformer("unknown")
		assert.Error(t, err)
		assert.Nil(t, trans)
		assert.Contains(t, err.Error(), "unknown format")
	})
}

func TestRegistryIntegration(t *testing.T) {
	tempDir := t.TempDir()
	dbManager := newMockDatabaseManager()

	// Add schema for test table
	dbManager.addSchema("test", "events", TableSchema{
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "data", Type: "TEXT", Nullable: true, IsPK: false},
		},
	})

	t.Run("end-to-end workflow", func(t *testing.T) {
		config := RegistryConfig{
			DataDir:   tempDir,
			DBManager: dbManager,
			SinkConfigs: []cfg.SinkConfiguration{
				{
					Name:            "integration-sink",
					Type:            "kafka",
					Format:          "debezium",
					Brokers:         []string{"localhost:9092"},
					TopicPrefix:     "test.cdc",
					FilterTables:    []string{"events"},
					FilterDatabases: []string{"test"},
					BatchSize:       10,
					PollIntervalMS:  50,
					RetryInitialMS:  100,
					RetryMaxMS:      1000,
					RetryMultiplier: 2.0,
				},
			},
		}

		// Create registry
		registry, err := NewRegistry(config)
		require.NoError(t, err)

		// Start registry
		err = registry.Start()
		require.NoError(t, err)

		// Append events
		events := []CDCEvent{
			{
				TxnID:     100,
				Database:  "test",
				Table:     "events",
				Operation: OpInsert,
				IntentKey: []byte("1"),
				After:     map[string][]byte{"id": []byte{1}, "data": []byte("test")},
				CommitTS:  time.Now().UnixMilli(),
				NodeID:    1,
			},
			{
				TxnID:     101,
				Database:  "test",
				Table:     "events",
				Operation: OpUpdate,
				IntentKey: []byte("1"),
				Before:    map[string][]byte{"id": []byte{1}, "data": []byte("test")},
				After:     map[string][]byte{"id": []byte{1}, "data": []byte("updated")},
				CommitTS:  time.Now().UnixMilli(),
				NodeID:    1,
			},
		}

		err = registry.Append(events)
		require.NoError(t, err)

		// Allow processing time
		time.Sleep(100 * time.Millisecond)

		// Stop registry
		registry.Stop()

		assert.False(t, registry.running.Load())
	})
}
