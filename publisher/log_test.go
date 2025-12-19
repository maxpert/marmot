package publisher

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPublishLog(t *testing.T) {
	tmpDir := t.TempDir()

	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	require.NotNil(t, pl)
	defer pl.Close()

	// Verify path is set correctly
	assert.Equal(t, filepath.Join(tmpDir, "publish_log"), pl.path)

	// Verify initial state
	assert.NotNil(t, pl.cursors)
	assert.Equal(t, uint64(0), pl.nextSeq.Load())
}

func TestPublishLogAppendAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Create test events
	events := []CDCEvent{
		{
			TxnID:     100,
			Database:  "db1",
			Table:     "users",
			Operation: OpInsert,
			IntentKey: []byte("user:1"),
			After:     map[string][]byte{"id": []byte("1")},
			CommitTS:  1000,
			NodeID:    1,
		},
		{
			TxnID:     101,
			Database:  "db1",
			Table:     "users",
			Operation: OpUpdate,
			IntentKey: []byte("user:2"),
			Before:    map[string][]byte{"status": []byte("active")},
			After:     map[string][]byte{"status": []byte("inactive")},
			CommitTS:  2000,
			NodeID:    1,
		},
	}

	// Append events
	err = pl.Append(events)
	require.NoError(t, err)

	// Verify sequence numbers were assigned
	assert.Equal(t, uint64(1), events[0].SeqNum)
	assert.Equal(t, uint64(2), events[1].SeqNum)

	// Read from beginning
	readEvents, err := pl.ReadFrom(0, 10)
	require.NoError(t, err)
	require.Len(t, readEvents, 2)

	// Verify first event
	assert.Equal(t, uint64(1), readEvents[0].SeqNum)
	assert.Equal(t, uint64(100), readEvents[0].TxnID)
	assert.Equal(t, "db1", readEvents[0].Database)
	assert.Equal(t, "users", readEvents[0].Table)

	// Verify second event
	assert.Equal(t, uint64(2), readEvents[1].SeqNum)
	assert.Equal(t, uint64(101), readEvents[1].TxnID)
}

func TestPublishLogReadWithLimit(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Create 10 events
	events := make([]CDCEvent, 10)
	for i := 0; i < 10; i++ {
		events[i] = CDCEvent{
			TxnID:     uint64(100 + i),
			Database:  "db",
			Table:     "tbl",
			Operation: OpInsert,
			IntentKey: []byte(string(rune('a' + i))),
			CommitTS:  int64(1000 * (i + 1)),
			NodeID:    1,
		}
	}

	err = pl.Append(events)
	require.NoError(t, err)

	// Read only first 5
	readEvents, err := pl.ReadFrom(0, 5)
	require.NoError(t, err)
	assert.Len(t, readEvents, 5)
	assert.Equal(t, uint64(1), readEvents[0].SeqNum)
	assert.Equal(t, uint64(5), readEvents[4].SeqNum)

	// Read next 3
	readEvents, err = pl.ReadFrom(5, 3)
	require.NoError(t, err)
	assert.Len(t, readEvents, 3)
	assert.Equal(t, uint64(6), readEvents[0].SeqNum)
	assert.Equal(t, uint64(8), readEvents[2].SeqNum)
}

func TestPublishLogCursorOperations(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Get cursor for new sink
	cursor, err := pl.GetCursor("sink1")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), cursor)

	// Advance cursor
	err = pl.AdvanceCursor("sink1", 10)
	require.NoError(t, err)

	// Read cursor again
	cursor, err = pl.GetCursor("sink1")
	require.NoError(t, err)
	assert.Equal(t, uint64(10), cursor)

	// Add another sink
	err = pl.AdvanceCursor("sink2", 5)
	require.NoError(t, err)

	cursor, err = pl.GetCursor("sink2")
	require.NoError(t, err)
	assert.Equal(t, uint64(5), cursor)
}

func TestPublishLogCursorPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create log and set cursors
	pl1, err := NewPublishLog(tmpDir)
	require.NoError(t, err)

	err = pl1.AdvanceCursor("sink1", 100)
	require.NoError(t, err)
	err = pl1.AdvanceCursor("sink2", 50)
	require.NoError(t, err)

	pl1.Close()

	// Reopen log
	pl2, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl2.Close()

	// Verify cursors were loaded
	cursor1, err := pl2.GetCursor("sink1")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), cursor1)

	cursor2, err := pl2.GetCursor("sink2")
	require.NoError(t, err)
	assert.Equal(t, uint64(50), cursor2)
}

func TestPublishLogSequenceNumberPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create log and append events
	pl1, err := NewPublishLog(tmpDir)
	require.NoError(t, err)

	events := []CDCEvent{
		{TxnID: 1, Database: "db", Table: "t", Operation: OpInsert, IntentKey: []byte("k1"), CommitTS: 1, NodeID: 1},
		{TxnID: 2, Database: "db", Table: "t", Operation: OpInsert, IntentKey: []byte("k2"), CommitTS: 2, NodeID: 1},
		{TxnID: 3, Database: "db", Table: "t", Operation: OpInsert, IntentKey: []byte("k3"), CommitTS: 3, NodeID: 1},
	}
	err = pl1.Append(events)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), events[2].SeqNum)

	pl1.Close()

	// Reopen and append more
	pl2, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl2.Close()

	moreEvents := []CDCEvent{
		{TxnID: 4, Database: "db", Table: "t", Operation: OpInsert, IntentKey: []byte("k4"), CommitTS: 4, NodeID: 1},
	}
	err = pl2.Append(moreEvents)
	require.NoError(t, err)

	// New event should get sequence 4
	assert.Equal(t, uint64(4), moreEvents[0].SeqNum)
}

func TestPublishLogEmptyAppend(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Append empty slice should be no-op
	err = pl.Append([]CDCEvent{})
	require.NoError(t, err)
}

func TestPublishLogReadFromEmptyLog(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Reading from empty log should return empty slice
	events, err := pl.ReadFrom(0, 10)
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestPublishLogCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Append 200 events
	events := make([]CDCEvent, 200)
	for i := 0; i < 200; i++ {
		events[i] = CDCEvent{
			TxnID:     uint64(i + 1),
			Database:  "db",
			Table:     "tbl",
			Operation: OpInsert,
			IntentKey: []byte(string(rune('a' + (i % 26)))),
			CommitTS:  int64(i + 1),
			NodeID:    1,
		}
	}
	err = pl.Append(events)
	require.NoError(t, err)

	// Set cursor for sink1 to 150
	err = pl.AdvanceCursor("sink1", 150)
	require.NoError(t, err)

	// Set cursor for sink2 to 128 (should trigger cleanup)
	err = pl.AdvanceCursor("sink2", 128)
	require.NoError(t, err)

	// Cleanup runs async, but we can manually trigger it
	pl.cleanup()

	// Events 1-127 should be deleted (min cursor is 128)
	// Try to read from beginning - should only get events from 128+
	readEvents, err := pl.ReadFrom(0, 200)
	require.NoError(t, err)
	if len(readEvents) > 0 {
		// First event should have seq >= 128
		assert.GreaterOrEqual(t, readEvents[0].SeqNum, uint64(128))
	}
}

func TestPublishLogMultipleSinks(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Append events
	events := make([]CDCEvent, 50)
	for i := 0; i < 50; i++ {
		events[i] = CDCEvent{
			TxnID:     uint64(i + 1),
			Database:  "db",
			Table:     "tbl",
			Operation: OpInsert,
			IntentKey: []byte("key"),
			CommitTS:  int64(i + 1),
			NodeID:    1,
		}
	}
	err = pl.Append(events)
	require.NoError(t, err)

	// Sink1 reads and advances to 30
	events1, err := pl.ReadFrom(0, 30)
	require.NoError(t, err)
	assert.Len(t, events1, 30)
	err = pl.AdvanceCursor("sink1", 30)
	require.NoError(t, err)

	// Sink2 reads and advances to 20
	events2, err := pl.ReadFrom(0, 20)
	require.NoError(t, err)
	assert.Len(t, events2, 20)
	err = pl.AdvanceCursor("sink2", 20)
	require.NoError(t, err)

	// Sink1 continues from 30
	eventsNext, err := pl.ReadFrom(30, 10)
	require.NoError(t, err)
	assert.Len(t, eventsNext, 10)
	assert.Equal(t, uint64(31), eventsNext[0].SeqNum)
}

func TestPublishLogConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(t, err)
	defer pl.Close()

	// Append events
	events := make([]CDCEvent, 100)
	for i := 0; i < 100; i++ {
		events[i] = CDCEvent{
			TxnID:     uint64(i + 1),
			Database:  "db",
			Table:     "tbl",
			Operation: OpInsert,
			IntentKey: []byte("key"),
			CommitTS:  int64(i + 1),
			NodeID:    1,
		}
	}
	err = pl.Append(events)
	require.NoError(t, err)

	// Concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			_, err := pl.ReadFrom(0, 50)
			assert.NoError(t, err)
		}()
	}

	// Wait for all reads to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestPublishLogInvalidPath(t *testing.T) {
	// Try to create log at invalid path (should fail gracefully)
	invalidPath := "/invalid/nonexistent/path/that/does/not/exist"
	pl, err := NewPublishLog(invalidPath)
	assert.Error(t, err)
	assert.Nil(t, pl)
}

func TestFormatPubLogKey(t *testing.T) {
	tests := []struct {
		seq      uint64
		expected []byte
	}{
		{0, []byte("/publog/\x00\x00\x00\x00\x00\x00\x00\x00")},
		{1, []byte("/publog/\x00\x00\x00\x00\x00\x00\x00\x01")},
		{255, []byte("/publog/\x00\x00\x00\x00\x00\x00\x00\xff")},
		{65535, []byte("/publog/\x00\x00\x00\x00\x00\x00\xff\xff")},
		{0xFFFFFFFFFFFFFFFF, []byte("/publog/\xff\xff\xff\xff\xff\xff\xff\xff")},
	}

	for _, tt := range tests {
		result := formatPubLogKey(tt.seq)
		assert.Equal(t, tt.expected, result, "seq=%d", tt.seq)
	}
}

func TestPrefixUpperBound(t *testing.T) {
	tests := []struct {
		prefix   []byte
		expected []byte
	}{
		{[]byte("/publog/"), []byte("/publog0")},
		{[]byte("/a"), []byte("/b")},
		{[]byte{0x00}, []byte{0x01}},
		{[]byte{0xff}, nil}, // All 0xff wraps around
	}

	for _, tt := range tests {
		result := prefixUpperBound(tt.prefix)
		assert.Equal(t, tt.expected, result, "prefix=%v", tt.prefix)
	}
}

func BenchmarkPublishLogAppend(b *testing.B) {
	tmpDir := b.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(b, err)
	defer pl.Close()

	event := CDCEvent{
		TxnID:     1,
		Database:  "db",
		Table:     "tbl",
		Operation: OpInsert,
		IntentKey: []byte("key"),
		After:     map[string][]byte{"id": []byte("1")},
		CommitTS:  1000,
		NodeID:    1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := []CDCEvent{event}
		_ = pl.Append(events)
	}
}

func BenchmarkPublishLogRead(b *testing.B) {
	tmpDir := b.TempDir()
	pl, err := NewPublishLog(tmpDir)
	require.NoError(b, err)
	defer pl.Close()

	// Prepopulate with 1000 events
	events := make([]CDCEvent, 1000)
	for i := 0; i < 1000; i++ {
		events[i] = CDCEvent{
			TxnID:     uint64(i + 1),
			Database:  "db",
			Table:     "tbl",
			Operation: OpInsert,
			IntentKey: []byte("key"),
			CommitTS:  int64(i + 1),
			NodeID:    1,
		}
	}
	_ = pl.Append(events)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pl.ReadFrom(0, 100)
	}
}
