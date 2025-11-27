package intentlog

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	dir := t.TempDir()

	log, err := New(12345, dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer log.Delete()

	if log.TxnID() != 12345 {
		t.Errorf("TxnID = %d, want 12345", log.TxnID())
	}

	if log.EntryCount() != 0 {
		t.Errorf("EntryCount = %d, want 0", log.EntryCount())
	}

	expectedPath := filepath.Join(dir, "intent_logs", "12345.log")
	if log.Path() != expectedPath {
		t.Errorf("Path = %s, want %s", log.Path(), expectedPath)
	}
}

func TestAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	log, err := New(100, dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Append INSERT
	err = log.Append(&Entry{
		Operation: OpInsert,
		Table:     "users",
		RowKey:    "123",
		NewValues: map[string][]byte{
			"id":   []byte("123"),
			"name": []byte("Alice"),
		},
	})
	if err != nil {
		t.Fatalf("Append INSERT failed: %v", err)
	}

	// Append UPDATE
	err = log.Append(&Entry{
		Operation: OpUpdate,
		Table:     "users",
		RowKey:    "456",
		OldValues: map[string][]byte{"status": []byte("active")},
		NewValues: map[string][]byte{"status": []byte("inactive")},
	})
	if err != nil {
		t.Fatalf("Append UPDATE failed: %v", err)
	}

	// Append DELETE
	err = log.Append(&Entry{
		Operation: OpDelete,
		Table:     "orders",
		RowKey:    "789",
		OldValues: map[string][]byte{"id": []byte("789")},
	})
	if err != nil {
		t.Fatalf("Append DELETE failed: %v", err)
	}

	if log.EntryCount() != 3 {
		t.Errorf("EntryCount = %d, want 3", log.EntryCount())
	}

	path := log.Path()
	log.Close()

	// Read back
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	if reader.TxnID() != 100 {
		t.Errorf("Reader TxnID = %d, want 100", reader.TxnID())
	}

	entries, err := reader.Entries()
	if err != nil {
		t.Fatalf("Entries failed: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}

	// Verify INSERT
	e := entries[0]
	if e.Seq != 1 || e.Operation != OpInsert || e.Table != "users" || e.RowKey != "123" {
		t.Errorf("entry 0 mismatch: %+v", e)
	}
	if e.OldValues != nil {
		t.Error("INSERT should have nil OldValues")
	}
	if string(e.NewValues["name"]) != "Alice" {
		t.Errorf("NewValues[name] = %s, want Alice", e.NewValues["name"])
	}

	// Verify UPDATE
	e = entries[1]
	if e.Seq != 2 || e.Operation != OpUpdate || e.Table != "users" {
		t.Errorf("entry 1 mismatch: %+v", e)
	}
	if string(e.OldValues["status"]) != "active" {
		t.Error("OldValues mismatch")
	}
	if string(e.NewValues["status"]) != "inactive" {
		t.Error("NewValues mismatch")
	}

	// Verify DELETE
	e = entries[2]
	if e.Seq != 3 || e.Operation != OpDelete || e.Table != "orders" {
		t.Errorf("entry 2 mismatch: %+v", e)
	}
	if e.NewValues != nil {
		t.Error("DELETE should have nil NewValues")
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()

	log, err := New(999, dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	path := log.Path()

	// File should exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("log file should exist")
	}

	// Delete
	if err := log.Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// File should not exist
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("log file should be deleted")
	}
}

func TestFlush(t *testing.T) {
	dir := t.TempDir()

	log, err := New(555, dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer log.Delete()

	err = log.Append(&Entry{
		Operation: OpInsert,
		Table:     "test",
		RowKey:    "1",
		NewValues: map[string][]byte{"x": []byte("y")},
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Flush should not error
	if err := log.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestClosedLogErrors(t *testing.T) {
	dir := t.TempDir()

	log, err := New(777, dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	log.Close()

	// Append after close should fail
	err = log.Append(&Entry{Operation: OpInsert, Table: "t", RowKey: "k"})
	if err != ErrLogClosed {
		t.Errorf("Append after close: got %v, want ErrLogClosed", err)
	}

	// Flush after close should fail
	err = log.Flush()
	if err != ErrLogClosed {
		t.Errorf("Flush after close: got %v, want ErrLogClosed", err)
	}

	// Double close should not error
	if err := log.Close(); err != nil {
		t.Errorf("Double close: got %v, want nil", err)
	}

	// Cleanup
	os.Remove(log.Path())
}

func TestInvalidMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.log")

	// Write invalid magic
	f, _ := os.Create(path)
	f.Write([]byte("BADD0000000000000000000000"))
	f.Close()

	_, err := Open(path)
	if err != ErrInvalidMagic {
		t.Errorf("Open bad magic: got %v, want ErrInvalidMagic", err)
	}
}

func TestVersionMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "badver.log")

	// Write valid magic but wrong version
	f, _ := os.Create(path)
	f.Write([]byte("MLOG"))
	f.Write([]byte{99, 0}) // version 99
	f.Write(make([]byte, 18))
	f.Close()

	_, err := Open(path)
	if err == nil || err == ErrInvalidMagic {
		t.Errorf("Open wrong version: got %v, want ErrVersionMismatch", err)
	}
}

func TestListLogs(t *testing.T) {
	dir := t.TempDir()

	// Create some logs
	log1, _ := New(1, dir)
	log2, _ := New(2, dir)
	log3, _ := New(3, dir)

	log1.Close()
	log2.Close()
	log3.Close()

	logs, err := ListLogs(dir)
	if err != nil {
		t.Fatalf("ListLogs failed: %v", err)
	}

	if len(logs) != 3 {
		t.Errorf("ListLogs returned %d logs, want 3", len(logs))
	}

	// Cleanup
	log1.Delete()
	log2.Delete()
	log3.Delete()
}

func TestReaderNext(t *testing.T) {
	dir := t.TempDir()

	log, _ := New(888, dir)
	log.Append(&Entry{Operation: OpInsert, Table: "t1", RowKey: "k1"})
	log.Append(&Entry{Operation: OpInsert, Table: "t2", RowKey: "k2"})
	path := log.Path()
	log.Close()

	reader, _ := Open(path)
	defer reader.Close()
	defer os.Remove(path)

	e1, err := reader.Next()
	if err != nil {
		t.Fatalf("Next 1 failed: %v", err)
	}
	if e1.Table != "t1" {
		t.Errorf("entry 1 table = %s, want t1", e1.Table)
	}

	e2, err := reader.Next()
	if err != nil {
		t.Fatalf("Next 2 failed: %v", err)
	}
	if e2.Table != "t2" {
		t.Errorf("entry 2 table = %s, want t2", e2.Table)
	}

	_, err = reader.Next()
	if err != io.EOF {
		t.Errorf("Next 3: got %v, want io.EOF", err)
	}
}

func TestLargeEntry(t *testing.T) {
	dir := t.TempDir()

	log, err := New(111, dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Create large values
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err = log.Append(&Entry{
		Operation: OpUpdate,
		Table:     "large_table",
		RowKey:    "big_key",
		OldValues: map[string][]byte{"data": largeData},
		NewValues: map[string][]byte{"data": largeData},
	})
	if err != nil {
		t.Fatalf("Append large failed: %v", err)
	}

	path := log.Path()
	log.Close()

	// Read back
	reader, _ := Open(path)
	defer reader.Close()
	defer os.Remove(path)

	e, err := reader.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if len(e.OldValues["data"]) != len(largeData) {
		t.Errorf("OldValues data length = %d, want %d", len(e.OldValues["data"]), len(largeData))
	}
}

func TestEmptyLog(t *testing.T) {
	dir := t.TempDir()

	log, err := New(222, dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	path := log.Path()
	log.Close()

	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()
	defer os.Remove(path)

	entries, err := reader.Entries()
	if err != nil {
		t.Fatalf("Entries failed: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("got %d entries, want 0", len(entries))
	}
}

// Benchmarks

func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	log, _ := New(uint64(b.N), dir)
	defer log.Delete()

	entry := &Entry{
		Operation: OpUpdate,
		Table:     "users",
		RowKey:    "user:12345",
		OldValues: map[string][]byte{"name": []byte("Alice"), "status": []byte("active")},
		NewValues: map[string][]byte{"name": []byte("Alice"), "status": []byte("inactive")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Append(entry)
	}
}

func BenchmarkAppendLarge(b *testing.B) {
	dir := b.TempDir()
	log, _ := New(uint64(b.N), dir)
	defer log.Delete()

	data := make([]byte, 10*1024) // 10KB
	entry := &Entry{
		Operation: OpInsert,
		Table:     "blobs",
		RowKey:    "blob:1",
		NewValues: map[string][]byte{"data": data},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Append(entry)
	}
}
