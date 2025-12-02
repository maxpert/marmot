package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
)

type OpType int

const (
	OpRead OpType = iota
	OpUpdate
	OpInsert
	OpDelete
	OpUpsert
)

func (o OpType) String() string {
	switch o {
	case OpRead:
		return "READ"
	case OpUpdate:
		return "UPDATE"
	case OpInsert:
		return "INSERT"
	case OpDelete:
		return "DELETE"
	case OpUpsert:
		return "UPSERT"
	default:
		return "UNKNOWN"
	}
}

// KeyGenerator generates sequential keys for uniform distribution.
// Thread-safe: uses atomic operations for counter/maxKey, caller provides rng.
type KeyGenerator struct {
	prefix        string
	counter       uint64
	maxKey        uint64  // Max key for reads/updates (existing rows)
	insertOverlap float64 // % of inserts that target existing keys (0-100)
}

// NewKeyGenerator creates a key generator.
func NewKeyGenerator(prefix string, existingRows int64, insertOverlap float64) *KeyGenerator {
	return &KeyGenerator{
		prefix:        prefix,
		maxKey:        uint64(existingRows),
		insertOverlap: insertOverlap,
	}
}

// NextInsertKey generates a key for inserts.
// With insertOverlap > 0, some inserts will target existing keys (causing conflicts).
// rng must be provided by caller (each worker has its own thread-local rng).
func (g *KeyGenerator) NextInsertKey(rng *rand.Rand) string {
	max := atomic.LoadUint64(&g.maxKey)
	// Check if this insert should overlap with existing keys
	if g.insertOverlap > 0 && max > 0 && rng.Float64()*100 < g.insertOverlap {
		// Return an existing key (will cause UNIQUE constraint conflict)
		n := uint64(rng.Int63n(int64(max))) + 1
		return fmt.Sprintf("%s_%012d", g.prefix, n)
	}
	// Generate a new unique key
	n := atomic.AddUint64(&g.counter, 1)
	return fmt.Sprintf("%s_%012d", g.prefix, max+n)
}

// RandomExistingKey returns a random key from existing rows.
// rng must be provided by caller (each worker has its own thread-local rng).
func (g *KeyGenerator) RandomExistingKey(rng *rand.Rand) string {
	max := atomic.LoadUint64(&g.maxKey)
	if max == 0 {
		return g.NextInsertKey(rng) // No existing rows, generate new
	}
	n := uint64(rng.Int63n(int64(max))) + 1
	return fmt.Sprintf("%s_%012d", g.prefix, n)
}

// UpdateMaxKey updates the max key after inserts.
func (g *KeyGenerator) UpdateMaxKey(delta int64) {
	atomic.AddUint64(&g.maxKey, uint64(delta))
}

// Operation represents a single database operation.
type Operation struct {
	Type  OpType
	Key   string
	Value string
}

// Executor is the interface for executing SQL statements.
// Both *sql.DB and *sql.Tx implement this interface.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// OpSelector selects operations based on workload distribution.
type OpSelector struct {
	dist       WorkloadDistribution
	thresholds [5]int // Cumulative thresholds for each op type
	rng        *rand.Rand
}

// NewOpSelector creates an operation selector.
func NewOpSelector(dist WorkloadDistribution, seed int64) *OpSelector {
	s := &OpSelector{
		dist: dist,
		rng:  rand.New(rand.NewSource(seed)),
	}

	// Build cumulative thresholds
	s.thresholds[0] = dist.Read
	s.thresholds[1] = s.thresholds[0] + dist.Update
	s.thresholds[2] = s.thresholds[1] + dist.Insert
	s.thresholds[3] = s.thresholds[2] + dist.Delete
	s.thresholds[4] = s.thresholds[3] + dist.Upsert

	return s
}

// Select returns a random operation type based on distribution.
func (s *OpSelector) Select() OpType {
	r := s.rng.Intn(100)

	if r < s.thresholds[0] {
		return OpRead
	}
	if r < s.thresholds[1] {
		return OpUpdate
	}
	if r < s.thresholds[2] {
		return OpInsert
	}
	if r < s.thresholds[3] {
		return OpDelete
	}
	return OpUpsert
}

// generateFieldValue generates a random value for a field.
func generateFieldValue(rng *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const length = 100
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rng.Intn(len(chars))]
	}
	return string(b)
}

// ExecuteOp executes a single operation using the given executor.
func ExecuteOp(ctx context.Context, exec Executor, table string, op Operation) error {
	switch op.Type {
	case OpRead:
		return executeRead(ctx, exec, table, op.Key)
	case OpUpdate:
		return executeUpdate(ctx, exec, table, op.Key, op.Value)
	case OpInsert:
		return executeInsert(ctx, exec, table, op.Key, op.Value)
	case OpDelete:
		return executeDelete(ctx, exec, table, op.Key)
	case OpUpsert:
		return executeUpsert(ctx, exec, table, op.Key, op.Value)
	default:
		return fmt.Errorf("unknown operation type: %v", op.Type)
	}
}

func executeRead(ctx context.Context, exec Executor, table, key string) error {
	row := exec.QueryRowContext(ctx,
		fmt.Sprintf("SELECT field0, field1, field2, field3, field4, field5, field6, field7, field8, field9 FROM %s WHERE id = ?", table),
		key)

	var f0, f1, f2, f3, f4, f5, f6, f7, f8, f9 sql.NullString
	err := row.Scan(&f0, &f1, &f2, &f3, &f4, &f5, &f6, &f7, &f8, &f9)
	if err == sql.ErrNoRows {
		// Not finding a row is not an error for benchmark purposes
		return nil
	}
	return err
}

func executeUpdate(ctx context.Context, exec Executor, table, key, value string) error {
	_, err := exec.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET field0 = ? WHERE id = ?", table),
		value, key)
	return err
}

func executeInsert(ctx context.Context, exec Executor, table, key, value string) error {
	_, err := exec.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", table),
		key, value, value, value, value, value, value, value, value, value, value)
	return err
}

func executeDelete(ctx context.Context, exec Executor, table, key string) error {
	_, err := exec.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE id = ?", table),
		key)
	return err
}

func executeUpsert(ctx context.Context, exec Executor, table, key, value string) error {
	// Use INSERT OR REPLACE for SQLite compatibility
	_, err := exec.ExecContext(ctx,
		fmt.Sprintf("INSERT OR REPLACE INTO %s (id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", table),
		key, value, value, value, value, value, value, value, value, value, value)
	return err
}

// IsRetryableError checks if an error is retryable.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// MySQL error codes
	if strings.Contains(errStr, "1213") { // Deadlock
		return true
	}
	if strings.Contains(errStr, "1205") { // Lock wait timeout
		return true
	}

	// Marmot conflict errors
	if strings.Contains(errStr, "conflict") {
		return true
	}
	if strings.Contains(errStr, "retry") {
		return true
	}

	return false
}
