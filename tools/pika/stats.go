package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const maxSampleErrors = 3

// Stats tracks benchmark statistics using atomic operations.
type Stats struct {
	// Counters per operation type
	readOps   uint64
	updateOps uint64
	insertOps uint64
	deleteOps uint64
	upsertOps uint64

	// Error counters per operation type
	readErrors   uint64
	updateErrors uint64
	insertErrors uint64
	deleteErrors uint64
	upsertErrors uint64

	// Error counters per category
	conflictErrors   uint64
	constraintErrors uint64
	timeoutErrors    uint64
	connectionErrors uint64
	unknownErrors    uint64

	// Sample error messages per category (first N unique)
	sampleErrorsMu sync.Mutex
	sampleErrors   map[ErrorCategory][]string

	// Retry counter
	retries uint64

	// Transaction counters (for batch mode)
	txCount   uint64
	txErrors  uint64
	txRetries uint64

	// Latency tracking (microseconds)
	mu        sync.Mutex
	latencies []int64
}

// NewStats creates a new stats tracker.
func NewStats() *Stats {
	return &Stats{
		latencies:    make([]int64, 0, 100000),
		sampleErrors: make(map[ErrorCategory][]string),
	}
}

// RecordOp records a successful operation.
func (s *Stats) RecordOp(opType OpType, latency time.Duration) {
	switch opType {
	case OpRead:
		atomic.AddUint64(&s.readOps, 1)
	case OpUpdate:
		atomic.AddUint64(&s.updateOps, 1)
	case OpInsert:
		atomic.AddUint64(&s.insertOps, 1)
	case OpDelete:
		atomic.AddUint64(&s.deleteOps, 1)
	case OpUpsert:
		atomic.AddUint64(&s.upsertOps, 1)
	}

	// Record latency
	s.mu.Lock()
	s.latencies = append(s.latencies, latency.Microseconds())
	s.mu.Unlock()
}

// RecordError records a failed operation.
func (s *Stats) RecordError(opType OpType) {
	switch opType {
	case OpRead:
		atomic.AddUint64(&s.readErrors, 1)
	case OpUpdate:
		atomic.AddUint64(&s.updateErrors, 1)
	case OpInsert:
		atomic.AddUint64(&s.insertErrors, 1)
	case OpDelete:
		atomic.AddUint64(&s.deleteErrors, 1)
	case OpUpsert:
		atomic.AddUint64(&s.upsertErrors, 1)
	}
}

// RecordCategorizedError records a failed operation with error categorization.
func (s *Stats) RecordCategorizedError(opType OpType, errCat ErrorCategory, errMsg string) {
	// Record per-operation error (backwards compatibility)
	s.RecordError(opType)

	// Record per-category error
	switch errCat {
	case ErrCatConflict:
		atomic.AddUint64(&s.conflictErrors, 1)
	case ErrCatConstraint:
		atomic.AddUint64(&s.constraintErrors, 1)
	case ErrCatTimeout:
		atomic.AddUint64(&s.timeoutErrors, 1)
	case ErrCatConnection:
		atomic.AddUint64(&s.connectionErrors, 1)
	default:
		atomic.AddUint64(&s.unknownErrors, 1)
	}

	// Record sample error message
	s.sampleErrorsMu.Lock()
	samples := s.sampleErrors[errCat]
	if len(samples) < maxSampleErrors {
		// Check if this error message is unique
		isUnique := true
		for _, existing := range samples {
			if existing == errMsg {
				isUnique = false
				break
			}
		}
		if isUnique {
			s.sampleErrors[errCat] = append(samples, errMsg)
		}
	}
	s.sampleErrorsMu.Unlock()
}

// RecordRetry records a retry attempt.
func (s *Stats) RecordRetry() {
	atomic.AddUint64(&s.retries, 1)
}

// RecordTx records a successful transaction.
func (s *Stats) RecordTx(latency time.Duration) {
	atomic.AddUint64(&s.txCount, 1)
	s.mu.Lock()
	s.latencies = append(s.latencies, latency.Microseconds())
	s.mu.Unlock()
}

// RecordTxError records a failed transaction.
func (s *Stats) RecordTxError() {
	atomic.AddUint64(&s.txErrors, 1)
}

// RecordTxRetry records a transaction retry attempt.
func (s *Stats) RecordTxRetry() {
	atomic.AddUint64(&s.txRetries, 1)
}

// TotalTx returns total transactions.
func (s *Stats) TotalTx() uint64 {
	return atomic.LoadUint64(&s.txCount)
}

// TotalTxErrors returns total transaction errors.
func (s *Stats) TotalTxErrors() uint64 {
	return atomic.LoadUint64(&s.txErrors)
}

// TxRetries returns transaction retry count.
func (s *Stats) TxRetries() uint64 {
	return atomic.LoadUint64(&s.txRetries)
}

// TotalOps returns total successful operations.
func (s *Stats) TotalOps() uint64 {
	return atomic.LoadUint64(&s.readOps) +
		atomic.LoadUint64(&s.updateOps) +
		atomic.LoadUint64(&s.insertOps) +
		atomic.LoadUint64(&s.deleteOps) +
		atomic.LoadUint64(&s.upsertOps)
}

// TotalErrors returns total errors.
func (s *Stats) TotalErrors() uint64 {
	return atomic.LoadUint64(&s.readErrors) +
		atomic.LoadUint64(&s.updateErrors) +
		atomic.LoadUint64(&s.insertErrors) +
		atomic.LoadUint64(&s.deleteErrors) +
		atomic.LoadUint64(&s.upsertErrors)
}

// Retries returns retry count.
func (s *Stats) Retries() uint64 {
	return atomic.LoadUint64(&s.retries)
}

// GetLatencyPercentiles returns p50, p90, p95, p99 in microseconds.
func (s *Stats) GetLatencyPercentiles() (p50, p90, p95, p99 int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.latencies) == 0 {
		return 0, 0, 0, 0
	}

	sorted := make([]int64, len(s.latencies))
	copy(sorted, s.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	n := len(sorted)
	p50 = sorted[n*50/100]
	p90 = sorted[n*90/100]
	p95 = sorted[n*95/100]
	p99 = sorted[n*99/100]

	return p50, p90, p95, p99
}

// GetLatencyStats returns min, max, avg in microseconds.
func (s *Stats) GetLatencyStats() (min, max, avg int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.latencies) == 0 {
		return 0, 0, 0
	}

	min = s.latencies[0]
	max = s.latencies[0]
	var sum int64

	for _, l := range s.latencies {
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
		sum += l
	}

	avg = sum / int64(len(s.latencies))
	return min, max, avg
}

// Snapshot returns a copy of current counters.
type Snapshot struct {
	ReadOps   uint64
	UpdateOps uint64
	InsertOps uint64
	DeleteOps uint64
	UpsertOps uint64
	Errors    uint64
	Retries   uint64
	TxCount   uint64
	TxErrors  uint64
	TxRetries uint64
}

// GetSnapshot returns current stats snapshot.
func (s *Stats) GetSnapshot() Snapshot {
	return Snapshot{
		ReadOps:   atomic.LoadUint64(&s.readOps),
		UpdateOps: atomic.LoadUint64(&s.updateOps),
		InsertOps: atomic.LoadUint64(&s.insertOps),
		DeleteOps: atomic.LoadUint64(&s.deleteOps),
		UpsertOps: atomic.LoadUint64(&s.upsertOps),
		Errors:    s.TotalErrors(),
		Retries:   atomic.LoadUint64(&s.retries),
		TxCount:   atomic.LoadUint64(&s.txCount),
		TxErrors:  atomic.LoadUint64(&s.txErrors),
		TxRetries: atomic.LoadUint64(&s.txRetries),
	}
}

// printErrorCategories prints error category breakdown and sample messages.
func (s *Stats) printErrorCategories() {
	conflictErrs := atomic.LoadUint64(&s.conflictErrors)
	constraintErrs := atomic.LoadUint64(&s.constraintErrors)
	timeoutErrs := atomic.LoadUint64(&s.timeoutErrors)
	connectionErrs := atomic.LoadUint64(&s.connectionErrors)
	unknownErrs := atomic.LoadUint64(&s.unknownErrors)

	// Only print if we have categorized errors
	if conflictErrs+constraintErrs+timeoutErrs+connectionErrs+unknownErrs == 0 {
		return
	}

	fmt.Println("Error Categories:")
	fmt.Printf("  Conflicts:    %d (expected in distributed writes)\n", conflictErrs)
	fmt.Printf("  Constraints:  %d (UNIQUE violations)\n", constraintErrs)
	fmt.Printf("  Timeouts:     %d\n", timeoutErrs)
	fmt.Printf("  Connections:  %d\n", connectionErrs)
	fmt.Printf("  Unknown:      %d\n", unknownErrs)
	fmt.Println()

	// Print sample errors
	s.sampleErrorsMu.Lock()
	defer s.sampleErrorsMu.Unlock()

	hasSamples := false
	for _, samples := range s.sampleErrors {
		if len(samples) > 0 {
			hasSamples = true
			break
		}
	}

	if !hasSamples {
		return
	}

	fmt.Println("Sample Errors (first 3 per category):")

	// Print in consistent order
	categories := []ErrorCategory{ErrCatConflict, ErrCatConstraint, ErrCatTimeout, ErrCatConnection, ErrCatUnknown}
	for _, cat := range categories {
		samples := s.sampleErrors[cat]
		if len(samples) > 0 {
			fmt.Printf("  %s:\n", cat.String())
			for _, msg := range samples {
				fmt.Printf("    - %q\n", msg)
			}
		}
	}
	fmt.Println()
}

// PrintFinal prints final statistics.
func (s *Stats) PrintFinal(elapsed time.Duration) {
	totalOps := s.TotalOps()
	totalErrors := s.TotalErrors()
	retries := s.Retries()
	totalTx := s.TotalTx()
	txErrors := s.TotalTxErrors()
	txRetries := s.TxRetries()

	throughput := float64(totalOps) / elapsed.Seconds()

	fmt.Println()
	fmt.Printf("Total time:    %.2fs\n", elapsed.Seconds())
	fmt.Printf("Throughput:    %.2f ops/sec\n", throughput)
	if totalTx > 0 {
		txThroughput := float64(totalTx) / elapsed.Seconds()
		fmt.Printf("TX Throughput: %.2f tx/sec\n", txThroughput)
	}
	fmt.Println()

	fmt.Println("Operations:")
	fmt.Printf("  READ:   %d\n", atomic.LoadUint64(&s.readOps))
	fmt.Printf("  UPDATE: %d\n", atomic.LoadUint64(&s.updateOps))
	fmt.Printf("  INSERT: %d\n", atomic.LoadUint64(&s.insertOps))
	fmt.Printf("  DELETE: %d\n", atomic.LoadUint64(&s.deleteOps))
	fmt.Printf("  UPSERT: %d\n", atomic.LoadUint64(&s.upsertOps))
	fmt.Printf("  TOTAL:  %d\n", totalOps)
	fmt.Println()

	if totalTx > 0 {
		fmt.Println("Transactions:")
		fmt.Printf("  Total:   %d\n", totalTx)
		fmt.Printf("  Errors:  %d\n", txErrors)
		fmt.Printf("  Retries: %d\n", txRetries)
		fmt.Println()
	}

	if totalErrors > 0 || retries > 0 {
		fmt.Println("Errors/Retries:")
		if atomic.LoadUint64(&s.readErrors) > 0 {
			fmt.Printf("  READ errors:   %d\n", atomic.LoadUint64(&s.readErrors))
		}
		if atomic.LoadUint64(&s.updateErrors) > 0 {
			fmt.Printf("  UPDATE errors: %d\n", atomic.LoadUint64(&s.updateErrors))
		}
		if atomic.LoadUint64(&s.insertErrors) > 0 {
			fmt.Printf("  INSERT errors: %d\n", atomic.LoadUint64(&s.insertErrors))
		}
		if atomic.LoadUint64(&s.deleteErrors) > 0 {
			fmt.Printf("  DELETE errors: %d\n", atomic.LoadUint64(&s.deleteErrors))
		}
		if atomic.LoadUint64(&s.upsertErrors) > 0 {
			fmt.Printf("  UPSERT errors: %d\n", atomic.LoadUint64(&s.upsertErrors))
		}
		fmt.Printf("  Total errors:  %d\n", totalErrors)
		fmt.Printf("  Retries:       %d\n", retries)
		fmt.Println()

		// Print error categories
		s.printErrorCategories()
	}

	min, max, avg := s.GetLatencyStats()
	p50, p90, p95, p99 := s.GetLatencyPercentiles()

	fmt.Println("Latency (microseconds):")
	fmt.Printf("  Min:   %d\n", min)
	fmt.Printf("  Avg:   %d\n", avg)
	fmt.Printf("  Max:   %d\n", max)
	fmt.Printf("  P50:   %d\n", p50)
	fmt.Printf("  P90:   %d\n", p90)
	fmt.Printf("  P95:   %d\n", p95)
	fmt.Printf("  P99:   %d\n", p99)
}
