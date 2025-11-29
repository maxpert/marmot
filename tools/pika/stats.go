package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

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

	// Retry counter
	retries uint64

	// Latency tracking (microseconds)
	mu        sync.Mutex
	latencies []int64
}

// NewStats creates a new stats tracker.
func NewStats() *Stats {
	return &Stats{
		latencies: make([]int64, 0, 100000),
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

// RecordRetry records a retry attempt.
func (s *Stats) RecordRetry() {
	atomic.AddUint64(&s.retries, 1)
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
	}
}

// PrintFinal prints final statistics.
func (s *Stats) PrintFinal(elapsed time.Duration) {
	totalOps := s.TotalOps()
	totalErrors := s.TotalErrors()
	retries := s.Retries()

	throughput := float64(totalOps) / elapsed.Seconds()

	fmt.Println()
	fmt.Printf("Total time:    %.2fs\n", elapsed.Seconds())
	fmt.Printf("Throughput:    %.2f ops/sec\n", throughput)
	fmt.Println()

	fmt.Println("Operations:")
	fmt.Printf("  READ:   %d\n", atomic.LoadUint64(&s.readOps))
	fmt.Printf("  UPDATE: %d\n", atomic.LoadUint64(&s.updateOps))
	fmt.Printf("  INSERT: %d\n", atomic.LoadUint64(&s.insertOps))
	fmt.Printf("  DELETE: %d\n", atomic.LoadUint64(&s.deleteOps))
	fmt.Printf("  UPSERT: %d\n", atomic.LoadUint64(&s.upsertOps))
	fmt.Printf("  TOTAL:  %d\n", totalOps)
	fmt.Println()

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
