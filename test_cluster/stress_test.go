//go:build ignore
// +build ignore

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// StressTest configuration
type StressTestConfig struct {
	MySQLHosts     []string
	Duration       time.Duration
	Concurrency    int
	HotKeys        int     // Number of hot keys to contend on
	ReadWriteRatio float64 // 0.0 = all writes, 1.0 = all reads
	ReportInterval time.Duration
}

// StressTestStats tracks test metrics
type StressTestStats struct {
	TotalOps       int64
	SuccessfulOps  int64
	FailedOps      int64
	ConflictErrors int64
	ReadOps        int64
	WriteOps       int64
	TotalLatencyMs int64
}

func main() {
	// Parse flags
	hosts := flag.String("hosts", "localhost:3307,localhost:3308,localhost:3309", "Comma-separated MySQL host:port list")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	hotKeys := flag.Int("hot-keys", 10, "Number of hot keys to contend on")
	readRatio := flag.Float64("read-ratio", 0.2, "Read/write ratio (0.0-1.0)")
	reportInterval := flag.Duration("report-interval", 5*time.Second, "Stats reporting interval")
	flag.Parse()

	config := StressTestConfig{
		MySQLHosts:     parseHosts(*hosts),
		Duration:       *duration,
		Concurrency:    *concurrency,
		HotKeys:        *hotKeys,
		ReadWriteRatio: *readRatio,
		ReportInterval: *reportInterval,
	}

	fmt.Println("=== Marmot Cluster Stress Test ===")
	fmt.Printf("Hosts: %v\n", config.MySQLHosts)
	fmt.Printf("Duration: %v\n", config.Duration)
	fmt.Printf("Concurrency: %d\n", config.Concurrency)
	fmt.Printf("Hot Keys: %d\n", config.HotKeys)
	fmt.Printf("Read/Write Ratio: %.2f\n", config.ReadWriteRatio)
	fmt.Println("===================================\n")

	// Run stress test
	stats := runStressTest(config)

	// Print final report
	printFinalReport(stats, config.Duration)
}

func parseHosts(hostsStr string) []string {
	var hosts []string
	current := ""
	for _, ch := range hostsStr {
		if ch == ',' {
			if current != "" {
				hosts = append(hosts, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		hosts = append(hosts, current)
	}
	return hosts
}

func runStressTest(config StressTestConfig) *StressTestStats {
	stats := &StressTestStats{}
	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()

	// Create database connections
	dbs := make([]*sql.DB, len(config.MySQLHosts))
	for i, host := range config.MySQLHosts {
		dsn := fmt.Sprintf("root@tcp(%s)/marmot", host)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			fmt.Printf("Failed to connect to %s: %v\n", host, err)
			continue
		}
		defer db.Close()
		dbs[i] = db

		// Setup test table
		setupTestTable(db)
	}

	fmt.Println("Starting stress test...")
	startTime := time.Now()

	// Start stats reporter
	go reportStats(ctx, stats, config.ReportInterval, startTime)

	// Launch worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, dbs, config, stats)
		}(i)
	}

	// Wait for workers to finish
	wg.Wait()

	return stats
}

func setupTestTable(db *sql.DB) {
	// Create test table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS stress_test (
			id INTEGER PRIMARY KEY,
			value INTEGER,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
	}

	// Insert initial data
	for i := 1; i <= 100; i++ {
		db.Exec("INSERT OR IGNORE INTO stress_test (id, value) VALUES (?, 0)", i)
	}
}

func runWorker(ctx context.Context, workerID int, dbs []*sql.DB, config StressTestConfig, stats *StressTestStats) {
	rand.Seed(time.Now().UnixNano() + int64(workerID))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Pick a random database connection
			db := dbs[rand.Intn(len(dbs))]

			// Decide read or write
			isRead := rand.Float64() < config.ReadWriteRatio

			start := time.Now()
			var err error

			if isRead {
				err = executeRead(db, config.HotKeys)
				atomic.AddInt64(&stats.ReadOps, 1)
			} else {
				err = executeWrite(db, config.HotKeys)
				atomic.AddInt64(&stats.WriteOps, 1)
			}

			latency := time.Since(start).Milliseconds()
			atomic.AddInt64(&stats.TotalLatencyMs, latency)
			atomic.AddInt64(&stats.TotalOps, 1)

			if err != nil {
				atomic.AddInt64(&stats.FailedOps, 1)
				if isConflictError(err) {
					atomic.AddInt64(&stats.ConflictErrors, 1)
				}
			} else {
				atomic.AddInt64(&stats.SuccessfulOps, 1)
			}

			// Small delay between operations
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
		}
	}
}

func executeRead(db *sql.DB, hotKeys int) error {
	key := rand.Intn(hotKeys) + 1
	var value int
	err := db.QueryRow("SELECT value FROM stress_test WHERE id = ?", key).Scan(&value)
	return err
}

func executeWrite(db *sql.DB, hotKeys int) error {
	key := rand.Intn(hotKeys) + 1
	newValue := rand.Intn(1000)

	_, err := db.Exec("UPDATE stress_test SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", newValue, key)
	return err
}

func isConflictError(err error) bool {
	if err == nil {
		return false
	}
	// Check for common conflict error messages
	errStr := err.Error()
	return false // TODO: Add actual conflict detection based on error messages
}

func reportStats(ctx context.Context, stats *StressTestStats, interval time.Duration, startTime time.Time) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printProgressReport(stats, time.Since(startTime))
		}
	}
}

func printProgressReport(stats *StressTestStats, elapsed time.Duration) {
	total := atomic.LoadInt64(&stats.TotalOps)
	success := atomic.LoadInt64(&stats.SuccessfulOps)
	failed := atomic.LoadInt64(&stats.FailedOps)
	conflicts := atomic.LoadInt64(&stats.ConflictErrors)
	reads := atomic.LoadInt64(&stats.ReadOps)
	writes := atomic.LoadInt64(&stats.WriteOps)
	totalLatency := atomic.LoadInt64(&stats.TotalLatencyMs)

	opsPerSec := float64(total) / elapsed.Seconds()
	avgLatency := float64(0)
	if total > 0 {
		avgLatency = float64(totalLatency) / float64(total)
	}

	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	fmt.Printf("[%6.1fs] Ops: %6d (%6.1f/s) | Success: %6d (%.1f%%) | Failed: %4d | Conflicts: %4d | Reads: %5d | Writes: %5d | Avg Latency: %.2fms\n",
		elapsed.Seconds(), total, opsPerSec, success, successRate, failed, conflicts, reads, writes, avgLatency)
}

func printFinalReport(stats *StressTestStats, duration time.Duration) {
	total := atomic.LoadInt64(&stats.TotalOps)
	success := atomic.LoadInt64(&stats.SuccessfulOps)
	failed := atomic.LoadInt64(&stats.FailedOps)
	conflicts := atomic.LoadInt64(&stats.ConflictErrors)
	reads := atomic.LoadInt64(&stats.ReadOps)
	writes := atomic.LoadInt64(&stats.WriteOps)
	totalLatency := atomic.LoadInt64(&stats.TotalLatencyMs)

	opsPerSec := float64(total) / duration.Seconds()
	avgLatency := float64(0)
	if total > 0 {
		avgLatency = float64(totalLatency) / float64(total)
	}

	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	fmt.Println("\n=== Final Test Report ===")
	fmt.Printf("Duration:           %v\n", duration)
	fmt.Printf("Total Operations:   %d\n", total)
	fmt.Printf("Throughput:         %.2f ops/sec\n", opsPerSec)
	fmt.Printf("Successful Ops:     %d (%.2f%%)\n", success, successRate)
	fmt.Printf("Failed Ops:         %d\n", failed)
	fmt.Printf("Conflict Errors:    %d\n", conflicts)
	fmt.Printf("Read Operations:    %d\n", reads)
	fmt.Printf("Write Operations:   %d\n", writes)
	fmt.Printf("Average Latency:    %.2f ms\n", avgLatency)
	fmt.Println("========================")
}
