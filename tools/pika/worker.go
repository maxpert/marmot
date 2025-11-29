package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Worker executes operations against the database.
type Worker struct {
	id         int
	pool       *Pool
	table      string
	keyGen     *KeyGenerator
	opSelector *OpSelector
	stats      *Stats
	retry      bool
	maxRetries int
	rng        *rand.Rand
}

// NewWorker creates a new worker.
func NewWorker(id int, pool *Pool, table string, keyGen *KeyGenerator, opSelector *OpSelector, stats *Stats, retry bool, maxRetries int) *Worker {
	return &Worker{
		id:         id,
		pool:       pool,
		table:      table,
		keyGen:     keyGen,
		opSelector: opSelector,
		stats:      stats,
		retry:      retry,
		maxRetries: maxRetries,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(id))),
	}
}

// RunLoad executes insert operations for the load phase.
func (w *Worker) RunLoad(ctx context.Context, startKey, endKey int, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := startKey; i < endKey; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("rec_%012d", i)
		value := generateFieldValue(w.rng)

		start := time.Now()
		err := w.executeWithRetry(ctx, Operation{Type: OpInsert, Key: key, Value: value})
		latency := time.Since(start)

		if err != nil {
			w.stats.RecordError(OpInsert)
		} else {
			w.stats.RecordOp(OpInsert, latency)
		}
	}
}

// RunBenchmark executes the benchmark workload.
func (w *Worker) RunBenchmark(ctx context.Context, opsChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-opsChan:
			if !ok {
				return
			}

			opType := w.opSelector.Select()
			op := w.generateOp(opType)

			start := time.Now()
			err := w.executeWithRetry(ctx, op)
			latency := time.Since(start)

			if err != nil {
				w.stats.RecordError(opType)
			} else {
				w.stats.RecordOp(opType, latency)
				if opType == OpInsert {
					w.keyGen.UpdateMaxKey(1)
				}
			}
		}
	}
}

func (w *Worker) generateOp(opType OpType) Operation {
	var key string
	switch opType {
	case OpInsert:
		key = w.keyGen.NextInsertKey()
	default:
		key = w.keyGen.RandomExistingKey()
	}

	return Operation{
		Type:  opType,
		Key:   key,
		Value: generateFieldValue(w.rng),
	}
}

func (w *Worker) executeWithRetry(ctx context.Context, op Operation) error {
	var lastErr error
	maxAttempts := 1
	if w.retry {
		maxAttempts = w.maxRetries + 1
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter
			backoff := time.Duration(1<<uint(attempt-1)) * 10 * time.Millisecond
			jitter := time.Duration(w.rng.Int63n(int64(backoff / 2)))
			time.Sleep(backoff + jitter)
			w.stats.RecordRetry()
		}

		err := ExecuteOp(ctx, w.pool.Get(), w.table, op)
		if err == nil {
			return nil
		}

		lastErr = err

		if !IsRetryableError(err) {
			break
		}
	}

	return lastErr
}

// executeLoad runs the load phase.
func executeLoad(ctx context.Context, cfg *Config) error {
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║            Pika Load Phase                           ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	fmt.Printf("Hosts:       %s\n", cfg.Hosts)
	fmt.Printf("Database:    %s\n", cfg.Database)
	fmt.Printf("Table:       %s\n", cfg.Table)
	fmt.Printf("Records:     %d\n", cfg.Records)
	fmt.Printf("Threads:     %d\n", cfg.Threads)
	fmt.Printf("CreateTable: %v\n", cfg.CreateTable)
	fmt.Printf("DropExisting: %v\n", cfg.DropExisting)
	fmt.Println()

	pool, err := NewPool(cfg.HostList(), cfg.Database, cfg.Threads)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}
	defer pool.Close()

	fmt.Printf("Connected to %d nodes\n", pool.Size())

	if cfg.CreateTable {
		fmt.Println("Creating table...")
		if err := pool.CreateTable(cfg.Table, cfg.DropExisting); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		fmt.Println("Table created successfully")
		fmt.Println("Waiting 5s for DDL replication...")
		time.Sleep(5 * time.Second) // Wait for DDL replication
	}

	// Get existing row count to offset key generation
	existingRows, err := pool.GetRowCount(cfg.Table)
	if err != nil {
		fmt.Printf("Warning: failed to get row count: %v, starting from 0\n", err)
		existingRows = 0
	} else {
		fmt.Printf("Existing rows: %d (starting from key %d)\n", existingRows, existingRows)
	}

	stats := NewStats()
	keyGen := NewKeyGenerator("rec", existingRows, time.Now().UnixNano())

	// Distribute records across workers
	recordsPerWorker := cfg.Records / cfg.Threads
	remainder := cfg.Records % cfg.Threads

	var wg sync.WaitGroup
	start := time.Now()

	fmt.Printf("Loading %d records with %d threads...\n", cfg.Records, cfg.Threads)

	// Start reporter
	reporterCtx, stopReporter := context.WithCancel(ctx)
	go reportProgress(reporterCtx, stats, "INSERT")

	for i := 0; i < cfg.Threads; i++ {
		wg.Add(1)
		// Offset by existing rows to avoid duplicate key conflicts
		startKey := int(existingRows) + i*recordsPerWorker
		endKey := startKey + recordsPerWorker
		if i == cfg.Threads-1 {
			endKey += remainder
		}

		opSelector := NewOpSelector(WorkloadDistribution{Insert: 100}, time.Now().UnixNano()+int64(i))
		worker := NewWorker(i, pool, cfg.Table, keyGen, opSelector, stats, true, 3)
		go worker.RunLoad(ctx, startKey, endKey, &wg)
	}

	wg.Wait()
	stopReporter()
	elapsed := time.Since(start)

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════")
	fmt.Println("                    LOAD COMPLETE                      ")
	fmt.Println("═══════════════════════════════════════════════════════")
	stats.PrintFinal(elapsed)

	return nil
}

// executeRun runs the benchmark phase.
func executeRun(ctx context.Context, cfg *Config) error {
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║            Pika Benchmark Phase                      ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	dist := cfg.GetWorkloadDistribution()
	if err := dist.Validate(); err != nil {
		return err
	}

	fmt.Printf("Hosts:       %s\n", cfg.Hosts)
	fmt.Printf("Database:    %s\n", cfg.Database)
	fmt.Printf("Table:       %s\n", cfg.Table)
	fmt.Printf("Workload:    %s\n", cfg.Workload)
	fmt.Printf("Distribution: R:%d%% U:%d%% I:%d%% D:%d%% P:%d%%\n",
		dist.Read, dist.Update, dist.Insert, dist.Delete, dist.Upsert)
	fmt.Printf("Operations:  %d\n", cfg.Operations)
	if cfg.Duration > 0 {
		fmt.Printf("Duration:    %s\n", cfg.Duration)
	}
	fmt.Printf("Threads:     %d\n", cfg.Threads)
	fmt.Printf("Retry:       %v (max: %d)\n", cfg.Retry, cfg.MaxRetries)
	fmt.Println()

	pool, err := NewPool(cfg.HostList(), cfg.Database, cfg.Threads)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}
	defer pool.Close()

	fmt.Printf("Connected to %d nodes\n", pool.Size())

	// Get existing row count
	rowCount, err := pool.GetRowCount(cfg.Table)
	if err != nil {
		return fmt.Errorf("failed to get row count: %w", err)
	}
	fmt.Printf("Existing rows: %d\n\n", rowCount)

	stats := NewStats()
	keyGen := NewKeyGenerator("rec", rowCount, time.Now().UnixNano())

	// Create operation channel
	opsChan := make(chan struct{}, cfg.Threads*10)

	var wg sync.WaitGroup
	start := time.Now()

	// Start workers
	for i := 0; i < cfg.Threads; i++ {
		wg.Add(1)
		opSelector := NewOpSelector(dist, time.Now().UnixNano()+int64(i))
		worker := NewWorker(i, pool, cfg.Table, keyGen, opSelector, stats, cfg.Retry, cfg.MaxRetries)
		go worker.RunBenchmark(ctx, opsChan, &wg)
	}

	// Start reporter
	reporterCtx, stopReporter := context.WithCancel(ctx)
	go reportProgress(reporterCtx, stats, cfg.Workload)

	// Feed operations
	if cfg.Duration > 0 {
		deadline := time.After(cfg.Duration)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-deadline:
				break loop
			case opsChan <- struct{}{}:
			}
		}
	} else {
		for i := 0; i < cfg.Operations; i++ {
			select {
			case <-ctx.Done():
				break
			case opsChan <- struct{}{}:
			}
		}
	}

	close(opsChan)
	wg.Wait()
	stopReporter()
	elapsed := time.Since(start)

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════")
	fmt.Println("                  BENCHMARK COMPLETE                   ")
	fmt.Println("═══════════════════════════════════════════════════════")
	stats.PrintFinal(elapsed)

	return nil
}
