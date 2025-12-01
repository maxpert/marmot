package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "load":
		runLoad(args)
	case "run":
		runBenchmark(args)
	case "version":
		fmt.Printf("pika version %s\n", version)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`pika - Marmot benchmark tool

Usage:
  pika <command> [options]

Commands:
  load      Load initial data into the database
  run       Run benchmark workload
  version   Print version
  help      Show this help

Load Options:
  --hosts         Comma-separated host:port pairs (default: 127.0.0.1:3307)
  --database      Database name (default: marmot)
  --table         Table name (default: benchmarks)
  --records       Number of records to load (default: 10000)
  --threads       Number of concurrent threads (default: 10)
  --create-table  Create table before loading (default: true)
  --drop-existing Drop existing table before creating (default: false)

Run Options:
  --hosts         Comma-separated host:port pairs (default: 127.0.0.1:3307)
  --database      Database name (default: marmot)
  --table         Table name (default: benchmarks)
  --workload      Workload type: mixed|write-only|read-only|update-heavy (default: mixed)
  --operations    Total operations to execute (default: 50000)
  --duration      Duration to run (e.g., 60s), overrides --operations
  --threads       Number of concurrent threads (default: 20)
  --read-pct      Read percentage (overrides workload default)
  --update-pct    Update percentage (overrides workload default)
  --insert-pct    Insert percentage (overrides workload default)
  --delete-pct    Delete percentage (overrides workload default)
  --upsert-pct    Upsert percentage (overrides workload default)
  --retry         Enable retry on conflict/deadlock (default: true)
  --max-retries   Maximum retry attempts (default: 3)

Examples:
  pika load --hosts=127.0.0.1:3307,127.0.0.1:3308 --records=10000 --create-table
  pika run --hosts=127.0.0.1:3307,127.0.0.1:3308 --workload=mixed --operations=50000`)
}

func runLoad(args []string) {
	cfg := &Config{}
	fs := flag.NewFlagSet("load", flag.ExitOnError)

	var timeLimit time.Duration
	fs.DurationVar(&timeLimit, "time-limit", 0, "Maximum time to run (e.g., 30s, 1m)")
	fs.StringVar(&cfg.Hosts, "hosts", "127.0.0.1:3307", "Comma-separated host:port pairs")
	fs.StringVar(&cfg.Database, "database", "marmot", "Database name")
	fs.StringVar(&cfg.Table, "table", "benchmarks", "Table name")
	fs.IntVar(&cfg.Records, "records", 10000, "Number of records to load")
	fs.IntVar(&cfg.Threads, "threads", 10, "Number of concurrent threads")
	fs.BoolVar(&cfg.CreateTable, "create-table", true, "Create table before loading")
	fs.BoolVar(&cfg.DropExisting, "drop-existing", false, "Drop existing table before creating")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if timeLimit > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeLimit)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	// Handle interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nInterrupted, shutting down...")
		cancel()
	}()

	if err := executeLoad(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Load failed: %v\n", err)
		os.Exit(1)
	}
}

func runBenchmark(args []string) {
	cfg := &Config{}
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	var timeLimit time.Duration
	fs.DurationVar(&timeLimit, "time-limit", 0, "Maximum time to run (e.g., 30s, 1m)")
	fs.StringVar(&cfg.Hosts, "hosts", "127.0.0.1:3307", "Comma-separated host:port pairs")
	fs.StringVar(&cfg.Database, "database", "marmot", "Database name")
	fs.StringVar(&cfg.Table, "table", "benchmarks", "Table name")
	fs.StringVar(&cfg.Workload, "workload", "mixed", "Workload type")
	fs.IntVar(&cfg.Operations, "operations", 50000, "Total operations to execute")
	fs.DurationVar(&cfg.Duration, "duration", 0, "Duration to run (overrides --operations)")
	fs.IntVar(&cfg.Threads, "threads", 20, "Number of concurrent threads")
	fs.IntVar(&cfg.ReadPct, "read-pct", -1, "Read percentage (overrides workload)")
	fs.IntVar(&cfg.UpdatePct, "update-pct", -1, "Update percentage (overrides workload)")
	fs.IntVar(&cfg.InsertPct, "insert-pct", -1, "Insert percentage (overrides workload)")
	fs.IntVar(&cfg.DeletePct, "delete-pct", -1, "Delete percentage (overrides workload)")
	fs.IntVar(&cfg.UpsertPct, "upsert-pct", -1, "Upsert percentage (overrides workload)")
	fs.BoolVar(&cfg.Retry, "retry", true, "Enable retry on conflict/deadlock")
	fs.IntVar(&cfg.MaxRetries, "max-retries", 3, "Maximum retry attempts")
	fs.Float64Var(&cfg.InsertOverlap, "insert-overlap", 0, "% of inserts targeting existing keys (0-100, for conflict testing)")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if timeLimit > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeLimit)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	// Handle interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nInterrupted, shutting down...")
		cancel()
	}()

	if err := executeRun(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
		os.Exit(1)
	}
}
