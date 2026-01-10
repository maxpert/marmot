package main

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	// Connection
	Hosts    string
	Database string
	Table    string

	// Load options
	Records      int
	CreateTable  bool
	DropExisting bool

	// Run options
	Workload   string
	Operations int
	Duration   time.Duration
	Threads    int

	// Workload percentages (-1 means use workload default)
	ReadPct   int
	UpdatePct int
	InsertPct int
	DeletePct int
	UpsertPct int

	// Retry
	Retry      bool
	MaxRetries int

	// Key generation
	InsertOverlap float64 // % of inserts that target existing keys (for conflict testing)

	// Batching
	BatchSize int // Number of operations per transaction (1 = no batching)

	// Verify options
	Verify        bool          // Run verification after benchmark (for run command)
	VerifyDelay   time.Duration // Delay before verification to allow replication (default: 5s)
	VerifySamples int           // Number of random rows to verify (default: 100)
	VerifyTimeout time.Duration // Timeout for verification queries

	// Derived
	hostList []string
}

func (c *Config) Validate() error {
	if c.Hosts == "" {
		return fmt.Errorf("hosts cannot be empty")
	}

	c.hostList = strings.Split(c.Hosts, ",")
	for i, h := range c.hostList {
		c.hostList[i] = strings.TrimSpace(h)
		if c.hostList[i] == "" {
			return fmt.Errorf("empty host in list")
		}
	}

	if c.Database == "" {
		return fmt.Errorf("database cannot be empty")
	}

	if c.Table == "" {
		return fmt.Errorf("table cannot be empty")
	}

	if c.Records < 0 {
		return fmt.Errorf("records must be non-negative")
	}

	if c.Threads < 1 {
		return fmt.Errorf("threads must be at least 1")
	}

	if c.Operations < 0 {
		return fmt.Errorf("operations must be non-negative")
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("max-retries must be non-negative")
	}

	if c.BatchSize < 1 {
		c.BatchSize = 1
	}

	// Validate workload type
	switch c.Workload {
	case "mixed", "write-only", "read-only", "update-heavy":
		// valid
	case "":
		c.Workload = "mixed"
	default:
		return fmt.Errorf("invalid workload: %s (must be mixed|write-only|read-only|update-heavy)", c.Workload)
	}

	return nil
}

func (c *Config) HostList() []string {
	return c.hostList
}

func (c *Config) GetWorkloadDistribution() WorkloadDistribution {
	var dist WorkloadDistribution

	// Start with defaults based on workload type
	switch c.Workload {
	case "mixed":
		dist = WorkloadDistribution{Read: 20, Update: 30, Insert: 35, Delete: 5, Upsert: 10}
	case "write-only":
		dist = WorkloadDistribution{Read: 0, Update: 35, Insert: 35, Delete: 10, Upsert: 20}
	case "read-only":
		dist = WorkloadDistribution{Read: 100, Update: 0, Insert: 0, Delete: 0, Upsert: 0}
	case "update-heavy":
		dist = WorkloadDistribution{Read: 10, Update: 60, Insert: 10, Delete: 5, Upsert: 15}
	}

	// Override with explicit percentages if provided
	if c.ReadPct >= 0 {
		dist.Read = c.ReadPct
	}
	if c.UpdatePct >= 0 {
		dist.Update = c.UpdatePct
	}
	if c.InsertPct >= 0 {
		dist.Insert = c.InsertPct
	}
	if c.DeletePct >= 0 {
		dist.Delete = c.DeletePct
	}
	if c.UpsertPct >= 0 {
		dist.Upsert = c.UpsertPct
	}

	return dist
}

type WorkloadDistribution struct {
	Read   int
	Update int
	Insert int
	Delete int
	Upsert int
}

func (w WorkloadDistribution) Total() int {
	return w.Read + w.Update + w.Insert + w.Delete + w.Upsert
}

func (w WorkloadDistribution) Validate() error {
	total := w.Total()
	if total != 100 {
		return fmt.Errorf("workload percentages must sum to 100, got %d", total)
	}
	return nil
}
