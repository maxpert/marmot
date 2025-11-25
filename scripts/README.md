# Marmot Benchmark Scripts

This directory contains benchmarking tools to measure Marmot's performance.

## Quick Start

```bash
# Quick test (5-10 minutes)
./scripts/benchmark-quick.sh

# Full comparison benchmark (15-20 minutes)
./scripts/benchmark-ycsb.sh
```

## Scripts

### `benchmark-ycsb.sh` - Full Benchmark Suite

**What it does:**
- Installs go-ycsb locally (latest version from pingcap/go-ycsb)
- Runs two complete benchmarks:
  1. WITH group commit enabled (batching on)
  2. WITHOUT group commit (batching off)
- Compares results and shows TPS improvement

**Duration:** ~15-20 minutes

**Output:**
```
╔════════════════════════════════════════════════╗
║            BENCHMARK RESULTS                   ║
╚════════════════════════════════════════════════╝

WITH Group Commit:
  Throughput: 523.4 ops/sec
  INSERT - Count=25000, Avg=15.2ms, 99th=45ms
  UPDATE - Count=15000, Avg=12.8ms, 99th=38ms
  READ - Count=10000, Avg=8.5ms, 99th=22ms

WITHOUT Group Commit:
  Throughput: 107.2 ops/sec
  INSERT - Count=25000, Avg=78.5ms, 99th=210ms
  UPDATE - Count=15000, Avg=65.3ms, 99th=185ms
  READ - Count=10000, Avg=42.1ms, 99th=98ms

════════════════════════════════════════════════
Performance Improvement: 388.1%
TPS Multiplier: 4.88x
════════════════════════════════════════════════
```

**Workload Profile:**
- 10,000 records loaded
- 50,000 operations executed
- 50% inserts, 30% updates, 20% reads
- 20 concurrent threads
- Zipfian distribution (realistic workload)

---

### `benchmark-quick.sh` - Fast Sanity Check

**What it does:**
- Runs a single quick benchmark with group commit enabled
- Useful for quick performance checks during development

**Duration:** ~5 minutes

**Output:**
```
Marmot Quick Benchmark (go-ycsb)

[1/4] Building Marmot...
✓ Built
[2/4] Cleaning up...
[3/4] Starting cluster...
✓ Cluster running
[4/4] Running benchmark...

Loading data (1,000 records)...
Running workload (5,000 operations, 10 threads)...

Run finished, takes 9.523s
Throughput: 525.1 ops/sec

✓ Quick benchmark complete!
```

**Workload Profile:**
- 1,000 records
- 5,000 operations
- 10 concurrent threads
- Uniform distribution

---

## go-ycsb Installation

Both scripts automatically install go-ycsb on first run.

**Location:** `/tmp/marmot-ycsb/bin/go-ycsb`

**Manual installation:**
```bash
mkdir -p /tmp/marmot-ycsb
cd /tmp/marmot-ycsb
git clone https://github.com/pingcap/go-ycsb.git source
cd source
make
mkdir -p /tmp/marmot-ycsb/bin
cp bin/go-ycsb /tmp/marmot-ycsb/bin/go-ycsb
```

**Version:** Latest from https://github.com/pingcap/go-ycsb

---

## Results Files

After running `benchmark-ycsb.sh`, results are saved to `/tmp/marmot-benchmark-results/`:

```
/tmp/marmot-benchmark-results/
├── results_with_batching.load      # Load phase with batching
├── results_with_batching.run       # Benchmark with batching
├── results_without_batching.load   # Load phase without batching
├── results_without_batching.run    # Benchmark without batching
├── workload_marmot                 # YCSB workload configuration
└── workload_quick                  # Quick benchmark workload
```

**Benefits of using /tmp:**
- No .gitignore needed
- Auto-cleanup on reboot
- No clutter in repo
- Standard location for temporary files

---

## Customizing Workloads

### Modify `workload_marmot` for different scenarios:

**Heavy Write Workload:**
```toml
readproportion=0.1
updateproportion=0.2
insertproportion=0.7
```

**Heavy Read Workload:**
```toml
readproportion=0.7
updateproportion=0.2
insertproportion=0.1
```

**Large Dataset:**
```toml
recordcount=100000
operationcount=500000
```

**More Concurrency:**
```toml
threadcount=50  # Change in script, not workload file
```

---

## Comparing with Other Databases

go-ycsb supports many databases. You can compare Marmot against:

```bash
# PostgreSQL
go-ycsb run postgresql -P workload -p pg.host=localhost -p pg.port=5432

# MySQL
go-ycsb run mysql -P workload -p mysql.host=localhost -p mysql.port=3306

# TiDB
go-ycsb run mysql -P workload -p mysql.host=localhost -p mysql.port=4000

# CockroachDB
go-ycsb run postgresql -P workload -p pg.host=localhost -p pg.port=26257
```

---

## Troubleshooting

### go-ycsb build fails
```bash
# Make sure you have Go 1.21+
go version

# Clean and rebuild
rm -rf /tmp/marmot-ycsb
./scripts/benchmark-ycsb.sh
```

### Cluster won't start
```bash
# Clean everything
pkill -f marmot-v2
rm -rf /tmp/marmot-node-*

# Check logs
tail -50 /tmp/node1.log
```

### Low TPS results
- Check CPU usage (`htop` or `top`)
- Ensure cluster is actually running (`ps aux | grep marmot`)
- Verify all 3 nodes started (`lsof -i :3307,3308,3309`)
- Check logs for errors

---

## Expected Performance

### 3-Node Cluster (MacBook Pro M1/M2)

| Configuration | Expected TPS | Latency (p99) |
|--------------|--------------|---------------|
| Group Commit ON | 400-600 | 30-50ms |
| Group Commit OFF | 80-120 | 150-250ms |
| **Improvement** | **5-7x** | **3-5x faster** |

### Single Node

| Configuration | Expected TPS | Latency (p99) |
|--------------|--------------|---------------|
| No replication | 2,000-5,000 | 5-15ms |

---

## CI/CD Integration

Add to your CI pipeline:

```yaml
# .github/workflows/benchmark.yml
- name: Run Performance Benchmark
  run: |
    ./scripts/benchmark-quick.sh
    # Fail if TPS < threshold
    if [ $(grep "Throughput" results.txt | awk '{print $2}' | cut -d. -f1) -lt 400 ]; then
      echo "Performance regression detected!"
      exit 1
    fi
```

---

## References

- **go-ycsb**: https://github.com/pingcap/go-ycsb
- **YCSB Workloads**: https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads
- **TiDB Benchmarks**: https://docs.pingcap.com/tidb/stable/benchmark-tidb-using-ch
