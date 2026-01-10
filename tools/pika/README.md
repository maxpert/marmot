# Pika - Marmot Benchmark Tool

A lightweight benchmark tool for testing Marmot cluster performance with built-in error categorization and cluster verification.

## Building

```bash
cd tools/pika
go build -o pika .
```

## Quick Start

```bash
# Start a 3-node cluster
./examples/run-cluster.sh

# Load initial data (10K records across all nodes)
./tools/pika/pika load \
  --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --records 10000 \
  --create-table

# Run mixed workload benchmark
./tools/pika/pika run \
  --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --workload mixed \
  --threads 16 \
  --time-limit 60s

# Verify cluster consistency
./tools/pika/pika verify \
  --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --samples 100
```

## Commands

### `pika load` - Load Initial Data

```bash
pika load [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--hosts` | `127.0.0.1:3307` | Comma-separated host:port pairs |
| `--database` | `marmot` | Database name |
| `--table` | `benchmarks` | Table name |
| `--records` | `10000` | Number of records to load |
| `--threads` | `10` | Concurrent threads |
| `--batch-size` | `1` | Operations per transaction |
| `--create-table` | `true` | Create table before loading |
| `--drop-existing` | `false` | Drop existing table first |
| `--time-limit` | none | Maximum time (e.g., `30s`, `5m`) |

### `pika run` - Run Benchmark

```bash
pika run [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--hosts` | `127.0.0.1:3307` | Comma-separated host:port pairs |
| `--database` | `marmot` | Database name |
| `--table` | `benchmarks` | Table name |
| `--workload` | `mixed` | Workload type (see below) |
| `--operations` | `50000` | Total operations |
| `--time-limit` | none | Duration (overrides operations) |
| `--threads` | `20` | Concurrent threads |
| `--batch-size` | `1` | Operations per transaction |
| `--retry` | `true` | Retry on conflict/deadlock |
| `--max-retries` | `3` | Max retry attempts |
| `--insert-overlap` | `0` | % inserts targeting existing keys |
| `--verify` | `false` | Run verification after benchmark |
| `--verify-delay` | `5s` | Delay before verification |
| `--verify-samples` | `100` | Rows to verify |

### `pika verify` - Verify Cluster Consistency

```bash
pika verify [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--hosts` | required | Comma-separated host:port pairs (min 2) |
| `--database` | `marmot` | Database name |
| `--table` | `benchmarks` | Table name |
| `--samples` | `100` | Number of random rows to verify |
| `--timeout` | `30s` | Query timeout |

Verification checks:
1. **Row counts** - Compares COUNT(*) across all nodes
2. **Data consistency** - Samples random rows and compares checksums across nodes

## Workload Types

| Workload | Read | Update | Insert | Delete | Upsert |
|----------|------|--------|--------|--------|--------|
| `mixed` | 20% | 30% | 35% | 5% | 10% |
| `read-only` | 100% | 0% | 0% | 0% | 0% |
| `write-only` | 0% | 35% | 35% | 10% | 20% |
| `update-heavy` | 10% | 60% | 10% | 5% | 15% |

Override defaults with `--read-pct`, `--update-pct`, `--insert-pct`, `--delete-pct`, `--upsert-pct`.

## Error Categories

Pika categorizes errors to help diagnose cluster issues:

| Category | Description | Examples |
|----------|-------------|----------|
| **Conflicts** | Distributed coordination failures | Write-write conflicts, partial commits, deadlocks (1213) |
| **Constraints** | Schema violations | UNIQUE constraint, duplicate key (1062) |
| **Timeouts** | Lock/query timeouts | Lock wait timeout (1205) |
| **Connections** | Network errors | Connection refused, reset, broken pipe |
| **Unknown** | Uncategorized | Other errors |

Sample output:
```
Error Categories:
  Conflicts:    42 (expected in distributed writes)
  Constraints:  15 (UNIQUE violations)
  Timeouts:     3
  Connections:  0
  Unknown:      0

Sample Errors (first 3 per category):
  Conflicts:
    - "Error 1213 (40001): Deadlock found when trying to get lock"
    - "Error 1105 (HY000): partial commit: got 0 remote commit acks"
```

## Examples

### High-throughput load test (200K records)
```bash
pika load --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --records 200000 --threads 16 --create-table --drop-existing
```

### Sustained write benchmark with verification
```bash
pika run --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --workload write-only --threads 20 --time-limit 120s \
  --verify --verify-delay 10s
```

### Conflict testing (inserts targeting existing keys)
```bash
pika run --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --workload mixed --insert-overlap 50 --threads 10
```

### Read-heavy workload
```bash
pika run --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --read-pct 80 --update-pct 15 --insert-pct 5 --threads 32
```

### Standalone verification
```bash
pika verify --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --samples 500 --timeout 60s
```

## Output

### Real-time Statistics
- Operations per second (OPS)
- Transaction throughput (for batch mode)
- Error and retry counts

### Final Report
- Per-operation breakdown (READ, UPDATE, INSERT, DELETE, UPSERT)
- Latency percentiles (p50, p90, p95, p99)
- Error categories with sample messages
- Verification results (if enabled)

### Verification Output
```
Row Counts:
  127.0.0.1:3307: 10000
  127.0.0.1:3308: 10000
  127.0.0.1:3309: 10000
  Status: MATCH

Sampled Verification:
  Sampled rows: 100
  Matching:     100
  Mismatched:   0

Cluster verification passed!
```

## Schema

Pika uses a simple key-value schema:

```sql
CREATE TABLE IF NOT EXISTS benchmarks (
    id VARCHAR(64) PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    ...
    field9 TEXT
)
```
