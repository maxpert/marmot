# Cluster Load Test

Run a full cluster load test cycle: start cluster, run load, verify replication.

## Parameters
- **rows**: Number of records to load (default: 100000)
- **workload**: Workload type - "insert" for INSERT-only, "mixed" for R/U/I/D (default: insert)
- **threads**: Number of concurrent threads (default: 16)
- **timeout**: Time limit for the test (default: 60s)
- **batch-size**: Operations per transaction (default: 1 = no batching)

## Instructions

Execute the following load test cycle:

### 1. Cleanup and Start Cluster
```bash
# Start cluster with debug logging
MARMOT_LOG_LEVEL=debug examples/run-cluster.sh
```

### 2. Verify Cluster Health
```bash
# Check all 3 nodes respond
for port in 3307 3308 3309; do
  /opt/homebrew/opt/mysql-client/bin/mysql -h 127.0.0.1 -P $port -u root -e "SELECT 1"
done
```

### 3. Run Load Test

Based on the **workload** parameter:

**If workload = "insert":**
```bash
./tools/pika/pika load \
  --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --database marmot \
  --table benchmarks \
  --records $rows \
  --threads $threads \
  --batch-size $batch-size \
  --time-limit $timeout \
  --create-table \
  --drop-existing
```

**If workload = "mixed":**
First load data if table is empty, then run mixed workload:
```bash
# Check if data exists
count=$(/opt/homebrew/opt/mysql-client/bin/mysql -h 127.0.0.1 -P 3307 -u root -N -e "SELECT COUNT(*) FROM marmot.benchmarks" 2>/dev/null || echo "0")

# Load initial data if needed
if [ "$count" -lt 1000 ]; then
  ./tools/pika/pika load \
    --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
    --database marmot \
    --table benchmarks \
    --records $rows \
    --threads $threads \
    --batch-size $batch-size \
    --create-table \
    --drop-existing
fi

# Run mixed workload
./tools/pika/pika run \
  --hosts "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309" \
  --database marmot \
  --table benchmarks \
  --threads $threads \
  --batch-size $batch-size \
  --time-limit $timeout \
  --workload mixed
```

### 4. Verify Replication
```bash
# Check row counts match across all nodes
for port in 3307 3308 3309; do
  echo "Node $port:"
  /opt/homebrew/opt/mysql-client/bin/mysql -h 127.0.0.1 -P $port -u root -e "SELECT COUNT(*) FROM marmot.benchmarks"
done
```

### 5. Report Results
Summarize:
- Total throughput (ops/sec)
- Transaction throughput (tx/sec) if batching enabled
- Error/retry counts
- Latency percentiles (P50, P90, P99)
- Replication verification (all nodes have same row count)

### 6. On Failure: Collect Diagnostics
If QPS drops to 0 or cluster becomes unresponsive:
```bash
# Collect pprof
for port in 8081 8082 8083; do
  curl -s "http://127.0.0.1:$port/debug/pprof/goroutine?debug=1" > /tmp/pprof-$port-goroutine.txt
  curl -s "http://127.0.0.1:$port/debug/pprof/heap?debug=1" > /tmp/pprof-$port-heap.txt
done

# Collect logs
tail -1000 /tmp/marmot-node-1/marmot.log > /tmp/node1-logs.txt
tail -1000 /tmp/marmot-node-2/marmot.log > /tmp/node2-logs.txt
tail -1000 /tmp/marmot-node-3/marmot.log > /tmp/node3-logs.txt
```

Analyze goroutine dumps and logs to identify the bottleneck.

## User Arguments
$ARGUMENTS
