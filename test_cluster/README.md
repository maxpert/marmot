# Marmot Test Cluster Harness

This directory contains tools for setting up and stress-testing a multi-node Marmot cluster.

## Components

### 1. Cluster Harness (`cluster_harness.sh`)

Manages a local multi-node Marmot cluster for testing.

**Features:**
- Builds real Marmot server binary
- Creates per-node configuration files
- Launches multiple Marmot processes
- Manages node lifecycle (start/stop/restart)
- Monitors cluster status
- Provides log access

### 2. Stress Test (`stress_test.go`)

Concurrent stress test tool that hammers the cluster with transactions.

**Features:**
- Concurrent workers hitting multiple nodes
- Configurable read/write ratios
- Hot key contention testing
- Real-time statistics reporting
- Throughput and latency metrics

## Quick Start

### 1. Setup a 3-Node Cluster

```bash
cd test_cluster
./cluster_harness.sh setup
```

This will:
- Build the `marmot` binary
- Create `cluster_data/node1`, `node2`, `node3` directories
- Generate config files for each node
- Configure gossip to connect nodes

### 2. Start the Cluster

```bash
./cluster_harness.sh start
```

Nodes will start on:
- Node 1: gRPC 8081, MySQL 3307
- Node 2: gRPC 8082, MySQL 3308
- Node 3: gRPC 8083, MySQL 3309

### 3. Check Cluster Status

```bash
./cluster_harness.sh status
```

Output:
```
Cluster Status:

● Node 1 - PID: 12345, gRPC: 8081, MySQL: 3307
● Node 2 - PID: 12346, gRPC: 8082, MySQL: 3308
● Node 3 - PID: 12347, gRPC: 8083, MySQL: 3309
```

### 4. Run Stress Test

```bash
# Build stress test
go build -o stress_test stress_test.go

# Run 30-second test with 10 concurrent workers
./stress_test \
  --hosts localhost:3307,localhost:3308,localhost:3309 \
  --duration 30s \
  --concurrency 10 \
  --hot-keys 10 \
  --read-ratio 0.2
```

Example output:
```
=== Marmot Cluster Stress Test ===
Hosts: [localhost:3307 localhost:3308 localhost:3309]
Duration: 30s
Concurrency: 10
Hot Keys: 10
Read/Write Ratio: 0.20
===================================

Starting stress test...
[   5.0s] Ops:    453 (  90.6/s) | Success:    445 (98.2%) | Failed:    8 | Conflicts:    0 | Reads:    91 | Writes:   362 | Avg Latency: 12.34ms
[  10.0s] Ops:    921 (  92.1/s) | Success:    903 (98.0%) | Failed:   18 | Conflicts:    0 | Reads:   184 | Writes:   737 | Avg Latency: 11.89ms
...

=== Final Test Report ===
Duration:           30s
Total Operations:   2764
Throughput:         92.13 ops/sec
Successful Ops:     2712 (98.12%)
Failed Ops:         52
Conflict Errors:    0
Read Operations:    553
Write Operations:   2211
Average Latency:    12.05 ms
========================
```

### 5. Monitor Node Logs

```bash
# Follow logs for node 1
./cluster_harness.sh logs 1
```

### 6. Stop the Cluster

```bash
./cluster_harness.sh stop
```

### 7. Clean Up Everything

```bash
./cluster_harness.sh cleanup
```

## Advanced Usage

### Custom Cluster Size

```bash
# Create a 5-node cluster
NUM_NODES=5 ./cluster_harness.sh setup
NUM_NODES=5 ./cluster_harness.sh start
```

### Restart Cluster

```bash
./cluster_harness.sh restart
```

### High Contention Test

Test with lots of concurrent workers hitting few hot keys:

```bash
./stress_test \
  --hosts localhost:3307,localhost:3308,localhost:3309 \
  --duration 60s \
  --concurrency 50 \
  --hot-keys 5 \
  --read-ratio 0.1 \
  --report-interval 2s
```

### Write-Heavy Workload

```bash
./stress_test \
  --hosts localhost:3307,localhost:3308,localhost:3309 \
  --duration 30s \
  --concurrency 20 \
  --hot-keys 20 \
  --read-ratio 0.0  # 100% writes
```

### Read-Heavy Workload

```bash
./stress_test \
  --hosts localhost:3307,localhost:3308,localhost:3309 \
  --duration 30s \
  --concurrency 20 \
  --hot-keys 50 \
  --read-ratio 0.8  # 80% reads
```

## Configuration

Each node gets an auto-generated `config.toml` with:

- **Node ID**: Unique integer
- **Ports**: gRPC (8080+N), MySQL (3306+N)
- **Data Directory**: `cluster_data/nodeN/`
- **Seed Nodes**: All other nodes in the cluster
- **MVCC Settings**: Default values from CONFIGURATION.md
- **Logging**: Verbose console logging enabled

You can manually edit `cluster_data/nodeN/config.toml` to customize settings.

## Troubleshooting

### Node Won't Start

Check the log file:
```bash
cat cluster_data/node1/marmot.log
```

Common issues:
- Port already in use
- Database corruption (delete `cluster_data/nodeN/marmot.db`)
- Build errors (run `./cluster_harness.sh setup` again)

### Nodes Not Discovering Each Other

- Check logs for gossip messages
- Verify seed nodes are correct in config files
- Ensure firewall isn't blocking localhost ports

### Stress Test Can't Connect

- Verify cluster is running: `./cluster_harness.sh status`
- Check MySQL ports are open: `nc -zv localhost 3307`
- Ensure MySQL protocol is enabled in config

## Directory Structure

```
test_cluster/
├── cluster_harness.sh    # Cluster management script
├── stress_test.go         # Stress test tool
├── README.md              # This file
├── marmot                 # Built binary (created by setup)
└── cluster_data/          # Created by setup
    ├── node1/
    │   ├── config.toml
    │   ├── marmot.db
    │   ├── marmot.log
    │   └── marmot.pid
    ├── node2/
    │   └── ...
    └── node3/
        └── ...
```

## Architecture

**Marmot v2.0 - Full Database Replication**

- **Replication Model**: Every node has a complete copy of the database
- **Write Path**: Coordinator broadcasts to ALL nodes, waits for QUORUM ACK
- **Read Path**: Can read from ANY node (all have complete data)
- **Consistency**: QUORUM-based commits (majority ACK, not all nodes)
- **Dead Nodes**: Catch up via snapshots when they rejoin
- **No Partitioning**: Unlike Cassandra, we don't partition data across nodes

## Implementation Status

✅ **Phase 1**: gRPC-based gossip protocol - COMPLETE
✅ **Phase 2**: MySQL protocol + HLC - COMPLETE
✅ **Phase 3**: Quorum replication - PARTIALLY COMPLETE
✅ **Phase 6**: MVCC implementation - COMPLETE
✅ **Phase 7**: Full database replication model - COMPLETE
⏳ **Phase 8**: MySQL server integration - IN PROGRESS
⏳ **Phase 4**: Fast snapshots - NOT STARTED
⏳ **Phase 5**: Production hardening - NOT STARTED

## Notes

- This harness uses real Marmot server processes
- Each node runs independently with its own database
- **Full replication**: ALL nodes get ALL data (no partitioning)
- Gossip protocol connects nodes automatically
- MySQL wire protocol allows standard MySQL clients to connect
- MVCC ensures no lost updates under high contention
- Write quorum: Only majority ACK needed (e.g., 2 out of 3 nodes)
