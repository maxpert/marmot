#!/bin/bash
# Marmot v2.9.0-beta - Single Node Example
# Simplest way to run Marmot

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Marmot v2.9.0-beta Single Node ==="
echo ""

# Kill any existing marmot processes
echo "Stopping any existing marmot processes..."
pkill -f "marmot-v2" 2>/dev/null || true
lsof -ti:3306 -ti:8080 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1

# Clean up old data
echo "Cleaning up old data..."
rm -rf /tmp/marmot-single

# Create data directory
mkdir -p /tmp/marmot-single

# Create config
cat > /tmp/marmot-single/config.toml <<'TOML'
# Marmot v2.9.0-beta - Single Node Configuration (Optimized for Benchmarks)

node_id = 1
data_dir = "/tmp/marmot-single"

[transaction]
heartbeat_timeout_seconds = 10
conflict_window_seconds = 10
lock_wait_timeout_seconds = 50

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_port = 8080
seed_nodes = []  # No peers - single node
gossip_interval_ms = 1000
gossip_fanout = 2
suspect_timeout_ms = 5000
dead_timeout_ms = 10000

[replication]
default_write_consistency = "LOCAL_ONE"
default_read_consistency = "LOCAL_ONE"
write_timeout_ms = 300000
read_timeout_ms = 2000
enable_anti_entropy = false
gc_min_retention_hours = 2
gc_max_retention_hours = 24

[connection_pool]
pool_size = 4
max_idle_time_seconds = 10
max_lifetime_seconds = 300

[grpc_client]
keepalive_time_seconds = 10
keepalive_timeout_seconds = 3
max_retries = 3
retry_backoff_ms = 100
compression_level = 1  # zstd: 0=disabled, 1=fastest, 4=best

[coordinator]
prepare_timeout_ms = 120000
commit_timeout_ms = 120000
abort_timeout_ms = 2000

[query_pipeline]
transpiler_cache_size = 10000
validator_pool_size = 8

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = 3306
max_connections = 100
auto_id_mode = "compact"

[metastore]
cache_size_mb = 128
memtable_size_mb = 64
memtable_count = 2
l0_compaction_threshold = 500
l0_stop_writes = 1000
wal_bytes_per_sync_kb = 512

[logging]
verbose = false
format = "json"

[prometheus]
enabled = true

[batch_commit]
enabled = true
max_batch_size = 100
max_wait_ms = 1
checkpoint_enabled = true
checkpoint_passive_thresh_mb = 4.0
checkpoint_restart_thresh_mb = 16.0
allow_dynamic_batch_size = true
TOML

echo "✓ Configuration created"

 go build -tags "sqlite_preupdate_hook sqlite_fts5 sqlite_json sqlite_math_functions sqlite_foreign_keys sqlite_stat4 sqlite_vacuum_incr" -o marmot-v2 .
echo "✓ Build complete"

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down..."
    kill "$pid" 2>/dev/null || true
    echo "✓ Stopped"
}

trap cleanup EXIT

echo ""
echo "Starting Marmot single node..."
"$REPO_ROOT/marmot-v2" --config /tmp/marmot-single/config.toml > /tmp/marmot-single/marmot.log 2>&1 &
pid=$!

sleep 2

if ps -p $pid > /dev/null; then
    echo "✓ Marmot started (PID: $pid)"
else
    echo "✗ Failed to start. Check /tmp/marmot-single/marmot.log"
    exit 1
fi

echo ""
echo "=== Node Running ==="
echo ""
echo "Connect via MySQL:"
echo "  mysql -h 127.0.0.1 -P 3306 -u root"
echo ""
echo "Example queries:"
echo "  CREATE TABLE marmot.users (id INT PRIMARY KEY, name TEXT);"
echo "  INSERT INTO marmot.users (id, name) VALUES (1, 'Alice');"
echo "  SELECT * FROM marmot.users;"
echo ""
echo "Logs:"
echo "  tail -f /tmp/marmot-single/marmot.log"
echo ""
echo "Ports:"
echo "  gRPC:  8080"
echo "  MySQL: 3306"
echo ""
echo "Press Ctrl+C to stop"
echo "==================="
echo ""

# Wait for process
wait $pid
