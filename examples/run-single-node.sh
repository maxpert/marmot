#!/bin/bash
# Marmot v2.0 - Single Node Example
# Simplest way to run Marmot

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Marmot v2.0 Single Node ==="
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
# Marmot v2.0 - Single Node Configuration

node_id = 1
data_dir = "/tmp/marmot-single"

[mvcc]
gc_interval_seconds = 30
gc_retention_hours = 1
heartbeat_timeout_seconds = 10
version_retention_count = 10
conflict_window_seconds = 10

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
write_timeout_ms = 5000
read_timeout_ms = 2000
enable_anti_entropy = false

[connection_pool]
pool_size = 4
max_idle_time_seconds = 10
max_lifetime_seconds = 300

[grpc_client]
keepalive_time_seconds = 10
keepalive_timeout_seconds = 3
max_retries = 3
retry_backoff_ms = 100

[coordinator]
prepare_timeout_ms = 2000
commit_timeout_ms = 2000
abort_timeout_ms = 2000

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = 3306
max_connections = 100

[logging]
verbose = true
format = "console"

[prometheus]
enabled = true
TOML

echo "✓ Configuration created"

 go build -tags sqlite_preupdate_hook -o marmot-v2 .
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
