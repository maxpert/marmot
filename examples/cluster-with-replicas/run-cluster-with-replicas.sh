#!/bin/bash
# Marmot v2.0 - 2-Node Cluster with Read-Only Replicas
#
# Topology:
#   Node 1 (primary)  ←→  Node 2 (primary)     [2-node cluster with QUORUM writes]
#      ↓                      ↓
#   Replica 1A             Replica 2A          [read-only replicas with failover]
#
# Ports:
#   Node 1:     gRPC 8081, MySQL 3307
#   Node 2:     gRPC 8082, MySQL 3308
#   Replica 1A: gRPC 8091, MySQL 3317
#   Replica 2A: gRPC 8092, MySQL 3318

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Generate random secrets
generate_secret() {
    openssl rand -hex 16 2>/dev/null || head -c 32 /dev/urandom | xxd -p | head -c 32
}
CLUSTER_SECRET=$(generate_secret)
REPLICA_SECRET=$(generate_secret)

echo "=== Marmot v2.0 Cluster with Replicas ==="
echo "Starting 2-node cluster + 2 read-only replicas"
echo ""

# Kill any existing marmot processes
echo "Stopping any existing marmot processes..."
pkill -f "marmot-v2" 2>/dev/null || true
sleep 1

# Clean up old data
echo "Cleaning up old data..."
rm -rf /tmp/marmot-cluster

# Build
echo "Building marmot-v2..."
cd "$REPO_ROOT"
go build -tags sqlite_preupdate_hook -o marmot-v2 .

echo "=========================="

# Create data directories
mkdir -p /tmp/marmot-cluster/node-1
mkdir -p /tmp/marmot-cluster/node-2
mkdir -p /tmp/marmot-cluster/replica-1a
mkdir -p /tmp/marmot-cluster/replica-2a

# Generate node config
generate_node_config() {
    local node_id=$1
    local grpc_port=$((8080 + node_id))
    local mysql_port=$((3306 + node_id))
    local other_node=$((3 - node_id))  # 1->2, 2->1
    local other_port=$((8080 + other_node))
    local config_file="/tmp/marmot-cluster/node-${node_id}.toml"

    cat > "$config_file" << EOF
# Marmot v2.0 - Node ${node_id} (auto-generated)
node_id = ${node_id}
data_dir = "/tmp/marmot-cluster/node-${node_id}"

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_advertise_address = "localhost:${grpc_port}"
grpc_port = ${grpc_port}
seed_nodes = ["localhost:${other_port}"]
cluster_secret = "${CLUSTER_SECRET}"
gossip_interval_ms = 1000
gossip_fanout = 2
suspect_timeout_ms = 5000
dead_timeout_ms = 10000

[replica]
secret = "${REPLICA_SECRET}"

[transaction]
heartbeat_timeout_seconds = 10
conflict_window_seconds = 10
lock_wait_timeout_seconds = 50

[replication]
default_write_consistency = "QUORUM"
default_read_consistency = "LOCAL_ONE"
write_timeout_ms = 300000
read_timeout_ms = 2000
enable_anti_entropy = true
anti_entropy_interval_seconds = 30
gc_interval_seconds = 60
delta_sync_threshold_transactions = 10000
delta_sync_threshold_seconds = 3600
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

[coordinator]
prepare_timeout_ms = 120000
commit_timeout_ms = 120000
abort_timeout_ms = 2000

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = ${mysql_port}
max_connections = 100
auto_id_mode = "compact"

[metastore]
cache_size_mb = 64
memtable_size_mb = 64
memtable_count = 2

[logging]
verbose = true
format = "console"

[prometheus]
enabled = true
EOF
    echo "$config_file"
}

# Generate replica config
generate_replica_config() {
    local replica_id=$1
    local grpc_port=$((8090 + replica_id))
    local mysql_port=$((3316 + replica_id))
    local config_file="/tmp/marmot-cluster/replica-${replica_id}a.toml"

    cat > "$config_file" << EOF
# Marmot v2.0 - Replica ${replica_id}A (auto-generated)
node_id = ${replica_id}00
data_dir = "/tmp/marmot-cluster/replica-${replica_id}a"

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_port = ${grpc_port}
grpc_advertise_address = "localhost:${grpc_port}"

[replica]
enabled = true
follow_addresses = ["localhost:8081", "localhost:8082"]
secret = "${REPLICA_SECRET}"
discovery_interval_seconds = 30
failover_timeout_seconds = 60
reconnect_interval_seconds = 5
reconnect_max_backoff_seconds = 30
initial_sync_timeout_minutes = 30

[transaction]
heartbeat_timeout_seconds = 10
conflict_window_seconds = 10
lock_wait_timeout_seconds = 50

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = ${mysql_port}
max_connections = 100

[metastore]
cache_size_mb = 64
memtable_size_mb = 64
memtable_count = 2

[logging]
verbose = true
format = "console"

[prometheus]
enabled = true
EOF
    echo "$config_file"
}

# Generate all configs
NODE1_CONFIG=$(generate_node_config 1)
NODE2_CONFIG=$(generate_node_config 2)
REPLICA1_CONFIG=$(generate_replica_config 1)
REPLICA2_CONFIG=$(generate_replica_config 2)

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down cluster..."
    kill $node1_pid $node2_pid $replica1a_pid $replica2a_pid 2>/dev/null || true
    wait 2>/dev/null || true
    echo "Cluster stopped"
}
trap cleanup EXIT

echo ""
echo "Starting cluster nodes..."

# Start Node 1 (primary)
"$REPO_ROOT/marmot-v2" --config "$NODE1_CONFIG" > /tmp/marmot-cluster/node-1/marmot.log 2>&1 &
node1_pid=$!
echo "Node 1 started (PID: $node1_pid) - gRPC 8081, MySQL 3307"

sleep 2

# Start Node 2 (primary)
"$REPO_ROOT/marmot-v2" --config "$NODE2_CONFIG" > /tmp/marmot-cluster/node-2/marmot.log 2>&1 &
node2_pid=$!
echo "Node 2 started (PID: $node2_pid) - gRPC 8082, MySQL 3308"

sleep 3

echo ""
echo "Starting read-only replicas..."

# Start Replica 1A
"$REPO_ROOT/marmot-v2" --config "$REPLICA1_CONFIG" > /tmp/marmot-cluster/replica-1a/marmot.log 2>&1 &
replica1a_pid=$!
echo "Replica 1A started (PID: $replica1a_pid) - MySQL 3317"

# Start Replica 2A
"$REPO_ROOT/marmot-v2" --config "$REPLICA2_CONFIG" > /tmp/marmot-cluster/replica-2a/marmot.log 2>&1 &
replica2a_pid=$!
echo "Replica 2A started (PID: $replica2a_pid) - MySQL 3318"

sleep 2

echo ""
echo "=== Cluster Running ==="
echo ""
echo "Primary Nodes (read/write):"
echo "  mysql -h 127.0.0.1 -P 3307 -u root   # Node 1"
echo "  mysql -h 127.0.0.1 -P 3308 -u root   # Node 2"
echo ""
echo "Read-Only Replicas (with transparent failover):"
echo "  mysql -h 127.0.0.1 -P 3317 -u root   # Replica 1A"
echo "  mysql -h 127.0.0.1 -P 3318 -u root   # Replica 2A"
echo ""
echo "Example:"
echo "  # Create a table on primary"
echo "  mysql -h 127.0.0.1 -P 3307 -u root -e \"CREATE TABLE marmot.users (id INT PRIMARY KEY, name TEXT)\""
echo ""
echo "  # Write to primary"
echo "  mysql -h 127.0.0.1 -P 3307 -u root -e \"INSERT INTO marmot.users (id, name) VALUES (1, 'Alice')\""
echo ""
echo "  # Read from replica"
echo "  mysql -h 127.0.0.1 -P 3317 -u root -e \"SELECT * FROM marmot.users\""
echo ""
echo "Logs:"
echo "  tail -f /tmp/marmot-cluster/node-1/marmot.log"
echo "  tail -f /tmp/marmot-cluster/node-2/marmot.log"
echo "  tail -f /tmp/marmot-cluster/replica-1a/marmot.log"
echo "  tail -f /tmp/marmot-cluster/replica-2a/marmot.log"
echo ""
echo "Press Ctrl+C to stop"
echo "================================"
echo ""

# Wait for all processes
wait $node1_pid $node2_pid $replica1a_pid $replica2a_pid
