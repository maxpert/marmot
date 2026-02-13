#!/bin/bash
# Marmot v2.1.0-beta - Example 3-Node Cluster
# Full Database Replication: ALL nodes get ALL data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# pprof options
PPROF_ENABLED=false
PPROF_DURATION=30
PPROF_OUTPUT_DIR="/tmp/marmot-pprof"

# Generate random replica secret for PSK auth
generate_secret() {
    openssl rand -hex 16 2>/dev/null || head -c 32 /dev/urandom | xxd -p | head -c 32
}
REPLICA_SECRET=$(generate_secret)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pprof) PPROF_ENABLED=true; shift ;;
        --pprof-duration) PPROF_DURATION="$2"; shift 2 ;;
        --pprof-output) PPROF_OUTPUT_DIR="$2"; shift 2 ;;
        --replica-secret) REPLICA_SECRET="$2"; shift 2 ;;
        --help)
            echo "Usage: $0 [options]"
            echo "  --pprof              Enable CPU profiling"
            echo "  --pprof-duration N   Profile duration in seconds (default: 30)"
            echo "  --pprof-output DIR   Output directory (default: /tmp/marmot-pprof)"
            echo "  --replica-secret S   Set replica PSK secret (default: auto-generated)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== Marmot v2.1.0-beta Example Cluster ==="
echo "Starting 3-node cluster with full database replication"
echo ""

# Kill any existing marmot processes
echo "Stopping any existing marmot processes..."
pkill -f "marmot-v2" 2>/dev/null || true
sleep 1

# Clean up old data
echo "Cleaning up old data..."
rm -rf /tmp/marmot-node-* /tmp/marmot-replica-*

echo "Building new bits..."
cd "$REPO_ROOT"
go build -tags "sqlite_preupdate_hook sqlite_fts5 sqlite_json sqlite_math_functions sqlite_foreign_keys sqlite_stat4 sqlite_vacuum_incr" -o marmot-v2 .

echo "=========================="

# Create database for initial data (will be replicated to all nodes)
create_db() {
    local db_file="$1"
    cat <<EOF | sqlite3 "$db_file"
DROP TABLE IF EXISTS Books;
CREATE TABLE Books (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    author TEXT NOT NULL,
    publication_year INTEGER
);
INSERT INTO Books (title, author, publication_year)
VALUES
('The Hitchhiker''s Guide to the Galaxy', 'Douglas Adams', 1979),
('The Lord of the Rings', 'J.R.R. Tolkien', 1954),
('Harry Potter and the Sorcerer''s Stone', 'J.K. Rowling', 1997),
('The Catcher in the Rye', 'J.D. Salinger', 1951),
('To Kill a Mockingbird', 'Harper Lee', 1960),
('1984', 'George Orwell', 1949),
('The Great Gatsby', 'F. Scott Fitzgerald', 1925);
EOF
    echo "Created initial database: $db_file"
}

# Generate node config
generate_node_config() {
    local node_id=$1
    local grpc_port=$((8080 + node_id))
    local mysql_port=$((3306 + node_id))
    local data_dir="/tmp/marmot-node-${node_id}"
    local config_file="/tmp/marmot-node-${node_id}.toml"

    # Build seed_nodes (all other nodes)
    local seeds=""
    for i in 1 2 3; do
        if [ $i -ne $node_id ]; then
            [ -n "$seeds" ] && seeds="$seeds, "
            seeds="${seeds}\"localhost:$((8080 + i))\""
        fi
    done

    cat > "$config_file" << EOF
# Marmot v2.1.0-beta - Node ${node_id} (auto-generated)
node_id = ${node_id}
data_dir = "${data_dir}"

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_advertise_address = "localhost:${grpc_port}"
grpc_port = ${grpc_port}
seed_nodes = [${seeds}]
gossip_interval_ms = 1000
gossip_fanout = 2
suspect_timeout_ms = 5000
dead_timeout_ms = 10000

[replica]
# PSK secret for read-only replicas to authenticate
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
compression_level = 1  # zstd: 0=disabled, 1=fastest, 4=best

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
cache_size_mb = 128
memtable_size_mb = 64
memtable_count = 2

[logging]
verbose = false
format = "json"

[prometheus]
enabled = true

[batch_commit]
enabled = true
max_batch_size = 350
max_wait_ms = 10
EOF
    echo "$config_file"
}

# Create data directories and databases
for i in 1 2 3; do
    mkdir -p "/tmp/marmot-node-${i}"
    create_db "/tmp/marmot-node-${i}/marmot.db"
done

# Generate configs
CONFIG1=$(generate_node_config 1)
CONFIG2=$(generate_node_config 2)
CONFIG3=$(generate_node_config 3)

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down cluster..."
    kill "$job1" "$job2" "$job3" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "Cluster stopped"
}
trap cleanup EXIT

echo ""
echo "Starting nodes..."
echo "  Node 1: gRPC 8081, MySQL 3307"
echo "  Node 2: gRPC 8082, MySQL 3308"
echo "  Node 3: gRPC 8083, MySQL 3309"
echo ""

# Start Node 1
"$REPO_ROOT/marmot-v2" --config "$CONFIG1" > /tmp/marmot-node-1/marmot.log 2>&1 &
job1=$!
echo "Node 1 started (PID: $job1)"

sleep 2

# Start Node 2
"$REPO_ROOT/marmot-v2" --config "$CONFIG2" > /tmp/marmot-node-2/marmot.log 2>&1 &
job2=$!
echo "Node 2 started (PID: $job2)"

sleep 2

# Start Node 3
"$REPO_ROOT/marmot-v2" --config "$CONFIG3" > /tmp/marmot-node-3/marmot.log 2>&1 &
job3=$!
echo "Node 3 started (PID: $job3)"

sleep 2

echo ""
echo "=== Cluster Running ==="
echo ""
echo "Connect to any node via MySQL protocol:"
echo "  mysql -h 127.0.0.1 -P 3307 -u root"
echo "  mysql -h 127.0.0.1 -P 3308 -u root"
echo "  mysql -h 127.0.0.1 -P 3309 -u root"
echo ""
echo "Example queries:"
echo "  SELECT * FROM Books;"
echo "  INSERT INTO Books (title, author, publication_year) VALUES ('New Book', 'Author', 2024);"
echo ""
echo "Logs:"
echo "  tail -f /tmp/marmot-node-1/marmot.log"
echo "  tail -f /tmp/marmot-node-2/marmot.log"
echo "  tail -f /tmp/marmot-node-3/marmot.log"
echo ""
echo "=== Read-Only Replica ==="
echo "Start a replica with:"
echo "  ./examples/start-replica.sh 1 localhost:8081,localhost:8082,localhost:8083 ${REPLICA_SECRET}"
echo ""
echo "Or set env and run:"
echo "  MARMOT_REPLICA_SECRET=${REPLICA_SECRET} ./examples/start-replica.sh"
echo ""
echo "Press Ctrl+C to stop the cluster"
echo "================================"
echo ""

# Start pprof capture if enabled
if [ "$PPROF_ENABLED" = true ]; then
    mkdir -p "$PPROF_OUTPUT_DIR"
    echo ""
    echo "=== pprof Capture Enabled ==="
    echo "Duration: ${PPROF_DURATION}s"
    echo "Output: $PPROF_OUTPUT_DIR"
    echo ""
    echo "Capturing CPU profiles from all nodes..."

    for port in 8081 8082 8083; do
        node=$((port - 8080))
        echo "  Starting profile capture for node $node (port $port)..."
        curl -s "http://127.0.0.1:${port}/debug/pprof/profile?seconds=${PPROF_DURATION}" \
            > "$PPROF_OUTPUT_DIR/cpu_node${node}.pprof" &
    done

    echo ""
    echo "Profiles will be saved to:"
    echo "  $PPROF_OUTPUT_DIR/cpu_node1.pprof"
    echo "  $PPROF_OUTPUT_DIR/cpu_node2.pprof"
    echo "  $PPROF_OUTPUT_DIR/cpu_node3.pprof"
    echo ""
    echo "Run your workload now! pprof is capturing for ${PPROF_DURATION}s..."
    echo ""
    echo "After capture completes, analyze with:"
    echo "  go tool pprof -http=:9090 $PPROF_OUTPUT_DIR/cpu_node1.pprof"
    echo ""
fi

# Wait for all jobs
wait $job1 $job2 $job3
