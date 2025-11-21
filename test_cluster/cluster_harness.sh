#!/bin/bash
set -e

# Marmot Test Cluster Harness
# Creates and manages a multi-node Marmot cluster for stress testing

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLUSTER_DIR="$SCRIPT_DIR/cluster_data"
NUM_NODES=${NUM_NODES:-3}
BASE_GRPC_PORT=8080
BASE_MYSQL_PORT=3306

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Build marmot binary
build_marmot() {
    log_info "Building marmot binary..."
    cd "$REPO_ROOT"
    go build -o "$SCRIPT_DIR/marmot" .
    log_success "Marmot binary built successfully"
}

# Create configuration for a node
create_node_config() {
    local node_id=$1
    local grpc_port=$((BASE_GRPC_PORT + node_id))
    local mysql_port=$((BASE_MYSQL_PORT + node_id))
    local data_dir="$CLUSTER_DIR/node$node_id"

    mkdir -p "$data_dir"

    # Build seed nodes list (all other nodes)
    local seed_nodes=""
    for i in $(seq 1 $NUM_NODES); do
        if [ $i -ne $node_id ]; then
            local seed_port=$((BASE_GRPC_PORT + i))
            seed_nodes="$seed_nodes\"localhost:$seed_port\", "
        fi
    done
    seed_nodes="${seed_nodes%, }" # Remove trailing comma

    cat > "$data_dir/config.toml" <<EOF
# Marmot v2.0 Test Node $node_id
# Full Database Replication: ALL nodes get ALL data

node_id = $node_id
data_dir = "$data_dir"

[mvcc]
gc_interval_seconds = 30
gc_retention_hours = 1
heartbeat_timeout_seconds = 10
version_retention_count = 10
conflict_window_seconds = 10

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

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_advertise_address = "localhost:$grpc_port"
grpc_port = $grpc_port
seed_nodes = [$seed_nodes]
gossip_interval_ms = 1000
gossip_fanout = 2
suspect_timeout_ms = 5000
dead_timeout_ms = 10000

[replication]
replication_factor = 3
virtual_nodes = 150
default_write_consistency = "QUORUM"
default_read_consistency = "LOCAL_ONE"
write_timeout_ms = 5000
read_timeout_ms = 2000
enable_anti_entropy = true
anti_entropy_interval_seconds = 600

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = $mysql_port
max_connections = 100

[logging]
verbose = true
format = "console"

[prometheus]
enabled = false
address = "0.0.0.0"
port = $((9090 + node_id))
EOF

    log_success "Created config for node $node_id (gRPC: $grpc_port, MySQL: $mysql_port)"
}

# Start a node
start_node() {
    local node_id=$1
    local data_dir="$CLUSTER_DIR/node$node_id"
    local log_file="$data_dir/marmot.log"
    local pid_file="$data_dir/marmot.pid"

    log_info "Starting node $node_id..."

    "$SCRIPT_DIR/marmot" --config "$data_dir/config.toml" > "$log_file" 2>&1 &
    local pid=$!
    echo $pid > "$pid_file"

    sleep 1

    if ps -p $pid > /dev/null; then
        log_success "Node $node_id started (PID: $pid)"
    else
        log_error "Node $node_id failed to start. Check $log_file"
        return 1
    fi
}

# Stop a node
stop_node() {
    local node_id=$1
    local pid_file="$CLUSTER_DIR/node$node_id/marmot.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null; then
            log_info "Stopping node $node_id (PID: $pid)..."
            kill $pid
            sleep 1
            if ps -p $pid > /dev/null; then
                kill -9 $pid
            fi
            log_success "Node $node_id stopped"
        fi
        rm -f "$pid_file"
    fi
}

# Setup cluster
setup_cluster() {
    log_info "Setting up $NUM_NODES-node cluster..."

    # Clean up existing cluster
    if [ -d "$CLUSTER_DIR" ]; then
        log_warn "Cleaning up existing cluster data..."
        rm -rf "$CLUSTER_DIR"
    fi

    mkdir -p "$CLUSTER_DIR"

    # Build marmot
    build_marmot

    # Create configs for all nodes
    for i in $(seq 1 $NUM_NODES); do
        create_node_config $i
    done

    log_success "Cluster setup complete"
}

# Start cluster
start_cluster() {
    log_info "Starting $NUM_NODES-node cluster..."

    for i in $(seq 1 $NUM_NODES); do
        start_node $i
    done

    log_success "All nodes started"
    log_info "Waiting for gossip to converge..."
    sleep 5
}

# Stop cluster
stop_cluster() {
    log_info "Stopping cluster..."

    for i in $(seq 1 $NUM_NODES); do
        stop_node $i
    done

    log_success "All nodes stopped"
}

# Show cluster status
cluster_status() {
    log_info "Cluster Status:"
    echo ""

    for i in $(seq 1 $NUM_NODES); do
        local pid_file="$CLUSTER_DIR/node$i/marmot.pid"
        local grpc_port=$((BASE_GRPC_PORT + i))
        local mysql_port=$((BASE_MYSQL_PORT + i))

        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if ps -p $pid > /dev/null; then
                echo -e "${GREEN}● Node $i${NC} - PID: $pid, gRPC: $grpc_port, MySQL: $mysql_port"
            else
                echo -e "${RED}● Node $i${NC} - DEAD (stale PID: $pid)"
            fi
        else
            echo -e "${RED}● Node $i${NC} - NOT RUNNING"
        fi
    done
    echo ""
}

# Show logs for a node
show_logs() {
    local node_id=$1
    local log_file="$CLUSTER_DIR/node$node_id/marmot.log"

    if [ -f "$log_file" ]; then
        tail -f "$log_file"
    else
        log_error "No log file found for node $node_id"
        exit 1
    fi
}

# Clean up cluster data
cleanup() {
    log_warn "Cleaning up cluster data..."
    stop_cluster
    rm -rf "$CLUSTER_DIR"
    rm -f "$SCRIPT_DIR/marmot"
    log_success "Cleanup complete"
}

# Show usage
usage() {
    cat <<EOF
Marmot Test Cluster Harness

Usage: $0 <command>

Commands:
    setup       - Build marmot and create configs for all nodes
    start       - Start all nodes in the cluster
    stop        - Stop all nodes in the cluster
    restart     - Restart the cluster (stop + start)
    status      - Show status of all nodes
    logs <N>    - Show logs for node N (tail -f)
    cleanup     - Stop cluster and remove all data
    help        - Show this help message

Environment Variables:
    NUM_NODES   - Number of nodes in cluster (default: 3)

Examples:
    NUM_NODES=5 $0 setup    # Create 5-node cluster
    $0 start                # Start the cluster
    $0 status               # Check node status
    $0 logs 1               # View logs for node 1
    $0 stop                 # Stop the cluster

EOF
}

# Main command dispatcher
case "${1:-}" in
    setup)
        setup_cluster
        ;;
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        stop_cluster
        sleep 2
        start_cluster
        ;;
    status)
        cluster_status
        ;;
    logs)
        if [ -z "${2:-}" ]; then
            log_error "Please specify node number"
            echo "Usage: $0 logs <node_number>"
            exit 1
        fi
        show_logs "$2"
        ;;
    cleanup)
        cleanup
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        log_error "Unknown command: ${1:-}"
        echo ""
        usage
        exit 1
        ;;
esac
