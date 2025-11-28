#!/bin/bash
# Marmot v2.0 - Example 3-Node Cluster
# Full Database Replication: ALL nodes get ALL data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# pprof options
PPROF_ENABLED=false
PPROF_DURATION=30
PPROF_OUTPUT_DIR="/tmp/marmot-pprof"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pprof) PPROF_ENABLED=true; shift ;;
        --pprof-duration) PPROF_DURATION="$2"; shift 2 ;;
        --pprof-output) PPROF_OUTPUT_DIR="$2"; shift 2 ;;
        --help)
            echo "Usage: $0 [options]"
            echo "  --pprof              Enable CPU profiling"
            echo "  --pprof-duration N   Profile duration in seconds (default: 30)"
            echo "  --pprof-output DIR   Output directory (default: /tmp/marmot-pprof)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== Marmot v2.0 Example Cluster ==="
echo "Starting 3-node cluster with full database replication"
echo ""

# Kill any existing marmot processes
echo "Stopping any existing marmot processes..."
pkill -f "marmot-v2" 2>/dev/null || true
sleep 1

# Clean up old data
echo "Cleaning up old data..."
rm -rf /tmp/marmot-node-*

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
    echo "✓ Created initial database: $db_file"
}

# Create data directories
mkdir -p /tmp/marmot-node-1 /tmp/marmot-node-2 /tmp/marmot-node-3

# Create initial database (will replicate to other nodes)
create_db /tmp/marmot-node-1/marmot.db
create_db /tmp/marmot-node-2/marmot.db
create_db /tmp/marmot-node-3/marmot.db

# Build marmot-v2 if needed
if [ ! -f "$REPO_ROOT/marmot-v2" ]; then
    echo "Building marmot-v2..."
    cd "$REPO_ROOT"
    go build -o marmot-v2 .
    echo "✓ Build complete"
fi

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down cluster..."
    kill "$job1" "$job2" "$job3" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "✓ Cluster stopped"
}
trap cleanup EXIT

echo ""
echo "Starting nodes..."
echo "  Node 1: gRPC 8081, MySQL 3307"
echo "  Node 2: gRPC 8082, MySQL 3308"
echo "  Node 3: gRPC 8083, MySQL 3309"
echo ""

# Start Node 1
"$REPO_ROOT/marmot-v2" --config "$SCRIPT_DIR/node-1-config.toml" > /tmp/marmot-node-1/marmot.log 2>&1 &
job1=$!
echo "✓ Node 1 started (PID: $job1)"

sleep 2

# Start Node 2
"$REPO_ROOT/marmot-v2" --config "$SCRIPT_DIR/node-2-config.toml" > /tmp/marmot-node-2/marmot.log 2>&1 &
job2=$!
echo "✓ Node 2 started (PID: $job2)"

sleep 2

# Start Node 3
"$REPO_ROOT/marmot-v2" --config "$SCRIPT_DIR/node-3-config.toml" > /tmp/marmot-node-3/marmot.log 2>&1 &
job3=$!
echo "✓ Node 3 started (PID: $job3)"

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
echo "  INSERT INTO Books (title, author, publication_year) VALUES ('New Book', 'New Author', 2024);"
echo ""
echo "Logs:"
echo "  tail -f /tmp/marmot-node-1/marmot.log"
echo "  tail -f /tmp/marmot-node-2/marmot.log"
echo "  tail -f /tmp/marmot-node-3/marmot.log"
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
