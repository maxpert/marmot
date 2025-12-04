#!/bin/bash
# Marmot v2.0 - 2-Node Cluster with Read-Only Replicas
#
# Topology:
#   Node 1 (primary)  ←→  Node 2 (primary)     [2-node cluster with QUORUM writes]
#      ↓                      ↓
#   Replica 1A             Replica 2A          [read-only replicas]
#
# Ports:
#   Node 1:     gRPC 8081, MySQL 3307
#   Node 2:     gRPC 8082, MySQL 3308
#   Replica 1A: gRPC 8091, MySQL 3317 (follows Node 1)
#   Replica 2A: gRPC 8092, MySQL 3318 (follows Node 2)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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
echo "✓ Build complete"

# Create data directories
mkdir -p /tmp/marmot-cluster/node-1
mkdir -p /tmp/marmot-cluster/node-2
mkdir -p /tmp/marmot-cluster/replica-1a
mkdir -p /tmp/marmot-cluster/replica-2a

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down cluster..."
    kill $node1_pid $node2_pid $replica1a_pid $replica2a_pid 2>/dev/null || true
    wait 2>/dev/null || true
    echo "✓ Cluster stopped"
}
trap cleanup EXIT

echo ""
echo "Starting cluster nodes..."

# Start Node 1 (primary)
"$REPO_ROOT/marmot-v2" --config "$SCRIPT_DIR/node-1-config.toml" > /tmp/marmot-cluster/node-1/marmot.log 2>&1 &
node1_pid=$!
echo "✓ Node 1 started (PID: $node1_pid) - gRPC 8081, MySQL 3307"

sleep 2

# Start Node 2 (primary)
"$REPO_ROOT/marmot-v2" --config "$SCRIPT_DIR/node-2-config.toml" > /tmp/marmot-cluster/node-2/marmot.log 2>&1 &
node2_pid=$!
echo "✓ Node 2 started (PID: $node2_pid) - gRPC 8082, MySQL 3308"

sleep 3

echo ""
echo "Starting read-only replicas..."

# Start Replica 1A (follows Node 1)
"$REPO_ROOT/marmot-v2" --config "$SCRIPT_DIR/replica-1a-config.toml" > /tmp/marmot-cluster/replica-1a/marmot.log 2>&1 &
replica1a_pid=$!
echo "✓ Replica 1A started (PID: $replica1a_pid) - follows Node 1, MySQL 3317"

# Start Replica 2A (follows Node 2)
"$REPO_ROOT/marmot-v2" --config "$SCRIPT_DIR/replica-2a-config.toml" > /tmp/marmot-cluster/replica-2a/marmot.log 2>&1 &
replica2a_pid=$!
echo "✓ Replica 2A started (PID: $replica2a_pid) - follows Node 2, MySQL 3318"

sleep 2

echo ""
echo "=== Cluster Running ==="
echo ""
echo "Primary Nodes (read/write):"
echo "  mysql -h 127.0.0.1 -P 3307 -u root   # Node 1"
echo "  mysql -h 127.0.0.1 -P 3308 -u root   # Node 2"
echo ""
echo "Read-Only Replicas:"
echo "  mysql -h 127.0.0.1 -P 3317 -u root   # Replica 1A (follows Node 1)"
echo "  mysql -h 127.0.0.1 -P 3318 -u root   # Replica 2A (follows Node 2)"
echo ""
echo "Example:"
echo "  # Create a table"
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
