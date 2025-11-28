#!/bin/bash
set -e

# Quick Marmot TPS Benchmark (single run)
# Use this for fast testing - runs only WITH group commit enabled

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
YCSB_BIN="/tmp/marmot-ycsb/bin/go-ycsb"
RESULTS_DIR="/tmp/marmot-benchmark-results"

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}Marmot Quick Benchmark (go-ycsb)${NC}"
echo ""

# Check if go-ycsb is installed
if [ ! -f "$YCSB_BIN" ]; then
    echo -e "${YELLOW}go-ycsb not found. Running full benchmark script to install...${NC}"
    exec "$SCRIPT_DIR/benchmark-ycsb.sh"
fi

# Build Marmot
echo -e "${YELLOW}[1/4] Building Marmot...${NC}"
cd "$REPO_ROOT"
go build -o marmot-v2 .
echo -e "${GREEN}✓ Built${NC}"

# Cleanup
echo -e "${YELLOW}[2/4] Cleaning up...${NC}"
pkill -9 -f "marmot-v2" 2>/dev/null || true
for port in 3307 3308 3309 8081 8082 8083; do
    lsof -ti:$port 2>/dev/null | xargs kill -9 2>/dev/null || true
done
sleep 2
rm -rf /tmp/marmot-node-* || true

# Start cluster
echo -e "${YELLOW}[3/4] Starting cluster...${NC}"
./marmot-v2 --config examples/node-1-config.toml > /tmp/node1.log 2>&1 &
sleep 5
./marmot-v2 --config examples/node-2-config.toml > /tmp/node2.log 2>&1 &
sleep 3
./marmot-v2 --config examples/node-3-config.toml > /tmp/node3.log 2>&1 &
sleep 5
echo -e "${YELLOW}Waiting for cluster to stabilize (10s)...${NC}"
sleep 10
echo -e "${GREEN}✓ Cluster running${NC}"

# Create minimal workload
mkdir -p "$RESULTS_DIR"
cat > "$RESULTS_DIR/workload_quick" <<EOF
recordcount=1000
operationcount=5000
workload=core
readproportion=0.2
updateproportion=0.3
insertproportion=0.5
requestdistribution=uniform
EOF

# Run benchmark
echo -e "${YELLOW}[4/4] Running benchmark...${NC}"
echo ""

echo -e "${BLUE}Loading data (1,000 records)...${NC}"
$YCSB_BIN load mysql \
    -P "$RESULTS_DIR/workload_quick" \
    -p mysql.host=127.0.0.1 \
    -p mysql.port=3307 \
    -p mysql.user=root \
    -p mysql.db=marmot \
    -p threadcount=5

echo ""
echo -e "${BLUE}Running workload (5,000 operations, 10 threads)...${NC}"
$YCSB_BIN run mysql \
    -P "$RESULTS_DIR/workload_quick" \
    -p mysql.host=127.0.0.1 \
    -p mysql.port=3307 \
    -p mysql.user=root \
    -p mysql.db=marmot \
    -p threadcount=10

# Cleanup
echo ""
echo -e "${YELLOW}Cleaning up...${NC}"
pkill -9 -f "marmot-v2" 2>/dev/null || true
for port in 3307 3308 3309 8081 8082 8083; do
    lsof -ti:$port 2>/dev/null | xargs kill -9 2>/dev/null || true
done

echo ""
echo -e "${GREEN}✓ Quick benchmark complete!${NC}"
echo -e "${YELLOW}For full comparison (with/without batching), run: ./scripts/benchmark-ycsb.sh${NC}"
