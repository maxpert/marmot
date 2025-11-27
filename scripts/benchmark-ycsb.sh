#!/bin/bash
set -e

# Marmot TPS Benchmark using go-ycsb
# Measures transaction throughput with lock waiting enabled

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
YCSB_DIR="/tmp/marmot-ycsb"
YCSB_BIN="$YCSB_DIR/bin/go-ycsb"
RESULTS_DIR="/tmp/marmot-benchmark-results"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Marmot TPS Benchmark Suite (go-ycsb)        ║${NC}"
echo -e "${BLUE}║   Hybrid Optimistic-Pessimistic Transactions  ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════╝${NC}"
echo ""

# Step 1: Install go-ycsb locally if not present
install_ycsb() {
    echo -e "${YELLOW}[1/6] Checking go-ycsb installation...${NC}"

    if [ -f "$YCSB_BIN" ]; then
        echo -e "${GREEN}✓ go-ycsb already installed at $YCSB_BIN${NC}"
        return 0
    fi

    echo -e "${YELLOW}Installing go-ycsb (latest version from pingcap)...${NC}"

    # Create directory structure
    mkdir -p "$YCSB_DIR"
    cd "$YCSB_DIR"

    # Clone the latest version
    if [ ! -d "source" ]; then
        git clone --depth 1 https://github.com/pingcap/go-ycsb.git source
    fi

    # Build only MySQL client (avoid SQLite CGO issues)
    cd source
    echo -e "${YELLOW}Building go-ycsb MySQL client (this may take 1-2 minutes)...${NC}"

    # Build the binary
    go build -o bin/go-ycsb cmd/go-ycsb/*.go

    if [ ! -f "bin/go-ycsb" ]; then
        echo -e "${RED}Build failed. Trying with make...${NC}"
        make
    fi

    # Copy binary to bin directory
    mkdir -p "$YCSB_DIR/bin"
    cp bin/go-ycsb "$YCSB_BIN"

    echo -e "${GREEN}✓ go-ycsb installed successfully${NC}"

    cd "$REPO_ROOT"
}

# Step 2: Build Marmot with preupdate hook support
build_marmot() {
    echo ""
    echo -e "${YELLOW}[2/6] Building Marmot with preupdate hook support...${NC}"
    cd "$REPO_ROOT"
    go build -tags=sqlite_preupdate_hook -o marmot-v2 .
    echo -e "${GREEN}✓ Marmot built successfully (with sqlite_preupdate_hook)${NC}"
}

# Step 3: Clean up any existing processes
cleanup() {
    echo ""
    echo -e "${YELLOW}[3/6] Cleaning up existing processes...${NC}"

    # Kill any marmot processes
    pkill -9 -f "marmot-v2" 2>/dev/null || true

    # Kill processes on our ports (MySQL wire protocol and gRPC)
    for port in 3307 3308 3309 8081 8082 8083; do
        lsof -ti:$port 2>/dev/null | xargs kill -9 2>/dev/null || true
    done

    sleep 3
    rm -rf /tmp/marmot-node-* || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Step 4: Start cluster
start_cluster() {
    echo ""
    echo -e "${YELLOW}[4/6] Starting 3-node cluster...${NC}"

    cd "$REPO_ROOT"

    # Start node 1 (seed)
    ./marmot-v2 --config examples/node-1-config.toml > /tmp/node1.log 2>&1 &
    NODE1_PID=$!
    sleep 5

    # Verify node 1 started
    if ! kill -0 $NODE1_PID 2>/dev/null; then
        echo -e "${RED}✗ Node 1 failed to start. Check /tmp/node1.log${NC}"
        tail -20 /tmp/node1.log
        exit 1
    fi

    # Start node 2
    ./marmot-v2 --config examples/node-2-config.toml > /tmp/node2.log 2>&1 &
    NODE2_PID=$!
    sleep 3

    # Verify node 2 started
    if ! kill -0 $NODE2_PID 2>/dev/null; then
        echo -e "${RED}✗ Node 2 failed to start. Check /tmp/node2.log${NC}"
        tail -20 /tmp/node2.log
        exit 1
    fi

    # Start node 3
    ./marmot-v2 --config examples/node-3-config.toml > /tmp/node3.log 2>&1 &
    NODE3_PID=$!
    sleep 3

    # Verify node 3 started
    if ! kill -0 $NODE3_PID 2>/dev/null; then
        echo -e "${RED}✗ Node 3 failed to start. Check /tmp/node3.log${NC}"
        tail -20 /tmp/node3.log
        exit 1
    fi

    echo -e "${GREEN}✓ Cluster started (nodes on ports 3307, 3308, 3309)${NC}"

    # Wait for cluster to stabilize and verify MySQL ports are listening
    echo -e "${YELLOW}Waiting for cluster to stabilize...${NC}"
    sleep 5

    # Verify all MySQL ports are listening
    for port in 3307 3308 3309; do
        if ! lsof -ti:$port >/dev/null 2>&1; then
            echo -e "${RED}✗ Port $port not listening${NC}"
            exit 1
        fi
    done

    echo -e "${GREEN}✓ All nodes ready${NC}"
}

# Step 5: Create YCSB workload file
create_workload() {
    echo ""
    echo -e "${YELLOW}[5/6] Creating YCSB workload configuration...${NC}"

    mkdir -p "$RESULTS_DIR"
    cat > "$RESULTS_DIR/workload_marmot" <<'EOF'
# YCSB Workload for Marmot
# Write-heavy workload to test TPS

recordcount=10000
operationcount=50000
workload=core

readallfields=true

readproportion=0.2
updateproportion=0.3
scanproportion=0.0
insertproportion=0.5

requestdistribution=zipfian
EOF

    echo -e "${GREEN}✓ Workload file created${NC}"
}

# Step 6: Run benchmark
run_benchmark() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Running Benchmark${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════${NC}"

    # Load phase
    echo -e "${YELLOW}Loading test data (10,000 records)...${NC}"
    $YCSB_BIN load mysql \
        -P "$RESULTS_DIR/workload_marmot" \
        -p mysql.host=127.0.0.1 \
        -p mysql.port=3307 \
        -p mysql.user=root \
        -p mysql.db=marmot \
        -p threadcount=10 \
        2>&1 | tee "$RESULTS_DIR/load.log"

    echo ""
    echo -e "${YELLOW}Running workload (50,000 operations)...${NC}"

    # Run phase
    $YCSB_BIN run mysql \
        -P "$RESULTS_DIR/workload_marmot" \
        -p mysql.host=127.0.0.1 \
        -p mysql.port=3307 \
        -p mysql.user=root \
        -p mysql.db=marmot \
        -p threadcount=20 \
        2>&1 | tee "$RESULTS_DIR/run.log"

    echo ""
    echo -e "${GREEN}✓ Benchmark complete${NC}"
}

# Display results
show_results() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║            BENCHMARK RESULTS                   ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════╝${NC}"
    echo ""

    # Extract key metrics from run phase
    local insert_ops=$(grep "^INSERT -" "$RESULTS_DIR/run.log" | grep -o "OPS: [0-9.]*" | awk '{print $2}' | head -1)
    local update_ops=$(grep "^UPDATE -" "$RESULTS_DIR/run.log" | grep -o "OPS: [0-9.]*" | awk '{print $2}' | head -1)
    local read_ops=$(grep "^READ -" "$RESULTS_DIR/run.log" | grep -o "OPS: [0-9.]*" | awk '{print $2}' | head -1)
    local total_ops=$(grep "^TOTAL -" "$RESULTS_DIR/run.log" | grep -o "OPS: [0-9.]*" | awk '{print $2}' | head -1)
    local runtime=$(grep "Run finished" "$RESULTS_DIR/run.log" | grep -o "takes [0-9.]*[a-z]*" | awk '{print $2}')

    echo -e "${GREEN}Transaction Throughput:${NC}"
    echo "  Total Runtime: $runtime"
    echo "  Total Throughput: $total_ops ops/sec"
    echo ""
    echo -e "${YELLOW}Operations:${NC}"
    echo "  INSERT: $insert_ops ops/sec"
    echo "  UPDATE: $update_ops ops/sec"
    echo "  READ: $read_ops ops/sec"
    echo ""

    echo -e "${BLUE}Detailed Stats:${NC}"
    grep "^INSERT -" "$RESULTS_DIR/run.log" | head -1
    grep "^UPDATE -" "$RESULTS_DIR/run.log" | head -1
    grep "^READ -" "$RESULTS_DIR/run.log" | head -1
}

# Main execution
main() {
    cd "$REPO_ROOT"

    # Run all steps
    install_ycsb
    build_marmot
    cleanup
    create_workload
    start_cluster
    run_benchmark

    # Show results
    show_results

    # Cleanup after benchmark
    cleanup

    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  Benchmark Complete!                           ║${NC}"
    echo -e "${GREEN}║  Results: $RESULTS_DIR         ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "View full results:"
    echo "  cat $RESULTS_DIR/run.log"
    echo "  cat $RESULTS_DIR/load.log"
}

# Run main function
main "$@"
