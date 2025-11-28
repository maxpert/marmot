#!/bin/bash
set -e

# Marmot Distributed Benchmark using go-ycsb
# Tests multi-node throughput, replication, and consistency

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
YCSB_DIR="/tmp/marmot-ycsb"
YCSB_BIN="$YCSB_DIR/bin/go-ycsb"
RESULTS_DIR="/tmp/marmot-benchmark-results"
MYSQL_CLI="${MYSQL_CLI:-/opt/homebrew/opt/mysql-client/bin/mysql}"

# Node configuration
NODE_PORTS=(3307 3308 3309)
GRPC_PORTS=(8081 8082 8083)

# Benchmark parameters (can override via environment)
RECORD_COUNT="${RECORD_COUNT:-10000}"
OPERATION_COUNT="${OPERATION_COUNT:-50000}"
THREAD_COUNT="${THREAD_COUNT:-20}"
REPLICATION_TIMEOUT="${REPLICATION_TIMEOUT:-60}"  # seconds to wait for replication

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Marmot Distributed Benchmark Suite (go-ycsb)       ║${NC}"
echo -e "${BLUE}║   Multi-Node Throughput & Replication Testing        ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

install_ycsb() {
    echo -e "${YELLOW}[1/6] Checking go-ycsb installation...${NC}"

    if [ -f "$YCSB_BIN" ]; then
        echo -e "${GREEN}  go-ycsb already installed${NC}"
        return 0
    fi

    echo -e "${YELLOW}  Installing go-ycsb...${NC}"
    mkdir -p "$YCSB_DIR"
    cd "$YCSB_DIR"

    if [ ! -d "source" ]; then
        git clone --depth 1 https://github.com/pingcap/go-ycsb.git source
    fi

    cd source
    go build -o bin/go-ycsb cmd/go-ycsb/*.go
    mkdir -p "$YCSB_DIR/bin"
    cp bin/go-ycsb "$YCSB_BIN"

    echo -e "${GREEN}  go-ycsb installed${NC}"
    cd "$REPO_ROOT"
}

build_marmot() {
    echo -e "${YELLOW}[2/6] Building Marmot...${NC}"
    cd "$REPO_ROOT"
    go build -o marmot-v2 .
    echo -e "${GREEN}  Built successfully${NC}"
}

cleanup() {
    echo -e "${YELLOW}[3/6] Cleaning up...${NC}"
    pkill -9 -f "marmot-v2" 2>/dev/null || true
    for port in "${NODE_PORTS[@]}" "${GRPC_PORTS[@]}"; do
        lsof -ti:$port 2>/dev/null | xargs kill -9 2>/dev/null || true
    done
    sleep 2
    rm -rf /tmp/marmot-node-* 2>/dev/null || true
    echo -e "${GREEN}  Cleanup complete${NC}"
}

start_cluster() {
    echo -e "${YELLOW}[4/6] Starting 3-node cluster...${NC}"
    cd "$REPO_ROOT"

    for i in 1 2 3; do
        ./marmot-v2 --config "examples/node-$i-config.toml" > "/tmp/node$i.log" 2>&1 &
        sleep 3
    done

    sleep 5

    # Verify all nodes
    for port in "${NODE_PORTS[@]}"; do
        if ! lsof -ti:$port >/dev/null 2>&1; then
            echo -e "${RED}  Port $port not listening${NC}"
            exit 1
        fi
    done

    echo -e "${GREEN}  Cluster ready (ports: ${NODE_PORTS[*]})${NC}"
}

create_workloads() {
    echo -e "${YELLOW}[5/6] Creating workload configurations...${NC}"
    mkdir -p "$RESULTS_DIR"

    # Standard YCSB workload (write-heavy)
    cat > "$RESULTS_DIR/workload_write" <<EOF
recordcount=$RECORD_COUNT
operationcount=$OPERATION_COUNT
workload=core
readallfields=true
readproportion=0.2
updateproportion=0.3
scanproportion=0.0
insertproportion=0.5
requestdistribution=zipfian
EOF

    echo -e "${GREEN}  Workloads created${NC}"
}

load_data() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Loading Data ($RECORD_COUNT records)${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"

    echo -e "${YELLOW}Loading data via node 1...${NC}"
    $YCSB_BIN load mysql \
        -P "$RESULTS_DIR/workload_write" \
        -p mysql.host=127.0.0.1 \
        -p mysql.port=3307 \
        -p mysql.user=root \
        -p mysql.db=marmot \
        -p threadcount=10 \
        2>&1 | tee "$RESULTS_DIR/load.log"
}

# Wait for replication to converge across all nodes
wait_for_replication() {
    local table="${1:-usertable}"
    local timeout=$REPLICATION_TIMEOUT
    local start_time=$(date +%s)
    local poll_interval=2

    echo -e "${YELLOW}Waiting for replication to converge (timeout: ${timeout}s)...${NC}"

    while true; do
        local counts=()

        # Get row count from each node
        for port in "${NODE_PORTS[@]}"; do
            count=$($MYSQL_CLI -h 127.0.0.1 -P $port -u root -N -e "SELECT COUNT(*) FROM $table" marmot 2>/dev/null || echo "-1")
            counts+=("$count")
        done

        # Check if all counts are the same and valid
        if [ "${counts[0]}" != "-1" ] && [ "${counts[0]}" = "${counts[1]}" ] && [ "${counts[1]}" = "${counts[2]}" ]; then
            echo -e "${GREEN}  Converged: all nodes have ${counts[0]} rows${NC}"
            return 0
        fi

        # Check timeout
        local elapsed=$(($(date +%s) - start_time))
        if [ $elapsed -ge $timeout ]; then
            echo -e "${RED}  Timeout after ${elapsed}s - counts: ${counts[*]}${NC}"
            return 1
        fi

        # Show progress
        printf "\r  Waiting... [%ds] counts: %s" "$elapsed" "${counts[*]}"
        sleep $poll_interval
    done
}

verify_replication() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Replication Verification${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"

    if ! wait_for_replication "usertable"; then
        echo -e "${RED}  Replication verification FAILED${NC}"

        # Show detailed counts
        echo -e "${CYAN}Final row counts:${NC}"
        for port in "${NODE_PORTS[@]}"; do
            count=$($MYSQL_CLI -h 127.0.0.1 -P $port -u root -N -e "SELECT COUNT(*) FROM usertable" marmot 2>/dev/null || echo "error")
            echo "  Node (port $port): $count rows"
        done
        return 1
    fi

    echo -e "${GREEN}  Replication consistent across all nodes${NC}"
    return 0
}

run_multi_node_benchmark() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Multi-Node Concurrent Writes${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"

    echo -e "${YELLOW}Running concurrent workloads against all 3 nodes...${NC}"

    # Run YCSB against each node in parallel
    # Stagger starts slightly to avoid concurrent database initialization
    local pids=()
    for i in "${!NODE_PORTS[@]}"; do
        port=${NODE_PORTS[$i]}
        node_num=$((i + 1))

        $YCSB_BIN run mysql \
            -P "$RESULTS_DIR/workload_write" \
            -p mysql.host=127.0.0.1 \
            -p mysql.port=$port \
            -p mysql.user=root \
            -p mysql.db=marmot \
            -p threadcount=$((THREAD_COUNT / 3)) \
            2>&1 > "$RESULTS_DIR/run_node${node_num}.log" &
        pids+=($!)
        sleep 1  # Stagger to avoid concurrent DDL
    done

    # Wait for all to complete
    echo -e "${YELLOW}Waiting for all workloads to complete...${NC}"
    for pid in "${pids[@]}"; do
        wait $pid
    done

    echo -e "${GREEN}  Multi-node benchmark complete${NC}"
}

run_read_after_write_test() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Read-After-Write Consistency Test${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"

    # Insert a unique row on node 1
    local test_key="raw_test_$(date +%s)"
    local test_value="consistency_check"

    echo -e "${YELLOW}Writing to node 1...${NC}"
    $MYSQL_CLI -h 127.0.0.1 -P 3307 -u root -e \
        "INSERT INTO usertable (YCSB_KEY, FIELD0) VALUES ('$test_key', '$test_value')" marmot

    # Poll until all nodes have the row or timeout
    echo -e "${YELLOW}Waiting for row to replicate...${NC}"
    local timeout=30
    local start_time=$(date +%s)
    local poll_interval=1

    while true; do
        local found_count=0

        for port in "${NODE_PORTS[@]}"; do
            result=$($MYSQL_CLI -h 127.0.0.1 -P $port -u root -N -e \
                "SELECT FIELD0 FROM usertable WHERE YCSB_KEY='$test_key'" marmot 2>/dev/null || echo "")
            if [ "$result" = "$test_value" ]; then
                ((found_count++))
            fi
        done

        if [ $found_count -eq 3 ]; then
            echo -e "${GREEN}  Row replicated to all 3 nodes${NC}"
            break
        fi

        local elapsed=$(($(date +%s) - start_time))
        if [ $elapsed -ge $timeout ]; then
            echo -e "${RED}  Timeout: row only found on $found_count/3 nodes${NC}"
            break
        fi

        printf "\r  Waiting... [%ds] found on %d/3 nodes" "$elapsed" "$found_count"
        sleep $poll_interval
    done

    # Final verification
    echo -e "${CYAN}Final verification:${NC}"
    local success=true
    for port in "${NODE_PORTS[@]}"; do
        result=$($MYSQL_CLI -h 127.0.0.1 -P $port -u root -N -e \
            "SELECT FIELD0 FROM usertable WHERE YCSB_KEY='$test_key'" marmot 2>/dev/null || echo "NOT_FOUND")

        if [ "$result" = "$test_value" ]; then
            echo -e "  Node (port $port): ${GREEN}FOUND${NC}"
        else
            echo -e "  Node (port $port): ${RED}NOT FOUND (got: $result)${NC}"
            success=false
        fi
    done

    if $success; then
        echo -e "${GREEN}  Read-after-write consistency verified${NC}"
    else
        echo -e "${RED}  Read-after-write consistency FAILED${NC}"
    fi
}

show_results() {
    echo ""
    echo -e "${BLUE}╔══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║              BENCHMARK RESULTS                       ║${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════╝${NC}"
    echo ""

    echo -e "${CYAN}Multi-Node Throughput (per node):${NC}"
    for i in 1 2 3; do
        if [ -f "$RESULTS_DIR/run_node${i}.log" ]; then
            echo "  Node $i:"
            grep "^TOTAL" "$RESULTS_DIR/run_node${i}.log" | tail -1 | sed 's/^/    /'
        fi
    done

    # Calculate combined multi-node throughput
    echo ""
    echo -e "${CYAN}Combined Multi-Node Throughput:${NC}"
    local total_ops=0
    for i in 1 2 3; do
        if [ -f "$RESULTS_DIR/run_node${i}.log" ]; then
            ops=$(grep "^TOTAL" "$RESULTS_DIR/run_node${i}.log" | tail -1 | grep -o "OPS: [0-9.]*" | awk '{print $2}')
            if [ -n "$ops" ]; then
                total_ops=$(echo "$total_ops + $ops" | bc)
            fi
        fi
    done
    echo "  Combined: $total_ops ops/sec (across 3 nodes)"
}

main() {
    cd "$REPO_ROOT"

    install_ycsb
    build_marmot
    cleanup
    create_workloads
    start_cluster

    echo -e "${YELLOW}[6/6] Running benchmark...${NC}"
    load_data
    verify_replication
    run_multi_node_benchmark
    verify_replication
    run_read_after_write_test

    show_results

    cleanup

    echo ""
    echo -e "${GREEN}Benchmark complete! Results in: $RESULTS_DIR${NC}"
}

main "$@"
