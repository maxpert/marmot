#!/bin/bash
set -e

# Marmot Sysbench Benchmark
# Based on CockroachDB/TiDB benchmark methodology
# Tests OLTP workloads with varying thread counts

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="/tmp/marmot-sysbench-results"

# Custom sysbench built against mariadb-connector-c 3.3.8 (without forced SSL)
# To rebuild: see scripts/README.md or run make sysbench-setup
SYSBENCH_BIN="${SYSBENCH_BIN:-/tmp/sysbench-custom/bin/sysbench}"
MARIADB_LIB="${MARIADB_LIB:-/tmp/mariadb-custom/lib/mariadb}"

# Configuration - tune these for your machine
TABLES=${TABLES:-4}              # Number of tables
TABLE_SIZE=${TABLE_SIZE:-10000}  # Rows per table
TIME=${TIME:-60}                 # Seconds per test
REPORT_INTERVAL=${REPORT_INTERVAL:-10}  # Report every N seconds
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_PORT=${MYSQL_PORT:-3307}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_DB=${MYSQL_DB:-marmot}

# pprof configuration
PPROF_ENABLED=${PPROF_ENABLED:-false}
PPROF_DURATION=${PPROF_DURATION:-30}  # seconds to capture

# Thread counts to test (CockroachDB style)
THREAD_COUNTS=${THREAD_COUNTS:-"1 4 8 16 32"}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║         Marmot Sysbench Benchmark Suite                    ║${NC}"
    echo -e "${BLUE}║     OLTP Performance Testing (CockroachDB-style)          ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

check_sysbench() {
    echo -e "${YELLOW}[1/6] Checking sysbench installation...${NC}"

    # Check for custom sysbench (required for macOS due to mariadb-connector SSL issue)
    if [ -x "$SYSBENCH_BIN" ] && [ -d "$MARIADB_LIB" ]; then
        export DYLD_LIBRARY_PATH="$MARIADB_LIB:$DYLD_LIBRARY_PATH"
        local version=$("$SYSBENCH_BIN" --version)
        echo -e "${GREEN}✓ Custom $version${NC}"
        echo -e "${GREEN}  Using: $SYSBENCH_BIN${NC}"
        # Define sysbench function for rest of script
        sysbench() { "$SYSBENCH_BIN" "$@"; }
        export -f sysbench
        return 0
    fi

    # Fall back to system sysbench
    if command -v sysbench &> /dev/null; then
        local version=$(sysbench --version)
        echo -e "${YELLOW}⚠ Using system sysbench: $version${NC}"
        echo -e "${YELLOW}  Note: May fail on macOS due to SSL requirements${NC}"
        return 0
    fi

    echo -e "${RED}✗ sysbench not found.${NC}"
    echo ""
    echo "To build custom sysbench (recommended for macOS):"
    echo "  # Clone and build mariadb-connector-c 3.3.8"
    echo "  git clone --depth 1 --branch v3.3.8 https://github.com/mariadb-corporation/mariadb-connector-c.git /tmp/mariadb-connector"
    echo "  cmake -S /tmp/mariadb-connector -B /tmp/mariadb-connector/build -DCMAKE_INSTALL_PREFIX=/tmp/mariadb-custom -DWITH_EXTERNAL_ZLIB=ON"
    echo "  cmake --build /tmp/mariadb-connector/build && cmake --install /tmp/mariadb-connector/build"
    echo ""
    echo "  # Clone and build sysbench"
    echo "  git clone --depth 1 https://github.com/akopytov/sysbench.git /tmp/sysbench-build/sysbench"
    echo "  cd /tmp/sysbench-build/sysbench && ./autogen.sh"
    echo "  ./configure --with-mysql --with-mysql-includes=/tmp/mariadb-custom/include/mariadb \\"
    echo "    --with-mysql-libs=/tmp/mariadb-custom/lib/mariadb --prefix=/tmp/sysbench-custom --with-system-luajit"
    echo "  make && make install"
    echo ""
    echo "Or install system sysbench (may have SSL issues):"
    echo "  brew install sysbench  (macOS)"
    echo "  apt install sysbench   (Ubuntu/Debian)"
    exit 1
}

build_marmot() {
    echo ""
    echo -e "${YELLOW}[2/6] Building Marmot with preupdate hook support...${NC}"
    cd "$REPO_ROOT"
    go build -tags=sqlite_preupdate_hook -o marmot-v2 .
    echo -e "${GREEN}✓ Marmot built successfully${NC}"
}

cleanup() {
    echo ""
    echo -e "${YELLOW}[3/6] Cleaning up existing processes...${NC}"
    pkill -9 -f "marmot-v2" 2>/dev/null || true
    for port in 3307 3308 3309 8081 8082 8083; do
        lsof -ti:$port 2>/dev/null | xargs kill -9 2>/dev/null || true
    done
    sleep 2
    rm -rf /tmp/marmot-node-* 2>/dev/null || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

start_cluster() {
    echo ""
    echo -e "${YELLOW}[4/6] Starting 3-node cluster...${NC}"
    cd "$REPO_ROOT"

    # Start all nodes quickly so they can discover each other
    ./marmot-v2 --config examples/node-1-config.toml > /tmp/node1.log 2>&1 &
    sleep 1
    ./marmot-v2 --config examples/node-2-config.toml > /tmp/node2.log 2>&1 &
    sleep 1
    ./marmot-v2 --config examples/node-3-config.toml > /tmp/node3.log 2>&1 &

    # Wait for cluster to form and stabilize
    echo "  Waiting for cluster to form..."
    sleep 10

    # Verify all ports are listening
    for port in 3307 3308 3309; do
        if ! lsof -ti:$port >/dev/null 2>&1; then
            echo -e "${RED}✗ Port $port not listening${NC}"
            exit 1
        fi
    done

    # Additional wait for gossip convergence
    sleep 5
    echo -e "${GREEN}✓ Cluster ready (ports 3307, 3308, 3309)${NC}"
}

prepare_data() {
    echo ""
    echo -e "${YELLOW}[5/6] Preparing sysbench data...${NC}"
    echo "  Tables: $TABLES"
    echo "  Rows per table: $TABLE_SIZE"
    echo "  Total rows: $((TABLES * TABLE_SIZE))"
    echo ""

    mkdir -p "$RESULTS_DIR"

    # Prepare tables
    sysbench oltp_read_write \
        --db-driver=mysql \
        --mysql-host=$MYSQL_HOST \
        --mysql-port=$MYSQL_PORT \
        --mysql-user=$MYSQL_USER \
        --mysql-db=$MYSQL_DB \
        --tables=$TABLES \
        --table-size=$TABLE_SIZE \
        prepare 2>&1 | tee "$RESULTS_DIR/prepare.log"

    echo -e "${GREEN}✓ Data prepared${NC}"
}

capture_pprof() {
    local workload=$1
    local threads=$2
    local duration=$3

    if [ "$PPROF_ENABLED" != "true" ]; then
        return
    fi

    echo -e "${YELLOW}  Capturing pprof profiles (${duration}s)...${NC}"

    # Capture CPU profiles from all 3 nodes in parallel
    for port in 8081 8082 8083; do
        local node=$((port - 8080))
        curl -s "http://127.0.0.1:${port}/debug/pprof/profile?seconds=${duration}" \
            > "$RESULTS_DIR/cpu_${workload}_t${threads}_node${node}.pprof" 2>/dev/null &
    done

    # Wait for profile capture to complete
    wait

    echo -e "${GREEN}  ✓ pprof captured${NC}"
}

run_workload() {
    local workload=$1
    local threads=$2
    local label=$3

    echo ""
    echo -e "${CYAN}Running: $label (threads=$threads, time=${TIME}s)${NC}"

    # Start pprof capture in background if enabled
    if [ "$PPROF_ENABLED" = "true" ]; then
        capture_pprof "$workload" "$threads" "$PPROF_DURATION" &
        PPROF_PID=$!
    fi

    sysbench $workload \
        --db-driver=mysql \
        --mysql-host=$MYSQL_HOST \
        --mysql-port=$MYSQL_PORT \
        --mysql-user=$MYSQL_USER \
        --mysql-db=$MYSQL_DB \
        --tables=$TABLES \
        --table-size=$TABLE_SIZE \
        --threads=$threads \
        --time=$TIME \
        --report-interval=$REPORT_INTERVAL \
        run 2>&1 | tee "$RESULTS_DIR/${workload}_t${threads}.log"

    # Wait for pprof to finish if it was started
    if [ "$PPROF_ENABLED" = "true" ] && [ -n "$PPROF_PID" ]; then
        wait $PPROF_PID 2>/dev/null || true
    fi
}

extract_metrics() {
    local logfile=$1
    local tps=$(grep "transactions:" "$logfile" | awk '{print $3}' | tr -d '(')
    local qps=$(grep "queries:" "$logfile" | awk '{print $3}' | tr -d '(')
    local lat_avg=$(grep "avg:" "$logfile" | tail -1 | awk '{print $2}')
    local lat_p95=$(grep "95th percentile:" "$logfile" | awk '{print $3}')
    local lat_p99=$(grep "99th percentile:" "$logfile" 2>/dev/null | awk '{print $3}')
    echo "$tps,$qps,$lat_avg,$lat_p95,$lat_p99"
}

run_benchmark() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Running Benchmarks${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"

    # Initialize CSV results
    echo "workload,threads,tps,qps,lat_avg_ms,lat_p95_ms,lat_p99_ms" > "$RESULTS_DIR/results.csv"

    # Test 1: OLTP Read/Write (mixed workload - most important)
    echo ""
    echo -e "${YELLOW}═══ Test 1: OLTP Read/Write (Mixed) ═══${NC}"
    for threads in $THREAD_COUNTS; do
        run_workload "oltp_read_write" $threads "OLTP Read/Write"
        metrics=$(extract_metrics "$RESULTS_DIR/oltp_read_write_t${threads}.log")
        echo "oltp_read_write,$threads,$metrics" >> "$RESULTS_DIR/results.csv"
    done

    # Test 2: OLTP Read Only
    echo ""
    echo -e "${YELLOW}═══ Test 2: OLTP Read Only ═══${NC}"
    for threads in $THREAD_COUNTS; do
        run_workload "oltp_read_only" $threads "OLTP Read Only"
        metrics=$(extract_metrics "$RESULTS_DIR/oltp_read_only_t${threads}.log")
        echo "oltp_read_only,$threads,$metrics" >> "$RESULTS_DIR/results.csv"
    done

    # Test 3: OLTP Write Only
    echo ""
    echo -e "${YELLOW}═══ Test 3: OLTP Write Only ═══${NC}"
    for threads in $THREAD_COUNTS; do
        run_workload "oltp_write_only" $threads "OLTP Write Only"
        metrics=$(extract_metrics "$RESULTS_DIR/oltp_write_only_t${threads}.log")
        echo "oltp_write_only,$threads,$metrics" >> "$RESULTS_DIR/results.csv"
    done

    # Test 4: Point Select (pure read latency)
    echo ""
    echo -e "${YELLOW}═══ Test 4: Point Select ═══${NC}"
    for threads in $THREAD_COUNTS; do
        run_workload "oltp_point_select" $threads "Point Select"
        metrics=$(extract_metrics "$RESULTS_DIR/oltp_point_select_t${threads}.log")
        echo "oltp_point_select,$threads,$metrics" >> "$RESULTS_DIR/results.csv"
    done

    # Test 5: Insert Only (pure write throughput)
    echo ""
    echo -e "${YELLOW}═══ Test 5: Insert Only ═══${NC}"
    for threads in $THREAD_COUNTS; do
        run_workload "oltp_insert" $threads "Insert Only"
        metrics=$(extract_metrics "$RESULTS_DIR/oltp_insert_t${threads}.log")
        echo "oltp_insert,$threads,$metrics" >> "$RESULTS_DIR/results.csv"
    done

    # Test 6: Update Index (write contention)
    echo ""
    echo -e "${YELLOW}═══ Test 6: Update Index ═══${NC}"
    for threads in $THREAD_COUNTS; do
        run_workload "oltp_update_index" $threads "Update Index"
        metrics=$(extract_metrics "$RESULTS_DIR/oltp_update_index_t${threads}.log")
        echo "oltp_update_index,$threads,$metrics" >> "$RESULTS_DIR/results.csv"
    done
}

cleanup_data() {
    echo ""
    echo -e "${YELLOW}Cleaning up sysbench tables...${NC}"
    sysbench oltp_read_write \
        --db-driver=mysql \
        --mysql-host=$MYSQL_HOST \
        --mysql-port=$MYSQL_PORT \
        --mysql-user=$MYSQL_USER \
        --mysql-db=$MYSQL_DB \
        --tables=$TABLES \
        cleanup 2>/dev/null || true
}

show_results() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║                    BENCHMARK RESULTS                       ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${GREEN}Configuration:${NC}"
    echo "  Tables: $TABLES"
    echo "  Rows per table: $TABLE_SIZE"
    echo "  Test duration: ${TIME}s per workload"
    echo "  Thread counts: $THREAD_COUNTS"
    echo ""

    echo -e "${GREEN}Results Summary (CSV):${NC}"
    echo ""
    column -t -s',' "$RESULTS_DIR/results.csv"
    echo ""

    # Find best TPS for each workload
    echo -e "${GREEN}Peak Performance:${NC}"
    for workload in oltp_read_write oltp_read_only oltp_write_only oltp_point_select oltp_insert oltp_update_index; do
        local best=$(grep "^$workload," "$RESULTS_DIR/results.csv" | sort -t',' -k3 -rn | head -1)
        if [ -n "$best" ]; then
            local threads=$(echo "$best" | cut -d',' -f2)
            local tps=$(echo "$best" | cut -d',' -f3)
            local lat=$(echo "$best" | cut -d',' -f5)
            printf "  %-20s %6s TPS @ %2s threads (P95: %sms)\n" "$workload:" "$tps" "$threads" "$lat"
        fi
    done
}

print_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --tables N        Number of tables (default: 4)"
    echo "  --table-size N    Rows per table (default: 10000)"
    echo "  --time N          Seconds per test (default: 60)"
    echo "  --threads 'N ...' Thread counts to test (default: '1 4 8 16 32')"
    echo "  --port N          MySQL port (default: 3307)"
    echo "  --skip-cluster    Don't start cluster (use existing)"
    echo "  --pprof           Enable CPU profiling during benchmarks"
    echo "  --pprof-duration N  Profile duration in seconds (default: 30)"
    echo "  --help            Show this help"
    echo ""
    echo "Environment variables:"
    echo "  TABLES, TABLE_SIZE, TIME, THREAD_COUNTS, MYSQL_PORT, PPROF_ENABLED"
    echo ""
    echo "Examples:"
    echo "  $0                           # Default settings"
    echo "  $0 --tables 8 --table-size 50000 --time 120"
    echo "  $0 --threads '1 2 4 8 16 32 64'"
    echo "  $0 --pprof --threads '8'     # Profile with 8 threads"
    echo "  TABLES=16 TABLE_SIZE=100000 $0"
}

# Parse arguments
SKIP_CLUSTER=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --tables) TABLES="$2"; shift 2 ;;
        --table-size) TABLE_SIZE="$2"; shift 2 ;;
        --time) TIME="$2"; shift 2 ;;
        --threads) THREAD_COUNTS="$2"; shift 2 ;;
        --port) MYSQL_PORT="$2"; shift 2 ;;
        --skip-cluster) SKIP_CLUSTER=true; shift ;;
        --pprof) PPROF_ENABLED=true; shift ;;
        --pprof-duration) PPROF_DURATION="$2"; shift 2 ;;
        --help) print_usage; exit 0 ;;
        *) echo "Unknown option: $1"; print_usage; exit 1 ;;
    esac
done

main() {
    cd "$REPO_ROOT"
    print_header

    check_sysbench

    if [ "$SKIP_CLUSTER" = false ]; then
        build_marmot
        cleanup
        start_cluster
    fi

    prepare_data
    run_benchmark
    show_results

    if [ "$SKIP_CLUSTER" = false ]; then
        cleanup_data
        cleanup
    fi

    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  Benchmark Complete!                                       ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Full results: $RESULTS_DIR/results.csv"
    echo "Individual logs: $RESULTS_DIR/*.log"
}

main "$@"
