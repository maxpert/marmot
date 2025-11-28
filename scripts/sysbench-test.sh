#!/bin/bash
# Sysbench test script for Marmot
# Usage: ./scripts/sysbench-test.sh [threads] [time]
#   threads: number of threads (default: 4)
#   time: test duration in seconds (default: 60)

set -e

THREADS=${1:-4}
TIME=${2:-60}
TABLE_SIZE=10000
TABLES=1
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3307
MYSQL_USER=root
MYSQL_DB=marmot

# Path setup
SYSBENCH_BIN="/tmp/sysbench-custom/bin/sysbench"
SYSBENCH_LUA="/tmp/sysbench-custom/share/sysbench/oltp_read_write.lua"
MARIADB_LIB="/tmp/mariadb-custom/lib/mariadb"

if [[ ! -f "$SYSBENCH_BIN" ]]; then
    echo "ERROR: sysbench not found at $SYSBENCH_BIN"
    echo "Run ./scripts/benchmark-sysbench.sh once to install it"
    exit 1
fi

export DYLD_LIBRARY_PATH="$MARIADB_LIB:$DYLD_LIBRARY_PATH"

COMMON_OPTS="--mysql-host=$MYSQL_HOST --mysql-port=$MYSQL_PORT --mysql-user=$MYSQL_USER --mysql-db=$MYSQL_DB --tables=$TABLES --table_size=$TABLE_SIZE"

echo "=== Sysbench Test ==="
echo "Threads: $THREADS"
echo "Time: ${TIME}s"
echo "Table size: $TABLE_SIZE"
echo ""

# Cleanup any existing tables
echo "=== Cleanup ==="
$SYSBENCH_BIN $SYSBENCH_LUA $COMMON_OPTS cleanup 2>/dev/null || true

# Prepare
echo ""
echo "=== Prepare ==="
$SYSBENCH_BIN $SYSBENCH_LUA $COMMON_OPTS --threads=1 prepare

# Run
echo ""
echo "=== Run (threads=$THREADS, time=${TIME}s) ==="
$SYSBENCH_BIN $SYSBENCH_LUA $COMMON_OPTS --threads=$THREADS --time=$TIME --report-interval=5 run

# Cleanup
echo ""
echo "=== Cleanup ==="
$SYSBENCH_BIN $SYSBENCH_LUA $COMMON_OPTS cleanup

echo ""
echo "=== Done ==="
