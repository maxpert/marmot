#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

MYSQL_CLI="/opt/homebrew/opt/mysql-client/bin/mysql --protocol=TCP"

# Test counter
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Helper function to run a test
run_test() {
    local test_name="$1"
    local test_func="$2"

    echo ""
    echo -e "${BLUE}════════════════════════════════════════${NC}"
    echo -e "${BLUE}Test: $test_name${NC}"
    echo -e "${BLUE}════════════════════════════════════════${NC}"

    TESTS_RUN=$((TESTS_RUN + 1))

    if $test_func; then
        echo -e "${GREEN}✓ PASS: $test_name${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}✗ FAIL: $test_name${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# Helper function to verify replication across all nodes
verify_replication() {
    local test_desc="$1"
    local query="$2"
    local expected="$3"

    sleep 2  # Give time for replication

    echo "Verifying: $test_desc"

    # Query all nodes
    result1=$($MYSQL_CLI -h localhost -P 3307 -u root testdb -e "$query" 2>&1 | tail -1 || echo "ERROR")
    result2=$($MYSQL_CLI -h localhost -P 3308 -u root testdb -e "$query" 2>&1 | tail -1 || echo "ERROR")
    result3=$($MYSQL_CLI -h localhost -P 3309 -u root testdb -e "$query" 2>&1 | tail -1 || echo "ERROR")

    if [[ "$result1" == "$expected" ]] && [[ "$result2" == "$expected" ]] && [[ "$result3" == "$expected" ]]; then
        echo -e "  ${GREEN}✓ All nodes converged: $expected${NC}"
        return 0
    else
        echo -e "  ${RED}✗ Nodes diverged!${NC}"
        echo "    Node 1: $result1"
        echo "    Node 2: $result2"
        echo "    Node 3: $result3"
        echo "    Expected: $expected"
        return 1
    fi
}

# Test functions

test_create_table() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            stock INTEGER DEFAULT 0
        );
    " 2>&1 > /dev/null

    verify_replication "products table exists on all nodes" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='products';" \
        "1"
}

test_alter_table() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        ALTER TABLE products ADD COLUMN description TEXT;
    " 2>&1 > /dev/null

    verify_replication "products table has description column" \
        "SELECT COUNT(*) FROM pragma_table_info('products') WHERE name='description';" \
        "1"
}

test_create_index() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        CREATE INDEX idx_product_name ON products(name);
    " 2>&1 > /dev/null

    verify_replication "index idx_product_name exists on all nodes" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_product_name';" \
        "1"
}

test_insert_data() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        INSERT INTO products (id, name, price, stock) VALUES
            (1, 'Laptop', 999.99, 10),
            (2, 'Mouse', 29.99, 50),
            (3, 'Keyboard', 79.99, 30);
    " 2>&1 > /dev/null

    verify_replication "3 products inserted on all nodes" \
        "SELECT COUNT(*) FROM products;" \
        "3"
}

test_update_data() {
    $MYSQL_CLI -h localhost -P 3308 -u root testdb -e "
        UPDATE products SET price = 899.99 WHERE id = 1;
    " 2>&1 > /dev/null

    verify_replication "laptop price updated on all nodes" \
        "SELECT price FROM products WHERE id = 1;" \
        "899.99"
}

test_delete_data() {
    $MYSQL_CLI -h localhost -P 3309 -u root testdb -e "
        DELETE FROM products WHERE id = 2;
    " 2>&1 > /dev/null

    verify_replication "mouse deleted on all nodes" \
        "SELECT COUNT(*) FROM products;" \
        "2"
}

test_replace_data() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        REPLACE INTO products (id, name, price, stock) VALUES (3, 'Mechanical Keyboard', 129.99, 25);
    " 2>&1 > /dev/null

    verify_replication "keyboard replaced on all nodes" \
        "SELECT price FROM products WHERE id = 3;" \
        "129.99"
}

test_complex_query_join() {
    # Create orders table
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            quantity INTEGER,
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        INSERT INTO orders (id, product_id, quantity) VALUES
            (1, 1, 2),
            (2, 3, 1);
    " 2>&1 > /dev/null

    sleep 2

    # Test JOIN query on all nodes
    result1=$($MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        SELECT COUNT(*) FROM orders o JOIN products p ON o.product_id = p.id;
    " 2>&1 | tail -1)

    result2=$($MYSQL_CLI -h localhost -P 3308 -u root testdb -e "
        SELECT COUNT(*) FROM orders o JOIN products p ON o.product_id = p.id;
    " 2>&1 | tail -1)

    result3=$($MYSQL_CLI -h localhost -P 3309 -u root testdb -e "
        SELECT COUNT(*) FROM orders o JOIN products p ON o.product_id = p.id;
    " 2>&1 | tail -1)

    if [[ "$result1" == "2" ]] && [[ "$result2" == "2" ]] && [[ "$result3" == "2" ]]; then
        echo -e "  ${GREEN}✓ JOIN query works on all nodes${NC}"
        return 0
    else
        echo -e "  ${RED}✗ JOIN query failed${NC}"
        return 1
    fi
}

test_aggregation() {
    result1=$($MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        SELECT SUM(stock) FROM products;
    " 2>&1 | tail -1)

    if [[ "$result1" == "35" ]]; then  # 10 + 25 = 35
        echo -e "  ${GREEN}✓ Aggregation query works${NC}"
        return 0
    else
        echo -e "  ${RED}✗ Aggregation query failed: got $result1, expected 35${NC}"
        return 1
    fi
}

test_group_by() {
    result1=$($MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        SELECT product_id, SUM(quantity) as total FROM orders GROUP BY product_id ORDER BY product_id;
    " 2>&1 | grep -c "^[0-9]")

    if [[ "$result1" == "2" ]]; then
        echo -e "  ${GREEN}✓ GROUP BY query works${NC}"
        return 0
    else
        echo -e "  ${RED}✗ GROUP BY query failed${NC}"
        return 1
    fi
}

test_subquery() {
    result1=$($MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        SELECT name FROM products WHERE id IN (SELECT product_id FROM orders);
    " 2>&1 | grep -c "Laptop\|Keyboard")

    if [[ "$result1" == "2" ]]; then
        echo -e "  ${GREEN}✓ Subquery works${NC}"
        return 0
    else
        echo -e "  ${RED}✗ Subquery failed${NC}"
        return 1
    fi
}

test_transaction_commit() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        BEGIN;
        INSERT INTO products (id, name, price, stock) VALUES (4, 'Monitor', 299.99, 15);
        INSERT INTO products (id, name, price, stock) VALUES (5, 'Webcam', 89.99, 20);
        COMMIT;
    " 2>&1 > /dev/null

    verify_replication "transaction committed on all nodes" \
        "SELECT COUNT(*) FROM products;" \
        "4"
}

test_transaction_rollback() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        BEGIN;
        DELETE FROM products WHERE id = 4;
        ROLLBACK;
    " 2>&1 > /dev/null

    sleep 1

    result1=$($MYSQL_CLI -h localhost -P 3307 -u root testdb -e "SELECT COUNT(*) FROM products;" 2>&1 | tail -1)

    if [[ "$result1" == "4" ]]; then
        echo -e "  ${GREEN}✓ Transaction rollback works${NC}"
        return 0
    else
        echo -e "  ${RED}✗ Transaction rollback failed${NC}"
        return 1
    fi
}

test_create_view() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        CREATE VIEW expensive_products AS
        SELECT * FROM products WHERE price > 100;
    " 2>&1 > /dev/null

    verify_replication "view exists on all nodes" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='expensive_products';" \
        "1"
}

test_drop_view() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        DROP VIEW expensive_products;
    " 2>&1 > /dev/null

    verify_replication "view dropped on all nodes" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='expensive_products';" \
        "0"
}

test_drop_index() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        DROP INDEX idx_product_name;
    " 2>&1 > /dev/null

    verify_replication "index dropped on all nodes" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_product_name';" \
        "0"
}

test_truncate_table() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        TRUNCATE TABLE orders;
    " 2>&1 > /dev/null

    verify_replication "orders table truncated on all nodes" \
        "SELECT COUNT(*) FROM orders;" \
        "0"
}

test_drop_table() {
    $MYSQL_CLI -h localhost -P 3307 -u root testdb -e "
        DROP TABLE orders;
    " 2>&1 > /dev/null

    verify_replication "orders table dropped on all nodes" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='orders';" \
        "0"
}

# Main test execution

echo "========================================"
echo "Marmot v2 - Comprehensive Integration Test"
echo "========================================"
echo ""

# Clean up any existing processes and data
echo "Cleaning up old processes and data..."
pkill -f "marmot-v2" 2>/dev/null || true
sleep 1

# Start 3-node cluster
echo "Starting 3-node cluster..."
./examples/start-seed.sh > /tmp/seed.log 2>&1 &
sleep 3

if ! lsof -ti:3307 > /dev/null 2>&1; then
    echo -e "${RED}✗ Failed to start seed node${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Seed node started (port 8081, mysql 3307)${NC}"

./examples/join-cluster.sh 2 localhost:8081 > /tmp/node2.log 2>&1 &
sleep 4

if ! lsof -ti:3308 > /dev/null 2>&1; then
    echo -e "${RED}✗ Failed to start node 2${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Node 2 started (port 8082, mysql 3308)${NC}"

./examples/join-cluster.sh 3 localhost:8081 > /tmp/node3.log 2>&1 &
sleep 4

if ! lsof -ti:3309 > /dev/null 2>&1; then
    echo -e "${RED}✗ Failed to start node 3${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Node 3 started (port 8083, mysql 3309)${NC}"

echo ""
echo "Waiting for all nodes to be ready..."
# Poll until all nodes can accept connections and execute queries
MAX_WAIT=30  # Maximum 30 seconds
WAIT_COUNT=0
ALL_READY=false

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    # Try to ping all three nodes
    NODE1_OK=false
    NODE2_OK=false
    NODE3_OK=false

    if $MYSQL_CLI -h localhost -P 3307 -u root -e "SELECT 1;" > /dev/null 2>&1; then
        NODE1_OK=true
    fi

    if $MYSQL_CLI -h localhost -P 3308 -u root -e "SELECT 1;" > /dev/null 2>&1; then
        NODE2_OK=true
    fi

    if $MYSQL_CLI -h localhost -P 3309 -u root -e "SELECT 1;" > /dev/null 2>&1; then
        NODE3_OK=true
    fi

    if [ "$NODE1_OK" = true ] && [ "$NODE2_OK" = true ] && [ "$NODE3_OK" = true ]; then
        ALL_READY=true
        break
    fi

    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$ALL_READY" = false ]; then
    echo -e "${RED}✗ Timeout waiting for nodes to be ready${NC}"
    exit 1
fi
echo -e "${GREEN}✓ All nodes ready (waited ${WAIT_COUNT}s)${NC}"

# Poll cluster state table to ensure all nodes are ALIVE
echo "Waiting for cluster membership to stabilize..."
MAX_CLUSTER_WAIT=30
CLUSTER_WAIT_COUNT=0
CLUSTER_STABLE=false

while [ $CLUSTER_WAIT_COUNT -lt $MAX_CLUSTER_WAIT ]; do
    # Query cluster state directly from SQLite (MySQL protocol has issues with system DB)
    ALIVE_COUNT=$(sqlite3 /tmp/marmot-node-1/__marmot_system.db \
        "SELECT COUNT(*) FROM __marmot_cluster_nodes WHERE status='ALIVE';" 2>/dev/null || echo "0")

    if [[ "$ALIVE_COUNT" == "3" ]]; then
        CLUSTER_STABLE=true
        break
    fi

    sleep 1
    CLUSTER_WAIT_COUNT=$((CLUSTER_WAIT_COUNT + 1))
done

if [ "$CLUSTER_STABLE" = false ]; then
    echo -e "${YELLOW}⚠ Warning: Not all nodes reached ALIVE state (${ALIVE_COUNT}/3), continuing anyway${NC}"
else
    echo -e "${GREEN}✓ Cluster membership stabilized - all 3 nodes ALIVE (waited ${CLUSTER_WAIT_COUNT}s)${NC}"
fi

echo ""
echo "Creating test database..."
$MYSQL_CLI -h localhost -P 3307 -u root -e "CREATE DATABASE testdb;" 2>&1 > /dev/null
sleep 2
echo -e "${GREEN}✓ Test database created${NC}"

# Run all tests
run_test "CREATE TABLE" test_create_table
run_test "ALTER TABLE ADD COLUMN" test_alter_table
run_test "CREATE INDEX" test_create_index
run_test "INSERT multiple rows" test_insert_data
run_test "UPDATE data (from different node)" test_update_data
run_test "DELETE data (from third node)" test_delete_data
run_test "REPLACE data" test_replace_data
run_test "Complex query (JOIN)" test_complex_query_join
run_test "Aggregation (SUM)" test_aggregation
run_test "GROUP BY" test_group_by
run_test "Subquery" test_subquery
run_test "Transaction COMMIT" test_transaction_commit
run_test "Transaction ROLLBACK" test_transaction_rollback
run_test "CREATE VIEW" test_create_view
run_test "DROP VIEW" test_drop_view
run_test "DROP INDEX" test_drop_index
run_test "TRUNCATE TABLE" test_truncate_table
run_test "DROP TABLE" test_drop_table

# Summary
echo ""
echo "========================================"
echo "Test Summary"
echo "========================================"
echo -e "Total tests run:    ${BLUE}$TESTS_RUN${NC}"
echo -e "Tests passed:       ${GREEN}$TESTS_PASSED${NC}"
echo -e "Tests failed:       ${RED}$TESTS_FAILED${NC}"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "\n${GREEN}✓ ALL TESTS PASSED!${NC}"
    EXIT_CODE=0
else
    echo -e "\n${RED}✗ SOME TESTS FAILED${NC}"
    EXIT_CODE=1
fi

echo ""
echo "Logs available at:"
echo "  Node 1: tail -f /tmp/seed.log"
echo "  Node 2: tail -f /tmp/node2.log"
echo "  Node 3: tail -f /tmp/node3.log"
echo ""
echo "To cleanup: pkill -f marmot-v2"

exit $EXIT_CODE
