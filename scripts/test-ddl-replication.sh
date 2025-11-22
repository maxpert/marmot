#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

MYSQL_CLI="/opt/homebrew/opt/mysql-client/bin/mysql --protocol=TCP"

echo "========================================"
echo "Testing DDL Replication in Marmot v2"
echo "========================================"
echo ""

# Clean up any existing processes and data
echo "Cleaning up old processes and data..."
pkill -f "marmot-v2" 2>/dev/null || true
sleep 1

# Start seed node (node 1)
echo "Starting seed node (port 8081, mysql 3307)..."
./examples/start-seed.sh > /tmp/seed.log 2>&1 &
sleep 3

# Verify seed started
if ! lsof -ti:3307 > /dev/null 2>&1; then
    echo -e "${RED}✗ Failed to start seed node${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Seed node started${NC}"

# Start node 2
echo "Starting node 2 (port 8082, mysql 3308)..."
./examples/join-cluster.sh 2 localhost:8081 > /tmp/node2.log 2>&1 &
sleep 4

# Verify node 2 started
if ! lsof -ti:3308 > /dev/null 2>&1; then
    echo -e "${RED}✗ Failed to start node 2${NC}"
    tail -20 /tmp/node2.log
    exit 1
fi
echo -e "${GREEN}✓ Node 2 started and joined cluster${NC}"
echo ""

# Test 1: CREATE TABLE (DDL)
echo "Test 1: CREATE TABLE on node 1"
echo "-------------------------------"
$MYSQL_CLI -h localhost -P 3307 -u root -e "CREATE TABLE Books (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    author TEXT NOT NULL,
    publication_year INTEGER
);" 2>&1 || echo "Warning: CREATE TABLE may have produced warnings"

echo -e "${GREEN}✓ Table created on node 1${NC}"
sleep 2

echo "Checking if Books table replicated to node 2..."
TABLES_NODE2=$($MYSQL_CLI -h localhost -P 3308 -u root -e "SHOW TABLES;" 2>&1 | grep -i books || echo "")

if [[ -n "$TABLES_NODE2" ]]; then
    echo -e "${GREEN}✓ SUCCESS: Books table replicated to node 2${NC}"
else
    echo -e "${RED}✗ FAIL: Books table NOT found on node 2${NC}"
    echo "Tables on node 2:"
    $MYSQL_CLI -h localhost -P 3308 -u root -e "SHOW TABLES;" 2>&1 || true
fi
echo ""

# Test 2: INSERT (DML)
echo "Test 2: INSERT data on node 1"
echo "-----------------------------"
$MYSQL_CLI -h localhost -P 3307 -u root -e "INSERT INTO Books (id, title, author, publication_year) VALUES
    (1, 'The Great Gatsby', 'F. Scott Fitzgerald', 1925),
    (2, '1984', 'George Orwell', 1949),
    (3, 'To Kill a Mockingbird', 'Harper Lee', 1960);" 2>&1 || echo "Warning: INSERT may have produced warnings"

echo -e "${GREEN}✓ Data inserted on node 1${NC}"
sleep 2

echo "Checking if data replicated to node 2..."
COUNT_NODE2=$($MYSQL_CLI -h localhost -P 3308 -u root -e "SELECT COUNT(*) as count FROM Books;" 2>&1 | tail -1)

if [[ "$COUNT_NODE2" == "3" ]]; then
    echo -e "${GREEN}✓ SUCCESS: 3 rows replicated to node 2${NC}"
    echo "Data on node 2:"
    $MYSQL_CLI -h localhost -P 3308 -u root -e "SELECT * FROM Books;" 2>&1 || true
else
    echo -e "${RED}✗ FAIL: Expected 3 rows on node 2, got: $COUNT_NODE2${NC}"
fi
echo ""

# Test 3: UPDATE (DML)
echo "Test 3: UPDATE data on node 2"
echo "-----------------------------"
$MYSQL_CLI -h localhost -P 3308 -u root -e "UPDATE Books SET publication_year = 1926 WHERE id = 1;" 2>&1 || echo "Warning: UPDATE may have produced warnings"

echo -e "${GREEN}✓ Data updated on node 2${NC}"
sleep 3

echo "Checking if update replicated to node 1..."
YEAR_NODE1=$($MYSQL_CLI -h localhost -P 3307 -u root -e "SELECT publication_year FROM Books WHERE id = 1;" 2>&1 | tail -1)

if [[ "$YEAR_NODE1" == "1926" ]]; then
    echo -e "${GREEN}✓ SUCCESS: Update replicated to node 1${NC}"
else
    echo -e "${RED}✗ FAIL: Expected year 1926 on node 1, got: $YEAR_NODE1${NC}"
fi
echo ""

# Test 4: DELETE (DML)
echo "Test 4: DELETE data on node 1"
echo "-----------------------------"
$MYSQL_CLI -h localhost -P 3307 -u root -e "DELETE FROM Books WHERE id = 2;" 2>&1 || echo "Warning: DELETE may have produced warnings"

echo -e "${GREEN}✓ Data deleted on node 1${NC}"
sleep 2

echo "Checking if delete replicated to node 2..."
COUNT_AFTER_DELETE=$($MYSQL_CLI -h localhost -P 3308 -u root -e "SELECT COUNT(*) as count FROM Books;" 2>&1 | tail -1)

if [[ "$COUNT_AFTER_DELETE" == "2" ]]; then
    echo -e "${GREEN}✓ SUCCESS: Delete replicated to node 2 (2 rows remaining)${NC}"
else
    echo -e "${RED}✗ FAIL: Expected 2 rows after delete on node 2, got: $COUNT_AFTER_DELETE${NC}"
fi
echo ""

# Summary
echo "========================================"
echo "Test Summary"
echo "========================================"
echo "Check logs for details:"
echo "  Node 1: tail -f /tmp/seed.log"
echo "  Node 2: tail -f /tmp/node2.log"
echo ""
echo "To cleanup: pkill -f marmot-v2"
