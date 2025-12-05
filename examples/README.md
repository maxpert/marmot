# Marmot v2.0 Examples

Quick start examples for running Marmot in different configurations.

## Quick Start

### Single Node (Simplest)

Run a single Marmot node - perfect for development and testing:

```bash
./run-single-node.sh
```

**What it does:**
- Creates `/tmp/marmot-single/` directory
- Builds marmot if needed
- Creates sample Books database
- Starts node on:
  - gRPC port: 8080
  - MySQL port: 3306 (standard)

**Connect:**
```bash
mysql -h 127.0.0.1 -P 3306 -u root
```

**Try it:**
```sql
SELECT * FROM Books;
INSERT INTO Books (title, author, publication_year) VALUES ('New Book', 'Author', 2024);
```

**Stop:**
Press `Ctrl+C`

---

### 3-Node Cluster (Full Replication)

Run a 3-node cluster to test distributed transactions:

```bash
./run-cluster.sh
```

**What it does:**
- Creates `/tmp/marmot-node-{1,2,3}/` directories
- Builds marmot if needed
- Creates identical Books database on all nodes
- Starts 3 nodes:
  - **Node 1**: gRPC 8081, MySQL 3307
  - **Node 2**: gRPC 8082, MySQL 3308
  - **Node 3**: gRPC 8083, MySQL 3309
- Nodes discover each other via gossip

**Architecture:**
- **Full database replication**: ALL nodes have complete copy
- **Write path**: Broadcast to all, wait for QUORUM (2 out of 3)
- **Read path**: Can read from ANY node
- **Dead nodes**: Catch up when they rejoin

**Connect to any node:**
```bash
# Node 1
mysql -h 127.0.0.1 -P 3307 -u root

# Node 2
mysql -h 127.0.0.1 -P 3308 -u root

# Node 3
mysql -h 127.0.0.1 -P 3309 -u root
```

**Try distributed writes:**
```sql
-- Write to node 1
INSERT INTO Books (title, author, publication_year) VALUES ('Distributed Book', 'Author', 2024);

-- Read from node 2 (should see the same data)
SELECT * FROM Books WHERE title = 'Distributed Book';

-- Read from node 3 (should also see it)
SELECT * FROM Books WHERE title = 'Distributed Book';
```

**Watch logs:**
```bash
tail -f /tmp/marmot-node-1/marmot.log
tail -f /tmp/marmot-node-2/marmot.log
tail -f /tmp/marmot-node-3/marmot.log
```

**Stop:**
Press `Ctrl+C`

---

## Configuration Files

Pre-configured TOML files for each node:

- `node-1-config.toml` - Node 1 (ports 8081/3307)
- `node-2-config.toml` - Node 2 (ports 8082/3308)
- `node-3-config.toml` - Node 3 (ports 8083/3309)

Each config includes:
- ✅ Transaction settings (garbage collection, conflict resolution)
- ✅ Cluster settings (gossip, seed nodes, timeouts)
- ✅ Replication settings (QUORUM consistency)
- ✅ MySQL protocol server
- ✅ Logging and metrics

## Next Steps

1. **Try the single node**: `./run-single-node.sh`
2. **Try the cluster**: `./run-cluster.sh`
3. **Advanced testing**: See `../test_cluster/README.md`
4. **Stress testing**: See `../test_cluster/stress_test.go`

## Architecture Notes

### Full Database Replication

Marmot v2.0 uses **full database replication**, not partitioning:

- **Every node** has a complete copy of the database
- **Writes** go to all nodes (broadcast)
- **QUORUM** acknowledgment needed (e.g., 2 out of 3 nodes)
- **Reads** can be served from any node (all have complete data)
- **Dead nodes** catch up via snapshots when they rejoin

This is different from Cassandra/DynamoDB which partition data across nodes.

### Consistency Levels

**Write Consistency:**
- `LOCAL_ONE`: Local node only (fast, less durable)
- `QUORUM`: Majority of nodes (recommended - default)
- `ALL`: All nodes (slowest, most durable)

**Read Consistency:**
- `LOCAL_ONE`: Read from local node (fastest - default)
- `ONE`: Read from any single node
- `QUORUM`: Read from majority (most consistent)
- `ALL`: Read from all nodes

### Trade-offs

**Pros:**
- ✅ Simple: No complex replica placement
- ✅ Fast reads: Read from local node
- ✅ Flexible: Any node can serve reads
- ✅ Consistent: QUORUM writes ensure majority has data

**Cons:**
- ⚠️ Write amplification: Every write goes to every node
- ⚠️ Cluster size: Practical limit ~5-7 nodes
- ⚠️ Network bandwidth: More traffic than partitioned model

**Future:**
- Multi-database support will use partitioning for scalability

## Troubleshooting

### Port already in use

```bash
# Find process using port 3306
lsof -i :3306
# Kill it
kill -9 <PID>
```

### Can't connect to MySQL

1. Check node is running: `ps aux | grep marmot`
2. Check logs: `tail -f /tmp/marmot-single/marmot.log`
3. Verify port: `nc -zv localhost 3306`

### Database file locked

```bash
# Stop all marmot processes
pkill marmot
# Remove lock files
rm -rf /tmp/marmot-*/
```

### Build errors

```bash
cd ..
go mod tidy
go build .
```
