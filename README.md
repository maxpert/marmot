# Marmot v2

[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/marmot)](https://goreportcard.com/report/github.com/maxpert/marmot)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label=Marmot)](https://discord.gg/AWUwY66XsE)
![GitHub](https://img.shields.io/github/license/maxpert/marmot)

## What & Why?

Marmot v2 is a leaderless, distributed SQLite replication system built on a gossip-based protocol with distributed transactions and eventual consistency.

**Key Features:**
- **Leaderless Architecture**: No single point of failure - any node can accept writes
- **MySQL Protocol Compatible**: Connect with any MySQL client (DBeaver, MySQL Workbench, mysql CLI)
- **WordPress Compatible**: Full MySQL function support for running distributed WordPress
- **Distributed Transactions**: Percolator-style write intents with conflict detection
- **Multi-Database Support**: Create and manage multiple databases per cluster
- **DDL Replication**: Distributed schema changes with automatic idempotency and cluster-wide locking
- **Production-Ready SQL Parser**: Powered by rqlite/sql AST parser for MySQL→SQLite transpilation
- **CDC-Based Replication**: Row-level change data capture for consistent replication

## Why Marmot?

### The Problem with Traditional Replication

MySQL active-active requires careful setup of replication, conflict avoidance, and monitoring. Failover needs manual intervention. Split-brain scenarios demand operational expertise. This complexity doesn't scale to edge deployments.

### Marmot's Approach

- **Zero operational overhead**: Automatic recovery from split-brain via eventual consistency + anti-entropy
- **No leader election**: Any node accepts writes, no failover coordination needed
- **Direct SQLite access**: Clients can read the local SQLite file directly for maximum performance
- **Tunable consistency**: Choose ONE/QUORUM/ALL per your latency vs durability needs

### Why MySQL Protocol?

- Ecosystem compatibility - existing drivers, ORMs, GUI tools work out-of-box
- Battle-tested wire protocol implementations
- Run real applications like WordPress without modification

### Ideal Use Cases

Marmot excels at **read-heavy edge scenarios**:

| Use Case | How Marmot Helps |
|----------|------------------|
| **Distributed WordPress** | Multi-region WordPress with replicated database |
| **Lambda/Edge sidecars** | Lightweight regional SQLite replicas, local reads |
| **Edge vector databases** | Distributed embeddings with local query |
| **Regional config servers** | Fast local config reads, replicated writes |
| **Product catalogs** | Geo-distributed catalog data, eventual sync |

### When to Consider Alternatives

- **Strong serializability required** → CockroachDB, Spanner
- **Single-region high-throughput** → PostgreSQL, MySQL directly
- **Large datasets (>100GB)** → Sharded solutions

## Quick Start

```bash
# Start a single-node cluster
./marmot-v2

# Connect with MySQL client
mysql -h localhost -P 3306 -u root

# Or use DBeaver, MySQL Workbench, etc.
```

### Testing Replication

```bash
# Test DDL and DML replication across a 2-node cluster
./scripts/test-ddl-replication.sh

# This script will:
# 1. Start a 2-node cluster
# 2. Create a table on node 1 and verify it replicates to node 2
# 3. Insert data on node 1 and verify it replicates to node 2
# 4. Update data on node 2 and verify it replicates to node 1
# 5. Delete data on node 1 and verify it replicates to node 2

# Manual cluster testing
./examples/start-seed.sh              # Start seed node (port 8081, mysql 3307)
./examples/join-cluster.sh 2 localhost:8081  # Join node 2 (port 8082, mysql 3308)
./examples/join-cluster.sh 3 localhost:8081  # Join node 3 (port 8083, mysql 3309)

# Connect to any node and run queries
mysql --protocol=TCP -h localhost -P 3307 -u root
mysql --protocol=TCP -h localhost -P 3308 -u root

# Cleanup
pkill -f marmot-v2
```

## WordPress Support

Marmot can run **distributed WordPress** with full database replication across nodes. Each WordPress instance connects to its local Marmot node, and all database changes replicate automatically.

### MySQL Compatibility for WordPress

Marmot implements MySQL functions required by WordPress:

| Category | Functions |
|----------|-----------|
| **Date/Time** | NOW, CURDATE, DATE_FORMAT, UNIX_TIMESTAMP, DATEDIFF, YEAR, MONTH, DAY, etc. |
| **String** | CONCAT_WS, SUBSTRING_INDEX, FIND_IN_SET, LPAD, RPAD, etc. |
| **Math/Hash** | RAND, MD5, SHA1, SHA2, POW, etc. |
| **DML** | ON DUPLICATE KEY UPDATE (transformed to SQLite ON CONFLICT) |

### Quick Start: 3-Node WordPress Cluster

```bash
cd examples/wordpress-cluster
./run.sh up
```

This starts:
- **3 Marmot nodes** with QUORUM write consistency
- **3 WordPress instances** each connected to its local Marmot node

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ WordPress-1 │  │ WordPress-2 │  │ WordPress-3 │
│ :9101       │  │ :9102       │  │ :9103       │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       ▼                ▼                ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Marmot-1   │◄─┤  Marmot-2   │◄─┤  Marmot-3   │
│ MySQL: 9191 │  │ MySQL: 9192 │  │ MySQL: 9193 │
└─────────────┘  └─────────────┘  └─────────────┘
       └──────────────┴──────────────┘
              QUORUM Replication
```

**Test it:**
1. Open http://localhost:9101 - complete WordPress installation
2. Open http://localhost:9102 or http://localhost:9103
3. See your content replicated across all nodes!

**Commands:**
```bash
./run.sh status   # Check cluster health
./run.sh logs-m   # Marmot logs only
./run.sh logs-wp  # WordPress logs only
./run.sh down     # Stop cluster
```

### Production Considerations for WordPress

- **Media uploads**: Use S3/NFS for shared media storage (files not replicated by Marmot)
- **Sessions**: Use Redis or database sessions for sticky-session-free load balancing
- **Caching**: Each node can use local object cache (Redis/Memcached per region)

## Architecture

Marmot v2 uses a fundamentally different architecture from other SQLite replication solutions:

**vs. rqlite/dqlite/LiteFS:**
- ❌ They require a primary node for all writes
- ✅ Marmot allows writes on **any node**
- ❌ They use leader election (Raft)
- ✅ Marmot uses **gossip protocol** (no leader)
- ❌ They require proxy layer or page-level interception
- ✅ Marmot uses **MySQL protocol** for direct database access

**How It Works:**
1. **Write Coordination**: 2PC (Two-Phase Commit) with configurable consistency (ONE, QUORUM, ALL)
2. **Conflict Resolution**: Last-Write-Wins (LWW) with HLC timestamps
3. **Cluster Membership**: SWIM-style gossip with failure detection
4. **Data Replication**: Full database replication - all nodes receive all data
5. **DDL Replication**: Cluster-wide schema changes with automatic idempotency

## Comparison with Alternatives

| Aspect | Marmot | MySQL Active-Active | rqlite | dqlite | TiDB |
|--------|--------|---------------------|--------|-------|------|
| **Leader** | None | None (but complex) | Yes (Raft) | Yes (Raft) | Yes (Raft) |
| **Failover** | Automatic | Manual intervention | Automatic | Automatic | Automatic |
| **Split-brain recovery** | Automatic (anti-entropy) | Manual | N/A (leader-based) | N/A (leader-based) | N/A |
| **Consistency** | Tunable (ONE/QUORUM/ALL) | Serializable | Tunabale (ONE/QUORUM/Linearizable) | Strong | Strong |
| **Direct file read** | ✅ SQLite file | ❌ | ✅ SQLite file | ❌ | ❌ |
| **JS-safe AUTO_INCREMENT** | ✅ Compact mode (53-bit) | N/A | N/A | ❌ 64-bit breaks JS |
| **Edge-friendly** | ✅ Lightweight | ❌ Heavy | ✅ Lightweight | ⚠️ Moderate | ❌ Heavy |
| **Operational complexity** | Low | High | Low | Low | High |

## DDL Replication

Marmot v2 supports **distributed DDL (Data Definition Language) replication** without requiring master election:

### How It Works

1. **Cluster-Wide Locking**: Each DDL operation acquires a distributed lock per database (default: 30-second lease)
   - Prevents concurrent schema changes on the same database
   - Locks automatically expire if a node crashes
   - Different databases can have concurrent DDL operations

2. **Automatic Idempotency**: DDL statements are automatically rewritten for safe replay
   ```sql
   CREATE TABLE users (id INT)
   → CREATE TABLE IF NOT EXISTS users (id INT)

   DROP TABLE users
   → DROP TABLE IF EXISTS users
   ```

3. **Schema Version Tracking**: Each database maintains a schema version counter
   - Incremented on every DDL operation
   - Exchanged via gossip protocol for drift detection
   - Used by delta sync to validate transaction applicability

4. **Quorum-Based Replication**: DDL replicates like DML through the same 2PC mechanism
   - No special master node needed
   - Works with existing consistency levels (QUORUM, ALL, etc.)

### Configuration

```toml
[ddl]
# DDL lock lease duration (seconds)
lock_lease_seconds = 30

# Automatically rewrite DDL for idempotency
enable_idempotent = true
```

### Best Practices

- ✅ **Do**: Execute DDL from a single connection/node at a time
- ✅ **Do**: Use qualified table names (`mydb.users` instead of `users`)
- ⚠️ **Caution**: ALTER TABLE is less idempotent - avoid replaying failed ALTER operations
- ❌ **Don't**: Run concurrent DDL on the same database from multiple nodes

## CDC-Based Replication

Marmot v2 uses **Change Data Capture (CDC)** for replication instead of SQL statement replay:

### How It Works

1. **Row-Level Capture**: Instead of replicating SQL statements, Marmot captures the actual row data changes (INSERT/UPDATE/DELETE)
2. **Binary Data Format**: Row data is serialized as CDC messages with column values, ensuring consistent replication regardless of SQL dialect
3. **Deterministic Application**: Row data is applied directly to the target database, avoiding parsing ambiguities

### Benefits

- **Consistency**: Same row data applied everywhere, no SQL parsing differences
- **Performance**: Binary format is more efficient than SQL text
- **Reliability**: No issues with SQL syntax variations between MySQL and SQLite

### Row Key Extraction

For UPDATE and DELETE operations, Marmot automatically extracts row keys:
- Uses PRIMARY KEY columns when available
- Falls back to ROWID for tables without explicit primary key
- Handles composite primary keys correctly

## CDC Publisher

Marmot can publish CDC events to external messaging systems, enabling real-time data pipelines, analytics, and event-driven architectures. Events follow the **[Debezium](https://debezium.io/) specification** for maximum compatibility with existing CDC tooling.

### Features

- **Debezium-Compatible Format**: Events conform to the [Debezium event structure](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events), compatible with Kafka Connect, Flink, Spark, and other CDC consumers
- **Multi-Sink Support**: Publish to multiple destinations simultaneously (Kafka, NATS)
- **Glob-Based Filtering**: Filter which tables and databases to publish
- **Automatic Retry**: Exponential backoff with configurable limits
- **Persistent Cursors**: Survives restarts without losing position

### Configuration

```toml
[publisher]
enabled = true

[[publisher.sinks]]
name = "kafka-main"
type = "kafka"                    # "kafka" or "nats"
format = "debezium"               # Debezium-compatible JSON format
brokers = ["localhost:9092"]      # Kafka broker addresses
topic_prefix = "marmot.cdc"       # Topics: {prefix}.{database}.{table}
filter_tables = ["*"]             # Glob patterns (e.g., "users", "order_*")
filter_databases = ["*"]          # Glob patterns (e.g., "prod_*")
batch_size = 100                  # Events per poll cycle
poll_interval_ms = 10             # Polling interval

# NATS sink example
[[publisher.sinks]]
name = "nats-events"
type = "nats"
format = "debezium"
nats_url = "nats://localhost:4222"
topic_prefix = "marmot.cdc"
filter_tables = ["*"]
filter_databases = ["*"]
```

### Event Format

Events follow the [Debezium envelope structure](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-change-events-value):

```json
{
  "schema": { ... },
  "payload": {
    "before": null,
    "after": {"id": 1, "name": "alice", "email": "alice@example.com"},
    "source": {
      "version": "2.0.0",
      "connector": "marmot",
      "name": "marmot",
      "ts_ms": 1702500000000,
      "db": "myapp",
      "table": "users"
    },
    "op": "c",
    "ts_ms": 1702500000000
  }
}
```

**Operation Types** (per [Debezium spec](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events)):
| Operation | `op` | `before` | `after` |
|-----------|------|----------|---------|
| INSERT | `c` (create) | `null` | row data |
| UPDATE | `u` (update) | old row | new row |
| DELETE | `d` (delete) | old row | `null` |

### Topic Naming

Topics follow the pattern: `{topic_prefix}.{database}.{table}`

Examples:
- `marmot.cdc.myapp.users`
- `marmot.cdc.myapp.orders`
- `marmot.cdc.analytics.events`

### Use Cases

- **Real-Time Analytics**: Stream changes to data warehouses (Snowflake, BigQuery, ClickHouse)
- **Event-Driven Microservices**: Trigger actions on data changes
- **Cache Invalidation**: Keep caches in sync with database changes
- **Audit Logging**: Capture all changes for compliance
- **Search Indexing**: Keep Elasticsearch/Algolia in sync

For more details, see the [Integrations documentation](https://maxpert.github.io/marmot/integrations).

## Edge Deployment Patterns

### Lambda Sidecar

Deploy Marmot as a lightweight regional replica alongside Lambda functions:
- Local SQLite reads (sub-ms latency)
- Writes replicate to cluster
- No cold-start database connections

### Read-Only Regional Replicas

Scale reads globally with replica mode and transparent failover:

```toml
[replica]
enabled = true
follow_addresses = ["central-cluster-1:8080", "central-cluster-2:8080", "central-cluster-3:8080"]
replicate_databases = []                    # Filter databases (empty = all, supports glob patterns)
database_discovery_interval_seconds = 10    # Poll for new databases (default: 10)
discovery_interval_seconds = 30             # Poll for cluster membership (default: 30)
failover_timeout_seconds = 60               # Failover timeout (default: 60)
snapshot_concurrency = 3                    # Parallel snapshot downloads (default: 3)
snapshot_cache_ttl_seconds = 30             # Snapshot cache TTL (default: 30)
```

- Discovers cluster nodes automatically via `GetClusterNodes` RPC
- Transparent failover when current source becomes unavailable
- Automatic discovery of new databases with configurable polling
- Per-database selective replication with glob pattern support
- Parallel snapshot downloads with configurable concurrency
- Snapshot caching for performance optimization
- Zero cluster participation overhead
- Auto-reconnect with exponential backoff

### Hybrid: Edge Reads, Central Writes

- Deploy full cluster in central region
- Deploy read replicas at edge locations
- Application routes writes to central, reads to local replica

## SQL Statement Compatibility

Marmot supports a wide range of MySQL/SQLite statements through its MySQL protocol server. The following table shows compatibility for different statement types:

| Statement Type | Support | Replication | Notes |
|---------------|---------|-------------|-------|
| **DML - Data Manipulation** |
| `INSERT` / `REPLACE` | ✅ Full | ✅ Yes | Includes qualified table names (db.table) |
| `UPDATE` | ✅ Full | ✅ Yes | Includes qualified table names |
| `DELETE` | ✅ Full | ✅ Yes | Includes qualified table names |
| `SELECT` | ✅ Full | N/A | Read operations |
| `LOAD DATA` | ✅ Full | ✅ Yes | Bulk data loading |
| **DDL - Data Definition** |
| `CREATE TABLE` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `ALTER TABLE` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `DROP TABLE` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `TRUNCATE TABLE` | ✅ Full | ✅ Yes | |
| `RENAME TABLE` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `CREATE/DROP INDEX` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `CREATE/DROP VIEW` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `CREATE/DROP TRIGGER` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| **Database Management** |
| `CREATE DATABASE` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `DROP DATABASE` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `ALTER DATABASE` | ✅ Full | ✅ Yes | Replicated with cluster-wide locking |
| `SHOW DATABASES` | ✅ Full | N/A | Metadata query |
| `SHOW TABLES` | ✅ Full | N/A | Metadata query |
| `USE database` | ✅ Full | N/A | Session state |
| **Transaction Control** |
| `BEGIN` / `START TRANSACTION` | ✅ Full | N/A | Transaction boundary |
| `COMMIT` | ✅ Full | ✅ Yes | Commits distributed transaction |
| `ROLLBACK` | ✅ Full | ✅ Yes | Aborts distributed transaction |
| `SAVEPOINT` | ✅ Full | ✅ Yes | Nested transaction support |
| **Locking** |
| `LOCK TABLES` | ✅ Parsed | ❌ No | Requires distributed locking coordination |
| `UNLOCK TABLES` | ✅ Parsed | ❌ No | Requires distributed locking coordination |
| **Session Configuration** |
| `SET` statements | ✅ Parsed | ❌ No | Session-local, not replicated |
| **XA Transactions** |
| `XA START/END/PREPARE` | ✅ Parsed | ❌ No | Marmot uses its own 2PC protocol |
| `XA COMMIT/ROLLBACK` | ✅ Parsed | ❌ No | Not compatible with Marmot's model |
| **DCL - Data Control** |
| `GRANT` / `REVOKE` | ✅ Parsed | ❌ No | User management not replicated |
| `CREATE/DROP USER` | ✅ Parsed | ❌ No | User management not replicated |
| `ALTER USER` | ✅ Parsed | ❌ No | User management not replicated |
| **Administrative** |
| `OPTIMIZE TABLE` | ✅ Parsed | ❌ No | Node-local administrative command |
| `REPAIR TABLE` | ✅ Parsed | ❌ No | Node-local administrative command |

### Legend
- ✅ **Full**: Fully supported and working
- ✅ **Parsed**: Statement is parsed and recognized
- ⚠️ **Limited**: Works but has limitations in distributed context
- ❌ **No**: Not supported or not replicated
- **N/A**: Not applicable (read-only or session-local)

### Important Notes

1. **Schema Changes (DDL)**: DDL statements are fully replicated with cluster-wide locking and automatic idempotency. See the DDL Replication section for details.

2. **XA Transactions**: Marmot has its own distributed transaction protocol based on 2PC. MySQL XA transactions are not compatible with Marmot's replication model.

3. **User Management (DCL)**: User and privilege management statements are local to each node. For production deployments, consider handling authentication at the application or proxy level.

4. **Table Locking**: `LOCK TABLES` statements are recognized but not enforced across the cluster. Use application-level coordination for distributed locking needs.

5. **Qualified Names**: Marmot fully supports qualified table names (e.g., `db.table`) in DML and DDL operations.

## MySQL Protocol & Metadata Queries

Marmot includes a MySQL-compatible protocol server, allowing you to connect using any MySQL client (DBeaver, MySQL Workbench, mysql CLI, etc.). The server supports:

### Metadata Query Support

Marmot provides full support for MySQL metadata queries, enabling GUI tools like DBeaver to browse databases, tables, and columns:

- **SHOW Commands**: `SHOW DATABASES`, `SHOW TABLES`, `SHOW COLUMNS FROM table`, `SHOW CREATE TABLE`, `SHOW INDEXES`
- **INFORMATION_SCHEMA**: Queries against `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.COLUMNS`, `INFORMATION_SCHEMA.SCHEMATA`, and `INFORMATION_SCHEMA.STATISTICS`
- **Type Conversion**: Automatic SQLite-to-MySQL type mapping for compatibility

These metadata queries are powered by the **rqlite/sql AST parser**, providing production-grade MySQL query compatibility.

### Connecting with MySQL Clients

```bash
# Using mysql CLI
mysql -h localhost -P 3306 -u root

# Connection string for applications
mysql://root@localhost:3306/marmot
```

## Recovery Scenarios

Marmot handles various failure and recovery scenarios automatically:

### Network Partition (Split-Brain)

| Scenario | Behavior |
|----------|----------|
| **Minority partition** | Writes **fail** - cannot achieve quorum |
| **Majority partition** | Writes **succeed** - quorum achieved |
| **Partition heals** | Delta sync + LWW merges divergent data |

**How it works:**
1. During partition, only the majority side can commit writes (quorum enforcement)
2. When partition heals, nodes exchange transaction logs via `StreamChanges` RPC
3. Conflicts resolved using Last-Writer-Wins (LWW) with HLC timestamps
4. Higher node ID breaks ties for simultaneous writes

### Node Failure & Recovery

| Scenario | Recovery Method |
|----------|-----------------|
| **Brief outage** | Delta sync - replay missed transactions |
| **Extended outage** | Snapshot transfer + delta sync |
| **New node joining** | Full snapshot from existing node |

**Anti-Entropy Background Process:**

Marmot v2 includes an automatic anti-entropy system that continuously monitors and repairs replication lag across the cluster:

1. **Lag Detection**: Every 60 seconds (configurable), each node queries peers for their replication state
2. **Smart Recovery Decision**:
   - **Delta Sync** if lag < 10,000 transactions AND < 1 hour: Streams missed transactions incrementally
   - **Snapshot Transfer** if lag exceeds thresholds: Full database file transfer for efficiency
3. **Gap Detection**: Detects when transaction logs have been GC'd and automatically falls back to snapshot
4. **Multi-Database Support**: Tracks and syncs each database independently
5. **GC Coordination**: Garbage collection respects peer replication state - logs aren't deleted until all peers have applied them

**Delta Sync Process:**
1. Lagging node queries `last_applied_txn_id` for each peer/database
2. Requests transactions since that ID via `StreamChanges` RPC
3. **Gap Detection**: Checks if first received txn_id has a large gap from requested ID
   - If gap > delta_sync_threshold_txns, indicates missing (GC'd) transactions
   - Automatically falls back to snapshot transfer to prevent data loss
4. Applies changes using LWW conflict resolution
5. Updates replication state tracking (per-database)
6. Progress logged every 100 transactions

**GC Coordination with Anti-Entropy:**
- Transaction logs are retained with a two-tier policy:
  - **Min retention** (2 hours): Must be >= delta sync threshold, respects peer lag
  - **Max retention** (24 hours): Force delete after this time to prevent unbounded growth
- Config validation enforces: `gc_min >= delta_threshold` and `gc_max >= 2x delta_threshold`
- Each database tracks replication progress per peer
- GC queries minimum applied txn_id across all peers before cleanup
- **Gap detection** prevents data loss if GC runs while nodes are offline

### Consistency Guarantees

| Write Consistency | Behavior |
|-------------------|----------|
| `ONE` | Returns after 1 node ACK (fast, less durable) |
| `QUORUM` | Returns after majority ACK (default, balanced) |
| `ALL` | Returns after all nodes ACK (slow, most durable) |

**Conflict Resolution:**
- All conflicts resolved via LWW using HLC timestamps
- No data loss - later write always wins deterministically
- Tie-breaker: higher node ID wins for equal timestamps

## Limitations

- **Selective Table Watching**: All tables in a database are replicated. Selective table replication is not supported.
- **WAL Mode Required**: SQLite must use WAL mode for reliable multi-process changes.
- **Eventually Consistent**: Rows may sync out of order. `SERIALIZABLE` transaction assumptions may not hold across nodes.
- **Concurrent DDL**: Avoid running concurrent DDL operations on the same database from multiple nodes (protected by cluster-wide lock with 30s lease).

## Auto-Increment & ID Generation

### The Problem with Distributed IDs

Distributed databases need globally unique IDs, but traditional solutions cause problems:

| Solution | Issue |
|----------|-------|
| **UUID** | 128-bit, poor index performance, not sortable |
| **Snowflake/HLC 64-bit** | Exceeds JavaScript's `Number.MAX_SAFE_INTEGER` (2^53-1) |
| **TiDB AUTO_INCREMENT** | Returns 64-bit IDs that **break JavaScript clients** silently |

**The JavaScript Problem:**

```javascript
// 64-bit ID from TiDB or other distributed DBs
const id = 7318624812345678901;
console.log(id);  // 7318624812345679000 - WRONG! Precision lost!

// JSON parsing also breaks
JSON.parse('{"id": 7318624812345678901}');  // {id: 7318624812345679000}
```

TiDB's answer? "Use strings." But that breaks ORMs, existing application code, and type safety.

### Marmot's Solution: Compact ID Mode

Marmot offers **two ID generation modes** to solve this:

| Mode | Bits | Range | Use Case |
|------|------|-------|----------|
| `extended` | 64-bit | Full HLC timestamp | New systems, non-JS clients |
| `compact` | 53-bit | JS-safe integers | **Legacy systems, JavaScript, REST APIs** |

```toml
[mysql]
auto_id_mode = "compact"  # Safe for JavaScript (default)
# auto_id_mode = "extended"  # Full 64-bit for new systems
```

**Compact Mode Guarantees:**
- IDs stay under `Number.MAX_SAFE_INTEGER` (9,007,199,254,740,991)
- Still globally unique across all nodes
- Still monotonically increasing (per node)
- No silent precision loss in JSON/JavaScript
- Works with existing ORMs expecting integer IDs

**With Marmot compact mode:**
```javascript
const id = 4503599627370496;
console.log(id);  // 4503599627370496 - Correct!
JSON.parse('{"id": 4503599627370496}');  // {id: 4503599627370496} - Correct!
```

### How Auto-Increment Works

> **Note:** Marmot automatically converts `INT AUTO_INCREMENT` to `BIGINT` to support distributed ID generation.

1. **DDL Transformation**: When you create a table with `AUTO_INCREMENT`:
   ```sql
   CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))
   -- Becomes internally:
   CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)
   ```

2. **DML ID Injection**: When inserting with `0` or `NULL` for an auto-increment column:
   ```sql
   INSERT INTO users (id, name) VALUES (0, 'alice')
   -- Becomes internally (compact mode):
   INSERT INTO users (id, name) VALUES (4503599627370496, 'alice')
   ```

3. **Explicit IDs Preserved**: If you provide an explicit non-zero ID, it is used as-is.

**Schema-Based Detection:**

Marmot automatically detects auto-increment columns by querying SQLite schema directly:
- Single-column `INTEGER PRIMARY KEY` (SQLite rowid alias)
- Single-column `BIGINT PRIMARY KEY` (Marmot's transformed columns)

No registration required - columns are detected from schema at runtime, works across restarts, and works with existing databases.

## Configuration

Marmot v2 uses a TOML configuration file (default: `config.toml`). All settings have sensible defaults.

### Core Configuration

```toml
node_id = 0  # 0 = auto-generate
data_dir = "./marmot-data"
```

### Transaction Manager

```toml
[transaction]
heartbeat_timeout_seconds = 10  # Transaction timeout without heartbeat
conflict_window_seconds = 10    # Conflict resolution window
lock_wait_timeout_seconds = 50  # Lock wait timeout (MySQL: innodb_lock_wait_timeout)
```

**Note**: Transaction log garbage collection is managed by the replication configuration to coordinate with anti-entropy. See `replication.gc_min_retention_hours` and `replication.gc_max_retention_hours`.

### Connection Pool

```toml
[connection_pool]
pool_size = 4              # Number of SQLite connections
max_idle_time_seconds = 10 # Max idle time before closing
max_lifetime_seconds = 300 # Max connection lifetime (0 = unlimited)
```

### gRPC Client

```toml
[grpc_client]
keepalive_time_seconds = 10    # Keepalive ping interval
keepalive_timeout_seconds = 3  # Keepalive ping timeout
max_retries = 3                # Max retry attempts
retry_backoff_ms = 100         # Retry backoff duration
```

### Coordinator

```toml
[coordinator]
prepare_timeout_ms = 2000 # Prepare phase timeout
commit_timeout_ms = 2000  # Commit phase timeout
abort_timeout_ms = 2000   # Abort phase timeout
```

### Cluster

```toml
[cluster]
grpc_bind_address = "0.0.0.0"
grpc_port = 8080
seed_nodes = []                # List of seed node addresses
cluster_secret = ""            # PSK for cluster authentication (see Security section)
gossip_interval_ms = 1000      # Gossip interval
gossip_fanout = 3              # Number of peers to gossip to
suspect_timeout_ms = 5000      # Suspect timeout
dead_timeout_ms = 10000        # Dead timeout
```

### Security

Marmot supports Pre-Shared Key (PSK) authentication for cluster communication. **This is strongly recommended for production deployments.**

```toml
[cluster]
# All nodes in the cluster must use the same secret
cluster_secret = "your-secret-key-here"
```

**Environment Variable (Recommended):**

For production, use the environment variable to avoid storing secrets in config files:

```bash
export MARMOT_CLUSTER_SECRET="your-secret-key-here"
./marmot
```

The environment variable takes precedence over the config file.

**Generating a Secret:**

```bash
# Generate a secure random secret
openssl rand -base64 32
```

**Behavior:**
- If `cluster_secret` is empty and `MARMOT_CLUSTER_SECRET` is not set, authentication is disabled
- A warning is logged at startup when authentication is disabled
- All gRPC endpoints (gossip, replication, snapshots) are protected when authentication is enabled
- Nodes with mismatched secrets will fail to communicate (connection rejected with "invalid cluster secret")

### Cluster Membership Management

Marmot provides admin HTTP endpoints for managing cluster membership (requires `cluster_secret` to be configured):

**Node Lifecycle:**
- New/restarted nodes **auto-join** via gossip - no manual intervention needed
- Nodes marked REMOVED via admin API **cannot auto-rejoin** - must be explicitly allowed
- This prevents decommissioned nodes from accidentally rejoining the cluster

```bash
# View cluster members and quorum info
curl -H "X-Marmot-Secret: your-secret" http://localhost:8080/admin/cluster/members

# Remove a node from the cluster (excludes from quorum, blocks auto-rejoin)
curl -X POST -H "X-Marmot-Secret: your-secret" http://localhost:8080/admin/cluster/remove/2

# Allow a removed node to rejoin (node must then restart to join)
curl -X POST -H "X-Marmot-Secret: your-secret" http://localhost:8080/admin/cluster/allow/2
```

See the [Operations documentation](https://maxpert.github.io/marmot/operations) for detailed usage and examples.

### Replica Mode

For read-only replicas that follow cluster nodes with transparent failover:

```toml
[replica]
enabled = true                                   # Enable read-only replica mode
follow_addresses = ["node1:8080", "node2:8080", "node3:8080"]  # Seed nodes for discovery
secret = "replica-secret"                        # PSK for authentication (required)
replicate_databases = []                         # Filter databases (empty = all, supports glob patterns like "prod_*")
database_discovery_interval_seconds = 10         # Poll for new databases (default: 10)
discovery_interval_seconds = 30                  # Poll for cluster membership (default: 30)
failover_timeout_seconds = 60                    # Max time to find alive node during failover (default: 60)
reconnect_interval_seconds = 5                   # Reconnect delay on disconnect (default: 5)
reconnect_max_backoff_seconds = 30               # Max reconnect backoff (default: 30)
initial_sync_timeout_minutes = 30                # Timeout for initial snapshot (default: 30)
snapshot_concurrency = 3                         # Parallel snapshot downloads (default: 3)
snapshot_cache_ttl_seconds = 30                  # Snapshot cache TTL in seconds (default: 30)
```

You can also specify follow addresses via CLI:
```bash
./marmot --config=replica.toml --follow-addresses=node1:8080,node2:8080,node3:8080
```

**Per-Database Selective Replication:**

Control which databases are replicated using the `replicate_databases` filter:

```toml
[replica]
# Replicate only specific databases
replicate_databases = ["myapp", "analytics"]

# Replicate databases matching glob patterns
replicate_databases = ["prod_*", "staging_*"]

# Replicate all databases (default)
replicate_databases = []
```

The system database (`__marmot_system`) is never replicated - each replica maintains its own independent system database.

**Snapshot Caching:**

Replicas use an LRU cache to avoid redundant snapshot creation:
- Cache TTL controlled by `snapshot_cache_ttl_seconds` (default: 30 seconds)
- Cached snapshots served from temp storage until expiration
- Background cleanup runs automatically
- Improves performance when multiple replicas bootstrap simultaneously

**Parallel Snapshot Downloads:**

Control download concurrency with `snapshot_concurrency`:
- Downloads multiple database snapshots in parallel (default: 3)
- Uses worker pool pattern to limit resource usage
- Partial failure handling: continues even if some databases fail
- Failed databases retry in background with exponential backoff

**Note:** Replica mode is mutually exclusive with cluster mode. A replica receives all data via streaming replication but cannot accept writes. It automatically discovers cluster nodes and fails over to another node if the current source becomes unavailable.

### Replication

```toml
[replication]
default_write_consistency = "QUORUM"      # Write consistency level: ONE, QUORUM, ALL
default_read_consistency = "LOCAL_ONE"    # Read consistency level
write_timeout_ms = 5000                   # Write operation timeout
read_timeout_ms = 2000                    # Read operation timeout

# Anti-Entropy: Background healing for eventual consistency
# - Detects and repairs divergence between replicas
# - Uses delta sync for small lags, snapshot for large lags
# - Includes gap detection to prevent incomplete data after GC
enable_anti_entropy = true                 # Enable automatic catch-up for lagging nodes
anti_entropy_interval_seconds = 60         # How often to check for lag (default: 60s)
delta_sync_threshold_transactions = 10000  # Delta sync if lag < 10K txns
delta_sync_threshold_seconds = 3600        # Snapshot if lag > 1 hour

# Garbage Collection: Reclaim disk space by deleting old transaction records
# - gc_min must be >= delta_sync_threshold (validated at startup)
# - gc_max should be >= 2x delta_sync_threshold (recommended)
# - Set gc_max = 0 for unlimited retention
gc_min_retention_hours = 2   # Keep at least 2 hours (>= 1 hour delta threshold)
gc_max_retention_hours = 24  # Force delete after 24 hours
```

**Anti-Entropy Tuning:**
- **Small clusters (2-3 nodes)**: Use default settings (60s interval)
- **Large clusters (5+ nodes)**: Consider increasing interval to 120-180s to reduce network overhead
- **High write throughput**: Increase `delta_sync_threshold_transactions` to 50000+
- **Long-running clusters**: Keep `gc_max_retention_hours` at 24+ to handle extended outages

**GC Configuration Rules (Validated at Startup):**
- `gc_min_retention_hours` must be >= `delta_sync_threshold_seconds` (in hours)
- `gc_max_retention_hours` should be >= 2x `delta_sync_threshold_seconds`
- Violating these rules will cause startup failure with helpful error messages

### Query Pipeline

```toml
[query_pipeline]
transpiler_cache_size = 10000  # LRU cache for MySQL→SQLite transpilation
validator_pool_size = 8        # SQLite connection pool for validation
```

### MySQL Protocol Server

```toml
[mysql]
enabled = true
bind_address = "0.0.0.0"
port = 3306
max_connections = 1000
unix_socket = ""              # Unix socket path (empty = disabled)
unix_socket_perm = 0660       # Socket file permissions
auto_id_mode = "compact"      # "compact" (53-bit, JS-safe) or "extended" (64-bit)
```

**Unix Socket Connection** (lower latency than TCP):
```bash
mysql --socket=/tmp/marmot/mysql.sock -u root
```

### CDC Publisher

```toml
[publisher]
enabled = false  # Enable CDC publishing to external systems

[[publisher.sinks]]
name = "kafka-main"              # Unique sink name
type = "kafka"                   # "kafka" or "nats"
format = "debezium"              # Debezium-compatible JSON (only option)
brokers = ["localhost:9092"]     # Kafka broker addresses
topic_prefix = "marmot.cdc"      # Topic pattern: {prefix}.{db}.{table}
filter_tables = ["*"]            # Glob patterns for table filtering
filter_databases = ["*"]         # Glob patterns for database filtering
batch_size = 100                 # Events to read per poll cycle
poll_interval_ms = 10            # Polling interval (default: 10ms)
retry_initial_ms = 100           # Initial retry delay on failure
retry_max_ms = 30000             # Max retry delay (30 seconds)
retry_multiplier = 2.0           # Exponential backoff multiplier
```

See the [Integrations documentation](https://maxpert.github.io/marmot/integrations) for details on event format, Kafka/NATS configuration, and use cases.

### Logging

```toml
[logging]
verbose = false          # Enable verbose logging
format = "console"       # Log format: console or json
```

### Prometheus Metrics

```toml
[prometheus]
enabled = true  # Metrics served on gRPC port at /metrics endpoint
```

**Accessing Metrics:**
```bash
# Metrics are multiplexed with gRPC on the same port
curl http://localhost:8080/metrics

# Prometheus scrape config
scrape_configs:
  - job_name: 'marmot'
    static_configs:
      - targets: ['node1:8080', 'node2:8080', 'node3:8080']
```

See `config.toml` for complete configuration reference with detailed comments.

## Benchmarks

Performance benchmarks on a local development machine (Apple M-series, 3-node cluster, single machine):

### Test Configuration

| Parameter | Value |
|-----------|-------|
| Nodes | 3 (ports 3307, 3308, 3309) |
| Threads | 16 |
| Batch Size | 10 ops/transaction |
| Consistency | QUORUM |

### Load Phase (INSERT-only)

| Metric | Value |
|--------|-------|
| Throughput | **4,175 ops/sec** |
| TX Throughput | **417 tx/sec** |
| Records Loaded | 200,000 |
| Errors | 0 |

### Mixed Workload

| Metric | Value |
|--------|-------|
| Throughput | **3,370 ops/sec** |
| TX Throughput | **337 tx/sec** |
| Duration | 120 seconds |
| Total Operations | 404,930 |
| Errors | 0 |
| Retries | 37 (0.09%) |

**Operation Distribution:**
- READ: 20%
- UPDATE: 30%
- INSERT: 35%
- DELETE: 5%
- UPSERT: 10%

### Latency (Mixed Workload)

| Percentile | Latency |
|------------|---------|
| P50 | 4.3ms |
| P90 | 14.0ms |
| P95 | 36.8ms |
| P99 | 85.1ms |

### Replication Verification

All 3 nodes maintained identical row counts (346,684 rows) throughout the test, confirming consistent replication.

> **Note**: These benchmarks are from a local development machine with all nodes on the same host. Production deployments across multiple machines will have different characteristics based on network latency. Expect P99 latencies of 50-200ms for cross-region QUORUM writes.

## Backup & Disaster Recovery

### Option 1: Litestream (Recommended)

Marmot's SQLite files are standard WAL-mode databases, compatible with [Litestream](https://litestream.io/):

```bash
litestream replicate /path/to/marmot-data/*.db s3://bucket/backup
```

### Option 2: CDC to External Storage

Enable CDC publisher to stream changes to Kafka/NATS, then archive to your preferred storage.

### Option 3: Filesystem Snapshots

Since Marmot uses SQLite with WAL mode, you can safely snapshot the data directory during operation.

## Stargazers over time
[![Stargazers over time](https://starchart.cc/maxpert/marmot.svg?variant=adaptive)](https://starchart.cc/maxpert/marmot)

## FAQs & Community

 - For FAQs visit [this page](https://maxpert.github.io/marmot/intro#faq)
 - For community visit our [discord](https://discord.gg/AWUwY66XsE) or discussions on GitHub
