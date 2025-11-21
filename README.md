# Marmot v2

[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/marmot)](https://goreportcard.com/report/github.com/maxpert/marmot)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label=Marmot)](https://discord.gg/AWUwY66XsE)
![GitHub](https://img.shields.io/github/license/maxpert/marmot)

## What & Why?

Marmot v2 is a leaderless, distributed SQLite replication system built on a gossip-based protocol with MVCC (Multi-Version Concurrency Control) and eventual consistency.

**Key Features:**
- **Leaderless Architecture**: No single point of failure - any node can accept writes
- **MySQL Protocol Compatible**: Connect with any MySQL client (DBeaver, MySQL Workbench, mysql CLI)
- **MVCC Transactions**: Snapshot isolation with conflict detection and resolution
- **Multi-Database Support**: Create and manage multiple databases per cluster
- **Production-Ready SQL Parser**: Powered by Vitess sqlparser (same tech as YouTube)

## Quick Start

```bash
# Start a single-node cluster
./marmot-v2

# Connect with MySQL client
mysql -h localhost -P 3306 -u root

# Or use DBeaver, MySQL Workbench, etc.
```

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
4. **Data Placement**: Consistent hashing with configurable replication factor

## Stargazers over time
[![Stargazers over time](https://starchart.cc/maxpert/marmot.svg?variant=adaptive)](https://starchart.cc/maxpert/marmot)

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
| `CREATE TABLE` | ✅ Full | ⚠️ Limited | Schema changes require coordination |
| `ALTER TABLE` | ✅ Full | ⚠️ Limited | Schema changes require coordination |
| `DROP TABLE` | ✅ Full | ⚠️ Limited | Schema changes require coordination |
| `TRUNCATE TABLE` | ✅ Full | ✅ Yes | |
| `RENAME TABLE` | ✅ Full | ⚠️ Limited | Schema changes require coordination |
| `CREATE/DROP INDEX` | ✅ Full | ⚠️ Limited | Schema changes require coordination |
| `CREATE/DROP VIEW` | ✅ Full | ⚠️ Limited | Schema changes require coordination |
| `CREATE/DROP TRIGGER` | ✅ Full | ⚠️ Limited | Schema changes require coordination |
| **Database Management** |
| `CREATE DATABASE` | ✅ Full | ⚠️ Limited | Database-level operations |
| `DROP DATABASE` | ✅ Full | ⚠️ Limited | Database-level operations |
| `ALTER DATABASE` | ✅ Full | ⚠️ Limited | Database-level operations |
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

1. **Schema Changes**: While DDL statements are parsed and executed locally, schema replication is limited. All nodes should start with the same schema, and schema changes should be coordinated manually across all nodes.

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

These metadata queries are powered by the **Vitess SQL parser**, providing production-grade MySQL query compatibility.

### Connecting with MySQL Clients

```bash
# Using mysql CLI
mysql -h localhost -P 3306 -u root

# Connection string for applications
mysql://root@localhost:3306/marmot
```

## Limitations

- **Schema Changes**: DDL operations work locally but are not automatically replicated. Coordinate schema changes manually across all nodes.
- **Selective Table Watching**: All tables in a database are replicated. Selective table replication is not supported.
- **WAL Mode Required**: SQLite must use WAL mode for reliable multi-process changes.
- **Eventually Consistent**: Rows may sync out of order. `SERIALIZABLE` transaction assumptions may not hold across nodes.

## Configuration

Marmot v2 uses a TOML configuration file (default: `config.toml`). All settings have sensible defaults.

### Core Configuration

```toml
node_id = 0  # 0 = auto-generate
data_dir = "./marmot-data"
```

### MVCC Transaction Manager

```toml
[mvcc]
gc_interval_seconds = 30        # Garbage collection interval
gc_retention_hours = 1          # Keep transaction records for this long
heartbeat_timeout_seconds = 10  # Transaction timeout without heartbeat
version_retention_count = 10    # MVCC versions to keep per row
conflict_window_seconds = 10    # LWW conflict resolution window
```

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
gossip_interval_ms = 1000      # Gossip interval
gossip_fanout = 3              # Number of peers to gossip to
suspect_timeout_ms = 5000      # Suspect timeout
dead_timeout_ms = 10000        # Dead timeout
```

### Replication

```toml
[replication]
replication_factor = 3                    # Number of replicas
virtual_nodes = 150                       # Virtual nodes for consistent hashing
default_write_consistency = "QUORUM"      # Write consistency level
default_read_consistency = "LOCAL_ONE"    # Read consistency level
write_timeout_ms = 5000                   # Write operation timeout
read_timeout_ms = 2000                    # Read operation timeout
enable_anti_entropy = true                # Enable anti-entropy repairs
anti_entropy_interval_seconds = 600       # Anti-entropy interval
```

### MySQL Protocol Server

```toml
[mysql]
enabled = true
bind_address = "0.0.0.0"
port = 3306
max_connections = 1000
```

### Logging

```toml
[logging]
verbose = false          # Enable verbose logging
format = "console"       # Log format: console or json
```

### Prometheus

```toml
[prometheus]
enabled = true
address = "0.0.0.0"
port = 9090
```

See `config.toml` for complete configuration reference with detailed comments.

## FAQs & Community

 - For FAQs visit [this page](https://maxpert.github.io/marmot/intro#faq)
 - For community visit our [discord](https://discord.gg/AWUwY66XsE) or discussions on GitHub
