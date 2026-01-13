# Configuration Reference

This document provides a comprehensive reference for all HarmonyLite configuration options. Use this guide to understand available settings and fine-tune your deployment.

## Configuration File Format

HarmonyLite uses a TOML configuration file format. By default, it looks for `config.toml` in the current directory, but you can specify a different path with the `-config` command-line parameter.

## Basic Configuration

```toml
# Database path (required)
db_path = "/path/to/your.db"

# Unique node identifier (required, integer)
node_id = 1

# Path to persist sequence map (required)
seq_map_path = "/path/to/seq-map.cbor"

# Enable/disable publishing changes (optional, default: true)
publish = true

# Enable/disable replicating changes (optional, default: true)
replicate = true

# Number of maximum rows to process per batch (optional, default: 512)
scan_max_changes = 512

# Cleanup interval in milliseconds (optional, default: 5000)
cleanup_interval = 5000

# Sleep timeout in milliseconds for serverless environments (optional, default: 0, disabled)
sleep_timeout = 0

# Polling interval in milliseconds (optional, default: 0, disabled)
# Only useful for broken or buggy file system watchers
polling_interval = 0
```

## Replication Log Settings

```toml
[replication_log]
# Number of shards for parallel processing (optional, default: 1)
shards = 1

# Maximum entries per stream (optional, default: 1024)
max_entries = 1024

# Number of stream replicas for fault tolerance (optional, default: 1)
replicas = 3

# Enable zstd compression for change logs (optional, default: false)
compress = true

# Update existing stream if configurations don't match (optional, default: false)
update_existing = false
```

## Snapshot Settings

```toml
[snapshot]
# Enable snapshot support (optional, default: false)
enabled = true

# Snapshot storage backend (required if enabled)
# Options: "nats", "s3", "webdav", "sftp"
store = "nats"

# Snapshot interval in milliseconds (optional, default: 0, disabled)
# If there was a snapshot saved within interval range due to log threshold triggers, 
# then new snapshot won't be saved
interval = 3600000

# Leader election TTL in milliseconds (optional, default: 30000)
# Used when multiple nodes have publish=true to coordinate who uploads snapshots
# Only one node will be elected as the snapshot leader at a time
leader_ttl = 30000
```

## NATS Configuration

```toml
[nats]
# List of NATS server URLs (optional, if empty uses embedded server)
urls = ["nats://localhost:4222"]

# Prefix for change log subjects (optional, default: "harmonylite-change-log")
subject_prefix = "harmonylite-change-log"

# Prefix for JetStream streams (optional, default: "harmonylite-changes")
stream_prefix = "harmonylite-changes"

# Bind address for embedded NATS server (optional, default: "0.0.0.0:4222")
bind_address = "0.0.0.0:4222"

# Path to custom NATS server config file (optional)
server_config = "/path/to/nats-server.conf"

# Connection retries for external servers (optional, default: 5)
connect_retries = 5

# Delay between reconnect attempts in seconds (optional, default: 2)
reconnect_wait_seconds = 2

# Authentication username (optional)
user_name = "harmonylite"

# Authentication password (optional)
user_password = "secure-password-here"

# Path to NKEY seed file (optional)
seed_file = "/path/to/user.seed"

# TLS configuration (optional)
ca_file = "/path/to/ca.pem"
cert_file = "/path/to/client-cert.pem"
key_file = "/path/to/client-key.pem"
```

## NATS Snapshot Storage

```toml
[snapshot.nats]
# Number of snapshot replicas (optional, default: 1)
replicas = 2

# Bucket name for object storage (optional)
bucket = "harmonylite-snapshots"
```

## S3 Snapshot Storage

```toml
[snapshot.s3]
# S3 endpoint (required for S3 storage)
endpoint = "s3.amazonaws.com"

# Path prefix inside bucket (optional)
path = "harmonylite/snapshots"

# Bucket name (required for S3 storage)
bucket = "your-backup-bucket"

# Use SSL for connections (optional, default: false)
use_ssl = true

# Access key for authentication (required for S3 storage)
access_key = "your-access-key"

# Secret key for authentication (required for S3 storage)
secret = "your-secret-key"

# Session token (optional)
session_token = ""
```

## WebDAV Snapshot Storage

```toml
[snapshot.webdav]
# WebDAV server URL with parameters (required for WebDAV storage)
url = "https://<webdav_server>/<web_dav_path>?dir=/snapshots/path/for/harmonylite&login=<username>&secret=<password>"
```

## SFTP Snapshot Storage

```toml
[snapshot.sftp]
# SFTP server URL with credentials (required for SFTP storage)
url = "sftp://<user>:<password>@<sftp_server>:<port>/path/to/save/snapshot"
```

## Logging Configuration

```toml
[logging]
# Enable verbose logging (optional, default: false)
verbose = true

# Log format (optional, default: "console")
# Options: "console", "json"
format = "json"
```

## Prometheus Metrics

```toml
[prometheus]
# Enable Prometheus metrics (optional, default: false)
enable = true

# Metrics HTTP listener address (optional, default: "0.0.0.0:3010")
bind = "0.0.0.0:3010"

# Metrics namespace (optional, default: "harmonylite")
namespace = "harmonylite"

# Metrics subsystem (optional, default: "")
subsystem = ""
```

## Health Check Endpoint

```toml
[health_check]
# Enable health check endpoint (optional, default: false)
enable = false

# Health check HTTP listener address (optional, default: "0.0.0.0:8090")
bind = "0.0.0.0:8090"

# Health check endpoint path (optional, default: "/health")
path = "/health"

# Include detailed information in response (optional, default: true)
detailed = true
```

## Example Configurations

### Basic Single Node

```toml
db_path = "/path/to/data.db"
node_id = 1
seq_map_path = "/path/to/seq-map.cbor"

[replication_log]
shards = 1
max_entries = 1024
replicas = 1
```

### Production Cluster

```toml
db_path = "/var/lib/harmonylite/data.db"
node_id = 1
seq_map_path = "/var/lib/harmonylite/seq-map.cbor"
cleanup_interval = 30000

[replication_log]
shards = 4
max_entries = 2048
replicas = 3
compress = true

[snapshot]
enabled = true
store = "s3"
interval = 3600000

[snapshot.s3]
endpoint = "s3.amazonaws.com"
path = "harmonylite/snapshots"
bucket = "your-backup-bucket"
use_ssl = true
access_key = "your-access-key"
secret = "your-secret-key"

[nats]
urls = ["nats://nats-server-1:4222", "nats://nats-server-2:4222"]
connect_retries = 10
reconnect_wait_seconds = 5

[prometheus]
enable = true
bind = "0.0.0.0:3010"

[health_check]
enable = true
bind = "0.0.0.0:8090"
path = "/health"
detailed = true

[logging]
verbose = true
format = "json"
```

### Edge Node (Read-Only)

```toml
db_path = "/var/lib/harmonylite/data.db"
node_id = 5
seq_map_path = "/var/lib/harmonylite/seq-map.cbor"
publish = false  # Don't publish changes
replicate = true  # Only receive changes

[replication_log]
shards = 2
max_entries = 1024
replicas = 2

[nats]
urls = ["nats://central-nats:4222"]
reconnect_wait_seconds = 10
```

## Command-Line Options

In addition to the configuration file, HarmonyLite accepts several command-line parameters:

| Parameter | Description |
|-----------|-------------|
| `-config` | Path to configuration file |
| `node-id` | Override node ID from config file |
| `-cluster-addr` | Embedded NATS server cluster address |
| `-cluster-peers` | Comma-separated list of peer URLs |
| `-leaf-servers` | Comma-separated list of leaf servers |
| `-cleanup` | Clean up triggers and log tables |
| `-save-snapshot` | Force snapshot creation |
| `-pprof` | Enable profiling server on specified address |
| `-help` | Display help information |

Example usage:
```bash
harmonylite -config /etc/harmonylite/config.toml -cluster-addr 127.0.0.1:4222 -cluster-peers nats://127.0.0.1:4223/
```