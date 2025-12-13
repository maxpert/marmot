# CDC Demo - Real-Time TODO App

This demo showcases Marmot's CDC Publisher feature with a real-time TODO application.

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Web UI    │◄────►│   Marmot    │─────►│    NATS     │
│  (React)    │      │  (SQLite)   │ CDC  │  JetStream  │
└─────────────┘      └─────────────┘      └──────┬──────┘
     :3000               :3306                   │
                                                 │
                    ┌────────────────────────────┘
                    │ Subscribe
                    ▼
              ┌─────────────┐
              │  Node.js    │
              │  Backend    │
              └─────────────┘
```

- **Marmot**: SQLite database with MySQL protocol, publishing CDC events to NATS
- **NATS JetStream**: Message broker receiving Debezium-format CDC events
- **Web App**: React frontend + Node.js backend showing real-time updates

## Event Format

CDC events follow the [Debezium specification](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events):

```json
{
  "payload": {
    "before": null,
    "after": {"id": 1, "title": "Buy groceries", "completed": false},
    "op": "c",
    "source": {"db": "default", "table": "todos"}
  }
}
```

## Quick Start

```bash
# Start the demo
./run.sh up

# Open in browser
open http://localhost:3000

# View logs
./run.sh logs

# Stop
./run.sh down
```

## What You'll See

1. Add a TODO item in the web UI
2. Watch the CDC event appear in real-time
3. Edit or delete items and see corresponding events
4. Events show `op: "c"` (create), `op: "u"` (update), `op: "d"` (delete)

## Configuration

The Marmot config (`config.toml`) enables NATS publishing:

```toml
[publisher]
enabled = true

[[publisher.sinks]]
name = "nats-cdc"
type = "nats"
format = "debezium"
nats_url = "nats://nats:4222"
topic_prefix = "cdc"
filter_tables = ["*"]
```

## Learn More

- [CDC Publisher Documentation](https://maxpert.github.io/marmot/replication#cdc-publisher)
- [Debezium Event Format](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events)
