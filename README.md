# Marmot
A distributed SQLite replicator. 

[![Go](https://github.com/maxpert/marmot/actions/workflows/go.yml/badge.svg)](https://github.com/maxpert/marmot/actions/workflows/go.yml)

## What is it useful for right now?
Marmot can give you a solid replication between your nodes as Marmot builds on top of fault-tolerant 
[NATS](https://nats.io/), thus allowing robust recovery and replication. This means if you are 
running a medium traffic website based on SQLite you should be easily able to handle load 
without any problems. Read heavy workloads won't be bottle-necked at all as Marmot serves 
as a side car letting you build replication cluster without making any changes to your 
application code, and allows you to keep using to your SQLite database file. 

## Dependencies
Starting 0.4+ Marmot depends on [nats-server](https://nats.io/download/) with JetStream support.

## Production status

 - `v0.4` is production ready.
 - `v0.3` is deprecated, and unstable. DO NOT USE IT IN PRODUCTION.

## Features

 - Built on top of NATS, abstracting stream distribution and replication
 - Bidirectional replication with almost masterless architecture
 - Ability to snapshot and fully recover from those snapshots
 - SQLite based log storage

To be implemented for next GA:
 - Command batching + compression for speeding up bulk load / commit commands to propagate quickly
 - Database snapshotting and restore for cold-start and out-of-date nodes

## Running

Build
```shell
go build -o build/marmot ./marmot.go
```

Make sure you have 2 SQLite DBs with exact same schemas (ideally exact same state):

```shell
nats-server --jetstream
build/marmot -nats-url nats://127.0.0.1:4222 -node-id 1 -db-path /tmp/cache-1.db
build/marmot -nats-url nats://127.0.0.1:4222 -node-id 2 -db-path /tmp/cache-2.db
```

## Demos
Demos for `v0.4.x`:
 - [Scaling Pocketbase with Marmot](https://www.youtube.com/watch?v=QqZl61bJ9BA)

Demos for `v0.3.x` (Legacy) with PocketBase `v0.7.5`:
 - [Scaling Pocketbase with Marmot](https://youtube.com/video/VSa-VJso050)
 - [Scaling Pocketbase with Marmot - Follow up](https://www.youtube.com/watch?v=Zapupe_FREc)

## CLI Documentation

Marmot picks simplicity, and lesser knobs to configure by choice. Here are command line options you can use to
configure marmot:

 - `cleanup` - Just cleanup and exit marmot. Useful for scenarios where you are performing a cleanup of hooks and 
   change logs. (default: `false`)
 - `db-path` - Path to DB from which all tables will be replicated (default: `/tmp/marmot.db`)
 - `node-id` - An ID number (positive integer) to represent an ID for this node, this is required to be a unique
   number per node, and used for consensus protocol. (default: random number)
 - `nats-url` - URL string for NATS servers, it can also point to multiple servers as long as its comma separated (e.g.
   `nats://user:pass@127.0.0.1:4222` or `nats://user:pass@host-a:4222, nats://user:pass@host-b:4222`)
 - `max-log-entries` - Number of change log entries that should be caped in NATS JetStream. This property only applies
   when stream has not been created. If stream was never created before Marmot with automatically created stream with
   name prefix `stream-prefix` with attached subjects prefixed by `subject-prefix` (default: `1024`). It's 
   recommended to use a good high value in production environments.
 - `log-replicas` - Number of replicas for each change log entry committed to NATS (default: `1`). **Set this value to 
   at least quorum of cluster nodes to make sure that your replication logs are fault-tolerant**.
 - `subject-prefix` - Prefix for subject over which change logs will be published. Marmot distributes load by sharding
   change logs over given `shards`. Each subject will have pattern `<subject-prefix>-<shard-id>` 
   (default: `marmot-change-log`).
 - `stream-prefix` - Prefix for JetStream names for against which each sharded subject is attached. This allows to
   distribute these JetStream leaders among nodes in cluster. Each stream will have pattern of 
   `<stream-prefix>-<shards>-<shard-id>` (default: `marmot-changes`).
 - `shards` - Number of shards over which the database tables replication will be distributed on. It serves as mechanism for
   consistently hashing JetStream from `Hash(<table_name> + <primary/composite_key>)`. This will allow NATS servers to
   distribute load and scale for wider clusters. Look at internal docs on how these JetStreams and subjects are named
   (default: `8`).
 - `verbose` - Specify if system should dump debug logs on console as well. Only use this for debugging. 

For more details and internal workings of marmot [go to these docs](https://github.com/maxpert/marmot/blob/master/docs/overview.md).

## Limitations
Right now there are a few limitations on current solution:
 - You can't watch tables selectively on a DB. This is due to various limitations around snapshot and restore mechanism.
 - WAL mode required - since your DB is going to be processed by multiple process the only way to have multi-process 
   changes reliably is via WAL. 
 - Downloading snapshots of database is still WIP. However, it doesn't affect replication functionality as everything 
   is upsert or delete. Right snapshots are not restore, or initialized.
 - Marmot is eventually consistent. This simply means rows can get synced out of order, and `SERIALIZABLE` assumptions 
   on transactions might not hold true anymore.

## FAQs

For FAQs visit [this page](https://sibte.notion.site/sibte/Marmot-056983fad27a49d4a16fb91031e6ab98)
