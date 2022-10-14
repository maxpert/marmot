# Marmot

![Eventually Consistent](https://img.shields.io/badge/Eventually%20Consistent-✔️-green)
![Multi-Master Replication](https://img.shields.io/badge/Multi--Master%20Replication-✔️-green)
![Fault Tolerant](https://img.shields.io/badge/Fault%20Tolerant-✔️-green)
![Built on NATS](https://img.shields.io/badge/Built%20on%20NATS-✔️-green)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label=Marmot)](https://discord.gg/AWUwY66XsE)
![GitHub](https://img.shields.io/github/license/maxpert/marmot)
[![Go](https://github.com/maxpert/marmot/actions/workflows/go.yml/badge.svg)](https://github.com/maxpert/marmot/actions/workflows/go.yml)

## What & Why?
Marmot can give you a solid replication between your nodes as Marmot builds on top of fault-tolerant 
[NATS](https://nats.io/), thus allowing robust recovery and replication. This means if you are 
running a read heavy website based on SQLite you should be easily able to scale it out by
adding more SQLite replicated nodes. SQLite is a probably the most ubiquitous DB that 
exists almost everywhere, this project aims to make it even more ubiquitous for 
server side applications by building a masterless replication layer on top.

## What is the difference from others?

There are a few solutions like [rqlite](https://github.com/rqlite/rqlite), [dqlite](https://dqlite.io/), and 
[LiteFS](https://github.com/superfly/litefs) etc. All of them either are layers on top of SQLite (e.g. 
rqlite, dqlite) that requires them to sit in the middle with network layer in order to provide 
replication; or intercept phsycial page level writes to stream them off to replicas. In both
cases they are mostly single primary where all the writes have to go, backed by multiple 
replicas that can only be readonly. 

Marmot on the other hand is born different. Instead of being single primary it is "masterless", instead of being strongly consistent, 
it's eventually consistent, does not require any changes to your application logic for reading/writing. This means:

 - You can read and write to your SQLite database like you normally do.
 - You can write on any node! You don't have to go to single master for writing your data.
 - As long as you start with same copy of database, all the mutations will eventually converge (hence eventually consistent).

Marmot is a CDC (Change Data Capture) pipeline running top of NATS. It can automatically confgure appropriate JetStreams making sure 
those streams evenly distribute load over those shards, so scaling simply boils down to adding more nodes, and rebalancing 
those JetStreams (To be automated in future versions). 

## Dependencies
Starting 0.4+ Marmot depends on [nats-server](https://nats.io/download/) with JetStream support.
Instead of building an in process consensus algorithm, this unlocks more use-cases like letting 
external applications subscribe to these changes and build more complex use-cases around their
application needs.

## Production status

 - `v0.4.x` onwards in pre production state. We have been using it to successfully run a read heavy site (138.3 writes / sec).
 - `v0.3.x` is deprecated, and unstable. DO NOT USE IT IN PRODUCTION.

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
 - [Scaling Keystone 6 with Marmot](https://youtu.be/GQ5x8pc9vuI)

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
   at least quorum of cluster nodes to make sure that your replication logs are fault-tolerant**. `Since 0.4.x`
 - `subject-prefix` - Prefix for subject over which change logs will be published. Marmot distributes load by sharding
   change logs over given `shards`. Each subject will have pattern `<subject-prefix>-<shard-id>` 
   (default: `marmot-change-log`).
 - `stream-prefix` - Prefix for JetStream names for against which each sharded subject is attached. This allows to
   distribute these JetStream leaders among nodes in cluster. Each stream will have pattern of 
   `<stream-prefix>-<shard-id>` (default: `marmot-changes`).
 - `shards` - Number of shards over which the database tables replication will be distributed on. It serves as mechanism for
   consistently hashing JetStream from `Hash(<table_name> + <primary/composite_key>)`. This will allow NATS servers to
   distribute load and scale for wider clusters. Look at internal docs on how these JetStreams and subjects are named
   (default: `8`).
 - `verbose` - Specify if system should dump debug logs on console as well. Only use this for debugging. 
 - `save-snapshot` - Just snapshot the local database, and upload snapshot to NATS server (default: `false`) `Since 0.6.x`
 - `enable-snapshot` - Enable capability to save and restore snapshots so that extremely lagging servers can restore snapshots at 
    boot time, and nodes can save snapshots everytime shard `1` hits sequence of multiples `max-log-entries/shards`. In theory 
    due to even distribution of logs among shards this will always cause a snapshot to be taken very close to 
    `max-log-entries`, and that snapshot will be saved in `OBJ_<stream-prefix>-snapshot-store`. (default: `true`) `Since 0.6.x`
 - `seq-map-path` - Path where Marmot saves copy of sequence entries processed so far for every shard as it 
    processes those entries. This helps recover from correct sequence 
    checkpoint (default: `/tmp/seq-map.cbor`) `Since 0.6.x`

For more details and internal workings of marmot [go to these docs](https://maxpert.github.io/marmot/).

## Limitations
Right now there are a few limitations on current solution:
 - You can't watch tables selectively on a DB. This is due to various limitations around snapshot and restore mechanism.
 - WAL mode required - since your DB is going to be processed by multiple process the only way to have multi-process 
   changes reliably is via WAL. 
 - Downloading snapshots of database is still WIP. However, it doesn't affect replication functionality as everything 
   is upsert or delete. Right snapshots are not restore, or initialized.
 - Marmot is eventually consistent. This simply means rows can get synced out of order, and `SERIALIZABLE` assumptions 
   on transactions might not hold true anymore.

## FAQs & Community 

 - For FAQs visit [this page](https://maxpert.github.io/marmot/#faq)
 - For community visit our [discord](https://discord.gg/AWUwY66XsE) or discussions on GitHub

## Our sponsor

Last but not least we will like to thank our sponsors who have been supporting development of this project.

[![DigitalOcean Referral Badge](https://web-platforms.sfo2.cdn.digitaloceanspaces.com/WWW/Badge%201.svg)](https://www.digitalocean.com/?utm_medium=opensource&utm_source=marmot)
