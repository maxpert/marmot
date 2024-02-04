# Marmot

[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/marmot)](https://goreportcard.com/report/github.com/maxpert/marmot)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label=Marmot)](https://discord.gg/AWUwY66XsE)
![GitHub](https://img.shields.io/github/license/maxpert/marmot)

## What & Why?

Marmot is a distributed SQLite replicator with leaderless, and eventual consistency. It allows you to build a robust replication 
between your nodes by building on top of fault-tolerant [NATS JetStream](https://nats.io/). 

So if you are running a read heavy website based on SQLite, you should be easily able to scale it out by adding more SQLite replicated nodes. 
SQLite is probably the most ubiquitous DB that exists almost everywhere, Marmot aims to make it even more ubiquitous for server 
side applications by building a replication layer on top.

## Quick Start

Download [latest](https://github.com/maxpert/marmot/releases/latest) Marmot and extract package using:

```
tar vxzf marmot-v*.tar.gz
```

From extracted directory run `examples/run-cluster.sh`. Make a change in `/tmp/marmot-1.db` using:

```
bash > sqlite3 /tmp/marmot-1.db
sqlite3 > INSERT INTO Books (title, author, publication_year) VALUES ('Pride and Prejudice', 'Jane Austen', 1813);
```

Now observe changes getting propagated to other database `/tmp/marmot-2.db`:

```
bash > sqlite3 /tmp/marmot-2.db
sqlite3 > SELECT * FROM Books;
```

You should be able to make changes interchangeably and see the changes getting propagated.

## Out in wild

Here are some official, and community demos/usages showing Marmot out in wild:
 - [2-node HA for edge Kubernetes - Using Marmot](https://www.youtube.com/watch?v=HycGtLjlikI)
 - [Scaling Isso with Marmot on Fly.io](https://maxpert.github.io/marmot/demo)
 - [Scaling PocketBase with Marmot on Fly.io](https://github.com/maxpert/marmot-pocketbase-flyio)
 - [Scaling PocketBase with Marmot 0.4.x](https://www.youtube.com/watch?v=QqZl61bJ9BA)
 - [Scaling Keystone 6 with Marmot 0.4.x](https://youtu.be/GQ5x8pc9vuI)

## What is the difference from others?

Marmot is essentially a CDC (Change Data Capture) and replication pipeline running top of NATS. It can automatically configure appropriate 
JetStreams making sure those streams evenly distribute load over those shards, so scaling simply boils down to adding more nodes, and 
re-balancing those JetStreams (auto rebalancing not implemented yet).

There are a few solutions like [rqlite](https://github.com/rqlite/rqlite), [dqlite](https://dqlite.io/), and 
[LiteFS](https://github.com/superfly/litefs) etc. All of them either are layers on top of SQLite (e.g. 
rqlite, dqlite) that requires them to sit in the middle with network layer in order to provide 
replication; or intercept physical page level writes to stream them off to replicas. In both
cases they require a single primary node where all the writes have to go, and then these 
changes are applied to multiple readonly replicas. 

Marmot on the other hand is born different. It's born to act as a side-car to your existing processes:
 - Instead of requiring single primary, there is **no primary**! Which means **any node can make changes to its local DB**.
   Marmot will use triggers to capture your changes, and then stream them off to NATS. 
 - Instead of being strongly consistent, Marmot is **eventually consistent**. Which means no locking, or blocking of nodes.
 - It does not require any changes to your existing SQLite application logic for reading/writing. 

Making these choices has multiple benefits:

- You can read, and write to your SQLite database like you normally do. No extension, or VFS changes.
- You can write on any node! You don't have to go to single primary for writing your data.
- As long as you start with same copy of database, all the mutations will eventually converge
  (hence eventually consistent).

## What happens when there is a race condition?

In Marmot every row is uniquely mapped to a JetStream. This guarantees that for any node to publish changes for a row it has to go through 
same JetStream as everyone else. If two nodes perform a change to same row in parallel, both of the nodes will compete to publish their 
change to JetStream cluster. Due to [RAFT quorum](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering#raft) 
constraint only one of the writer will be able to get its changes published first. Now as these changes are applied (even the publisher applies 
its own changes to database) the **last writer** will always win. This means there is NO serializability guarantee of a transaction 
spanning multiple tables. This is a design choice, in order to avoid any sort of global locking, and performance. 

## Stargazers over time
[![Stargazers over time](https://starchart.cc/maxpert/marmot.svg?variant=adaptive)](https://starchart.cc/maxpert/marmot)

## Limitations
Right now there are a few limitations on current solution:
 - Marmot does not support schema changes propagation, so any tables you create or columns you change won't be reflected.
   This feature is being [debated](https://github.com/maxpert/marmot/discussions/59) and will be available in future
   versions of Marmot. 
 - You can't watch tables selectively on a DB. This is due to various limitations around snapshot and restore mechanism.
 - WAL mode required - since your DB is going to be processed by multiple processes the only way to have multi-process 
   changes reliably is via WAL. 
 - Marmot is eventually consistent - This simply means rows can get synced out of order, and `SERIALIZABLE` assumptions 
   on transactions might not hold true anymore. However your application can choose to redirect writes to single node
   so that your changes are always replayed in order.  

## Features

![Eventually Consistent](https://img.shields.io/badge/Eventually%20Consistent-✔️-green)
![Leaderless Replication](https://img.shields.io/badge/Leaderless%20Replication-✔️-green)
![Fault Tolerant](https://img.shields.io/badge/Fault%20Tolerant-✔️-green)
![Built on NATS](https://img.shields.io/badge/Built%20on%20NATS-✔️-green)

 - Leaderless replication never requiring a single node to handle all write load.
 - Ability to snapshot and fully recover from those snapshots. Multiple storage options for snapshot:
   - ![NATS Blob Storage](https://img.shields.io/badge/NATS%20Blob-%E2%9C%94%EF%B8%8F-green)
   - ![WebDAV](https://img.shields.io/badge/WebDAV-%E2%9C%94%EF%B8%8F-green)
   - ![SFTP](https://img.shields.io/badge/SFTP-%E2%9C%94%EF%B8%8F-green)
   - S3 Compatible:
     - ![AWS S3](https://img.shields.io/badge/AWS%20S3-%E2%9C%94%EF%B8%8F-green)
     - ![Minio](https://img.shields.io/badge/Minio-%E2%9C%94%EF%B8%8F-green)
     - ![Blackblaze](https://img.shields.io/badge/Blackblaze-%E2%9C%94%EF%B8%8F-green)
     - ![SeaweedFS](https://img.shields.io/badge/SeaweedFS-%E2%9C%94%EF%B8%8F-green)

 - Built with NATS, abstracting stream distribution and replication.
 - Support for log entry compression, handling content heavy CMS needs.
 - Sleep timeout support for serverless scenarios.
 

## Dependencies
Starting 0.8+ Marmot comes with embedded [nats-server](https://nats.io/download/) with JetStream support. This not only reduces 
the dependencies/processes that one might have to spin up, but also provides with out-of-box tooling like 
[nat-cli](https://github.com/nats-io/natscli). You can also use existing libraries to build additional
tooling and scripts due to standard library support. Here is one example using Deno:

```
deno run --allow-net https://gist.githubusercontent.com/maxpert/d50a49dfb2f307b30b7cae841c9607e1/raw/6d30803c140b0ba602545c1c0878d3394be548c3/watch-marmot-change-logs.ts -u <nats_username> -p <nats_password> -s <comma_seperated_server_list>
```

The output will look something like this:
![image](https://user-images.githubusercontent.com/22441/196061378-21f885b3-7958-4a7e-994b-09d4e86df721.png)

## Production status

 - `v0.8.x` introduced support for embedded NATS. This is recommended version for production.
 - `v0.7.x` moves to file based configuration rather than CLI flags, and S3 compatible snapshot storage. 
 - `v0.6.x` introduces snapshot save/restore. It's in pre-production state.
 - `v0.5.x` introduces change log compression with zstd.
 - `v0.4.x` introduces NATS based change log streaming, and continuous multi-directional sync.
 - `v0.3.x` is deprecated, and unstable. DO NOT USE IT IN PRODUCTION.

## CLI Documentation

Marmot picks simplicity, and lesser knobs to configure by choice. Here are command line options you can use to
configure marmot:

 - `config` - Path to a TOML configuration file. Check out `config.toml` comments for detailed documentation
   on various configurable options. 
 - `cleanup` (default: `false`) - Just cleanup and exit marmot. Useful for scenarios where you are 
   performing a cleanup of hooks and change logs. 
 - `save-snapshot` (default: `false` `Since 0.6.x`) - Just snapshot the local database, and upload snapshot 
   to NATS/S3 server
 - `cluster-addr` (default: none `Since 0.8.x`) - Sets the binding address for cluster, when specifying
   this flag at-least two nodes will be required (or `replication_log.replicas`). It's a simple 
   `<bind_address>:<port>` pair that can be used to bind cluster listening server. 
   - Since `v0.8.4` Marmot will automatically expose a leaf server on `<bind_address>:<port + 1>`. This is
     intended to reduce the number for flags. So if you expose cluster on port `4222` the port `4223` will
     be automatically a leaf server listener.
 - `cluster-peers` (default: none `Since 0.8.x`) - Comma separated list of `nats://<host>:<port>/` peers of 
   NATS cluster. You can also use (Since version `v0.8.4` ) `dns://<dns>:<port>/` to A/AAAA record lookups. 
   Marmot will automatically resolve the DNS IPs at boot time to expand the routes with value of 
   `nats://<ip>:<port>/` value, where `<ip>` is replaced with all the DNS entries queried. There
   are two additional query parameters you can use:
   - `min` - forcing Marmot to wait for minimum number of entries (e.g. `dns://foo:4222/?min=3` will require
     3 DNS entries to be present before embedded NATs server is started)
   - `interval_ms` - delay between DNS queries, which will prevent Marmot from flooding DNS server.
 - `leaf-server` (default: none `Since v0.8.4` )- Comma separated list of `nats://<host>:<port>/` 
   or `dns://<dns>:<port>/` just like `cluster-peers` can be used to connect to a cluster 
   as a leaf node. 

For more details and internal workings of marmot [go to these docs](https://maxpert.github.io/marmot/).

## FAQs & Community 

 - For FAQs visit [this page](https://maxpert.github.io/marmot/intro#faq)
 - For community visit our [discord](https://discord.gg/AWUwY66XsE) or discussions on GitHub

## Our sponsor

Last but not least we would like to thank our sponsors who have been supporting development of this project.

[<img src="https://resources.jetbrains.com/storage/products/company/brand/logos/GoLand_icon.png" alt="GoLand logo." height="64" />
<img src="https://resources.jetbrains.com/storage/products/company/brand/logos/jb_beam.png" alt="JetBrains Logo (Main) logo." height="64">](https://www.jetbrains.com/?utm_medium=opensource&utm_source=marmot)
