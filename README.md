# Marmot

[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/marmot)](https://goreportcard.com/report/github.com/maxpert/marmot)
[![Go](https://github.com/maxpert/marmot/actions/workflows/go.yml/badge.svg)](https://github.com/maxpert/marmot/actions/workflows/go.yml)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label=Marmot)](https://discord.gg/AWUwY66XsE)
![GitHub](https://img.shields.io/github/license/maxpert/marmot)

## What & Why?
Marmot is a distributed SQLite replicator with leaderless, and eventual consistency. It allows you to build a robust replication 
between your nodes by building on top of fault-tolerant [NATS Jetstream](https://nats.io/). This means if you are running a read 
heavy website based on SQLite, you should be easily able to scale it out by adding more SQLite replicated nodes. SQLite is 
probably the most ubiquitous DB that exists almost everywhere, Marmot aims to make it even more ubiquitous for server 
side applications by building a replication layer on top.

## What is the difference from others?

There are a few solutions like [rqlite](https://github.com/rqlite/rqlite), [dqlite](https://dqlite.io/), and 
[LiteFS](https://github.com/superfly/litefs) etc. All of them either are layers on top of SQLite (e.g. 
rqlite, dqlite) that requires them to sit in the middle with network layer in order to provide 
replication; or intercept physical page level writes to stream them off to replicas. In both
cases they require a single primary node where all the writes have to go, and then these 
changes are applied to multiple readonly replicas. 

Marmot on the other hand is born different. It's born to act as a side-car to your existing processes:
 - Instead of requiring single primary, there is no primary! Which means any node can make changes to its local DB.
   Marmot will use triggers to capture your changes (hence atomic records), and then stream them off to NATS. 
 - Instead of being strongly consistent, it's eventually consistent. Which means no locking, or blocking of nodes.
 - It does not require any changes to your application logic for reading/writing. 

Making these choices has multiple benefits:

- You can read and write to your SQLite database like you normally do. No extension, or VFS changes.
- You can write on any node! You don't have to go to single primary for writing your data.
- As long as you start with same copy of database, all the mutations will eventually converge
  (hence eventually consistent).

Marmot is a CDC (Change Data Capture) pipeline running top of NATS. It can automatically configure appropriate JetStreams making sure
those streams evenly distribute load over those shards, so scaling simply boils down to adding more nodes, and re-balancing
those JetStreams (To be automated in future versions).

## Dependencies
Starting 0.4+ Marmot depends on [nats-server](https://nats.io/download/) with JetStream support.
Instead of building an in process consensus algorithm, this unlocks more use-cases like letting 
external applications subscribe to these changes and build more complex use-cases around their
application needs. Here is one example that you can just run locally using Deno:

```
deno run --allow-net https://gist.githubusercontent.com/maxpert/d50a49dfb2f307b30b7cae841c9607e1/raw/6d30803c140b0ba602545c1c0878d3394be548c3/watch-marmot-change-logs.ts -u <nats_username> -p <nats_password> -s <comma_seperated_server_list>
```

The output will look something like this:
![image](https://user-images.githubusercontent.com/22441/196061378-21f885b3-7958-4a7e-994b-09d4e86df721.png)

## Production status

 - `v0.6.x` introduces snapshot save/restore. It's in pre-production state. Is being used successfully 
    to run a read heavy site (per node 4,796 reads /sec, 138.3 writes / sec).
 - `v0.5.x` introduces change log compression with zstd.
 - `v0.4.x` introduces NATS based change log streaming, and continuous multi-directional sync.
 - `v0.3.x` is deprecated, and unstable. DO NOT USE IT IN PRODUCTION.

## Features

![Eventually Consistent](https://img.shields.io/badge/Eventually%20Consistent-✔️-green)
![Leaderless Replication](https://img.shields.io/badge/Leaderless%20Replication-✔️-green)
![Fault Tolerant](https://img.shields.io/badge/Fault%20Tolerant-✔️-green)
![Built on NATS](https://img.shields.io/badge/Built%20on%20NATS-✔️-green)

 - Leaderless replication never requiring a single node to handle all write load.
 - Built on top of NATS, abstracting stream distribution and replication.
 - Ability to snapshot and fully recover from those snapshots.
 - SQLite based log storage, so all the tooling with SQLite is at your disposal.
 - Support for log entry compression, handling content heavy CMS needs.

## Running

Build
```shell
go build -o build/marmot ./marmot.go
```

Make sure you have 2 SQLite DBs with exact same schemas and just run:

```shell
nats-server --jetstream
build/marmot -config examples/config-1.toml -verbose
build/marmot -config examples/config-2.toml -verbose
```

## Demos
Demos for `v0.4.x`:
 - [Scaling PocketBase with Marmot](https://www.youtube.com/watch?v=QqZl61bJ9BA)
 - [Scaling Keystone 6 with Marmot](https://youtu.be/GQ5x8pc9vuI)

Demos for `v0.3.x` (Legacy) with PocketBase `v0.7.5`:
 - [Scaling PocketBase with Marmot](https://youtube.com/video/VSa-VJso050)
 - [Scaling PocketBase with Marmot - Follow up](https://www.youtube.com/watch?v=Zapupe_FREc)

## CLI Documentation

Marmot picks simplicity, and lesser knobs to configure by choice. Here are command line options you can use to
configure marmot:

 - `config` - Path to a TOML configuration file. Check out `config.toml` comments for detailed documentation
   on various configurable options. 
 - `cleanup` - Just cleanup and exit marmot. Useful for scenarios where you are performing a cleanup of hooks and 
   change logs. (default: `false`)
 - `save-snapshot` - Just snapshot the local database, and upload snapshot to NATS server (default: `false`) 
   `Since 0.6.x`

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

Last but not least we would like to thank our sponsors who have been supporting development of this project.

[![DigitalOcean Referral Badge](https://web-platforms.sfo2.cdn.digitaloceanspaces.com/WWW/Badge%201.svg)](https://www.digitalocean.com/?utm_medium=opensource&utm_source=marmot)
