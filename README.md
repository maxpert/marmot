# Marmot
A distributed SQLite replicator. 

[![Go](https://github.com/maxpert/marmot/actions/workflows/go.yml/badge.svg)](https://github.com/maxpert/marmot/actions/workflows/go.yml)

## What is it useful for right now?
If you are using SQLite as ephemeral storage, or a scenario where eventual consistency is fine for you.
Marmot can give you a solid replication between your nodes as Marmot builds on top of fault-tolerant
consensus protocol (Multi-Group Raft), thus allowing robust recovery and replication. This means 
if you are running a medium traffic website based on SQLite you should be easily able to handle 
load without any problems. Read heavy workloads won't be bottle-neck at all.

## Running

Build
```shell
go build -o build/marmot ./marmot.go
```

Make sure you have 2 SQLite DBs with exact same schemas (ideally exact same state):

```shell
rm -rf /tmp/raft # Clear out previous raft state, only do for cold start
build/marmot -bootstrap 2@127.0.0.1:8162 -bind 127.0.0.1:8161 -bind-pane localhost:6001 -node-id 1 -db-path /tmp/cache-1.db
build/marmot -bootstrap 1@127.0.0.1:8161 -bind 127.0.0.1:8162 -bind-pane localhost:6002 -node-id 2 -db-path /tmp/cache-2.db
```

## Documentation

Marmot is picks simplicity, and lesser knobs to configure by choice. Here are command line options you can use to
configure marmot:

 - `cleanup` - Just cleanup and exit marmot. Useful for scenarios where you are performing a cleanup of hooks and 
   change logs. (default: `false`)
 - `db-path` - Path to DB from which given tables will be replicated. These tables can be specified in `replicate`
   option. (default: `/tmp/marmot.db`)
 - `replicate` - A comma seperated list of tables to replicate with no spaces in between (e.g. news,history) 
   (default: [empty]) **DEPRECATED after 0.3.11 now all tables are parsed and listed, this is required for
   snapshots to recover quickly**
 - `node-id` - An ID number (positive integer) to represent an ID for this node, this is required to be a unique
   number per node, and used for consensus protocol. (default: 0)
 - `bind` - A `host:port` combination of listen for other nodes on (default: `0.0.0.0:8610`)
 - `raft-path` - Path of directory to save consensus related logs, states, and snapshots (default: `/tmp/raft`)
 - `shards` - Number of shards over which the tables will be replicated on, marmot uses shards to distribute the 
   ownership of replication. Which allows you to distribute load over multiple nodes rather than single master. 
   By default, there are 16 shards which means you should be easily able to have upto 16 master nodes. Beyond
   that you should this flag to have a bigger cluster. Higher shards also mean more disk space, and memory 
   usage per node. Ideally these shards should be elastic (to be implemented). 
 - `bootstrap` - A comma seperated list of initial bootstrap nodes `<node_id>@<ip>:<port>` (e.g. 
   `2@127.0.0.1:8162,3@127.0.0.1:8163` will specify 2 bootstrap nodes for cluster).
 - `bind-pane` - A `host:port` combination for control panel address (default: `localhost:6010`). All the endpoints
   are basic auth protected which should be set via `AUTH_KEY` env variable (e.g. `AUTH_KEY='Basic ...'`). This 
   address should not be a public accessible, and should be only used for cluster management. This in future 
   will serve as full control panel hosting and cluster management API. **EXPERIMENTAL**
 - `verbose` - Specify if system should dump debug logs on console as well. Only use this for debugging. 

## How does it work?

Marmot works by using a pretty basic trick so that each process that's access database can capture changes,
and then Marmot can publish them to rest of the nodes. This is how it works internally:

 - Each table gets a `__marmot__<table_name>_change_log` that will record a every change via triggers to
   change log.
 - Each table gets `insert`, `update`, `delete` triggers in that kicks in `AFTER` the changes have been
   committed to the table. These triggers record `OLD` or `NEW` values into the table.

When you are running Marmot process, it's watching for changes on DB file and WAL file. Everytime there is a change
Marmot scans these tables to publish them to other nodes in cluster by:
 - Gather all change records, and for each record calculate a consistent hash based on table name + primary keys.
 - Using the hash decide the primary cluster the change belongs to.
 - Propose the change to the calculated cluster, before marking entry to be applied to cluster.
 - As soon as quorum of nodes accept and confirm change log (See `SQLiteLogDB`), row changes are applied via state machine
   to local tables of the node and primary proposer will remove from change log table. This means every row in database 
   due to consensus will have only one deterministic order of changes getting in cluster in case of race-conditions. 
 - Once the order is determined for a change it's applied in an upsert or delete manner to the table. So it's quite 
   possible that a row committed locally is overwritten or replaced later because it was not the last one 
   in order of cluster wide commit order. 

## Limitations
Right now there are a few limitations on current solution:
 - You can't watch tables selectively on a DB. This is due to various limitations around snapshot and restore mechanism.
 - WAL mode required - since your DB is going to be processed by multiple process the only way to have multi-process 
   changes reliably is via WAL. 
 - Booting off a snapshot is WIP so there might be problems in cold start yet. However, if you can have start off node
   with same copies of DB it will work flawlessly. 

## Production status

Being used for ephemeral cache storage in production services, on a very read heavy site. 
You can view my personal [status board here](https://sibte.notion.site/Marmot-056983fad27a49d4a16fb91031e6ab98). 
Here is an image from a production server running Marmot:

![image](https://user-images.githubusercontent.com/22441/189140305-3b7849dc-bd26-4059-bef4-bec6549ac5a7.png)

## FAQs

For FAQs visit [this page](https://sibte.notion.site/sibte/Marmot-056983fad27a49d4a16fb91031e6ab98)
