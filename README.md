# Marmot
A passive multi-master SQLite replicator. 

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

Make sure you have 2 SQLite DBs with exact same schemas (ideally empty):

```shell
build/marmot -bootstrap 2@127.0.0.1:8162 -bind 127.0.0.1:8161 -bind-pane localhost:6001 -node-id 1 -replicate table1,table2 -db-path /tmp/cache-1.db
build/marmot -bootstrap 1@127.0.0.1:8161 -bind 127.0.0.1:8162 -bind-pane localhost:6002 -node-id 2 -replicate table1,table2 -db-path /tmp/cache-2.db
```

## Limitations
Right now there are a few limitations on current solution:
 - Only incremental, change stream i.e. tables should exist with matching schema, and existing rows won't be copied over. SQLite tools are enough for that.
 - Only WAL mode supported.
 - Won't create DB file if it doesn't exist.
 - Right now no support for copying previous data of table into existing table, or copy schema. Would be introduced in future though.
 

## Production status

Being used for ephemeral cache storage in production services, on a very read heavy site. You can view my personal [status board here](https://sibte.notion.site/Marmot-056983fad27a49d4a16fb91031e6ab98).

## FAQs

For FAQs visit [this page](https://sibte.notion.site/sibte/Marmot-056983fad27a49d4a16fb91031e6ab98)
