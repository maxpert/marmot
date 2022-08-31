# Marmot
A distributed SQLite ephemeral replicator.  

[![Go](https://github.com/maxpert/marmot/actions/workflows/go.yml/badge.svg)](https://github.com/maxpert/marmot/actions/workflows/go.yml)

## What is it useful for right now?
If you are using SQLite as ephemeral storage or a scenario where eventual consistency is fine for you.
Marmot can give you a solid replication between your nodes. Marmot builds on top of fault-tolerant
consensus protocol (Multi-Group Raft), thus allowing robust recovery and replication. 

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

## Production status

Highly experimental. You can view my personal [status board here](https://sibte.notion.site/Marmot-Plan-056983fad27a49d4a16fb91031e6ab98).
