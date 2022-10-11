# What is Marmot?

Marmot can give you a solid replication between your nodes as Marmot builds on top of fault-tolerant [NATS](https://nats.io/), thus allowing robust recovery and replication. Marmot is designed to be a side car that is eventually consistent, thus optimized for read heavy workloads. 

# Why?

SQLite is a probably the most ubiquitous DB that exists almost everywhere, this project aims to make it even more ubiquitous for server 
side applications by building a masterless replication layer on top. This means if you are running a read heavy website based on SQLite 
you should be easily able to scale it out by adding more nodes of your app with SQLite replicated nodes. 

# Why not others?

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

## FAQ

### Won’t capturing changes with triggers use more disk space?

Yes it will require additional storage to old/new values from triggers. But right now that is the only way sqlite can and should allow one to capture changes. However, in a typical setting these captured changes will be picked up pretty quickly. Disk space is usually cheapest part of modern cloud, so I won’t obsess over it.

### How do I do a fresh restart?

Ask marmot to remove hooks and log tables by:
`marmot -db-path /path/to/your/db.db -cleanup`

### How would many shards should I have?

It depends on your usecase and what problem you are solving for. In a typical setting you should not need more than couple of dozen shards. While read scaling won't be a problem, your write throughput will depend on your network and
disk speeds (Network being the biggest culprit).
