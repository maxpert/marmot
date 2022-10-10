# How does it work?

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

## FAQ

### Won’t capturing changes with triggers use more disk space?

Yes it will require additional storage to old/new values from triggers. But right now that is the only way sqlite can and should allow one to capture changes. However, in a typical setting these captured changes will be picked up pretty quickly. Disk space is usually cheapest part of modern cloud, so I won’t obsess over it.

### How do I do a fresh restart?

Ask marmot to remove hooks and log tables by:
`marmot -db-path /path/to/your/db.db -cleanup`

### How would many shards should I have?

It depends on your usecase and what problem you are solving for. In a typical setting you should not need more than couple of dozen shards. While read scaling won't be a problem, your write throughput will depend on your network and
disk speeds (Network being the biggest culprit).
