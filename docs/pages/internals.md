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
- Using the hash decide JetStream and subject the change belongs to. And publish the change into that specific JetStream.
- Once JetStream has replicated the change log, mark the change published.
- As soon as change is published to JetStream rest of the nodes replay that log, and row changes are applied via state machine
  to local tables of the node. This means every row in database due to RAFT consensus at stream level will have only one 
  deterministic order of changes getting in cluster in case of race-conditions.
- Once the order is determined for a change it's applied in an upsert or delete manner to the table. So it's quite
  possible that a row committed locally is overwritten or replaced later because it was not the last one
  in order of cluster wide commit order.

