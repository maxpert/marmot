### DOCKER-COMPOSE EXAMPLE

- Single file docker-compose creates a network with three hosts.
- Host `marmot-leader` creates an embedded NATS server
- Clients `marmot-follower-1` and `marmot-follower-2` connect to `marmot-leader`
- Each service has a copy of it's own DB which is mounted at /app/example.db
- Leader has:
  - a `marmot.toml` and a `nats.config` to configure the server
  - a replica and snapshot setup
- Followers have a `marmot.toml` that points to `marmot-leader` and no other configuration
- SEE: (https://www.sqlitetutorial.net/sqlite-sample-database/)[https://www.sqlitetutorial.net/sqlite-sample-database/] for `chinook.db` sample