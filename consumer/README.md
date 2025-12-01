# Consumer

```sh
NATS_STREAM_NAME=cdc_rt iex -S mix
```

```elixir
iex> Producer.run_test(20, "table name")
```

## Optimisations

Parse WAL -> Decode tuple -> build event -> add to batch -> flush conditions and msgpck encode

Currently, 30 kEvt/s sustained with WAL lag <1MB (33µs pg to nats), peeks to 45-50 kEvt/s as long as CPU stays low and memory low (20MB).

Test: a bulk 150 INSERT requests per ms.

- Tune PostgreSQL

-- Reduce WAL verbosity for bulk operations
`SET synchronous_commit = off;` -- For the replication connection

-- Increase WAL buffers
``ALTER SYSTEM SET wal_buffers = '64MB';

-- Tune checkpoint frequency
`ALTER SYSTEM SET checkpoint_timeout = '30min';`

- Table Partitioning (Horizontal Scaling): run multiple bridges:
Bridge 1: --table users → stream CDC_USERS
Bridge 2: --table orders → stream CDC_ORDERS
Each bridge handles 30K events/sec independently
