# Kafka delivery and testing

## Acknowledgement and checkpoint order

Binflux buffers a MySQL transaction until its commit boundary, then allows up to `pipeline.max_in_flight_transactions` transactions (default 8) to await Kafka acknowledgements at once. Transactions are enqueued to partition workers in source order; workers preserve that order and combine adjacent transaction batches where possible. The in-memory checkpoint advances only through the highest consecutive transaction whose batches have all been acknowledged. On shutdown, Binflux drains outstanding deliveries before flushing the checkpoint. The durable checkpoint is atomically persisted every `checkpoint.flush_interval_ms` (default 1000 ms) and on graceful shutdown. The Kafka client requests `acks=all`, so the broker responds after all in-sync replicas have acknowledged the write. Use an appropriate topic replication factor and `min.insync.replicas` for the durability level your deployment requires.

The Kafka acknowledgement and local checkpoint are separate commits. If Kafka stores a request but Binflux loses the response, or if Binflux crashes before the latest checkpoint is durable, source transactions since the previous flush may be replayed. Delivery is therefore at-least-once, with up to `checkpoint.flush_interval_ms` of acknowledged events eligible for replay after a crash. A transaction is never checkpointed past an earlier transaction whose Kafka acknowledgement is pending or failed. Use the stable `event_id` field as a consumer-side deduplication key. Binflux does not use Kafka producer transactions, Kafka offsets, or a distributed transaction with the checkpoint.

By default, records use the configured partition within each resolved topic, preserving source order. Set `kafka.partition_by` to `database`, `table`, or `primary_key` to hash records across the topic's current partitions. Hash mode preserves ordering for a given database/table/key, but one MySQL transaction can be split across multiple Kafka partitions. Binflux waits for all concurrent produce calls to succeed before advancing the in-memory checkpoint; Kafka consumers should not assume atomic visibility across those partitions.

`kafka.topic` can include Maxwell-style `%{database}` and `%{table}` substitutions, for example `cdc_%{database}_%{table}`. Each generated topic must exist and contain the selected partition or all partitions needed by the chosen hash mode. The partition list is discovered from broker metadata when the topic is first used; restart Binflux after changing a topic's partition count.

## Manual end-to-end check

Start a Kafka or Kafka-compatible broker reachable from the host. Create the topic and partition first; for example, using `rpk` against a local broker:

```sh
rpk topic create binflux.cdc --partitions 1
```

Set `kafka.bootstrap_servers` in `binflux.yaml` to the broker address and `kafka.partition: 0`. Start the existing MySQL Compose service and confirm that the table you plan to change already exists, since unsupported DDL stops the CDC stream.

In one terminal, start a consumer:

```sh
rpk topic consume binflux.cdc --num 1 --format '%v'
```

In a second terminal, start Binflux:

```sh
BINFLUX_MYSQL_PASSWORD=binflux-test-only \
  target/debug/binflux run --config binflux.yaml --sink kafka
```

Then commit a row change in a third terminal:

```sh
docker compose exec -T mysql sh -c \
  'MYSQL_PWD="$MYSQL_PASSWORD" mysql -h127.0.0.1 -ubinflux shop -e "UPDATE demo SET note=CONCAT(note, \"!\") WHERE id=1"'
```

The consumer should print one JSON event with `operation` set to `UPDATE` and a stable `event_id`. Its Kafka key contains the source database, table, and primary-key values when available. Check `checkpoint.path`; its position should have advanced only after Kafka acknowledged the produce request. Replaying from a checkpoint saved before that event can intentionally deliver the same `event_id` again.

For a secured broker, set `kafka.tls: true`, set `kafka.sasl_mechanism` to `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`, and point `sasl_username_env` and `sasl_password_env` to environment variable names containing credentials. Keep credentials out of YAML and logs.
