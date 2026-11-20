# Binflux

Binflux is a Rust MySQL change data capture engine. It reads MySQL row-based binary logs, assembles committed INSERT/UPDATE/DELETE changes, and stores a durable checkpoint. It can write JSON Lines to stdout or deliver events to a Kafka-compatible broker. Phase 2 adds opt-in GTID restart, schema fingerprints with supported single-column `ALTER TABLE` changes, configurable reconnect retries, and an optional Prometheus endpoint.

## Build and quick start

Requires stable Rust with edition 2024 support.

```sh
cargo build --release
cp binflux.example.yaml binflux.yaml
export BINFLUX_MYSQL_PASSWORD='your-replication-user-password'
binflux validate --config binflux.yaml
binflux source check --config binflux.yaml
binflux run --config binflux.yaml --sink stdout
```

The replication account needs `REPLICATION SLAVE` (or its server-version equivalent) and read access to `information_schema.columns`. The source must have binary logging enabled with `binlog_format=ROW`. TLS is required. Binflux verifies the server certificate against platform roots by default; set `source.tls_ca_cert` to a PEM CA bundle for a private CA. The replication reader and metadata client both verify certificates and host names. Passwords are read from the configured environment variable and never logged.

With no checkpoint or explicit initial position, Binflux starts at the current end of the source binary log. It does not take an initial snapshot. The example configuration captures only the `shop` database. `include_databases: []` captures all databases.

## Commands

```sh
binflux init --output binflux.yaml
binflux validate --config binflux.yaml
binflux source check --config binflux.yaml
binflux run --config binflux.yaml --sink stdout
binflux run --config binflux.yaml --sink kafka
binflux status --config binflux.yaml
binflux checkpoint show --config binflux.yaml
binflux checkpoint reset --config binflux.yaml --yes
```

Checkpoint reset deletes the restart position. The explicit `--yes` flag is required.

## Delivery and current limits

Delivery is at-least-once. Binflux writes and flushes the complete transaction to stdout before atomically replacing the checkpoint. Kafka delivery waits for the broker's `acks=all` response before replacing the checkpoint. A crash or checkpoint write failure after Kafka acknowledges can replay the same transaction. Consumers should deduplicate by `event_id` when they need idempotent processing. Kafka and the local checkpoint cannot participate in one atomic commit, so exactly-once delivery is not claimed.

Transaction buffering, source event count, and queued source wire bytes are bounded. `max_event_bytes` rejects oversized individual events, while `max_buffered_bytes` applies a byte budget to queued binlog frames (a proxy for decoded memory, which also includes parser and Rust object overhead). If a transaction exceeds its configured event or byte limit, Binflux stops without checkpointing it. Slow stdout blocks processing and eventually blocks the bounded reader channel. Reconnection starts at the last durable position (or GTID set when enabled); partial in-memory transactions are discarded and replayed. On Unix, an advisory lock next to the checkpoint prevents two Binflux processes from using the same checkpoint concurrently; the OS releases it automatically after a crash.

This release supports MySQL 8.x; MariaDB compatibility has not been verified. GTID restart is opt-in and requires `gtid_mode=ON`; see [Phase 2 reliability](docs/phase2-reliability.md) before enabling it with an existing checkpoint. Single-column `ALTER TABLE` add/drop/modify/change/rename statements are accepted; unsupported DDL stops the stream. XA transactions, compressed `TRANSACTION_PAYLOAD_EVENT`s, partial JSON updates, and unknown events also stop safely. Each row event includes the column metadata and a stable schema fingerprint. UTF-8 and ASCII string columns emit text when valid; other character sets and binary strings emit a `$base64` object. Blob and JSON bytes are emitted as `$base64`; temporal timestamps preserve their source microsecond integer. Unsigned integer values are converted using table metadata. Partial row images remain sparse maps.

To expose Prometheus metrics, set `metrics.listen_address` (the example binds to `127.0.0.1:9090`) and scrape `/metrics`. The endpoint has no authentication; bind it to localhost or protect it with a trusted network boundary.

## Kafka sink

The Kafka sink supports Apache Kafka and Kafka protocol-compatible brokers such as Redpanda. Set `kafka.bootstrap_servers`, `kafka.topic`, and `kafka.partition`, then run `binflux run --config binflux.yaml --sink kafka`. The topic and selected partition must already exist. Each record contains the same CDC JSON event used by stdout, uses a database/table/primary-key JSON key when a complete primary key is available (otherwise its stable event ID), and includes `binflux-event-id`, `binflux-database`, and `binflux-table` headers. The topic accepts Maxwell-style `%{database}` and `%{table}` substitutions for routing tables to separate topics. Set `kafka.partition_by` to `database`, `table`, or `primary_key` to deterministically spread records across the topic's existing partitions while preserving ordering for that key; `fixed` (the default) sends records to the configured partition.

When a committed MySQL transaction maps to one Kafka topic and partition, Binflux sends it in one produce request. Otherwise, it waits for each topic/partition batch to be acknowledged before checkpointing. The broker is asked to acknowledge writes from all in-sync replicas. If a connection fails after the broker stored a request but before Binflux received its response, Binflux leaves the MySQL checkpoint unchanged; the transaction can be delivered again after restart. Kafka producer transactions and cross-system exactly-once coordination are not implemented. See [Kafka delivery and testing](docs/kafka-delivery.md) for the acknowledgement contract and a manual end-to-end test.

TLS uses the bundled WebPKI root set when `kafka.tls: true`. SASL supports `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`; credentials are read from the configured environment variable names, and SASL requires TLS. OAuth and custom Kafka CA bundles are not supported yet. Hash partitioning preserves per-key order, but events in one MySQL transaction may span Kafka partitions; Binflux waits for all relevant produce acknowledgements before updating its checkpoint. See the [Maxwell comparison](docs/maxwell-comparison.md) for adopted Kafka patterns and the next reliability work.

The protocol adapter is vendored under `vendor/mysql-binlog-connector-rust` because upstream's rustls implementation accepted any server certificate. Binflux uses a local patch that validates certificates using web roots or `source.tls_ca_cert`. See [the architecture proposal](docs/architecture-proposal.md) and [delivery guarantees](docs/delivery-guarantees.md).

## Integration test

The real-server smoke test builds a MySQL 8.4 container with ROW binlogs and TLS, then verifies INSERT/UPDATE/DELETE delivery and restart recovery:

```sh
./scripts/test-mysql-integration.sh
```

It uses a disposable Docker volume and removes it on exit. The certificates under `tests/certs` are test-only. Do not reuse them outside this test.

The reliability suite uses a separate Compose project, disposable volume, and dynamically selected host port. It covers commit boundaries, multi-row and large transactions, NULL/DECIMAL/JSON/blob decoding, binlog rotation, MySQL interruption and reconnect, process crash recovery, stable duplicate replay IDs, downstream write failure before checkpoint advancement, and graceful shutdown:

```sh
./scripts/test-mysql-reliability.sh
```

## Development checks

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
