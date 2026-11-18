# Binflux

Binflux is a Rust MySQL change data capture engine. It reads MySQL row-based binary logs, assembles committed INSERT/UPDATE/DELETE changes, writes JSON Lines to stdout, and stores a durable checkpoint. Phase 2 adds opt-in GTID restart, schema fingerprints with supported single-column `ALTER TABLE` changes, configurable reconnect retries, and an optional Prometheus endpoint.

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
binflux status --config binflux.yaml
binflux checkpoint show --config binflux.yaml
binflux checkpoint reset --config binflux.yaml --yes
```

Checkpoint reset deletes the restart position. The explicit `--yes` flag is required.

## Delivery and current limits

Delivery is at-least-once. Binflux writes and flushes the complete transaction to stdout before atomically replacing the checkpoint. A crash after stdout delivery but before checkpoint replacement can replay the same transaction. Consumers should deduplicate by source file/position and row index when they need idempotent processing. Stdout cannot participate in an atomic commit with the checkpoint, so exactly-once delivery is not claimed.

Transaction buffering, source event count, and queued source wire bytes are bounded. `max_event_bytes` rejects oversized individual events, while `max_buffered_bytes` applies a byte budget to queued binlog frames (a proxy for decoded memory, which also includes parser and Rust object overhead). If a transaction exceeds its configured event or byte limit, Binflux stops without checkpointing it. Slow stdout blocks processing and eventually blocks the bounded reader channel. Reconnection starts at the last durable position (or GTID set when enabled); partial in-memory transactions are discarded and replayed. On Unix, an advisory lock next to the checkpoint prevents two Binflux processes from using the same checkpoint concurrently; the OS releases it automatically after a crash.

This release supports MySQL 8.x; MariaDB compatibility has not been verified. GTID restart is opt-in and requires `gtid_mode=ON`; see [Phase 2 reliability](docs/phase2-reliability.md) before enabling it with an existing checkpoint. Single-column `ALTER TABLE` add/drop/modify/change/rename statements are accepted; unsupported DDL stops the stream. XA transactions, compressed `TRANSACTION_PAYLOAD_EVENT`s, partial JSON updates, and unknown events also stop safely. Each row event includes the column metadata and a stable schema fingerprint. UTF-8 and ASCII string columns emit text when valid; other character sets and binary strings emit a `$base64` object. Blob and JSON bytes are emitted as `$base64`; temporal timestamps preserve their source microsecond integer. Unsigned integer values are converted using table metadata. Partial row images remain sparse maps.

To expose Prometheus metrics, set `metrics.listen_address` (the example binds to `127.0.0.1:9090`) and scrape `/metrics`. The endpoint has no authentication; bind it to localhost or protect it with a trusted network boundary.

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
