# Binflux Phase 0: Architecture proposal

## Scope

Phase 1 is one end-to-end MySQL 8 CDC path: row-based binlog replication, INSERT/UPDATE/DELETE decoding, commit-bounded transactions, JSON Lines on stdout, and durable file/position checkpoints. Kafka and other destinations, GTID checkpointing, MariaDB compatibility, metrics, and schema evolution remain later work. The initial no-checkpoint position is `latest`, so Binflux captures changes made after it starts; it does not perform an initial snapshot.

## Dependency evaluation

The selected protocol adapter is `mysql-binlog-connector-rust` 0.3.5 (MIT OR Apache-2.0). Its current README lists MySQL 5.6, 5.7, 8.0, and 8.4.13; file/position and GTID start modes; row event V1/V2; JSON, decimal, temporal, blob, and transaction-compressed event support; and MySQL-generated integration tests. It has rustls and OpenSSL TLS features. It does not claim MariaDB compatibility. It represents row values without column names and documents raw bytes for strings/JSON and signed Rust integer variants for unsigned MySQL columns, so Binflux must add schema metadata and explicit value conversion rather than treat its output as a complete CDC model.

The crate uses `async-std`, while the application uses Tokio. To avoid coupling the rest of the pipeline to that runtime, one dedicated OS thread owns the library's async-std replication stream and sends parsed events through a bounded Tokio channel using blocking sends. A slow sink therefore blocks the reader and propagates backpressure to the source connection. This adapter boundary can later be replaced without changing the event, checkpoint, or sink interfaces.

The alternative `mysql_cdc` project advertises MySQL and MariaDB GTID support, but its README lists no TLS and no handling for packets of 16 MB or more. Those are unacceptable defaults for this production-oriented source. `mysql_binlog` is an offline file parser and explicitly says it is untested with MySQL 8 and MariaDB, so it is not a live source choice.

## Data and correctness model

- Convert each supported row event into a typed `CdcEvent` containing source, operation, event timestamp, server ID, transaction GTID if present, and file/position coordinates.
- Resolve a `TABLE_MAP_EVENT` against `information_schema.columns`; require exact column-count agreement. Column names and unsignedness come from metadata. Unknown/ambiguous mappings fail closed.
- Preserve partial row images as sparse maps. Binary values and invalid UTF-8 remain base64-tagged data; JSON binary data is preserved as bytes in this phase rather than incorrectly decoded as text.
- Buffer events until XID or a recognized commit boundary. Emit only complete transactions. Enforce configurable event and byte caps; an oversized transaction fails without advancing the checkpoint.
- Write all transaction JSONL records and flush stdout before atomically replacing the checkpoint file. A crash between delivery and checkpoint persistence can replay a transaction, so the guarantee is at-least-once. Stdout itself is not a transactional destination and consumers should deduplicate by the stable source coordinates.
- The checkpoint records a committed file and position. It is written to a temporary file in the same directory, flushed, synced, renamed, and followed by a parent-directory sync.
- Use bounded channel capacity and bounded transaction accumulation. Source read errors trigger reconnect from the last durable checkpoint; no buffered partial transaction is checkpointed.

## Phase 1 dependencies

`tokio` for the application runtime and signal handling; `mysql-binlog-connector-rust` for live replication and event parsing; `mysql_async` for schema metadata; `serde`/`serde_yaml` for configuration and JSON; `clap` for CLI; `tracing`/`tracing-subscriber` for logs; `anyhow`/`thiserror` for errors; and `url`/`percent-encoding` to safely construct the connector URL from environment-supplied credentials. No sink-specific dependencies are included in this phase.

## Implementation plan

1. Create the modular crate, CLI/config validation, event model, and checkpoint module.
2. Add schema lookup, protocol reader adapter, row conversion, and transaction assembly.
3. Add stdout JSONL delivery with flush-before-checkpoint ordering and bounded memory.
4. Add MySQL 8 Docker Compose configuration, fixture-free unit coverage, and a real-server integration test for INSERT/UPDATE/DELETE, transaction commit, and restart recovery.
5. Format, lint, run unit and integration coverage where Docker is available, and document tested support and known limits.

## Known Phase 1 limits

MySQL is the only supported source. File/position checkpoints are implemented first; GTID may be captured on events but GTID-based recovery is not claimed. The engine starts at the current end when no checkpoint exists. DDL is not decoded into schema changes; metadata mismatches stop processing safely. XA transactions, partial JSON updates, unsupported event kinds, and oversized transactions stop the stream. No exactly-once guarantee is claimed.
