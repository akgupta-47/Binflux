# Maxwell Kafka feature comparison

Reviewed the local Maxwell source tree at `/Users/akshatgupta/Developer/maxwell`, especially `producer/MaxwellKafkaProducer.java`, `producer/partitioners/AbstractMaxwellPartitioner.java`, and `docs/docs/producers.md`.

## Adopted in Binflux

- **Topic interpolation:** `kafka.topic` supports `%{database}` and `%{table}` substitutions, inspired by Maxwell's dynamic Kafka topics.
- **Compaction-friendly keys:** Binflux uses a JSON key containing database, table, and primary-key values when the event has a complete primary key. Without one, it uses the event ID so unrelated rows do not collapse to the same key.
- **Partition selection:** `kafka.partition_by` supports `fixed`, `database`, `table`, and `primary_key`. Hashing uses SHA-256 over the selected routing key, so assignment is deterministic across restarts and languages. The fixed mode remains the default. Hash modes preserve order for each selected key; a MySQL transaction may span Kafka partitions, and consumers must not treat it as atomically visible across partitions.
- **Acknowledgement before checkpoint:** Binflux waits for the produce response for each partition batch before advancing its local source checkpoint. It does not claim exactly-once delivery because Kafka and the checkpoint are not one transaction.

## Strong next candidates

1. **DDL event output.** Maxwell can publish schema changes to a dedicated DDL topic. Binflux currently applies its supported single-column ALTER handling through live MySQL metadata, but does not emit DDL records. Add an opt-in, versioned schema-change event before broadening DDL support.
2. **Richer table and column filters.** Maxwell supports include/exclude patterns over databases, tables, and columns. Binflux currently has a database allowlist. Add table filtering first, then column redaction with explicit key-column behavior.
3. **Poison-event handling.** Maxwell supports a dead-letter destination for selected publish failures. Binflux currently stops without checkpointing when a transaction cannot be delivered. A safe Binflux DLQ must preserve the original event ID and source coordinates, and must checkpoint only after the DLQ broker acknowledges the complete affected transaction. Keep fail-stop as the default.
4. **Health and producer diagnostics.** Maxwell exposes publish success/failure, latency, and health diagnostics. Binflux has source and checkpoint metrics; add sink acknowledgement counters, latency, and a readiness signal tied to both source and sink state.
5. **Bootstrap snapshots.** Maxwell includes table bootstrapping to seed downstream state. Binflux starts at the current binlog end and has no snapshot support. A correct snapshot needs a consistent snapshot/binlog handoff and snapshot progress checkpoints, so this is a separate project phase.

## Deliberate differences

Binflux keeps row events as one normalized envelope with before/after images, schema version, source position, and stable event ID. It does not copy Maxwell's output envelope or asynchronous callback model. Binflux's pipeline needs a synchronous acknowledgement boundary so it can checkpoint only after sink success; buffered asynchronous sending would need an explicit in-flight ledger and bounded queue to preserve that guarantee.

Kafka partition metadata is cached when each topic is first used. If partitions are added or removed while Binflux runs in a hash mode, restart Binflux to refresh the assignment. Create every configured or interpolated topic before its first event arrives.
