# Delivery guarantees

The Phase 1 guarantee is at-least-once to stdout. A transaction is buffered until its XID or recognized COMMIT boundary. Binflux writes each normalized event in source order, flushes stdout, and only then atomically persists the corresponding source file and position.

If the process stops after emitting rows but before the checkpoint rename, those rows are replayed after restart. If checkpoint persistence finishes, that transaction is not replayed. This ordering avoids checkpointing before the destination accepted the data, but stdout and a local file cannot be committed atomically. Consumers should derive idempotency keys from the source file, row-event position, transaction ID when present, and row ordinal.

The checkpoint is written to a sibling temporary file, `sync_all` is called on the file, the file is atomically renamed over the previous checkpoint, and the containing directory is synced. A partially received transaction never advances the checkpoint. Reconnect starts from the last durable checkpoint and may replay already-delivered work.

On Unix, a non-blocking advisory lock is held for the duration of a run using a sibling `.lock` file. A second process configured with the same checkpoint path exits before it starts reading MySQL. The lock file may remain after shutdown; the kernel lock is released on normal exit or process crash, so file presence alone does not indicate an active process.

There is no exactly-once claim. GTID is included in output and checkpoint metadata when available, but restart currently uses file/position. Kafka transactions or a transactional database sink would need a source/sink coordination protocol before stronger guarantees could be claimed.
