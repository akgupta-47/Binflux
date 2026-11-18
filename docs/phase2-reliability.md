# Phase 2 reliability features

## GTID restart

Set `source.gtid_restart: true` only when MySQL reports `gtid_mode=ON`. Binflux seeds a fresh checkpoint from `@@GLOBAL.gtid_executed`, then adds each committed GTID before checkpointing. On restart or reconnect it asks MySQL to continue after that executed set. The file and position remain in the checkpoint for diagnostics and output coordinates.

GTID mode cannot be combined with `source.initial_file` / `source.initial_position`: a full executed set for an arbitrary file coordinate cannot be inferred safely. Checkpoints created before `gtid_set` was added have no restartable set. Continue those with GTID restart disabled, or intentionally reset to the current binlog end before enabling it. Resetting skips events before the new start point.

GTID restart remains at-least-once with stdout. If the process writes rows and crashes before replacing the checkpoint, MySQL can replay the transaction.

## Reconnect retries

`source.retry_initial_delay_ms` and `source.retry_max_delay_ms` define exponential backoff. `source.retry_max_attempts: 0` retries indefinitely; a positive value stops the run after that many retries. `source.retry_reset_after_ms` resets the retry counter after a connection remains healthy for that duration. Partial transactions are discarded on disconnect and replayed from the durable file/position or GTID checkpoint.

## Metrics

Set `metrics.listen_address` to enable a Prometheus text endpoint at `/metrics`. Metrics include delivered transactions and rows, reconnect attempts, connection state, queued source messages, checkpoint timestamp and position, and schema versions observed. The listener has no authentication; bind it to localhost or secure it outside Binflux.

## Schema evolution

Supported DDL and output schema fields are described in [schema-evolution.md](schema-evolution.md). The supported ALTER subset intentionally fails closed for multi-action and unrecognized statements.
