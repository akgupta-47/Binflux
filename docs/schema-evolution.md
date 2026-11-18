# Schema version tracking and DDL support

On each `TABLE_MAP_EVENT`, Binflux reads the current column metadata from `information_schema.columns` in ordinal order. It requires the metadata width to match the binlog table map. The output row includes the SQL column types, unsigned/binary flags, and a `schema_version` SHA-256 fingerprint derived from the ordered column metadata. Consumers can persist the included schema beside each version and use it to interpret rows after a restart.

The currently supported schema evolution is one column action per `ALTER TABLE`: `ADD COLUMN`, `DROP COLUMN`, `MODIFY COLUMN`, `CHANGE COLUMN`, or `RENAME COLUMN`. After the DDL event is checkpointed, Binflux clears its in-memory table-map cache and reloads metadata at the next table-map event. The next row event therefore carries the new schema and fingerprint. Multi-action ALTER statements, other DDL, and DDL that appears inside a transaction containing row changes fail safely without advancing the checkpoint.

The schema is refreshed from the live MySQL metadata service, rather than reconstructed from SQL text. Run schema migrations normally against the same MySQL source being replicated. If the metadata width differs from the binlog event, or metadata cannot be loaded, Binflux stops before delivering or checkpointing the affected row transaction.
