# Schema tracking in Phase 1

On each TABLE_MAP event, Binflux reads the matching `information_schema.columns` rows in ordinal order. The column count must exactly match the binlog table map, and schema names/types are refreshed for each new table map instead of assumed constant for the entire process. Unsigned values are reconstructed from the SQL column type.

This is deliberately conservative. DDL on an included database stops processing because Phase 1 does not parse and version CREATE/ALTER/DROP/RENAME statements. Restarting with a reviewed schema or a future schema-evolution implementation is required before continuing. If metadata cannot be read or the binlog and SQL schema widths differ, Binflux stops before delivering or checkpointing the affected transaction.
