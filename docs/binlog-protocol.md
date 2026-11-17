# Binlog protocol support

Binflux uses the `mysql-binlog-connector-rust` parser and replication client, currently pinned by the vendored 0.3.5 source. Its upstream support notes list MySQL 5.6, 5.7, 8.0, and 8.4.13, file/position and GTID stream starts, ROW event V1/V2, JSON and decimal values, TLS support, and MySQL transaction compression. Binflux has only integration-targeted MySQL 8.4 so far; it does not claim the entire upstream compatibility matrix.

The adapter runs on a dedicated thread because the dependency uses async-std and Binflux uses Tokio. Events pass through a bounded Tokio channel. The local patch changes TLS certificate verification to use web roots or the configured PEM CA path. This avoids upstream's permissive certificate verifier. `source.tls_ca_cert` must name a PEM trust bundle if the server uses a private CA.

The Phase 1 decoder handles format description, previous GTIDs, heartbeat, rotate, GTID, query, table-map, write/update/delete rows, and XID events. Row values are mapped using table metadata from `information_schema.columns`, and the binlog/table schema widths must match. Unsupported events and ambiguous schema fail the stream without advancing the checkpoint.

MariaDB is not supported or tested. DDL is not applied to a live schema registry; DDL in an included database stops processing. XA prepare, compressed transaction payload events, partial JSON updates, and unknown event kinds stop explicitly. Unsignedness comes from SQL column metadata because the upstream parser's integer enum is signed. Character sets are not carried by the binlog row data; Binflux uses column metadata to emit UTF-8/ASCII text and preserves other encodings and binary strings as base64-tagged bytes.
