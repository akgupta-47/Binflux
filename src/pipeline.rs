use crate::{
    checkpoint::{self, Checkpoint},
    checkpoint_lock::CheckpointLock,
    config::Config,
    event::{self, CdcEvent, ColumnSchema, EventPosition, Operation, SourceIdentity},
    sink::{Sink, stdout::StdoutSink},
    source::{self, SharedCheckpoint, SourceMessage},
};
use anyhow::{Context, Result, bail};
use mysql_async::{Pool, prelude::*};
use mysql_binlog_connector_rust::event::{
    event_data::EventData, event_header::EventHeader, table_map_event::TableMapEvent,
};
use std::{
    collections::HashMap,
    io,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

struct Transaction {
    active: bool,
    gtid: Option<String>,
    events: Vec<CdcEvent>,
    bytes: usize,
}
impl Transaction {
    fn new() -> Self {
        Self {
            active: false,
            gtid: None,
            events: Vec::new(),
            bytes: 0,
        }
    }
    fn begin(&mut self, gtid: Option<String>) -> Result<()> {
        if self.active {
            bail!("new transaction boundary arrived before the previous transaction committed");
        }
        self.active = true;
        self.gtid = gtid;
        Ok(())
    }
    fn clear(&mut self) {
        *self = Self::new();
    }
}

#[derive(Clone)]
struct ResolvedTable {
    database: String,
    table: String,
    columns: Option<Vec<ColumnSchema>>,
}

pub async fn run(config: Config) -> Result<()> {
    let path = Path::new(&config.checkpoint.path).to_path_buf();
    let _checkpoint_lock = CheckpointLock::acquire(&path)?;
    let pool = source::query_pool(&config)?;
    let initial_checkpoint = checkpoint::load(&path)?;
    let shared: SharedCheckpoint = Arc::new(Mutex::new(initial_checkpoint.clone()));
    let (tx, mut rx) = mpsc::channel(config.pipeline.channel_capacity);
    let _reader = source::spawn(config.clone(), shared.clone(), tx)?;
    let mut transaction = Transaction::new();
    let mut table_maps: HashMap<u64, ResolvedTable> = HashMap::new();
    let mut current_file = initial_checkpoint.map(|c| c.binlog_file);
    let mut sink = StdoutSink::new(io::stdout().lock());
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown requested; only committed transactions are retained");
                sink.close()?;
                break;
            }
            message = rx.recv() => {
                let Some(message) = message else { break; };
                match message {
                    SourceMessage::Started { file, position } => {
                        current_file = Some(file.clone());
                        transaction.clear();
                        table_maps.clear();
                        let mut saved = shared.lock().unwrap_or_else(|p| p.into_inner());
                        if saved.is_none() {
                            let baseline = Checkpoint { binlog_file: file, binlog_position: position.max(4), gtid: None };
                            checkpoint::persist(&path, &baseline)?;
                            *saved = Some(baseline);
                        }
                    }
                    SourceMessage::Disconnected(reason) => {
                        warn!(%reason, "source disconnected; dropping the uncommitted transaction buffer");
                        transaction.clear();
                        table_maps.clear();
                    }
                    SourceMessage::Fatal(reason) => bail!("source adapter stopped safely: {reason}"),
                    SourceMessage::Event { header, data, _budget } => {
                        process_event(&config, &pool, &path, &shared, &mut sink, &mut transaction,
                            &mut table_maps, &mut current_file, header, *data).await?;
                        drop(_budget);
                    }
                }
            }
        }
    }
    pool.disconnect()
        .await
        .context("closing schema lookup pool")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_event(
    config: &Config,
    pool: &Pool,
    checkpoint_path: &Path,
    shared: &SharedCheckpoint,
    sink: &mut impl Sink,
    transaction: &mut Transaction,
    table_maps: &mut HashMap<u64, ResolvedTable>,
    current_file: &mut Option<String>,
    header: EventHeader,
    data: EventData,
) -> Result<()> {
    debug!(event_type = header.event_type, "processing binlog event");
    let event_pos = || -> Result<EventPosition> {
        Ok(EventPosition {
            binlog_file: current_file.clone().context("binlog filename is unknown")?,
            binlog_position: header.next_event_position,
        })
    };
    match data {
        EventData::FormatDescription(_)
        | EventData::PreviousGtids(_)
        | EventData::HeartBeat
        | EventData::RowsQuery(_) => {}
        EventData::Rotate(rotate) => {
            if transaction.active {
                bail!("binlog rotated inside an uncommitted transaction");
            }
            let pos = u32::try_from(rotate.binlog_position)
                .context("rotated binlog position exceeds supported range")?;
            *current_file = Some(rotate.binlog_filename);
            let cp = Checkpoint {
                binlog_file: current_file.clone().unwrap(),
                binlog_position: pos,
                gtid: None,
            };
            checkpoint::persist(checkpoint_path, &cp)?;
            *shared.lock().unwrap_or_else(|p| p.into_inner()) = Some(cp);
        }
        EventData::Gtid(gtid) => transaction.begin(Some(gtid.gtid))?,
        EventData::Query(query) => {
            let sql = query
                .query
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_ascii_lowercase();
            if sql == "begin" || sql.starts_with("begin ") || sql == "start transaction" {
                if !transaction.active {
                    transaction.begin(None)?;
                }
            } else if sql == "commit" {
                if !transaction.active {
                    bail!("COMMIT query received without an active transaction");
                }
                commit(
                    checkpoint_path,
                    shared,
                    sink,
                    transaction,
                    &header,
                    current_file,
                    None,
                )?;
            } else if is_ddl(&sql) {
                if config.source.include_databases.is_empty()
                    || config
                        .source
                        .include_databases
                        .iter()
                        .any(|db| db == &query.schema)
                {
                    bail!(
                        "DDL affecting included database {} is unsupported in Phase 1; stopping to avoid decoding with a stale schema",
                        query.schema
                    );
                }
            } else if sql == "rollback" {
                transaction.clear();
            }
        }
        EventData::TableMap(map) => {
            let included = config.source.include_databases.is_empty()
                || config
                    .source
                    .include_databases
                    .iter()
                    .any(|db| db == &map.database_name);
            let columns = if included {
                Some(load_schema(pool, &map).await?)
            } else {
                None
            };
            if let Some(cols) = &columns
                && cols.len() != map.column_types.len()
            {
                bail!(
                    "schema width mismatch for {}.{} (metadata {}, binlog {})",
                    map.database_name,
                    map.table_name,
                    cols.len(),
                    map.column_types.len()
                );
            }
            table_maps.insert(
                map.table_id,
                ResolvedTable {
                    database: map.database_name,
                    table: map.table_name,
                    columns,
                },
            );
        }
        EventData::WriteRows(rows) => {
            require_transaction(transaction)?;
            let table = table_maps
                .get(&rows.table_id)
                .context("row event has no preceding table-map metadata")?;
            if let Some(schema) = &table.columns {
                for (row_ordinal, row) in rows.rows.into_iter().enumerate() {
                    let after = event::row(&row.column_values, &rows.included_columns, schema)?;
                    let position = event_pos()?;
                    append_event(
                        config,
                        transaction,
                        CdcEvent {
                            event_id: format!(
                                "{}:{}:{}:{}",
                                header.server_id,
                                position.binlog_file,
                                position.binlog_position,
                                row_ordinal
                            ),
                            source: SourceIdentity {
                                database: table.database.clone(),
                                table: table.table.clone(),
                                server_id: header.server_id,
                            },
                            operation: Operation::Insert,
                            timestamp_unix_seconds: header.timestamp,
                            transaction_id: transaction.gtid.clone(),
                            position,
                            row_ordinal,
                            before: None,
                            after: Some(after),
                        },
                    )?;
                }
            }
        }
        EventData::UpdateRows(rows) => {
            require_transaction(transaction)?;
            let table = table_maps
                .get(&rows.table_id)
                .context("row event has no preceding table-map metadata")?;
            if let Some(schema) = &table.columns {
                for (row_ordinal, (before, after)) in rows.rows.into_iter().enumerate() {
                    let before =
                        event::row(&before.column_values, &rows.included_columns_before, schema)?;
                    let after =
                        event::row(&after.column_values, &rows.included_columns_after, schema)?;
                    let position = event_pos()?;
                    append_event(
                        config,
                        transaction,
                        CdcEvent {
                            event_id: format!(
                                "{}:{}:{}:{}",
                                header.server_id,
                                position.binlog_file,
                                position.binlog_position,
                                row_ordinal
                            ),
                            source: SourceIdentity {
                                database: table.database.clone(),
                                table: table.table.clone(),
                                server_id: header.server_id,
                            },
                            operation: Operation::Update,
                            timestamp_unix_seconds: header.timestamp,
                            transaction_id: transaction.gtid.clone(),
                            position,
                            row_ordinal,
                            before: Some(before),
                            after: Some(after),
                        },
                    )?;
                }
            }
        }
        EventData::DeleteRows(rows) => {
            require_transaction(transaction)?;
            let table = table_maps
                .get(&rows.table_id)
                .context("row event has no preceding table-map metadata")?;
            if let Some(schema) = &table.columns {
                for (row_ordinal, row) in rows.rows.into_iter().enumerate() {
                    let before = event::row(&row.column_values, &rows.included_columns, schema)?;
                    let position = event_pos()?;
                    append_event(
                        config,
                        transaction,
                        CdcEvent {
                            event_id: format!(
                                "{}:{}:{}:{}",
                                header.server_id,
                                position.binlog_file,
                                position.binlog_position,
                                row_ordinal
                            ),
                            source: SourceIdentity {
                                database: table.database.clone(),
                                table: table.table.clone(),
                                server_id: header.server_id,
                            },
                            operation: Operation::Delete,
                            timestamp_unix_seconds: header.timestamp,
                            transaction_id: transaction.gtid.clone(),
                            position,
                            row_ordinal,
                            before: Some(before),
                            after: None,
                        },
                    )?;
                }
            }
        }
        EventData::Xid(_) => {
            require_transaction(transaction)?;
            commit(
                checkpoint_path,
                shared,
                sink,
                transaction,
                &header,
                current_file,
                transaction.gtid.clone(),
            )?;
        }
        EventData::XaPrepare(_) => bail!(
            "XA_PREPARE events are unsupported; refusing to checkpoint an ambiguous transaction"
        ),
        EventData::TransactionPayload(_) => {
            bail!("transaction payload event was not decoded by the selected source adapter")
        }
        EventData::NotSupported => bail!("unsupported binlog event type {}", header.event_type),
    }
    Ok(())
}

async fn load_schema(pool: &Pool, map: &TableMapEvent) -> Result<Vec<ColumnSchema>> {
    let mut conn = pool.get_conn().await.with_context(|| {
        format!(
            "reading schema for {}.{}",
            map.database_name, map.table_name
        )
    })?;
    let rows: Vec<(String, String, Option<String>, String)> = conn.exec(
        "SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME, DATA_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        (&map.database_name, &map.table_name),
    ).await.with_context(|| format!("querying schema for {}.{}", map.database_name, map.table_name))?;
    if rows.is_empty() {
        bail!(
            "no schema metadata is available for {}.{}",
            map.database_name,
            map.table_name
        );
    }
    Ok(rows
        .into_iter()
        .map(|(name, kind, charset, data_type)| ColumnSchema {
            unsigned: kind.to_ascii_lowercase().contains("unsigned"),
            binary: !matches!(
                charset.as_deref().map(str::to_ascii_lowercase).as_deref(),
                Some("utf8" | "utf8mb3" | "utf8mb4" | "ascii")
            ) && matches!(
                data_type.as_str(),
                "char"
                    | "varchar"
                    | "tinytext"
                    | "text"
                    | "mediumtext"
                    | "longtext"
                    | "binary"
                    | "varbinary"
            ),
            name,
        })
        .collect())
}

fn require_transaction(transaction: &Transaction) -> Result<()> {
    if !transaction.active {
        bail!("row/commit event arrived outside a transaction; refusing to emit or checkpoint it");
    }
    Ok(())
}

fn append_event(config: &Config, tx: &mut Transaction, event: CdcEvent) -> Result<()> {
    let bytes = serde_json::to_vec(&event)?.len();
    if tx.events.len() + 1 > config.pipeline.max_transaction_events {
        bail!("transaction exceeds max_transaction_events; no checkpoint advanced");
    }
    if tx.bytes.saturating_add(bytes) > config.pipeline.max_transaction_bytes {
        bail!("transaction exceeds max_transaction_bytes; no checkpoint advanced");
    }
    tx.bytes += bytes;
    tx.events.push(event);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn commit<S: Sink>(
    checkpoint_path: &Path,
    shared: &SharedCheckpoint,
    sink: &mut S,
    tx: &mut Transaction,
    header: &EventHeader,
    current_file: &Option<String>,
    gtid: Option<String>,
) -> Result<()> {
    if !tx.active {
        bail!("commit boundary received without an active transaction");
    }
    sink.write_transaction(&tx.events)?;
    sink.flush()?;
    let checkpoint = Checkpoint {
        binlog_file: current_file
            .clone()
            .context("cannot checkpoint before binlog filename is known")?,
        binlog_position: header.next_event_position,
        gtid: gtid.or_else(|| tx.gtid.clone()),
    };
    checkpoint::persist(checkpoint_path, &checkpoint)?;
    *shared.lock().unwrap_or_else(|p| p.into_inner()) = Some(checkpoint);
    info!(
        event_count = tx.events.len(),
        "committed transaction delivered"
    );
    tx.clear();
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
        } else {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn is_ddl(sql: &str) -> bool {
    [
        "create ",
        "alter ",
        "drop ",
        "rename ",
        "truncate ",
        "repair ",
        "optimize ",
    ]
    .iter()
    .any(|prefix| sql.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_binlog_connector_rust::event::event_header::EventHeader;

    struct TestSink {
        bytes: Vec<u8>,
    }
    impl Sink for TestSink {
        fn write_transaction(&mut self, events: &[CdcEvent]) -> Result<()> {
            for event in events {
                self.bytes.extend(serde_json::to_vec(event)?);
                self.bytes.push(b'\n');
            }
            Ok(())
        }
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn transaction_buffer_enforces_limits() {
        let mut c = Config::default();
        c.pipeline.max_transaction_events = 0;
        let mut tx = Transaction::new();
        tx.begin(None).unwrap();
        let event = CdcEvent {
            event_id: "test".into(),
            source: SourceIdentity {
                database: "d".into(),
                table: "t".into(),
                server_id: 1,
            },
            operation: Operation::Insert,
            timestamp_unix_seconds: 1,
            transaction_id: None,
            position: EventPosition {
                binlog_file: "b".into(),
                binlog_position: 10,
            },
            row_ordinal: 0,
            before: None,
            after: Some(Default::default()),
        };
        assert!(append_event(&c, &mut tx, event).is_err());
    }

    #[test]
    fn ddl_detection_is_conservative_for_common_statements() {
        assert!(is_ddl("alter table orders add column x int"));
        assert!(!is_ddl("insert into orders values (1)"));
    }

    #[test]
    fn checkpoint_only_moves_after_stdout_flush_and_full_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let shared = Arc::new(Mutex::new(None));
        let mut tx = Transaction::new();
        tx.begin(Some("uuid:9".into())).unwrap();
        let mut output = TestSink { bytes: Vec::new() };
        let header = EventHeader {
            timestamp: 1,
            event_type: 16,
            server_id: 1,
            event_length: 27,
            next_event_position: 99,
            event_flags: 0,
        };
        commit(
            &path,
            &shared,
            &mut output,
            &mut tx,
            &header,
            &Some("bin.000001".into()),
            None,
        )
        .unwrap();
        assert_eq!(
            checkpoint::load(&path).unwrap().unwrap().binlog_position,
            99
        );
    }
}
