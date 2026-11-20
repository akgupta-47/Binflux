use crate::{
    checkpoint::{self, Checkpoint},
    checkpoint_lock::CheckpointLock,
    config::Config,
    event::{self, CdcEvent, ColumnSchema, EventPosition, Operation, SourceIdentity},
    metrics::Metrics,
    sink::{Sink, kafka::KafkaSink, stdout::StdoutSink},
    source::{self, SharedCheckpoint, SourceMessage},
};
use anyhow::{Context, Result, bail};
use mysql_async::{Pool, prelude::*};
use mysql_binlog_connector_rust::command::gtid_set::GtidSet;
use mysql_binlog_connector_rust::event::{
    event_data::EventData, event_header::EventHeader, table_map_event::TableMapEvent,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinSet};
use tracing::{debug, info, warn};

struct Transaction {
    active: bool,
    gtid: Option<String>,
    gtid_set: Option<String>,
    events: Vec<CdcEvent>,
    bytes: usize,
}

struct PendingDelivery {
    sequence: usize,
    checkpoint: Checkpoint,
    event_count: usize,
    acknowledged: bool,
}
impl Transaction {
    fn new() -> Self {
        Self {
            active: false,
            gtid: None,
            gtid_set: None,
            events: Vec::new(),
            bytes: 0,
        }
    }
    fn begin(&mut self, gtid: Option<String>, mut gtid_set: Option<String>) -> Result<()> {
        if self.active {
            bail!("new transaction boundary arrived before the previous transaction committed");
        }
        self.active = true;
        if let Some(id) = &gtid {
            let mut set = GtidSet::new(gtid_set.as_deref().unwrap_or_default())?;
            set.add(id)?;
            gtid_set = Some(set.to_string());
        }
        self.gtid = gtid;
        self.gtid_set = gtid_set;
        Ok(())
    }
    fn clear(&mut self) {
        *self = Self::new();
    }
}

struct CheckpointWriter {
    path: PathBuf,
    flush_interval: Duration,
    last_flush: Instant,
    latest: Option<Checkpoint>,
    dirty: bool,
}

impl CheckpointWriter {
    fn new(path: PathBuf, initial: Option<Checkpoint>, flush_interval: Duration) -> Self {
        Self {
            path,
            flush_interval,
            last_flush: Instant::now(),
            latest: initial,
            dirty: false,
        }
    }

    fn advance(
        &mut self,
        checkpoint: Checkpoint,
        shared: &SharedCheckpoint,
        metrics: &Metrics,
        force: bool,
    ) -> Result<()> {
        *shared.lock().unwrap_or_else(|p| p.into_inner()) = Some(checkpoint.clone());
        self.latest = Some(checkpoint);
        self.dirty = true;
        if force || self.last_flush.elapsed() >= self.flush_interval {
            self.flush(metrics)?;
        }
        Ok(())
    }

    fn flush_if_due(&mut self, metrics: &Metrics) -> Result<()> {
        if self.dirty && self.last_flush.elapsed() >= self.flush_interval {
            self.flush(metrics)?;
        }
        Ok(())
    }

    fn flush(&mut self, metrics: &Metrics) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let checkpoint = self
            .latest
            .as_ref()
            .context("no checkpoint is available to persist")?;
        checkpoint::persist(&self.path, checkpoint)?;
        metrics.checkpoint_position(checkpoint.binlog_position);
        self.last_flush = Instant::now();
        self.dirty = false;
        Ok(())
    }
}

#[derive(Clone)]
struct ResolvedTable {
    database: String,
    table: String,
    columns: Option<Vec<ColumnSchema>>,
    schema_version: Option<String>,
}

pub async fn run(config: Config, sink_kind: crate::cli::SinkKind) -> Result<()> {
    let path = Path::new(&config.checkpoint.path).to_path_buf();
    let _checkpoint_lock = CheckpointLock::acquire(&path)?;
    let pool = source::query_pool(&config)?;
    let initial_checkpoint = checkpoint::load(&path)?;
    let initial_gtid_set = if config.source.gtid_restart {
        match &initial_checkpoint {
            Some(cp) => Some(cp.gtid_set.clone().context(
                "this checkpoint has no full GTID set; keep source.gtid_restart=false to resume it, or reset the checkpoint to start at the current binlog end",
            )?),
            None => Some(source::gtid_executed(&pool).await?),
        }
    } else {
        None
    };
    let metrics = Arc::new(Metrics::default());
    let _metrics_server = if let Some(address) = config.metrics.listen_address.clone() {
        Some(metrics.clone().serve(address).await?)
    } else {
        None
    };
    let shared: SharedCheckpoint = Arc::new(Mutex::new(initial_checkpoint.clone()));
    let mut checkpoint_writer = CheckpointWriter::new(
        path,
        initial_checkpoint.clone(),
        Duration::from_millis(config.checkpoint.flush_interval_ms),
    );
    let (tx, mut rx) = mpsc::channel(config.pipeline.channel_capacity);
    let mut transaction = Transaction::new();
    let mut table_maps: HashMap<u64, ResolvedTable> = HashMap::new();
    let mut current_file = initial_checkpoint.as_ref().map(|c| c.binlog_file.clone());
    let sink: Arc<dyn Sink> = if matches!(sink_kind, crate::cli::SinkKind::Kafka) {
        Arc::new(KafkaSink::new(&config.kafka).await?)
    } else {
        Arc::new(StdoutSink::new(io::stdout()))
    };
    let _reader = source::spawn(
        config.clone(),
        shared.clone(),
        initial_gtid_set.clone(),
        metrics.clone(),
        tx,
    )?;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut checkpoint_tick = tokio::time::interval(checkpoint_writer.flush_interval);
    checkpoint_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut delivery_tasks: JoinSet<Result<(usize, usize)>> = JoinSet::new();
    let mut pending_deliveries = VecDeque::new();
    let mut next_delivery_sequence = 0usize;
    let mut planned_checkpoint = initial_checkpoint;
    let max_in_flight_transactions = if matches!(sink_kind, crate::cli::SinkKind::Kafka) {
        config.pipeline.max_in_flight_transactions
    } else {
        1
    };

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown requested; only committed transactions are retained");
                drain_deliveries(&mut delivery_tasks, &mut pending_deliveries, &mut checkpoint_writer, &shared, &metrics).await?;
                sink.close().await?;
                checkpoint_writer.flush(&metrics)?;
                break;
            }
            _ = checkpoint_tick.tick() => {
                checkpoint_writer.flush_if_due(&metrics)?;
            }
            result = delivery_tasks.join_next(), if !delivery_tasks.is_empty() => {
                apply_delivery_result(result, &mut pending_deliveries, &mut checkpoint_writer, &shared, &metrics)?;
            }
            message = rx.recv(), if pending_deliveries.len() < max_in_flight_transactions => {
                let Some(message) = message else { break; };
                match message {
                    SourceMessage::Started { file, position } => {
                        metrics.connected(true);
                        current_file = if file.is_empty() {
                            None
                        } else {
                            Some(file.clone())
                        };
                        transaction.clear();
                        table_maps.clear();
                        let has_checkpoint = shared.lock().unwrap_or_else(|p| p.into_inner()).is_some();
                        if !has_checkpoint && !file.is_empty() {
                            let baseline = Checkpoint { binlog_file: file, binlog_position: position.max(4), gtid: None, gtid_set: initial_gtid_set.clone() };
                            checkpoint_writer.advance(baseline.clone(), &shared, &metrics, true)?;
                            planned_checkpoint = Some(baseline);
                        }
                    }
                    SourceMessage::Disconnected(reason) => {
                        drain_deliveries(&mut delivery_tasks, &mut pending_deliveries, &mut checkpoint_writer, &shared, &metrics).await?;
                        planned_checkpoint = shared.lock().unwrap_or_else(|p| p.into_inner()).clone();
                        metrics.connected(false);
                        warn!(%reason, "source disconnected; dropping the uncommitted transaction buffer");
                        transaction.clear();
                        table_maps.clear();
                    }
                    SourceMessage::Fatal(reason) => bail!("source adapter stopped safely: {reason}"),
                    SourceMessage::Event { header, data, _budget } => {
                        if matches!(data.as_ref(), EventData::Rotate(_)) {
                            drain_deliveries(&mut delivery_tasks, &mut pending_deliveries, &mut checkpoint_writer, &shared, &metrics).await?;
                            planned_checkpoint = shared.lock().unwrap_or_else(|p| p.into_inner()).clone();
                        }
                        metrics.queue_depth(rx.len());
                        process_event(&config, &pool, &mut checkpoint_writer, &shared, &sink, &mut transaction,
                            &mut table_maps, &mut current_file, &initial_gtid_set, &mut planned_checkpoint,
                            &mut delivery_tasks, &mut pending_deliveries, &mut next_delivery_sequence,
                            &metrics, header, *data).await?;
                        metrics.queue_depth(rx.len());
                        drop(_budget);
                    }
                }
            }
        }
    }
    drain_deliveries(
        &mut delivery_tasks,
        &mut pending_deliveries,
        &mut checkpoint_writer,
        &shared,
        &metrics,
    )
    .await?;
    checkpoint_writer.flush(&metrics)?;
    pool.disconnect()
        .await
        .context("closing schema lookup pool")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_event(
    config: &Config,
    pool: &Pool,
    checkpoint_writer: &mut CheckpointWriter,
    shared: &SharedCheckpoint,
    sink: &Arc<dyn Sink>,
    transaction: &mut Transaction,
    table_maps: &mut HashMap<u64, ResolvedTable>,
    current_file: &mut Option<String>,
    initial_gtid_set: &Option<String>,
    planned_checkpoint: &mut Option<Checkpoint>,
    delivery_tasks: &mut JoinSet<Result<(usize, usize)>>,
    pending_deliveries: &mut VecDeque<PendingDelivery>,
    next_delivery_sequence: &mut usize,
    metrics: &Metrics,
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
        EventData::Stop
        | EventData::FormatDescription(_)
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
            let previous = shared.lock().unwrap_or_else(|p| p.into_inner()).clone();
            if previous.as_ref().is_some_and(|cp| {
                cp.binlog_file == current_file.clone().unwrap() && pos < cp.binlog_position
            }) {
                return Ok(());
            }
            let cp = Checkpoint {
                binlog_file: current_file.clone().unwrap(),
                binlog_position: pos,
                gtid: previous.as_ref().and_then(|cp| cp.gtid.clone()),
                gtid_set: previous
                    .as_ref()
                    .and_then(|cp| cp.gtid_set.clone())
                    .or_else(|| initial_gtid_set.clone()),
            };
            checkpoint_writer.advance(cp.clone(), shared, metrics, false)?;
            *planned_checkpoint = Some(cp);
        }
        EventData::Gtid(gtid) => {
            let prior_set = if config.source.gtid_restart {
                planned_checkpoint
                    .as_ref()
                    .and_then(|cp| cp.gtid_set.clone())
                    .or_else(|| initial_gtid_set.clone())
            } else {
                None
            };
            transaction.begin(Some(gtid.gtid), prior_set)?;
        }
        EventData::Query(query) => {
            let sql = query
                .query
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_ascii_lowercase();
            if sql == "begin" || sql.starts_with("begin ") || sql == "start transaction" {
                if !transaction.active {
                    let prior_set = if config.source.gtid_restart {
                        planned_checkpoint
                            .as_ref()
                            .and_then(|cp| cp.gtid_set.clone())
                            .or_else(|| initial_gtid_set.clone())
                    } else {
                        None
                    };
                    transaction.begin(None, prior_set)?;
                }
            } else if sql == "commit" {
                if !transaction.active {
                    bail!("COMMIT query received without an active transaction");
                }
                commit(
                    sink,
                    transaction,
                    planned_checkpoint,
                    delivery_tasks,
                    pending_deliveries,
                    next_delivery_sequence,
                    &header,
                    current_file,
                    None,
                )
                .await?;
            } else if is_ddl(&sql) {
                let included = config.source.include_databases.is_empty()
                    || query.schema.is_empty()
                    || config
                        .source
                        .include_databases
                        .iter()
                        .any(|db| db == &query.schema);
                if included && !is_supported_column_ddl(&sql) {
                    bail!(
                        "unsupported DDL affecting database {}; only single-column ALTER TABLE ADD/DROP/MODIFY/CHANGE/RENAME is currently supported",
                        query.schema
                    );
                }
                if transaction.active && !transaction.events.is_empty() {
                    bail!(
                        "DDL appeared inside a transaction containing row changes; refusing to checkpoint partial work"
                    );
                }
                if !transaction.active {
                    let prior_set = if config.source.gtid_restart {
                        planned_checkpoint
                            .as_ref()
                            .and_then(|cp| cp.gtid_set.clone())
                    } else {
                        None
                    };
                    transaction.begin(None, prior_set)?;
                }
                commit(
                    sink,
                    transaction,
                    planned_checkpoint,
                    delivery_tasks,
                    pending_deliveries,
                    next_delivery_sequence,
                    &header,
                    current_file,
                    transaction.gtid.clone(),
                )
                .await?;
                // Force a fresh TABLE_MAP lookup after schema-changing DDL.
                table_maps.clear();
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
            let (columns, schema_version) = if included {
                metrics.schema_version_seen();
                let (columns, version) = load_schema(pool, &map).await?;
                (Some(columns), Some(version))
            } else {
                (None, None)
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
                    schema_version,
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
                            schema_version: table
                                .schema_version
                                .clone()
                                .context("included table has no schema version")?,
                            schema: schema.clone(),
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
                            schema_version: table
                                .schema_version
                                .clone()
                                .context("included table has no schema version")?,
                            schema: schema.clone(),
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
                            schema_version: table
                                .schema_version
                                .clone()
                                .context("included table has no schema version")?,
                            schema: schema.clone(),
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
                sink,
                transaction,
                planned_checkpoint,
                delivery_tasks,
                pending_deliveries,
                next_delivery_sequence,
                &header,
                current_file,
                transaction.gtid.clone(),
            )
            .await?;
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

async fn load_schema(pool: &Pool, map: &TableMapEvent) -> Result<(Vec<ColumnSchema>, String)> {
    let mut conn = pool.get_conn().await.with_context(|| {
        format!(
            "reading schema for {}.{}",
            map.database_name, map.table_name
        )
    })?;
    let rows: Vec<(String, String, Option<String>, String, Option<String>)> = conn.exec(
        "SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME, DATA_TYPE, COLUMN_KEY FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        (&map.database_name, &map.table_name),
    ).await.with_context(|| format!("querying schema for {}.{}", map.database_name, map.table_name))?;
    if rows.is_empty() {
        bail!(
            "no schema metadata is available for {}.{}",
            map.database_name,
            map.table_name
        );
    }
    let canonical = serde_json::to_vec(&rows)?;
    let version = format!("sha256:{:x}", Sha256::digest(canonical));
    let columns = rows
        .into_iter()
        .map(|(name, kind, charset, data_type, key_type)| ColumnSchema {
            unsigned: kind.to_ascii_lowercase().contains("unsigned"),
            sql_type: kind,
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
            primary_key: key_type.as_deref() == Some("PRI"),
            name,
        })
        .collect();
    Ok((columns, version))
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
async fn commit(
    sink: &Arc<dyn Sink>,
    tx: &mut Transaction,
    planned_checkpoint: &mut Option<Checkpoint>,
    delivery_tasks: &mut JoinSet<Result<(usize, usize)>>,
    pending_deliveries: &mut VecDeque<PendingDelivery>,
    next_delivery_sequence: &mut usize,
    header: &EventHeader,
    current_file: &Option<String>,
    gtid: Option<String>,
) -> Result<()> {
    if !tx.active {
        bail!("commit boundary received without an active transaction");
    }
    let checkpoint = Checkpoint {
        binlog_file: current_file
            .clone()
            .context("cannot checkpoint before binlog filename is known")?,
        binlog_position: header.next_event_position,
        gtid: gtid.or_else(|| tx.gtid.clone()),
        gtid_set: if tx.gtid_set.is_some() {
            tx.gtid_set.clone()
        } else {
            planned_checkpoint
                .as_ref()
                .and_then(|cp| cp.gtid_set.clone())
        },
    };
    let event_count = tx.events.len();
    let events = std::mem::take(&mut tx.events);
    let sequence = *next_delivery_sequence;
    *next_delivery_sequence = next_delivery_sequence.wrapping_add(1);
    let sink = sink.clone();
    delivery_tasks.spawn(async move {
        sink.write_transaction(sequence, &events).await?;
        sink.flush().await?;
        Ok((sequence, event_count))
    });
    pending_deliveries.push_back(PendingDelivery {
        sequence,
        checkpoint: checkpoint.clone(),
        event_count,
        acknowledged: false,
    });
    *planned_checkpoint = Some(checkpoint);
    tx.clear();
    Ok(())
}

fn apply_delivery_result(
    result: Option<std::result::Result<Result<(usize, usize)>, tokio::task::JoinError>>,
    pending: &mut VecDeque<PendingDelivery>,
    checkpoint_writer: &mut CheckpointWriter,
    shared: &SharedCheckpoint,
    metrics: &Metrics,
) -> Result<()> {
    let task_result = result.context("Kafka delivery worker ended unexpectedly")?;
    let delivery_result = task_result.context("Kafka delivery worker panicked")?;
    let (sequence, event_count) = delivery_result.context("Kafka transaction delivery failed")?;
    let delivery = pending
        .iter_mut()
        .find(|delivery| delivery.sequence == sequence)
        .context("delivery acknowledgement has no matching source transaction")?;
    delivery.acknowledged = true;
    delivery.event_count = event_count;

    while pending
        .front()
        .is_some_and(|delivery| delivery.acknowledged)
    {
        let delivery = pending.pop_front().expect("front delivery was present");
        checkpoint_writer.advance(delivery.checkpoint, shared, metrics, false)?;
        metrics.transaction_delivered(delivery.event_count);
        info!(
            event_count = delivery.event_count,
            "committed transaction delivered"
        );
    }
    Ok(())
}

async fn drain_deliveries(
    delivery_tasks: &mut JoinSet<Result<(usize, usize)>>,
    pending: &mut VecDeque<PendingDelivery>,
    checkpoint_writer: &mut CheckpointWriter,
    shared: &SharedCheckpoint,
    metrics: &Metrics,
) -> Result<()> {
    while !pending.is_empty() {
        let result = delivery_tasks.join_next().await;
        apply_delivery_result(result, pending, checkpoint_writer, shared, metrics)?;
    }
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

fn is_supported_column_ddl(sql: &str) -> bool {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 6 || tokens[0] != "alter" || tokens[1] != "table" {
        return false;
    }
    let Some(action_index) = tokens.iter().enumerate().skip(3).find_map(|(i, token)| {
        matches!(*token, "add" | "drop" | "modify" | "change" | "rename").then_some(i)
    }) else {
        return false;
    };
    let action = tokens[action_index];
    let rest = &tokens[action_index + 1..];
    let valid = match action {
        "add" | "drop" | "modify" => {
            let rest = rest.strip_prefix(&["column"]).unwrap_or(rest);
            !rest.is_empty()
                && !matches!(
                    rest[0],
                    "index"
                        | "key"
                        | "constraint"
                        | "primary"
                        | "unique"
                        | "foreign"
                        | "check"
                        | "fulltext"
                        | "spatial"
                )
        }
        "change" => {
            let rest = rest.strip_prefix(&["column"]).unwrap_or(rest);
            rest.len() >= 3
        }
        "rename" => rest.len() >= 4 && rest[0] == "column" && rest[2] == "to",
        _ => false,
    };
    valid && !has_top_level_comma(&sql[sql.find(action).unwrap_or(0)..])
}

fn has_top_level_comma(sql: &str) -> bool {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for ch in sql.chars() {
        if let Some(current_quote) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' && current_quote != '`' {
                escaped = true;
            } else if ch == current_quote {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return true,
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_binlog_connector_rust::event::event_header::EventHeader;

    struct TestSink {
        bytes: Mutex<Vec<u8>>,
    }
    #[async_trait::async_trait]
    impl Sink for TestSink {
        async fn write_transaction(&self, _sequence: usize, events: &[CdcEvent]) -> Result<()> {
            let mut bytes = self.bytes.lock().unwrap();
            for event in events {
                bytes.extend(serde_json::to_vec(event)?);
                bytes.push(b'\n');
            }
            Ok(())
        }
        async fn flush(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn transaction_buffer_enforces_limits() {
        let mut c = Config::default();
        c.pipeline.max_transaction_events = 0;
        let mut tx = Transaction::new();
        tx.begin(None, None).unwrap();
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
            schema_version: "sha256:test".into(),
            schema: Vec::new(),
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
    fn transaction_gtid_set_includes_the_committed_gtid() {
        let uuid = "24bc7850-2c16-11e6-a073-0242ac110002";
        let mut tx = Transaction::new();
        tx.begin(Some(format!("{uuid}:9")), Some(format!("{uuid}:1-8")))
            .unwrap();
        assert_eq!(tx.gtid_set.as_deref(), Some(format!("{uuid}:1-9").as_str()));
    }

    #[test]
    fn ddl_detection_is_conservative_for_common_statements() {
        assert!(is_ddl("alter table orders add column x int"));
        assert!(!is_ddl("insert into orders values (1)"));
        assert!(is_supported_column_ddl(
            "alter table orders add column x int"
        ));
        assert!(is_supported_column_ddl("alter table orders drop column x"));
        assert!(is_supported_column_ddl(
            "alter table orders rename column x to y"
        ));
        assert!(!is_supported_column_ddl(
            "alter table orders add column x int, drop column y"
        ));
        assert!(!is_supported_column_ddl(
            "alter table orders add index idx_x (x)"
        ));
    }

    #[tokio::test]
    async fn checkpoint_only_moves_after_stdout_flush_and_full_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let shared = Arc::new(Mutex::new(None));
        let mut checkpoint_writer =
            CheckpointWriter::new(path.clone(), None, Duration::from_secs(60));
        let mut tx = Transaction::new();
        tx.begin(Some("24bc7850-2c16-11e6-a073-0242ac110002:9".into()), None)
            .unwrap();
        let output: Arc<dyn Sink> = Arc::new(TestSink {
            bytes: Mutex::new(Vec::new()),
        });
        let metrics = Metrics::default();
        let mut delivery_tasks = JoinSet::new();
        let mut pending_deliveries = VecDeque::new();
        let mut next_sequence = 0;
        let mut planned_checkpoint = None;
        let header = EventHeader {
            timestamp: 1,
            event_type: 16,
            server_id: 1,
            event_length: 27,
            next_event_position: 99,
            event_flags: 0,
        };
        commit(
            &output,
            &mut tx,
            &mut planned_checkpoint,
            &mut delivery_tasks,
            &mut pending_deliveries,
            &mut next_sequence,
            &header,
            &Some("bin.000001".into()),
            None,
        )
        .await
        .unwrap();
        assert!(shared.lock().unwrap().is_none());
        drain_deliveries(
            &mut delivery_tasks,
            &mut pending_deliveries,
            &mut checkpoint_writer,
            &shared,
            &metrics,
        )
        .await
        .unwrap();
        assert_eq!(shared.lock().unwrap().as_ref().unwrap().binlog_position, 99);
        checkpoint_writer.flush(&metrics).unwrap();
        assert_eq!(
            checkpoint::load(&path).unwrap().unwrap().binlog_position,
            99
        );
    }
}
