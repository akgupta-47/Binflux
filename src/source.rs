use crate::{checkpoint::Checkpoint, config::Config, metrics::Metrics};
use anyhow::{Context, Result};
use mysql_async::prelude::Queryable;
use mysql_binlog_connector_rust::{
    binlog_client::{BinlogClient, StartPosition},
    event::{event_data::EventData, event_header::EventHeader},
};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::{debug, info, warn};

pub enum SourceMessage {
    Started {
        file: String,
        position: u32,
    },
    Event {
        header: EventHeader,
        data: Box<EventData>,
        _budget: OwnedSemaphorePermit,
    },
    Disconnected(String),
    Fatal(String),
}

pub type SharedCheckpoint = Arc<Mutex<Option<Checkpoint>>>;

pub fn spawn(
    config: Config,
    checkpoint: SharedCheckpoint,
    initial_gtid_set: Option<String>,
    metrics: Arc<Metrics>,
    tx: mpsc::Sender<SourceMessage>,
) -> Result<thread::JoinHandle<()>> {
    let url = config.connection_url()?;
    let source = config.source.clone();
    let pipeline = config.pipeline.clone();
    let byte_budget = Arc::new(Semaphore::new(pipeline.max_buffered_bytes));
    let handle = thread::Builder::new()
        .name("binflux-mysql-reader".into())
        .spawn(move || {
            async_std::task::block_on(async move {
                let mut consecutive_retries = 0u32;
                loop {
                    if tx.is_closed() {
                        return;
                    }
                    let saved = checkpoint.lock().unwrap_or_else(|p| p.into_inner()).clone();
                    let (start, saved_file, saved_pos) = if let Some(cp) = saved {
                        let start = if source.gtid_restart {
                            cp.gtid_set
                                .clone()
                                .map(StartPosition::Gtid)
                                .unwrap_or_else(|| StartPosition::BinlogPosition(
                                    cp.binlog_file.clone(), cp.binlog_position,
                                ))
                        } else {
                            StartPosition::BinlogPosition(cp.binlog_file.clone(), cp.binlog_position)
                        };
                        (start, cp.binlog_file, cp.binlog_position)
                    } else if source.gtid_restart {
                        match &initial_gtid_set {
                            Some(set) => (StartPosition::Gtid(set.clone()), String::new(), 0),
                            None => (StartPosition::Latest, String::new(), 0),
                        }
                    } else if let (Some(file), Some(pos)) =
                        (source.initial_file.clone(), source.initial_position)
                    {
                        (StartPosition::BinlogPosition(file.clone(), pos), file, pos)
                    } else {
                        (StartPosition::Latest, String::new(), 0)
                    };
                    let heartbeat_secs = source.heartbeat_interval_ms.div_ceil(1000);
                    let mut client = BinlogClient::new(&url, source.server_id, start)
                        .with_master_heartbeat(Duration::from_secs(heartbeat_secs));
                    let mut session_started = None;
                    match client.connect().await {
                        Ok(mut stream) => {
                            session_started = Some(Instant::now());
                            metrics.connected(true);
                            let file = if saved_file.is_empty() {
                                client.binlog_filename.clone()
                            } else {
                                saved_file
                            };
                            let pos = if saved_pos == 0 {
                                client.binlog_position
                            } else {
                                saved_pos
                            };
                            if tx
                                .send(SourceMessage::Started {
                                    file,
                                    position: pos,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                            info!(
                                server_id = source.server_id,
                                "connected to MySQL binlog stream"
                            );
                            loop {
                                match stream.read().await {
                                    Ok((header, data)) => {
                                        debug!(
                                            event_type = header.event_type,
                                            event_length = header.event_length,
                                            "received binlog event"
                                        );
                                        let event_bytes = header.event_length as usize;
                                        if event_bytes > pipeline.max_event_bytes {
                                            let _ = tx.send(SourceMessage::Fatal(format!(
                                                "binlog event is {event_bytes} bytes, exceeding pipeline.max_event_bytes ({})",
                                                pipeline.max_event_bytes
                                            ))).await;
                                            return;
                                        }
                                        let permits = (event_bytes as u32).max(1);
                                        let budget = match byte_budget.clone().acquire_many_owned(permits).await {
                                            Ok(permit) => permit,
                                            Err(_) => return,
                                        };
                                        if tx
                                            .send(SourceMessage::Event {
                                                header,
                                                data: Box::new(data),
                                                _budget: budget,
                                            })
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                    Err(error) => {
                                        let _ = tx
                                            .send(SourceMessage::Disconnected(error.to_string()))
                                            .await;
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            let _ = tx
                                .send(SourceMessage::Disconnected(error.to_string()))
                                .await;
                        }
                    }
                    if session_started.is_some_and(|started| {
                        started.elapsed() >= Duration::from_millis(source.retry_reset_after_ms)
                    }) {
                        consecutive_retries = 0;
                    }
                    metrics.reconnect();
                    consecutive_retries = consecutive_retries.saturating_add(1);
                    if source.retry_max_attempts > 0
                        && consecutive_retries > source.retry_max_attempts
                    {
                        let _ = tx.send(SourceMessage::Fatal(format!(
                            "MySQL reconnect retry limit reached after {} retries",
                            source.retry_max_attempts
                        ))).await;
                        return;
                    }
                    let delay = retry_delay(
                        source.retry_initial_delay_ms,
                        source.retry_max_delay_ms,
                        consecutive_retries.saturating_sub(1),
                    );
                    warn!(
                        retry = consecutive_retries,
                        retry_seconds = delay.as_secs_f64(),
                        "binlog connection ended; reconnecting from durable checkpoint"
                    );
                    async_std::task::sleep(delay).await;
                }
            });
        })
        .context("starting MySQL reader thread")?;
    Ok(handle)
}

fn retry_delay(initial_ms: u64, max_ms: u64, retry_index: u32) -> Duration {
    let multiplier = 1u32.checked_shl(retry_index.min(31)).unwrap_or(u32::MAX);
    Duration::from_millis(initial_ms.saturating_mul(multiplier as u64).min(max_ms))
}

#[cfg(test)]
mod tests {
    use super::retry_delay;
    use std::time::Duration;

    #[test]
    fn retry_backoff_doubles_and_caps() {
        assert_eq!(retry_delay(500, 3_000, 0), Duration::from_millis(500));
        assert_eq!(retry_delay(500, 3_000, 1), Duration::from_millis(1_000));
        assert_eq!(retry_delay(500, 3_000, 2), Duration::from_millis(2_000));
        assert_eq!(retry_delay(500, 3_000, 3), Duration::from_millis(3_000));
        assert_eq!(retry_delay(500, 3_000, 20), Duration::from_millis(3_000));
    }
}

pub async fn check(config: &Config) -> Result<()> {
    let pool = query_pool(config)?;
    let mut conn = pool.get_conn().await.context("connecting to MySQL")?;
    let row: Option<(u64, String, String)> = conn
        .query_first("SELECT @@server_id, @@log_bin, @@binlog_format")
        .await?;
    let (server_id, log_bin, format) = row.context("MySQL status query returned no row")?;
    if log_bin != "1" && !log_bin.eq_ignore_ascii_case("ON") {
        anyhow::bail!("MySQL binary logging is disabled");
    }
    if !format.eq_ignore_ascii_case("ROW") {
        anyhow::bail!("binlog_format must be ROW (found {format})");
    }
    if config.source.gtid_restart {
        let mode: Option<String> = conn.query_first("SELECT @@GLOBAL.gtid_mode").await?;
        if !mode
            .as_deref()
            .is_some_and(|mode| mode.eq_ignore_ascii_case("ON"))
        {
            anyhow::bail!("source.gtid_restart requires @@GLOBAL.gtid_mode=ON");
        }
    }
    info!(
        server_id,
        "MySQL is reachable with row-based binary logging enabled"
    );
    conn.disconnect().await?;
    pool.disconnect().await?;
    Ok(())
}

pub async fn gtid_executed(pool: &mysql_async::Pool) -> Result<String> {
    let mut conn = pool.get_conn().await.context("reading MySQL GTID state")?;
    let mode: Option<String> = conn.query_first("SELECT @@GLOBAL.gtid_mode").await?;
    if !mode
        .as_deref()
        .is_some_and(|mode| mode.eq_ignore_ascii_case("ON"))
    {
        anyhow::bail!("source.gtid_restart requires @@GLOBAL.gtid_mode=ON");
    }
    let executed: Option<String> = conn.query_first("SELECT @@GLOBAL.gtid_executed").await?;
    Ok(executed.unwrap_or_default())
}

pub fn query_pool(config: &Config) -> Result<mysql_async::Pool> {
    use mysql_async::{OptsBuilder, SslOpts};
    let opts = mysql_async::Opts::from_url(&config.query_connection_url()?)
        .context("building MySQL query connection")?;
    let mut ssl = SslOpts::default();
    if let Some(ca) = &config.source.tls_ca_cert {
        ssl = ssl.with_root_certs(vec![std::path::PathBuf::from(ca).into()]);
    }
    Ok(mysql_async::Pool::new(
        OptsBuilder::from_opts(opts).ssl_opts(Some(ssl)),
    ))
}
