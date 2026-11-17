use crate::{checkpoint::Checkpoint, config::Config};
use anyhow::{Context, Result};
use mysql_async::prelude::Queryable;
use mysql_binlog_connector_rust::{
    binlog_client::{BinlogClient, StartPosition},
    event::{event_data::EventData, event_header::EventHeader},
};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
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
                let mut delay = Duration::from_secs(1);
                loop {
                    if tx.is_closed() {
                        return;
                    }
                    let saved = checkpoint.lock().unwrap_or_else(|p| p.into_inner()).clone();
                    let (start, saved_file, saved_pos) = if let Some(cp) = saved {
                        (
                            StartPosition::BinlogPosition(
                                cp.binlog_file.clone(),
                                cp.binlog_position,
                            ),
                            cp.binlog_file,
                            cp.binlog_position,
                        )
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
                    match client.connect().await {
                        Ok(mut stream) => {
                            delay = Duration::from_secs(1);
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
                    warn!(
                        retry_seconds = delay.as_secs(),
                        "binlog connection ended; reconnecting from durable checkpoint"
                    );
                    async_std::task::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            });
        })
        .context("starting MySQL reader thread")?;
    Ok(handle)
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
    info!(
        server_id,
        "MySQL is reachable with row-based binary logging enabled"
    );
    conn.disconnect().await?;
    pool.disconnect().await?;
    Ok(())
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
