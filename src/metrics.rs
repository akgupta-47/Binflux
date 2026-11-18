use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tracing::info;

#[derive(Default)]
pub struct Metrics {
    transactions_total: AtomicU64,
    events_total: AtomicU64,
    reconnects_total: AtomicU64,
    source_connected: AtomicI64,
    queue_depth: AtomicI64,
    last_checkpoint_timestamp_seconds: AtomicU64,
    last_checkpoint_position: AtomicU64,
    schema_versions_total: AtomicU64,
}

impl Metrics {
    pub fn transaction_delivered(&self, event_count: usize) {
        self.transactions_total.fetch_add(1, Ordering::Relaxed);
        self.events_total
            .fetch_add(event_count as u64, Ordering::Relaxed);
        self.last_checkpoint_timestamp_seconds
            .store(unix_seconds(), Ordering::Relaxed);
    }

    pub fn checkpoint_position(&self, position: u32) {
        self.last_checkpoint_position
            .store(position as u64, Ordering::Relaxed);
        self.last_checkpoint_timestamp_seconds
            .store(unix_seconds(), Ordering::Relaxed);
    }

    pub fn reconnect(&self) {
        self.reconnects_total.fetch_add(1, Ordering::Relaxed);
        self.source_connected.store(0, Ordering::Relaxed);
    }

    pub fn connected(&self, connected: bool) {
        self.source_connected
            .store(i64::from(connected), Ordering::Relaxed);
    }

    pub fn queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth as i64, Ordering::Relaxed);
    }

    pub fn schema_version_seen(&self) {
        self.schema_versions_total.fetch_add(1, Ordering::Relaxed);
    }

    fn exposition(&self) -> String {
        format!(
            "# HELP binflux_transactions_delivered_total Committed source transactions delivered.\n# TYPE binflux_transactions_delivered_total counter\nbinflux_transactions_delivered_total {}\n# HELP binflux_events_delivered_total Row events delivered.\n# TYPE binflux_events_delivered_total counter\nbinflux_events_delivered_total {}\n# HELP binflux_source_reconnects_total Source reconnect attempts.\n# TYPE binflux_source_reconnects_total counter\nbinflux_source_reconnects_total {}\n# HELP binflux_source_connected Whether the source binlog stream is connected.\n# TYPE binflux_source_connected gauge\nbinflux_source_connected {}\n# HELP binflux_source_queue_depth Number of source messages waiting in the pipeline.\n# TYPE binflux_source_queue_depth gauge\nbinflux_source_queue_depth {}\n# HELP binflux_checkpoint_timestamp_seconds Unix time of the latest committed checkpoint.\n# TYPE binflux_checkpoint_timestamp_seconds gauge\nbinflux_checkpoint_timestamp_seconds {}\n# HELP binflux_checkpoint_position Binlog position of the latest checkpoint.\n# TYPE binflux_checkpoint_position gauge\nbinflux_checkpoint_position {}\n# HELP binflux_schema_versions_seen_total Table-map schema versions observed.\n# TYPE binflux_schema_versions_seen_total counter\nbinflux_schema_versions_seen_total {}\n",
            self.transactions_total.load(Ordering::Relaxed),
            self.events_total.load(Ordering::Relaxed),
            self.reconnects_total.load(Ordering::Relaxed),
            self.source_connected.load(Ordering::Relaxed),
            self.queue_depth.load(Ordering::Relaxed),
            self.last_checkpoint_timestamp_seconds
                .load(Ordering::Relaxed),
            self.last_checkpoint_position.load(Ordering::Relaxed),
            self.schema_versions_total.load(Ordering::Relaxed),
        )
    }

    pub async fn serve(self: std::sync::Arc<Self>, address: String) -> Result<JoinHandle<()>> {
        let listener = TcpListener::bind(&address)
            .await
            .with_context(|| format!("binding Prometheus metrics listener at {address}"))?;
        info!(%address, "Prometheus metrics endpoint listening");
        Ok(tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let metrics = self.clone();
                tokio::spawn(async move {
                    let _ = respond(stream, metrics).await;
                });
            }
        }))
    }
}

async fn respond(mut stream: TcpStream, metrics: std::sync::Arc<Metrics>) -> Result<()> {
    let mut request = [0u8; 1024];
    let read = stream.read(&mut request).await?;
    let request = String::from_utf8_lossy(&request[..read]);
    let (status, content_type, body) = if request.starts_with("GET /metrics ") {
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics.exposition(),
        )
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".into(),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::Metrics;
    use std::sync::Arc;

    #[test]
    fn exposition_reports_transaction_and_connection_metrics() {
        let metrics = Arc::new(Metrics::default());
        metrics.transaction_delivered(3);
        metrics.connected(true);
        let output = metrics.exposition();
        assert!(output.contains("binflux_transactions_delivered_total 1"));
        assert!(output.contains("binflux_events_delivered_total 3"));
        assert!(output.contains("binflux_source_connected 1"));
    }
}
