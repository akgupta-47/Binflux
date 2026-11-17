use anyhow::{Context, Result, bail};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use url::Url;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub source: SourceConfig,
    pub checkpoint: CheckpointConfig,
    pub pipeline: PipelineConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SourceConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password_env: String,
    pub tls_ca_cert: Option<String>,
    pub server_id: u64,
    pub heartbeat_interval_ms: u64,
    pub initial_file: Option<String>,
    pub initial_position: Option<u32>,
    pub include_databases: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckpointConfig {
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    pub channel_capacity: usize,
    pub max_buffered_bytes: usize,
    pub max_event_bytes: usize,
    pub max_transaction_events: usize,
    pub max_transaction_bytes: usize,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 3306,
            username: "binflux".into(),
            password_env: "BINFLUX_MYSQL_PASSWORD".into(),
            tls_ca_cert: None,
            server_id: 90001,
            heartbeat_interval_ms: 10_000,
            initial_file: None,
            initial_position: None,
            include_databases: Vec::new(),
        }
    }
}
impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            path: "./data/checkpoint.json".into(),
        }
    }
}
impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 8,
            max_buffered_bytes: 32 * 1024 * 1024,
            max_event_bytes: 16 * 1024 * 1024,
            max_transaction_events: 100_000,
            max_transaction_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let body = fs::read_to_string(path)
            .with_context(|| format!("reading configuration {}", path.display()))?;
        serde_yaml::from_str(&body)
            .with_context(|| format!("parsing configuration {}", path.display()))
    }

    pub fn validate(&self) -> Result<()> {
        if self.source.host.trim().is_empty() {
            bail!("source.host must not be empty");
        }
        if self.source.port == 0 {
            bail!("source.port must be between 1 and 65535");
        }
        if self.source.server_id == 0 {
            bail!("source.server_id must be nonzero");
        }
        if self.source.username.is_empty() {
            bail!("source.username must not be empty");
        }
        if self.source.initial_file.is_some() != self.source.initial_position.is_some() {
            bail!("source.initial_file and source.initial_position must be set together");
        }
        if self.source.initial_position.is_some_and(|p| p < 4) {
            bail!("source.initial_position must be at least 4");
        }
        if self.pipeline.channel_capacity == 0
            || self.pipeline.max_buffered_bytes == 0
            || self.pipeline.max_event_bytes == 0
            || self.pipeline.max_transaction_events == 0
            || self.pipeline.max_transaction_bytes == 0
        {
            bail!("pipeline capacity and transaction limits must be nonzero");
        }
        if self.pipeline.max_event_bytes > self.pipeline.max_buffered_bytes {
            bail!("pipeline.max_buffered_bytes must be at least max_event_bytes");
        }
        if self.pipeline.max_buffered_bytes > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("pipeline.max_buffered_bytes exceeds the bounded queue limit");
        }
        if self.checkpoint.path.is_empty() {
            bail!("checkpoint.path must not be empty");
        }
        Ok(())
    }

    pub fn connection_url(&self) -> Result<String> {
        let password = std::env::var(&self.source.password_env).with_context(|| {
            format!(
                "environment variable {} is required",
                self.source.password_env
            )
        })?;
        let username = utf8_percent_encode(&self.source.username, NON_ALPHANUMERIC);
        let password = utf8_percent_encode(&password, NON_ALPHANUMERIC);
        let host = if self.source.host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{}]", self.source.host)
        } else {
            self.source.host.clone()
        };
        let raw = format!("mysql://{username}:{password}@{host}:{}/", self.source.port);
        let mut url = Url::parse(&raw).context("invalid MySQL source URL")?;
        // Normalize through URL parsing before the URL is handed to either client.
        url.set_path("");
        url.query_pairs_mut().append_pair("ssl-mode", "required");
        if let Some(ca) = &self.source.tls_ca_cert {
            url.query_pairs_mut().append_pair("ssl-ca", ca);
        }
        Ok(url.to_string())
    }

    pub fn query_connection_url(&self) -> Result<String> {
        let mut url = Url::parse(&self.connection_url()?)?;
        url.set_query(None);
        Ok(url.to_string())
    }
}

pub fn write_example(path: &Path) -> Result<()> {
    let cfg = Config::default();
    let rendered = serde_yaml::to_string(&cfg)?;
    fs::write(path, rendered)
        .with_context(|| format!("writing configuration {}", path.display()))?;
    println!("wrote {}", path.display());
    Ok(())
}
