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
    pub metrics: MetricsConfig,
    pub kafka: KafkaConfig,
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
    pub gtid_restart: bool,
    pub initial_file: Option<String>,
    pub initial_position: Option<u32>,
    pub include_databases: Vec<String>,
    pub retry_initial_delay_ms: u64,
    pub retry_max_delay_ms: u64,
    /// Maximum retries after the initial connection attempt; 0 means unlimited.
    pub retry_max_attempts: u32,
    pub retry_reset_after_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckpointConfig {
    pub path: String,
    /// Maximum delay before an acknowledged checkpoint is durably written.
    pub flush_interval_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    pub channel_capacity: usize,
    /// Maximum committed source transactions awaiting sink acknowledgements.
    pub max_in_flight_transactions: usize,
    pub max_buffered_bytes: usize,
    pub max_event_bytes: usize,
    pub max_transaction_events: usize,
    pub max_transaction_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Optional Prometheus HTTP bind address. Keep this on localhost unless protected externally.
    pub listen_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub client_id: String,
    pub partition: i32,
    pub partition_by: String,
    pub message_timeout_ms: u64,
    pub tls: bool,
    pub sasl_mechanism: Option<String>,
    pub sasl_username_env: Option<String>,
    pub sasl_password_env: Option<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "127.0.0.1:9092".into(),
            topic: "binflux.cdc".into(),
            client_id: "binflux".into(),
            partition: 0,
            partition_by: "fixed".into(),
            message_timeout_ms: 30_000,
            tls: false,
            sasl_mechanism: None,
            sasl_username_env: None,
            sasl_password_env: None,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen_address: None,
        }
    }
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
            gtid_restart: false,
            initial_file: None,
            initial_position: None,
            include_databases: Vec::new(),
            retry_initial_delay_ms: 1_000,
            retry_max_delay_ms: 30_000,
            retry_max_attempts: 0,
            retry_reset_after_ms: 60_000,
        }
    }
}
impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            path: "./data/checkpoint.json".into(),
            flush_interval_ms: 1_000,
        }
    }
}
impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 8,
            max_in_flight_transactions: 8,
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
        if self.pipeline.max_in_flight_transactions == 0 {
            bail!("pipeline.max_in_flight_transactions must be at least 1");
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
        if self.source.gtid_restart && self.source.initial_file.is_some() {
            bail!("source.gtid_restart cannot be combined with an explicit initial file/position");
        }
        if self.source.retry_initial_delay_ms == 0
            || self.source.retry_max_delay_ms < self.source.retry_initial_delay_ms
            || self.source.retry_reset_after_ms == 0
        {
            bail!(
                "source retry delays must be nonzero and retry_max_delay_ms must be at least retry_initial_delay_ms"
            );
        }
        if let Some(address) = &self.metrics.listen_address
            && address.trim().is_empty()
        {
            bail!("metrics.listen_address must not be empty when configured");
        }
        if self.kafka.bootstrap_servers.trim().is_empty()
            || self.kafka.topic.trim().is_empty()
            || self.kafka.client_id.trim().is_empty()
            || self.kafka.message_timeout_ms == 0
        {
            bail!(
                "Kafka bootstrap_servers, topic, client_id, and nonzero message_timeout_ms are required"
            );
        }
        for broker in self.kafka.bootstrap_servers.split(',') {
            let (host, port) = broker.trim().rsplit_once(':').ok_or_else(|| {
                anyhow::anyhow!("Kafka broker {broker:?} must use host:port format")
            })?;
            if host.trim().is_empty() || port.parse::<u16>().ok().is_none_or(|port| port == 0) {
                bail!("Kafka broker {broker:?} must use a nonempty host and port 1-65535");
            }
        }
        if self.kafka.partition < 0 {
            bail!("kafka.partition must be zero or greater");
        }
        if !["fixed", "database", "table", "primary_key"]
            .iter()
            .any(|option| self.kafka.partition_by.eq_ignore_ascii_case(option))
        {
            bail!("kafka.partition_by must be fixed, database, table, or primary_key");
        }
        let topic_pattern = self
            .kafka
            .topic
            .replace("%{database}", "")
            .replace("%{table}", "");
        if topic_pattern.contains("%{") || topic_pattern.contains('}') {
            bail!("kafka.topic supports only the %{{database}} and %{{table}} substitutions");
        }
        if self.kafka.sasl_username_env.is_some() != self.kafka.sasl_password_env.is_some()
            || self.kafka.sasl_username_env.is_some() != self.kafka.sasl_mechanism.is_some()
        {
            bail!(
                "Kafka SASL mechanism, username, and password environment variable names must be configured together"
            );
        }
        if self.kafka.sasl_mechanism.is_some() && !self.kafka.tls {
            bail!("Kafka SASL authentication requires kafka.tls: true");
        }
        if let Some(mechanism) = &self.kafka.sasl_mechanism
            && !["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
                .iter()
                .any(|supported| mechanism.eq_ignore_ascii_case(supported))
        {
            bail!("kafka.sasl_mechanism must be PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512");
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
        if self.checkpoint.path.is_empty() || self.checkpoint.flush_interval_ms == 0 {
            bail!("checkpoint.path and checkpoint.flush_interval_ms must be valid");
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
