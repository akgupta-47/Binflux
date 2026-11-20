use crate::{
    config::KafkaConfig,
    event::{CdcEvent, Operation},
};
use anyhow::{Context, Result, bail};
use chrono::{TimeZone, Utc};
use rskafka::{
    client::{
        Client, ClientBuilder, Credentials, SaslConfig,
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    sync::Arc,
    time::Duration,
};
use tracing::info;

use super::Sink;

pub struct KafkaSink {
    client: Client,
    partitions: HashMap<(String, i32), rskafka::client::partition::PartitionClient>,
    topic_partitions: HashMap<String, Vec<i32>>,
    topic_template: String,
    partition_id: i32,
    partition_by: String,
    timeout: Duration,
}

impl KafkaSink {
    pub async fn new(config: &KafkaConfig) -> Result<Self> {
        let brokers = config
            .bootstrap_servers
            .split(',')
            .map(str::trim)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if brokers.iter().any(String::is_empty) {
            bail!("kafka.bootstrap_servers must be a comma-separated list of host:port values");
        }
        let mut builder = ClientBuilder::new(brokers).client_id(config.client_id.clone());
        if config.tls {
            let roots =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .context("configuring Kafka TLS protocol versions")?
            .with_root_certificates(roots)
            .with_no_client_auth();
            builder = builder.tls_config(Arc::new(tls));
        }
        if let (Some(mechanism), Some(username_env), Some(password_env)) = (
            config.sasl_mechanism.as_deref(),
            config.sasl_username_env.as_deref(),
            config.sasl_password_env.as_deref(),
        ) {
            let username = env::var(username_env).with_context(|| {
                format!("Kafka username environment variable {username_env} is required")
            })?;
            let password = env::var(password_env).with_context(|| {
                format!("Kafka password environment variable {password_env} is required")
            })?;
            let credentials = Credentials::new(username, password);
            let sasl = match mechanism.to_ascii_uppercase().as_str() {
                "PLAIN" => SaslConfig::Plain(credentials),
                "SCRAM-SHA-256" => SaslConfig::ScramSha256(credentials),
                "SCRAM-SHA-512" => SaslConfig::ScramSha512(credentials),
                _ => bail!("unsupported Kafka SASL mechanism {mechanism}"),
            };
            builder = builder.sasl_config(sasl);
        }
        let client = builder
            .build()
            .await
            .context("connecting to Kafka bootstrap brokers")?;
        info!(
            brokers = %config.bootstrap_servers,
            topic = %config.topic,
            partition = config.partition,
            "Kafka sink connected"
        );
        Ok(Self {
            client,
            partitions: HashMap::new(),
            topic_partitions: HashMap::new(),
            topic_template: config.topic.clone(),
            partition_id: config.partition,
            partition_by: config.partition_by.clone(),
            timeout: Duration::from_millis(config.message_timeout_ms),
        })
    }

    fn topic_for(&self, event: &CdcEvent) -> Result<String> {
        let topic = self
            .topic_template
            .replace("%{database}", &event.source.database)
            .replace("%{table}", &event.source.table);
        if topic.is_empty()
            || topic.len() > 249
            || topic == "."
            || topic == ".."
            || !topic
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            bail!("resolved Kafka topic name {topic:?} is invalid");
        }
        Ok(topic)
    }

    async fn partition_ids(&mut self, topic: &str) -> Result<Vec<i32>> {
        if let Some(partitions) = self.topic_partitions.get(topic) {
            return Ok(partitions.clone());
        }
        let topics = self
            .client
            .list_topics()
            .await
            .context("reading Kafka topic metadata")?;
        let partitions = topics
            .into_iter()
            .find(|metadata| metadata.name == topic)
            .map(|metadata| metadata.partitions.into_iter().collect::<Vec<_>>())
            .filter(|partitions| !partitions.is_empty())
            .with_context(|| {
                format!("Kafka topic {topic:?} does not exist or has no partitions")
            })?;
        self.topic_partitions
            .insert(topic.to_owned(), partitions.clone());
        Ok(partitions)
    }

    async fn produce_to_partition(
        &mut self,
        topic: &str,
        partition_id: i32,
        records: Vec<Record>,
    ) -> Result<()> {
        let cache_key = (topic.to_owned(), partition_id);
        if !self.partitions.contains_key(&cache_key) {
            let partition_client = self
                .client
                .partition_client(topic.to_owned(), partition_id, UnknownTopicHandling::Error)
                .await
                .with_context(|| format!("opening Kafka topic {topic} partition {partition_id}"))?;
            self.partitions.insert(cache_key.clone(), partition_client);
        }
        let partition = self
            .partitions
            .get(&cache_key)
            .expect("partition client was inserted");
        tokio::time::timeout(
            self.timeout,
            partition.produce(records, Compression::default()),
        )
        .await
        .context("timed out waiting for Kafka transaction acknowledgement")?
        .context("Kafka broker rejected CDC transaction")?;
        Ok(())
    }

    async fn produce_to_topic(
        &mut self,
        topic: &str,
        records: Vec<(Record, Vec<u8>)>,
    ) -> Result<()> {
        if self.partition_by.eq_ignore_ascii_case("fixed") {
            return self
                .produce_to_partition(
                    topic,
                    self.partition_id,
                    records.into_iter().map(|(record, _)| record).collect(),
                )
                .await;
        }

        let partition_ids = self.partition_ids(topic).await?;
        let mut batches: BTreeMap<i32, Vec<Record>> = BTreeMap::new();
        for (record, key) in records {
            let digest = Sha256::digest(key);
            let hash = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix"));
            let partition_id = partition_ids[(hash % partition_ids.len() as u64) as usize];
            batches.entry(partition_id).or_default().push(record);
        }
        for (partition_id, records) in batches {
            self.produce_to_partition(topic, partition_id, records)
                .await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Sink for KafkaSink {
    async fn write_transaction(&mut self, events: &[CdcEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut batches: Vec<(String, Vec<(Record, Vec<u8>)>)> = Vec::new();
        for event in events {
            let payload = serde_json::to_vec(event).context("serializing Kafka CDC record")?;
            let primary_key = primary_key(event)?;
            let key = primary_key
                .clone()
                .unwrap_or_else(|| event.event_id.as_bytes().to_vec());
            let partition_key = match self.partition_by.to_ascii_lowercase().as_str() {
                "database" => event.source.database.as_bytes().to_vec(),
                "table" => format!("{}.{}", event.source.database, event.source.table).into_bytes(),
                "primary_key" => primary_key.unwrap_or_else(|| {
                    format!("{}.{}", event.source.database, event.source.table).into_bytes()
                }),
                _ => Vec::new(),
            };
            let timestamp = Utc
                .timestamp_opt(i64::from(event.timestamp_unix_seconds), 0)
                .single()
                .unwrap_or_else(Utc::now);
            let topic = self.topic_for(event)?;
            let record = Record {
                key: Some(key),
                value: Some(payload),
                headers: BTreeMap::from([
                    (
                        "binflux-event-id".to_owned(),
                        event.event_id.as_bytes().to_vec(),
                    ),
                    (
                        "binflux-database".to_owned(),
                        event.source.database.as_bytes().to_vec(),
                    ),
                    (
                        "binflux-table".to_owned(),
                        event.source.table.as_bytes().to_vec(),
                    ),
                ]),
                timestamp,
            };
            if let Some((previous_topic, records)) = batches.last_mut()
                && *previous_topic == topic
            {
                records.push((record, partition_key));
            } else {
                batches.push((topic, vec![(record, partition_key)]));
            }
        }
        for (topic, records) in batches {
            self.produce_to_topic(&topic, records).await?;
        }
        Ok(())
    }

    // Each produce call waits for the broker response before returning, so there
    // is no client-side queue left to drain here.
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn primary_key(event: &CdcEvent) -> Result<Option<Vec<u8>>> {
    let image = match &event.operation {
        Operation::Delete => event.before.as_ref(),
        Operation::Insert | Operation::Update => event.after.as_ref().or(event.before.as_ref()),
    };
    if let Some(image) = image {
        let mut primary_key = Vec::new();
        for column in event.schema.iter().filter(|column| column.primary_key) {
            let Some(value) = image.get(&column.name) else {
                primary_key.clear();
                break;
            };
            primary_key.push((&column.name, value));
        }
        if !primary_key.is_empty() {
            let mut key_fields = serde_json::Map::new();
            for (name, value) in primary_key {
                key_fields.insert(name.clone(), value.clone());
            }
            return serde_json::to_vec(&serde_json::json!({
                "database": event.source.database,
                "table": event.source.table,
                "primary_key": key_fields,
            }))
            .context("serializing Kafka primary-key record key")
            .map(Some);
        }
    }

    // Rows without a complete primary key must not collapse to one compacted key.
    Ok(None)
}
