use crate::{
    config::KafkaConfig,
    event::{CdcEvent, Operation},
};
use anyhow::{Context, Result, bail};
use chrono::{TimeZone, Utc};
use futures::future::join_all;
use rskafka::{
    client::{
        Client, ClientBuilder, Credentials, SaslConfig,
        partition::UnknownTopicHandling,
        producer::{
            BatchProducerBuilder,
            aggregator::{Aggregator, RecordAggregatorStatusDeaggregator, TryPush},
        },
    },
    record::Record,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::info;

use super::Sink;

pub struct KafkaSink {
    client: Client,
    partition_workers: Mutex<HashMap<(String, i32), mpsc::Sender<PartitionWrite>>>,
    topic_partitions: Mutex<HashMap<String, Vec<i32>>>,
    dispatch_sequence: watch::Sender<usize>,
    topic_template: String,
    partition_id: i32,
    partition_by: String,
    timeout: Duration,
}

struct PartitionWrite {
    records: Vec<Record>,
    acknowledgement: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
struct KafkaBatchAggregator {
    max_batch_size: usize,
    batch_size: usize,
    records: Vec<Record>,
}

impl Aggregator for KafkaBatchAggregator {
    type Input = Record;
    type Tag = usize;
    type StatusDeaggregator = RecordAggregatorStatusDeaggregator;

    fn try_push(
        &mut self,
        record: Self::Input,
    ) -> std::result::Result<
        TryPush<Self::Input, Self::Tag>,
        rskafka::client::producer::aggregator::Error,
    > {
        let size = record.approximate_size();
        if !self.records.is_empty() && self.batch_size.saturating_add(size) > self.max_batch_size {
            return Ok(TryPush::NoCapacity(record));
        }
        let tag = self.records.len();
        self.batch_size = self.batch_size.saturating_add(size);
        self.records.push(record);
        Ok(TryPush::Aggregated(tag))
    }

    fn flush(
        &mut self,
    ) -> std::result::Result<
        (Vec<Record>, Self::StatusDeaggregator),
        rskafka::client::producer::aggregator::Error,
    > {
        self.batch_size = 0;
        Ok((
            std::mem::take(&mut self.records),
            RecordAggregatorStatusDeaggregator::default(),
        ))
    }
}

impl KafkaBatchAggregator {
    fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            batch_size: 0,
            records: Vec::new(),
        }
    }
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
        let (dispatch_sequence, _) = watch::channel(0usize);
        Ok(Self {
            client,
            partition_workers: Mutex::new(HashMap::new()),
            topic_partitions: Mutex::new(HashMap::new()),
            dispatch_sequence,
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

    async fn partition_ids(&self, topic: &str) -> Result<Vec<i32>> {
        if let Some(partitions) = self.topic_partitions.lock().await.get(topic) {
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
            .lock()
            .await
            .insert(topic.to_owned(), partitions.clone());
        Ok(partitions)
    }

    async fn partition_worker(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Result<mpsc::Sender<PartitionWrite>> {
        let cache_key = (topic.to_owned(), partition_id);
        let mut workers = self.partition_workers.lock().await;
        if let Some(worker) = workers.get(&cache_key) {
            return Ok(worker.clone());
        }
        let partition = Arc::new(
            self.client
                .partition_client(topic.to_owned(), partition_id, UnknownTopicHandling::Error)
                .await
                .with_context(|| format!("opening Kafka topic {topic} partition {partition_id}"))?,
        );
        let (sender, mut receiver) = mpsc::channel::<PartitionWrite>(32);
        let timeout = self.timeout;
        let topic_name = topic.to_owned();
        let producer = Arc::new(
            BatchProducerBuilder::new(partition)
                .with_linger(Duration::from_millis(5))
                .build(KafkaBatchAggregator::new(16 * 1024)),
        );
        tokio::spawn(async move {
            while let Some(write) = receiver.recv().await {
                let mut pending: Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>> = Vec::new();
                let mut enqueue_error = None;

                // Poll each produce future once. BatchProducer queues the record
                // synchronously before that future waits for its broker delivery
                // acknowledgement, so the worker can enqueue later transactions
                // in order while earlier requests remain in flight.
                for record in write.records {
                    let producer = producer.clone();
                    let topic_name = topic_name.clone();
                    let mut delivery: Pin<Box<dyn Future<Output = Result<()>> + Send>> =
                        Box::pin(async move {
                            tokio::time::timeout(timeout, producer.produce(record))
                            .await
                            .context("timed out waiting for Kafka record acknowledgement")?
                            .context("Kafka broker rejected CDC record")
                            .map(|_| ())
                            .with_context(|| {
                                format!(
                                    "producing to Kafka topic {topic_name} partition {partition_id}"
                                )
                            })
                        });
                    let first_poll = std::future::poll_fn(|cx| match delivery.as_mut().poll(cx) {
                        std::task::Poll::Ready(result) => std::task::Poll::Ready(Some(result)),
                        std::task::Poll::Pending => std::task::Poll::Ready(None),
                    })
                    .await;
                    match first_poll {
                        Some(Ok(())) => {}
                        Some(Err(error)) => {
                            enqueue_error = Some(error);
                            break;
                        }
                        None => pending.push(delivery),
                    }
                }

                if let Some(error) = enqueue_error {
                    let _ = write.acknowledgement.send(Err(error));
                    continue;
                }

                let acknowledgement = write.acknowledgement;
                tokio::spawn(async move {
                    let mut first_error = None;
                    for result in join_all(pending).await {
                        if let Err(error) = result {
                            first_error.get_or_insert(error);
                        }
                    }
                    let result = first_error.map_or(Ok(()), Err);
                    let _ = acknowledgement.send(result);
                });
            }
        });
        workers.insert(cache_key, sender.clone());
        Ok(sender)
    }

    async fn enqueue_topic(
        &self,
        topic: &str,
        records: Vec<(Record, Vec<u8>)>,
    ) -> Result<Vec<oneshot::Receiver<Result<()>>>> {
        if self.partition_by.eq_ignore_ascii_case("fixed") {
            let worker = self.partition_worker(topic, self.partition_id).await?;
            let (acknowledgement, receiver) = oneshot::channel();
            worker
                .send(PartitionWrite {
                    records: records.into_iter().map(|(record, _)| record).collect(),
                    acknowledgement,
                })
                .await
                .context("Kafka partition delivery queue closed")?;
            return Ok(vec![receiver]);
        }

        let partition_ids = self.partition_ids(topic).await?;
        let mut batches: BTreeMap<i32, Vec<Record>> = BTreeMap::new();
        for (record, key) in records {
            let digest = Sha256::digest(key);
            let hash = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix"));
            let partition_id = partition_ids[(hash % partition_ids.len() as u64) as usize];
            batches.entry(partition_id).or_default().push(record);
        }

        let mut acknowledgements = Vec::new();
        for (partition_id, records) in batches {
            let worker = self.partition_worker(topic, partition_id).await?;
            let (acknowledgement, receiver) = oneshot::channel();
            worker
                .send(PartitionWrite {
                    records,
                    acknowledgement,
                })
                .await
                .context("Kafka partition delivery queue closed")?;
            acknowledgements.push(receiver);
        }
        Ok(acknowledgements)
    }

    async fn wait_dispatch_turn(&self, sequence: usize) {
        let mut dispatch_sequence = self.dispatch_sequence.subscribe();
        loop {
            if *dispatch_sequence.borrow() == sequence {
                return;
            }
            if dispatch_sequence.changed().await.is_err() {
                return;
            }
        }
    }

    fn release_dispatch_turn(&self) {
        self.dispatch_sequence
            .send_modify(|sequence| *sequence = sequence.wrapping_add(1));
    }
}

#[async_trait::async_trait]
impl Sink for KafkaSink {
    async fn write_transaction(&self, sequence: usize, events: &[CdcEvent]) -> Result<()> {
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
        self.wait_dispatch_turn(sequence).await;
        let mut acknowledgements = Vec::new();
        let mut enqueue_result = Ok(());
        for (topic, records) in batches {
            match self.enqueue_topic(&topic, records).await {
                Ok(mut topic_acknowledgements) => {
                    acknowledgements.append(&mut topic_acknowledgements);
                }
                Err(error) => {
                    enqueue_result = Err(error);
                    break;
                }
            }
        }
        self.release_dispatch_turn();
        enqueue_result?;
        for acknowledgement in acknowledgements {
            acknowledgement
                .await
                .context("Kafka partition worker dropped its acknowledgement")??;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
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
