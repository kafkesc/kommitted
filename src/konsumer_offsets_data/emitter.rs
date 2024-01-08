use konsumer_offsets::KonsumerOffsetsData;
use rdkafka::error::KafkaError;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
};
use tokio::{sync::mpsc, task::JoinHandle, time::Duration};
use tokio_util::sync::CancellationToken;

use crate::constants::{KOMMITTED_CONSUMER_OFFSETS_CONSUMER, KONSUMER_OFFSETS_DATA_TOPIC};
use crate::internals::Emitter;

const CHANNEL_SIZE: usize = 10_000;

/// Emits [`KonsumerOffsetsData`] via a provided [`mpsc::channel`].
///
/// It wraps a Kafka Client, consumes the `__consumer_offsets` topic, and emits its records
/// parsed into [`KonsumerOffsetsData`].
///
/// It shuts down when the provided [`CancellationToken`] is cancelled.
pub struct KonsumerOffsetsDataEmitter {
    consumer_client_config: ClientConfig,
}

impl KonsumerOffsetsDataEmitter {
    pub fn new(client_config: ClientConfig) -> Self {
        Self {
            consumer_client_config: client_config,
        }
    }

    /// Sets the desired Kafka Configuration on the given [`ClientConfig`] object.
    ///
    /// Ref: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md.
    fn set_kafka_consumer_config(mut client_config: ClientConfig) -> ClientConfig {
        client_config.set("enable.auto.commit", "true");
        client_config.set("auto.commit.interval.ms", "5000");
        client_config.set("session.timeout.ms", "10000"); //< must be greater than `auto.commit.interval.ms`
        client_config.set("auto.offset.reset", "earliest");
        client_config.set("enable.partition.eof", "false");

        if client_config.get("group.id").is_none() {
            client_config.set("group.id", KOMMITTED_CONSUMER_OFFSETS_CONSUMER);
        }

        client_config.set_log_level(RDKafkaLogLevel::Warning);

        client_config
    }

    async fn assign_and_seek_to_earliest_all_partitions(
        consumer: &KonsumerOffsetsDataConsumer,
        topic: &str,
    ) -> KafkaResult<()> {
        // Fetch topic metadata
        let meta = consumer.fetch_metadata(Some(topic), Duration::from_secs(5))?;
        let topic_meta = meta.topics().first().ok_or(KafkaError::Subscription(format!(
            "Unable to (self)assign '{}' and seek to earliest offsets",
            topic
        )))?;

        // Prepare desired assignment, setting offset to earliest available for each partition
        let mut desired_assignment =
            TopicPartitionList::with_capacity(topic_meta.partitions().len());
        for partition_meta in topic_meta.partitions().iter() {
            let (earliest, _) = consumer.fetch_watermarks(
                topic,
                partition_meta.id(),
                Duration::from_millis(500),
            )?;
            desired_assignment.add_partition_offset(
                topic,
                partition_meta.id(),
                Offset::Offset(earliest),
            )?;
        }

        // Finally, self-assign
        consumer.assign(&desired_assignment)?;

        Ok(())
    }
}

struct KonsumerOffsetsDataContext;

impl ClientContext for KonsumerOffsetsDataContext {}

impl ConsumerContext for KonsumerOffsetsDataContext {
    fn post_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                trace!("Assigned '{}' partitions of {}", tpl.count(), KONSUMER_OFFSETS_DATA_TOPIC);
            },
            Rebalance::Revoke(tpl) => {
                trace!("Revoked {} partitions of {}", tpl.count(), KONSUMER_OFFSETS_DATA_TOPIC);
            },
            Rebalance::Error(e) => {
                error!("Rebalance Failed: {}", e);
            },
        }
    }

    fn commit_callback(&self, _: KafkaResult<()>, offsets: &TopicPartitionList) {
        for assigned_tp in offsets.elements().into_iter() {
            match assigned_tp.offset() {
                Offset::Invalid => {
                    // No-Op
                },
                _ => {
                    trace!(
                        "Committed '{}:{}' at {:?}",
                        assigned_tp.topic(),
                        assigned_tp.partition(),
                        assigned_tp.offset()
                    )
                },
            }
        }
    }
}

type KonsumerOffsetsDataConsumer = StreamConsumer<KonsumerOffsetsDataContext>;

impl Emitter for KonsumerOffsetsDataEmitter {
    type Emitted = KonsumerOffsetsData;

    /// Spawn a new async task to run the business logic of this struct.
    ///
    /// When this emitter gets spawned, it returns a [`mpsc::Receiver`] for [`KonsumerOffsetsData`],
    /// and a [`JoinHandle`] to help join on the task spawned internally.
    /// The task concludes (joins) only ones the inner task of the emitter terminates.
    ///
    /// # Arguments
    ///
    /// * `shutdown_token`: A [`CancellationToken`] that, when cancelled, will make the internal loop terminate.
    ///
    fn spawn(
        &self,
        shutdown_token: CancellationToken,
    ) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>) {
        let consumer_context = KonsumerOffsetsDataContext;

        let consumer_client: KonsumerOffsetsDataConsumer =
            Self::set_kafka_consumer_config(self.consumer_client_config.clone())
                .create_with_context(consumer_context)
                .expect("Failed to create Consumer Client");

        let (sx, rx) = mpsc::channel::<KonsumerOffsetsData>(CHANNEL_SIZE);

        let join_handle = tokio::spawn(async move {
            match Self::assign_and_seek_to_earliest_all_partitions(&consumer_client, KONSUMER_OFFSETS_DATA_TOPIC).await
            {
                Ok(_) => info!(
                    "(Self) Assigned all partitions of {KONSUMER_OFFSETS_DATA_TOPIC} and sought offsets to earliest"
                ),
                Err(e) => panic!("Failed to (self) assign '{KONSUMER_OFFSETS_DATA_TOPIC}': {e}"),
            }

            loop {
                tokio::select! {
                    r_msg = consumer_client.recv() => {
                        match r_msg {
                            Ok(m) => {
                                match konsumer_offsets::KonsumerOffsetsData::try_from_bytes(m.key(), m.payload()) {
                                    Ok(kod) => {
                                        if let Err(e) = Self::emit(&sx, kod).await {
                                            error!("Failed to emit {}: {e}", std::any::type_name::<KonsumerOffsetsData>());
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to consume from {}: {e}", KONSUMER_OFFSETS_DATA_TOPIC);
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to fetch cluster metadata: {e}");
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("Shutting down");
                        break;
                    }
                }
            }
        });

        (rx, join_handle)
    }
}
