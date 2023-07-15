use async_trait::async_trait;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaResult,
    ClientConfig, Message, Offset,
};
use tokio::{sync::mpsc, task::JoinHandle, time::Duration};
use tokio_util::sync::CancellationToken;

use konsumer_offsets::KonsumerOffsetsData;

use crate::constants::{KONSUMER_OFFSETS_DATA_TOPIC, KONSUMER_OFFSETS_KCL_CONSUMER};
use crate::internals::Emitter;

const CHANNEL_SIZE: usize = 2000;

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

    fn set_kafka_config(mut client_config: ClientConfig) -> ClientConfig {
        client_config.set("enable.auto.commit", "true");
        client_config.set("auto.commit.interval.ms", "5000");
        client_config.set("auto.offset.reset", "earliest");
        if client_config.get("group.id").is_none() {
            client_config.set("group.id", KONSUMER_OFFSETS_KCL_CONSUMER);
        }

        client_config
    }
}

#[async_trait]
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
    fn spawn(&self, shutdown_token: CancellationToken) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>) {
        let consumer_client: StreamConsumer = Self::set_kafka_config(self.consumer_client_config.clone())
            .create()
            .expect("Failed to create Consumer Client");

        let (sx, rx) = mpsc::channel::<KonsumerOffsetsData>(CHANNEL_SIZE);

        let join_handle = tokio::spawn(async move {
            match subscribe_and_seek_to_beginning(&consumer_client, KONSUMER_OFFSETS_DATA_TOPIC).await {
                Ok(_) => info!("Subscribed to {KONSUMER_OFFSETS_DATA_TOPIC} and sought to beginning"),
                Err(e) => panic!("Failed to subscribe and seek to beginning '{KONSUMER_OFFSETS_DATA_TOPIC}': {e}"),
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
                                        error!("Failed to consume from __consumer_offsets: {e}");
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

async fn subscribe_and_seek_to_beginning(consumer: &StreamConsumer, topic: &str) -> KafkaResult<()> {
    // Subscribe to topic: note, the actual subscription is finalized only later
    consumer.subscribe(&[topic])?;

    // Wait for messages to start coming in.
    // Once the first message is received, subscription has happened
    let _ = consumer.recv().await?;

    // Fetch the current position of all the subscribed partitions
    let mut subscriptions = consumer.position()?;

    // Seek back to the beginning all the subscribed partitions
    subscriptions.set_all_offsets(Offset::Beginning)?;
    consumer.seek_partitions(subscriptions, Duration::from_secs(5))?;

    Ok(())
}
