use async_trait::async_trait;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

use konsumer_offsets::KonsumerOffsetsData;

use crate::internals::Emitter;

const CHANNEL_SIZE: usize = 1000;

const KONSUMER_OFFSETS_DATA_TOPIC: &str = "__consumer_offsets";

/// Emits [`KonsumerOffsetsData`] via a provided [`mpsc::channel`].
///
/// It wraps a Kafka Client, consumes the `__consumer_offsets` topic, and emits its records
/// parsed into [`KonsumerOffsetsData`].
///
/// It shuts down by sending a unit via a provided [`broadcast`].
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
        client_config.set("auto.offset.reset", "earliest");
        if client_config.get("group.id").is_none() {
            client_config.set("group.id", std::any::type_name::<Self>());
        }

        client_config
    }
}

#[async_trait]
impl Emitter for KonsumerOffsetsDataEmitter {
    type Emitted = KonsumerOffsetsData;

    fn spawn(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>) {
        let config =
            Self::set_kafka_config(self.consumer_client_config.clone());

        let consumer_client: StreamConsumer =
            config.create().expect("Failed to create Consumer Client");

        consumer_client
            .subscribe(&[KONSUMER_OFFSETS_DATA_TOPIC])
            .unwrap_or_else(|_| panic!("Failed to subscribe to '{}'", KONSUMER_OFFSETS_DATA_TOPIC));

        // TODO
        //   1. Define configuration/logic to start the read of the topic from "earliest" or
        //   from X hours ago?
        //   2. Seek to that point for all topic/partition/offset triplets
        //   3. Begin consumption

        let (sx, rx) = mpsc::channel::<KonsumerOffsetsData>(CHANNEL_SIZE);

        let join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    r_msg = consumer_client.recv() => {
                        match r_msg {
                            Ok(m) => {
                                let res_kod = konsumer_offsets::KonsumerOffsetsData::try_from_bytes(m.key(), m.payload());

                                match res_kod {
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
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal");
                        break;
                    }
                }
            }
        });

        (rx, join_handle)
    }
}
