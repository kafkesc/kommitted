use rdkafka::{
    admin::AdminClient,
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    groups::GroupList,
    ClientConfig, Message,
};
use tokio::{
    sync::broadcast,
    task::JoinHandle,
    time::{interval, Duration},
};

use konsumer_offsets::KonsumerOffsetsData;

use crate::internals::BroadcastEmitter;

const CHANNEL_SIZE: usize = 1000;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const KONSUMER_OFFSETS_DATA_TOPIC: &'static str = "__consumer_offsets";

/// Emits [`KonsumerOffsetsData`] via a provided [`broadcast::channel`].
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

impl BroadcastEmitter for KonsumerOffsetsDataEmitter {
    type Emitted = KonsumerOffsetsData;

    fn spawn(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> (broadcast::Receiver<Self::Emitted>, JoinHandle<()>) {
        let config =
            Self::set_kafka_config(self.consumer_client_config.clone());

        let consumer_client: StreamConsumer =
            config.create().expect("Failed to create Consumer Client");

        consumer_client.subscribe(&[KONSUMER_OFFSETS_DATA_TOPIC]).expect(
            format!("Failed to subscribe to '{}'", KONSUMER_OFFSETS_DATA_TOPIC)
                .as_str(),
        );

        // TODO
        //   1. Define configuration/logic to start the read of the topic from "earliest" or
        //   from X hours ago?
        //   2. Seek to that point for all topic/partition/offset triplets
        //   3. Begin consumption

        let (sx, rx) = broadcast::channel::<KonsumerOffsetsData>(CHANNEL_SIZE);

        let join_handle = tokio::spawn(async move {
            let mut interval = interval(POLL_INTERVAL);

            loop {
                tokio::select! {
                    r_msg = consumer_client.recv() => {
                        match r_msg {
                            Ok(m) => {
                                let res_kod = konsumer_offsets::KonsumerOffsetsData::try_from_bytes(m.key(), m.payload());

                                match res_kod {
                                    Ok(kod) => {
                                        let ch_cap = CHANNEL_SIZE - sx.len();
                                        if ch_cap == 0 {
                                            warn!("Emitting channel saturated: receivers too slow?");
                                        }

                                        if let Err(e) = sx.send(kod) {
                                            error!("Failed to emit KonsumerOffsetsData: {e}");
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

                interval.tick().await;
            }
        });

        (rx, join_handle)
    }
}
