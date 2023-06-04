use crate::internals::BroadcastEmitter;
use konsumer_offsets::KonsumerOffsetsData;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time::interval;

const CHANNEL_SIZE: usize = 1000;
const POLL_INTERVAL: Duration = Duration::from_millis(5);

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
}

impl BroadcastEmitter for KonsumerOffsetsDataEmitter {
    type Emitted = KonsumerOffsetsData;

    fn spawn(
        &self,
        mut shutdown_rx: Receiver<()>,
    ) -> (Receiver<Self::Emitted>, JoinHandle<()>) {
        let mut config = self.consumer_client_config.clone();
        let config = config
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest");
        if config.get("group.id").is_none() {
            config.set("group.id", "TODO");
        }

        let consumer_client: StreamConsumer =
            config.create().expect("Failed to allocate Consumer Client");

        consumer_client
            .subscribe(&["__consumer_offsets"])
            .expect("Failed to subscribe to '__consumer_offsets'");

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

                    // Initiate shutdown: by letting this task conclude,
                    // the receiver will detect the channel is closing
                    // on the sender end, and conclude its own activity/task.
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
