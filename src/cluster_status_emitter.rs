use std::time::Duration;

use rdkafka::{admin::AdminClient, client::DefaultClientContext, ClientConfig};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};

use crate::kafka_types::{Broker, TopicPartitionsStatus};

const CHANNEL_SIZE: usize = 1;
const CHANNEL_SEND_TIMEOUT: Duration = Duration::from_millis(100);

const METADATA_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const METADATA_FETCH_INTERVAL: Duration = Duration::from_secs(10);

/// Emits [`ClusterStatus`] via a provided [`mpsc::channel`].
///
/// It wraps an Admin Kafka Client, regularly requests it for the cluster metadata,
/// and then emits it as [`ClusterStatus`].
///
/// It shuts down by sending a unit via a provided [`broadcast`].
pub struct ClusterStatusEmitter {
    admin_client_config: ClientConfig,
}

/// This is a `Send`-able struct to carry Kafka Cluster status across thread boundaries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClusterStatus {
    /// A vector of [`TopicPartitionsStatus`].
    ///
    /// For each topic it describes where each partition is, which broker leads it,
    /// and which follower brokers are in sync.
    pub topics: Vec<TopicPartitionsStatus>,

    /// A vector of [`Broker`].
    ///
    /// Brokers that are part of the Cluster, ID, host and port.
    pub brokers: Vec<Broker>,
}

impl ClusterStatusEmitter {
    pub fn new(client_config: ClientConfig) -> ClusterStatusEmitter {
        ClusterStatusEmitter {
            admin_client_config: client_config,
        }
    }

    /// Spawn a new async task to run the business logic of this struct.
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx`: A [`broadcast::Receiver`] to request the internal async task to shutdown.
    ///
    pub fn spawn(&self, mut shutdown_rx: broadcast::Receiver<()>) -> (mpsc::Receiver<ClusterStatus>, JoinHandle<()>) {
        let admin_client: AdminClient<DefaultClientContext> =
            self.admin_client_config.create().expect("Failed to allocate Admin Client");

        let (sx, rx) = mpsc::channel::<ClusterStatus>(CHANNEL_SIZE);

        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(METADATA_FETCH_INTERVAL);

            loop {
                match admin_client.inner().fetch_metadata(None, METADATA_FETCH_TIMEOUT) {
                    Ok(m) => {
                        // NOTE: ClusterMeta is Send
                        let status = ClusterStatus {
                            topics: m.topics().iter().map(TopicPartitionsStatus::from).collect(),
                            brokers: m.brokers().iter().map(Broker::from).collect(),
                        };

                        tokio::select! {
                            // Send the latest `ClusterStatus`
                            res = sx.send_timeout(status, CHANNEL_SEND_TIMEOUT) => {
                                if let Err(e) = res {
                                    error!("Failed to emit cluster status: {e}");
                                }
                            },

                            // Initiate shutdown: by letting this task conclude,
                            // the receiver of `ClusterStatus` will detect the channel is closing
                            // on the sender end, and conclude its own activity/task.
                            _ = shutdown_rx.recv() => {
                                info!("Received shutdown signal");
                                break;
                            },
                        }
                    },
                    Err(e) => {
                        error!("Failed to fetch cluster metadata: {e}");
                    },
                }

                interval.tick().await;
            }
        });

        (rx, join_handle)
    }
}
