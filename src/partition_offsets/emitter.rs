use chrono::{DateTime, Utc};
use std::sync::Arc;

use rdkafka::{admin::AdminClient, client::DefaultClientContext, ClientConfig};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{interval, Duration},
};

use crate::cluster_status::ClusterStatusRegister;
use crate::internals::Emitter;

const CHANNEL_SIZE: usize = 1;
const SEND_TIMEOUT: Duration = Duration::from_millis(100);

const FETCH_TIMEOUT: Duration = Duration::from_millis(100);
const FETCH_INTERVAL: Duration = Duration::from_secs(1);

/// Offset information for a Topic Partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PartitionOffset {
    /// Topic of the Partition
    pub topic: String,
    /// Partition
    pub partition: u32,
    /// Partition earliest available offset
    pub earliest_offset: u64,
    /// Partition latest available offset
    pub latest_offset: u64,
    /// [`DateTime<Utc>`] when this information was read from the Cluster
    pub read_datetime: DateTime<Utc>,
}

pub struct PartitionOffsetsEmitter {
    client_config: ClientConfig,
    cluster_register: Arc<ClusterStatusRegister>,
}

impl PartitionOffsetsEmitter {
    /// Creates a new [`PartitionOffsetsEmitter`].
    ///
    /// # Arguments
    ///
    /// * `client_config` - Kafka admin client configuration, used to fetch the Topic Partitions offsets (earliest, latest)
    pub fn new(
        client_config: ClientConfig,
        cluster_register: Arc<ClusterStatusRegister>,
    ) -> PartitionOffsetsEmitter {
        PartitionOffsetsEmitter {
            client_config,
            cluster_register,
        }
    }
}

impl Emitter for PartitionOffsetsEmitter {
    type Emitted = PartitionOffset;

    fn spawn(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>) {
        let admin_client: AdminClient<DefaultClientContext> = self
            .client_config
            .create()
            .expect("Failed to allocate Admin Client");

        let (sx, rx) = mpsc::channel::<PartitionOffset>(CHANNEL_SIZE);

        let csr = self.cluster_register.clone();
        let join_handle = tokio::spawn(async move {
            let mut interval = interval(FETCH_INTERVAL);

            'outer: loop {
                for t in csr.get_topics().await {
                    trace!("Fetching earlist/latest offset for Partitions of Topic '{}'", t);

                    for p in csr
                        .get_topic_partitions(t.as_str())
                        .await
                        .unwrap_or_default()
                    {
                        match admin_client.inner().fetch_watermarks(
                            t.as_str(),
                            p as i32,
                            FETCH_TIMEOUT,
                        ) {
                            Ok((earliest, latest)) => {
                                let po = PartitionOffset {
                                    topic: t.clone(),
                                    partition: p,
                                    earliest_offset: earliest as u64,
                                    latest_offset: latest as u64,
                                    read_datetime: Utc::now(),
                                };

                                tokio::select! {
                                    // Send the latest `ClusterStatus`
                                    res = sx.send_timeout(po, SEND_TIMEOUT) => {
                                        if let Err(e) = res {
                                            error!("Failed to emit partition offsets: {e}");
                                        }
                                    },

                                    // Initiate shutdown: by letting this task conclude,
                                    // the receiver of `PartitionOffsets` will detect the channel is closing
                                    // on the sender end, and conclude its own activity/task.
                                    _ = shutdown_rx.recv() => {
                                        info!("Received shutdown signal");
                                        break 'outer;
                                    },
                                }
                            },
                            Err(e) => {
                                error!("Failed to fetch partition '{t}:{p}' begin/end offsets: {e}");
                            },
                        }
                    }
                }

                interval.tick().await;
            }
        });

        (rx, join_handle)
    }
}
