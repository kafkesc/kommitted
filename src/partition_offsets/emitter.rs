use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prometheus::{
    register_histogram_vec_with_registry, register_int_gauge_with_registry, HistogramVec, IntGauge,
    Registry,
};
use rdkafka::{admin::AdminClient, client::DefaultClientContext, ClientConfig};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::cluster_status::ClusterStatusRegister;
use crate::internals::Emitter;
use crate::prometheus_metrics::{LABEL_PARTITION, LABEL_TOPIC};

const CHANNEL_SIZE: usize = 10_000;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const FETCH_INTERVAL: Duration = Duration::from_millis(10);

const MET_FETCH_NAME: &str = "partition_offsets_emitter_fetch_time_milliseconds";
const MET_FETCH_HELP: &str =
    "Time (ms) taken to fetch earliest/latest (watermark) offsets of a specific topic partition in cluster";
const MET_CH_CAP_NAME: &str = "partition_offsets_emitter_channel_capacity";
const MET_CH_CAP_HELP: &str =
    "Capacity of internal channel used to send partition watermark offsets to rest of the service";

/// Offset information for a Topic Partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
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

/// Emits Topic Partitions offset watermarks as [`PartitionOffset`] instances.
///
/// The watermarks are the "earliest" and "latest" known offset of a specific partition.
/// Additionally, the "read time" wall clock is provided, so _when_ the watermarks were
/// read is also known.
///
/// It shuts down when the provided [`CancellationToken`] is cancelled.
pub struct PartitionOffsetsEmitter {
    client_config: ClientConfig,
    cluster_register: Arc<ClusterStatusRegister>,

    // Prometheus Metrics
    metric_fetch: HistogramVec,
    metric_ch_cap: IntGauge,
}

impl PartitionOffsetsEmitter {
    /// Creates a new [`PartitionOffsetsEmitter`].
    ///
    /// # Arguments
    ///
    /// * `client_config` - Kafka client configuration, used to fetch the Topic Partitions offset watermarks (earliest, latest)
    pub fn new(
        client_config: ClientConfig,
        cluster_register: Arc<ClusterStatusRegister>,
        metrics: Arc<Registry>,
    ) -> Self {
        Self {
            client_config,
            cluster_register,
            metric_fetch: register_histogram_vec_with_registry!(
                MET_FETCH_NAME,
                MET_FETCH_HELP,
                &[LABEL_TOPIC, LABEL_PARTITION],
                metrics
            )
            .unwrap_or_else(|_| panic!("Failed to create metric: {MET_FETCH_NAME}")),
            metric_ch_cap: register_int_gauge_with_registry!(
                MET_CH_CAP_NAME,
                MET_CH_CAP_HELP,
                metrics
            )
            .unwrap_or_else(|_| panic!("Failed to create metric: {MET_CH_CAP_NAME}")),
        }
    }
}

#[async_trait]
impl Emitter for PartitionOffsetsEmitter {
    type Emitted = PartitionOffset;

    /// Spawn a new async task to run the business logic of this struct.
    ///
    /// When this emitter gets spawned, it returns a [`mpsc::Receiver`] for [`PartitionOffset`],
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
        let admin_client: AdminClient<DefaultClientContext> =
            self.client_config.create().expect("Failed to allocate Admin Client");

        let (sx, rx) = mpsc::channel::<PartitionOffset>(CHANNEL_SIZE);

        // Clone metrics so they can be used in the spawned future
        let metric_cg_fetch = self.metric_fetch.clone();
        let metric_cg_ch_cap = self.metric_ch_cap.clone();

        let csr = self.cluster_register.clone();
        let join_handle = tokio::spawn(async move {
            let mut interval = interval(FETCH_INTERVAL);

            'outer: loop {
                for t in csr.get_topics().await {
                    trace!("Fetching earliest/latest offset for Partitions of Topic '{}'", t);

                    for p in csr.get_partitions_for_topic(&t).await.unwrap_or_default() {
                        // Fetch Partition Watermarks and update timer metrics
                        let timer =
                            metric_cg_fetch.with_label_values(&[&t, &p.to_string()]).start_timer();
                        let res_watermarks =
                            admin_client.inner().fetch_watermarks(&t, p as i32, FETCH_TIMEOUT);
                        timer.observe_duration();

                        match res_watermarks {
                            Ok((earliest, latest)) => {
                                let po = PartitionOffset {
                                    topic: t.clone(),
                                    partition: p,
                                    earliest_offset: earliest as u64,
                                    latest_offset: latest as u64,
                                    read_datetime: Utc::now(),
                                };

                                // Update channel capacity metric
                                metric_cg_ch_cap.set(sx.capacity() as i64);

                                tokio::select! {
                                    res = Self::emit(&sx, po) => {
                                        if let Err(e) = res {
                                            error!("Failed to emit {}: {e}", std::any::type_name::<PartitionOffset>());
                                        }
                                    },
                                    _ = shutdown_token.cancelled() => {
                                        info!("Shutting down");
                                        break 'outer;
                                    },
                                }
                            },
                            Err(e) => {
                                error!(
                                    "Failed to fetch partition '{t}:{p}' begin/end offsets: {e}"
                                );
                            },
                        }
                    }
                }

                // Wait for next "tick", or get interrupted by shutdown
                tokio::select! {
                    _ = interval.tick() => {
                        // No-op
                    },
                    _ = shutdown_token.cancelled() => {
                        info!("Shutting down");
                        break 'outer;
                    },
                }
            }
        });

        (rx, join_handle)
    }
}
