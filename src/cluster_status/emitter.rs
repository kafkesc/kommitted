use std::sync::Arc;

use prometheus::{
    register_histogram_with_registry, register_int_gauge_with_registry, Histogram, IntGauge,
    Registry,
};
use rdkafka::{admin::AdminClient, client::DefaultClientContext, metadata::Metadata, ClientConfig};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::constants::{DEFAULT_CLUSTER_ID, KONSUMER_OFFSETS_DATA_TOPIC};
use crate::internals::Emitter;
use crate::kafka_types::{Broker, TopicPartitionsStatus};

const CHANNEL_SIZE: usize = 5;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const FETCH_INTERVAL: Duration = Duration::from_secs(60);

const MET_FETCH_NAME: &str = "cluster_status_emitter_fetch_time_milliseconds";
const MET_FETCH_HELP: &str = "Time (ms) taken to fetch cluster status metadata";
const MET_CH_CAP_NAME: &str = "cluster_status_emitter_channel_capacity";
const MET_CH_CAP_HELP: &str =
    "Capacity of internal channel used to send cluster status metadata to rest of the service";

/// This is a `Send`-able struct to carry Kafka Cluster status across thread boundaries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct ClusterStatus {
    /// Cluster identifier, defined as `cluster.id` in Brokers' configuration.
    /// It will be `__none__` if not set on Brokers.
    pub id: String,

    /// A vector of [`TopicPartitionsStatus`].
    ///
    /// It reflects the status of Topics (and Partitions) as reported by the Kafka cluster.
    pub topics: Vec<TopicPartitionsStatus>,

    /// A vector of [`Broker`].
    ///
    /// It reflects the status of Brokers as reported by the Kafka cluster.
    pub brokers: Vec<Broker>,
}

impl ClusterStatus {
    fn from(id: Option<String>, m: Metadata) -> Self {
        Self {
            id: id.unwrap_or_else(|| DEFAULT_CLUSTER_ID.to_string()),
            topics: m
                .topics()
                .iter()
                // Ignore `__consumer_offsets` topic
                .filter(|mt| mt.name() != KONSUMER_OFFSETS_DATA_TOPIC)
                .map(TopicPartitionsStatus::from)
                .collect(),
            brokers: m.brokers().iter().map(Broker::from).collect(),
        }
    }
}

/// Emits [`ClusterStatus`] via a provided [`mpsc::channel`].
///
/// It wraps an Admin Kafka Client, regularly requests it for the cluster metadata,
/// and then emits it as [`ClusterStatus`].
///
/// It shuts down when the provided [`CancellationToken`] is cancelled.
pub struct ClusterStatusEmitter {
    admin_client_config: ClientConfig,

    // Prometheus Metrics
    metric_fetch: Histogram,
    metric_ch_cap: IntGauge,
}

impl ClusterStatusEmitter {
    /// Create a new [`ClusterStatusEmitter`]
    ///
    /// # Arguments
    ///
    /// * `client_config` - Kafka admin client configuration, used to fetch the Cluster current status
    pub fn new(client_config: ClientConfig, metrics: Arc<Registry>) -> Self {
        Self {
            admin_client_config: client_config,
            metric_fetch: register_histogram_with_registry!(
                MET_FETCH_NAME,
                MET_FETCH_HELP,
                metrics
            )
            .unwrap_or_else(|e| panic!("Failed to create metric '{MET_FETCH_NAME}': {e}")),
            metric_ch_cap: register_int_gauge_with_registry!(
                MET_CH_CAP_NAME,
                MET_CH_CAP_HELP,
                metrics
            )
            .unwrap_or_else(|e| panic!("Failed to create metric '{MET_CH_CAP_NAME}': {e}")),
        }
    }
}

impl Emitter for ClusterStatusEmitter {
    type Emitted = ClusterStatus;

    /// Spawn a new async task to run the business logic of this struct.
    ///
    /// When this emitter gets spawned, it returns a [`mpsc::Receiver`] for [`ClusterStatus`],
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
            self.admin_client_config.create().expect("Failed to allocate Admin Client");

        let (sx, rx) = mpsc::channel::<Self::Emitted>(CHANNEL_SIZE);

        // Clone metrics so they can be used in the spawned future
        let metric_fetch = self.metric_fetch.clone();
        let metric_ch_cap = self.metric_ch_cap.clone();

        let join_handle = tokio::spawn(async move {
            let mut interval = interval(FETCH_INTERVAL);

            loop {
                // Fetch metadata and update timer metric
                let timer = metric_fetch.start_timer();
                let res_status =
                    admin_client.inner().fetch_metadata(None, FETCH_TIMEOUT).map(|m| {
                        Self::Emitted::from(admin_client.inner().fetch_cluster_id(FETCH_TIMEOUT), m)
                    });
                timer.observe_duration();

                match res_status {
                    Ok(status) => {
                        // Update channel capacity metric
                        metric_ch_cap.set(sx.capacity() as i64);

                        tokio::select! {
                            res = Self::emit_with_interval(&sx, status, &mut interval) => {
                                if let Err(e) = res {
                                    error!("Failed to emit {}: {e}", std::any::type_name::<ClusterStatus>());
                                }
                            },
                            _ = shutdown_token.cancelled() => {
                                info!("Shutting down");
                                break;
                            },
                        }
                    },
                    Err(e) => {
                        error!("Failed to fetch cluster metadata: {e}");
                    },
                }
            }
        });

        (rx, join_handle)
    }
}
