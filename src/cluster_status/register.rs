use std::sync::Arc;

use prometheus::{
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, IntGauge, IntGaugeVec,
    Registry,
};
use tokio::sync::{mpsc::Receiver, RwLock};

use super::emitter::ClusterStatus;

use crate::constants::DEFAULT_CLUSTER_ID;
use crate::internals::Awaitable;
use crate::kafka_types::{Broker, TopicPartition};
use crate::prometheus_metrics::LABEL_TOPIC;

const MET_BROKERS_TOT_NAME: &str = "cluster_brokers_total";
const MET_BROKERS_TOT_HELP: &str = "Brokers currently in cluster";
const MET_TOPICS_TOT_NAME: &str = "cluster_topics_total";
const MET_TOPICS_TOT_HELP: &str = "Topics currently in cluster";
const MET_PARTITIONS_TOT_NAME: &str = "cluster_partitions_total";
const MET_PARTITIONS_TOT_HELP: &str = "Partitions currently in cluster";
const MET_TOPIC_PARTITIONS_TOT_NAME: &str = "cluster_topic_partitions_total";
const MET_TOPIC_PARTITIONS_TOT_HELP: &str = "Topic's Partitions currently in cluster";

/// Registers and exposes the latest [`ClusterStatus`].
///
/// It exposes the accessor methods via an async interface,
/// while dealing internally with concurrency and synchronization.
#[derive(Debug)]
pub struct ClusterStatusRegister {
    latest_status: Arc<RwLock<Option<ClusterStatus>>>,

    // Prometheus Metrics
    metric_brokers: IntGauge,
    metric_topics: IntGauge,
    metric_partitions: IntGauge,
    metric_topic_partitions: IntGaugeVec,
}

impl ClusterStatusRegister {
    pub fn new(
        cluster_id_override: Option<String>,
        mut rx: Receiver<ClusterStatus>,
        metrics: Arc<Registry>,
    ) -> Self {
        let csr = Self {
            latest_status: Arc::new(RwLock::new(None)),
            metric_brokers: register_int_gauge_with_registry!(
                MET_BROKERS_TOT_NAME,
                MET_BROKERS_TOT_HELP,
                metrics
            )
            .unwrap_or_else(|e| panic!("Failed to create metric '{MET_BROKERS_TOT_NAME}': {e}")),
            metric_topics: register_int_gauge_with_registry!(
                MET_TOPICS_TOT_NAME,
                MET_TOPICS_TOT_HELP,
                metrics
            )
            .unwrap_or_else(|e| panic!("Failed to create metric '{MET_TOPICS_TOT_NAME}': {e}")),
            metric_partitions: register_int_gauge_with_registry!(
                MET_PARTITIONS_TOT_NAME,
                MET_PARTITIONS_TOT_HELP,
                metrics
            )
            .unwrap_or_else(|e| panic!("Failed to create metric '{MET_PARTITIONS_TOT_NAME}': {e}")),
            metric_topic_partitions: register_int_gauge_vec_with_registry!(
                MET_TOPIC_PARTITIONS_TOT_NAME,
                MET_TOPIC_PARTITIONS_TOT_HELP,
                &[LABEL_TOPIC],
                metrics
            )
            .unwrap_or_else(|e| {
                panic!("Failed to create metric '{MET_TOPIC_PARTITIONS_TOT_NAME}': {e}")
            }),
        };

        // A clone of the `csr.latest_status` will be moved into the async task
        // that updates the register.
        let latest_status_arc_clone = csr.latest_status.clone();

        // Clone metrics so they can be used in the spawned future
        let metric_brokers = csr.metric_brokers.clone();
        let metric_topics = csr.metric_topics.clone();
        let metric_partitions = csr.metric_partitions.clone();
        let metric_topic_partitions = csr.metric_topic_partitions.clone();

        // The Register is essentially "self updating" its data, by listening
        // on a channel for updates.
        //
        // The internal async task will terminate when the internal loop breaks:
        // that will happen when the `Receiver` `rx` receives `None`.
        // And, in turn, that will happen when the `Sender` part of the channel is dropped.
        tokio::spawn(async move {
            debug!("Begin receiving ClusterStatus updates");

            loop {
                tokio::select! {
                    Some(mut cs) = rx.recv() => {
                        trace!("Received:\n{:#?}", cs);

                        // Override cluster identifier, if present
                        if let Some(c_id_over) = &cluster_id_override {
                            cs.id = c_id_over.to_string();
                        }

                        info!(
                            "Updated cluster status: {:?} cluster.id, {} topics, {} brokers",
                            cs.id, cs.topics.len(), cs.brokers.len()
                        );

                        // Update cluster status metrics (broker, topics, partitions)
                        metric_brokers.set(cs.brokers.len() as i64);
                        metric_topics.set(cs.topics.len() as i64);
                        let mut partitions_total = 0;
                        for t in cs.topics.iter() {
                            metric_topic_partitions
                                .with_label_values(&[&t.name])
                                .set(t.partitions.len() as i64);
                            partitions_total += t.partitions.len();
                        }
                        metric_partitions.set(partitions_total as i64);

                        // Set the latest cluster status
                        *(latest_status_arc_clone.write().await) = Some(cs);
                    },
                    else => {
                        info!("Emitters stopping: breaking (internal) loop");
                        break;
                    }
                }
            }
        });

        csr
    }

    /// Current identifier of the Kafka cluster.
    pub async fn get_cluster_id(&self) -> String {
        match &*(self.latest_status.read().await) {
            None => DEFAULT_CLUSTER_ID.to_string(),
            Some(cs) => cs.id.clone(),
        }
    }

    /// Current Topics present in the Kafka cluster.
    pub async fn get_topics(&self) -> Vec<String> {
        match &*(self.latest_status.read().await) {
            None => Vec::new(),
            Some(cs) => cs.topics.iter().map(|t| t.name.clone()).collect(),
        }
    }

    /// Current Partitions for a Topic present in the Kafka cluster.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topics we want to know the Partitions of.
    pub async fn get_partitions_for_topic(&self, topic: &str) -> Option<Vec<u32>> {
        match &*(self.latest_status.read().await) {
            None => None,
            Some(cs) => cs
                .topics
                .iter()
                .find(|t| t.name == topic)
                .map(|t| t.partitions.iter().map(|p| p.id).collect()),
        }
    }

    /// Current [`TopicPartition`]s in the Kafka cluster.
    pub async fn get_topic_partitions(&self) -> Vec<TopicPartition> {
        match &*(self.latest_status.read().await) {
            None => Vec::new(),
            Some(cs) => cs
                .topics
                .iter()
                .flat_map(|tps| {
                    let t = tps.name.clone();
                    tps.partitions
                        .iter()
                        .map(|ps| TopicPartition::new(t.clone(), ps.id))
                        .collect::<Vec<TopicPartition>>()
                })
                .collect(),
        }
    }

    /// Current Brokers constituting the Kafka cluster.
    #[allow(unused)]
    pub async fn get_brokers(&self) -> Vec<Broker> {
        match &*(self.latest_status.read().await) {
            None => Vec::new(),
            Some(cs) => cs.brokers.clone(),
        }
    }
}

impl Awaitable for ClusterStatusRegister {
    /// [`Self`] ready when its internal copy of [`ClusterStatus`] has been populated.
    async fn is_ready(&self) -> bool {
        self.latest_status.read().await.is_some()
    }
}
