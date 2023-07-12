use std::sync::Arc;

use tokio::sync::{mpsc::Receiver, RwLock};

use super::emitter::ClusterStatus;
use crate::kafka_types::Broker;

/// Registers and exposes the latest [`ClusterStatus`].
///
/// It exposes the accessor methods via an async interface,
/// while dealing internally with concurrency and synchronization.
#[derive(Debug)]
pub struct ClusterStatusRegister {
    latest_status: Arc<RwLock<Option<ClusterStatus>>>,
}

impl ClusterStatusRegister {
    pub fn new(cluster_id_override: Option<String>, mut rx: Receiver<ClusterStatus>) -> Self {
        let csr = Self {
            latest_status: Arc::new(RwLock::new(None)),
        };

        // A clone of the `csr.latest_status` will be moved into the async task
        // that updates the register.
        let latest_status_arc_clone = csr.latest_status.clone();

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
            None => super::emitter::CLUSTER_ID_NONE.to_string(),
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
    pub async fn get_topic_partitions(&self, topic: &str) -> Option<Vec<u32>> {
        match &*(self.latest_status.read().await) {
            None => None,
            Some(cs) => cs.topics.iter().find(|t| t.name == topic).map(|t| t.partitions.iter().map(|p| p.id).collect()),
        }
    }

    /// Current Brokers constituting the Kafka cluster.
    #[allow(unused)] // TODO Remove
    pub async fn get_brokers(&self) -> Vec<Broker> {
        match &*(self.latest_status.read().await) {
            None => Vec::new(),
            Some(cs) => cs.brokers.clone(),
        }
    }
}
