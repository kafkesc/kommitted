use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

use crate::cluster_status::ClusterStatus;
use crate::internals::Register;
use crate::kafka_types::Broker;

/// Registers and exposes the latest [`ClusterStatus`].
///
/// It exposes the accessor methods via an async interface,
/// while dealing internally with concurrency and synchronization.
#[derive(Debug)]
pub struct ClusterStatusRegister {
    latest_status: Arc<RwLock<Option<ClusterStatus>>>,
}

impl Register for ClusterStatusRegister {
    type Registered = ClusterStatus;

    fn new(mut rx: Receiver<Self::Registered>) -> Self {
        let csr = ClusterStatusRegister {
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
            while let Some(cs) = rx.recv().await {
                *(latest_status_arc_clone.write().await) = Some(cs);
            }
        });

        csr
    }
}

impl ClusterStatusRegister {
    /// Current Topics present in the Kafka cluster.
    pub async fn get_topics(&self) -> Vec<String> {
        match &*(self.latest_status.read().await) {
            None => Vec::new(),
            Some(cs) => cs.topics.iter().map(|t| t.name.clone()).collect(),
        }
    }

    /// Current Partitions for a Topic present in the Kafka cluster.
    pub async fn get_topic_partitions(&self, topic: &str) -> Option<Vec<u32>> {
        match &*(self.latest_status.read().await) {
            None => None,
            Some(cs) => cs.topics.iter().find(|t| t.name == topic).map(|t| t.partitions.iter().map(|p| p.id).collect()),
        }
    }

    /// Current Brokers constituting the Kafka cluster.
    pub async fn get_brokers(&self) -> Vec<Broker> {
        match &*(self.latest_status.read().await) {
            None => Vec::new(),
            Some(cs) => cs.brokers.clone(),
        }
    }
}
