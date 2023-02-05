use std::time::Duration;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::metadata::{MetadataBroker, MetadataTopic};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

use crate::kafka_types::Broker;
use crate::kafka_types::Topic;

pub struct ClusterMetaEmitter {
    admin_client_config: ClientConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClusterMeta {
    topics: Vec<Topic>,
    brokers: Vec<Broker>,
}

impl ClusterMetaEmitter {
    pub fn new(client_config: ClientConfig) -> ClusterMetaEmitter {
        ClusterMetaEmitter {
            admin_client_config: client_config,
        }
    }

    pub fn spawn(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> (mpsc::Receiver<ClusterMeta>, JoinHandle<()>) {
        let admin_client: AdminClient<DefaultClientContext> = self.admin_client_config.create().expect("TODO");

        let (tx, rx) = mpsc::channel::<ClusterMeta>(1);

        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(time::Duration::from_secs(5));

            let mut shutdown_requested = false;
            while !shutdown_requested {
                let metadata = admin_client.inner().fetch_metadata(None, Timeout::Never).expect("TODO");

                let latest_cluster_meta = ClusterMeta {
                    topics: metadata.topics().iter().map(|t| Topic::from(t)).collect(),
                    brokers: metadata.brokers().iter().map(|b| Broker::from(b)).collect(),
                };

                tokio::select! {
                    send_res = tx.send_timeout(latest_cluster_meta, Duration::from_millis(10)) => {
                        if let Err(e) = send_res {
                            error!("Failed to emit ClusterMeta: {e}");
                        }
                    },

                    // Initiate shutdown: by letting this task conclude,
                    // the "tap" `records_tx` will close, causing the "sink" `records_rx`
                    // to return `None` and conclude its own task.
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal");
                        shutdown_requested = true;
                    },
                }

                interval.tick().await;
            }
        });

        (rx, join_handle)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    #[test]
    fn test_none() {}
}
