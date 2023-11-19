pub mod bespoke;

use std::collections::HashMap;

use prometheus::Registry;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use tokio::time::Duration;

use crate::constants::DEFAULT_CLUSTER_ID;

pub const NAMESPACE: &str = "kmtd";

pub const LABEL_CLUSTER_ID: &str = "cluster_id";
pub const LABEL_GROUP: &str = "group";
pub const LABEL_TOPIC: &str = "topic";
pub const LABEL_PARTITION: &str = "partition";
pub const LABEL_MEMBER_ID: &str = "member_id";
pub const LABEL_MEMBER_HOST: &str = "member_host";
pub const LABEL_MEMBER_CLIENT_ID: &str = "member_client_id";

pub const UNKNOWN_VAL: &str = "UNKNOWN";

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

pub fn init(client_config: ClientConfig, cluster_id_override: Option<String>) -> Registry {
    let cluster_id = match cluster_id_override {
        Some(cid) => cid,
        None => client_config
            .create::<AdminClient<DefaultClientContext>>()
            .expect("Failed to allocate Admin Client")
            .inner()
            .fetch_cluster_id(FETCH_TIMEOUT)
            .unwrap_or_else(|| DEFAULT_CLUSTER_ID.to_string()),
    };

    let prom_def_labels = HashMap::from([(LABEL_CLUSTER_ID.to_string(), cluster_id)]);

    info!("Prometheus Metrics default labels:\n{:#?}", prom_def_labels);

    Registry::new_custom(Some(NAMESPACE.to_string()), Some(prom_def_labels))
        .expect("Unable to create a Prometheus Metrics Registry")
}
