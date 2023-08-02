pub mod bespoke;

use std::collections::HashMap;

use prometheus::Registry;

pub const NAMESPACE: &str = "kcl";

pub const LABEL_CLUSTER_ID: &str = "cluster_id";
pub const LABEL_GROUP: &str = "group";
pub const LABEL_TOPIC: &str = "topic";
pub const LABEL_PARTITION: &str = "partition";
pub const LABEL_MEMBER_ID: &str = "member_id";
pub const LABEL_MEMBER_HOST: &str = "member_host";
pub const LABEL_MEMBER_CLIENT_ID: &str = "member_client_id";

pub const UNKNOWN_VAL: &str = "UNKNOWN";

pub fn init(cluster_id: String) -> Registry {
    let prom_def_labels = HashMap::from([(LABEL_CLUSTER_ID.to_string(), cluster_id)]);
    Registry::new_custom(Some(NAMESPACE.to_string()), Some(prom_def_labels))
        .expect("Unable to create a Prometheus Metrics Registry")
}
