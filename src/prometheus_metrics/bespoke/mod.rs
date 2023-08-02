pub mod consumer_partition_lag_milliseconds;
pub mod consumer_partition_lag_offset;
pub mod consumer_partition_offset;
pub mod partition_earliest_available_offset;
pub mod partition_earliest_tracked_offset;
pub mod partition_latest_available_offset;
pub mod partition_latest_tracked_offset;

use crate::kafka_types::Member;
use crate::lag_register::{Lag, LagRegister};

use super::UNKNOWN_VAL;

#[allow(unused)]
pub(self) const TYPE_COUNTER: &str = "counter";
pub(self) const TYPE_GAUGE: &str = "gauge";

pub(self) const HEADER_HELP: &str = "# HELP";
pub(self) const HEADER_TYPE: &str = "# TYPE";

pub(self) fn normalize_owner_data(opt_owner: Option<&Member>) -> (&str, &str, &str) {
    if let Some(o) = opt_owner {
        (o.id.as_ref(), o.client_host.as_ref(), o.client_id.as_ref())
    } else {
        (UNKNOWN_VAL, UNKNOWN_VAL, UNKNOWN_VAL)
    }
}

type IterLagRegisterFn = fn(
    cluster_id: &str,
    group: &str,
    topic: &str,
    partition: u32,
    owner: Option<&Member>,
    lag: Option<&Lag>,
    res: &mut Vec<String>,
);

/// Helper to iterate over the content of a [`LagRegister`], to apply a given [`IterLagRegisterFn`].
pub async fn iter_lag_reg(
    lag_reg: &LagRegister,
    metrics_vec: &mut Vec<String>,
    cluster_id: &str,
    ilrf: IterLagRegisterFn,
) {
    for (g, gwl) in lag_reg.lag_by_group.read().await.iter() {
        for (tp, lwo) in gwl.lag_by_topic_partition.iter() {
            ilrf(cluster_id, g, tp.topic.as_ref(), tp.partition, lwo.owner.as_ref(), lwo.lag.as_ref(), metrics_vec);
        }
    }
}
