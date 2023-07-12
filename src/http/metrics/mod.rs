pub mod consumer_partition_lag_milliseconds;
pub mod consumer_partition_lag_offset;
pub mod consumer_partition_offset;

use crate::kafka_types::Member;

pub(self) const UNKNOWN_VAL: &str = "UNKNOWN";

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
