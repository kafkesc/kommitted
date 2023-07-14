use const_format::formatcp;

use super::{HEADER_HELP, HEADER_TYPE, TYPE_GAUGE};

const NAME: &str = "kcl_kafka_partition_earliest_tracked_offset";
const HELP: &str =
    formatcp!("{HEADER_HELP} {NAME} Earliest offset tracked to estimate the lag of consumers of the topic partition.");
const TYPE: &str = formatcp!("{HEADER_TYPE} {NAME} {TYPE_GAUGE}");

pub(in super::super) fn append_headers(res: &mut Vec<String>) {
    res.push(HELP.into());
    res.push(TYPE.into());
}

pub(in super::super) fn append_metric(
    cluster_id: &str,
    topic: &str,
    partition: u32,
    offset: u64,
    offset_timestamp_utc_ms: i64,
    res: &mut Vec<String>,
) {
    res.push(format!(
        "{NAME}\
        {{\
            cluster_id=\"{cluster_id}\",\
            topic=\"{topic}\",\
            partition=\"{partition}\"\
        }} \
        {offset} \
        {offset_timestamp_utc_ms}"
    ));
}
