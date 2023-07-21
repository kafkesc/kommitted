use const_format::formatcp;

use crate::kafka_types::Member;
use crate::lag_register::Lag;

use super::{normalize_owner_data, HEADER_HELP, HEADER_TYPE, TYPE_GAUGE};

const NAME: &str = "kcl_kafka_consumer_partition_lag_offset";
const HELP: &str =
    formatcp!("{HEADER_HELP} {NAME} The difference (lag) between the last produced offset and the last consumed offset, by the consumer of the topic partition. NOTE: '0, -1' means 'unknown'.");
const TYPE: &str = formatcp!("{HEADER_TYPE} {NAME} {TYPE_GAUGE}");

pub(in super::super) fn append_headers(res: &mut Vec<String>) {
    res.push(HELP.into());
    res.push(TYPE.into());
}

pub(in super::super) fn append_metric(
    cluster_id: &str,
    group: &str,
    topic: &str,
    partition: u32,
    owner: Option<&Member>,
    lag: Option<&Lag>,
    res: &mut Vec<String>,
) {
    let (member_id, member_host, member_client_id) = normalize_owner_data(owner);

    let (offset_lag, offset_timestamp_utc_ms) = if let Some(l) = lag {
        (l.offset_lag, l.offset_timestamp.timestamp_millis())
    } else {
        (0, -1)
    };

    res.push(format!(
        "{NAME}\
        {{\
            cluster_id=\"{cluster_id}\",\
            group=\"{group}\",\
            topic=\"{topic}\",\
            partition=\"{partition}\",\
            member_id=\"{member_id}\",\
            member_host=\"{member_host}\",\
            member_client_id=\"{member_client_id}\"\
        }} \
        {offset_lag} \
        {offset_timestamp_utc_ms}"
    ));
}
