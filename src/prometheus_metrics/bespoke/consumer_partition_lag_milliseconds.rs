use const_format::formatcp;

use crate::kafka_types::Member;
use crate::lag_register::Lag;

use super::super::{
    LABEL_CLUSTER_ID, LABEL_GROUP, LABEL_MEMBER_CLIENT_ID, LABEL_MEMBER_HOST, LABEL_MEMBER_ID, LABEL_PARTITION,
    LABEL_TOPIC, NAMESPACE,
};
use super::{normalize_owner_data, HEADER_HELP, HEADER_TYPE, TYPE_GAUGE};

const NAME: &str = formatcp!("{NAMESPACE}_kafka_consumer_partition_lag_milliseconds");
const HELP: &str =
    formatcp!("{HEADER_HELP} {NAME} The time difference (time lag) between when the latest offset was produced and the latest consumed offset was consumed, by the consumer of the topic partition, expressed in milliseconds. NOTE: '-1, -1' means 'unknown'.");
const TYPE: &str = formatcp!("{HEADER_TYPE} {NAME} {TYPE_GAUGE}");

pub(crate) fn append_headers(res: &mut Vec<String>) {
    res.push(HELP.into());
    res.push(TYPE.into());
}

pub(crate) fn append_metric(
    cluster_id: &str,
    group: &str,
    topic: &str,
    partition: u32,
    owner: Option<&Member>,
    lag: Option<&Lag>,
    res: &mut Vec<String>,
) {
    let (member_id, member_host, member_client_id) = normalize_owner_data(owner);

    let (time_lag, offset_timestamp_utc_ms) = if let Some(l) = lag {
        (l.time_lag.num_milliseconds(), l.offset_timestamp.timestamp_millis())
    } else {
        (-1, -1)
    };

    res.push(format!(
        "{NAME}\
        {{\
            {LABEL_CLUSTER_ID}=\"{cluster_id}\",\
            {LABEL_GROUP}=\"{group}\",\
            {LABEL_TOPIC}=\"{topic}\",\
            {LABEL_PARTITION}=\"{partition}\",\
            {LABEL_MEMBER_ID}=\"{member_id}\",\
            {LABEL_MEMBER_HOST}=\"{member_host}\",\
            {LABEL_MEMBER_CLIENT_ID}=\"{member_client_id}\"\
        }} \
        {time_lag} \
        {offset_timestamp_utc_ms}"
    ));
}
