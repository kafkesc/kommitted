use const_format::formatcp;

use crate::kafka_types::Member;
use crate::lag_register::Lag;

use super::super::{
    LABEL_CLUSTER_ID, LABEL_GROUP, LABEL_MEMBER_CLIENT_ID, LABEL_MEMBER_HOST, LABEL_MEMBER_ID,
    LABEL_PARTITION, LABEL_TOPIC, NAMESPACE,
};
use super::{normalize_owner_data, HEADER_HELP, HEADER_TYPE, TYPE_GAUGE};

const NAME: &str = formatcp!("{NAMESPACE}_kafka_consumer_partition_offset");
const HELP: &str = formatcp!("{HEADER_HELP} {NAME} The last consumed offset by the consumer of the topic partition. NOTE: '-1' means 'unknown'.");
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

    let value_and_ts = if let Some(l) = lag {
        format!("{} {}", l.offset, l.offset_timestamp.timestamp_millis())
    } else {
        "-1".into()
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
        {value_and_ts}"
    ));
}
