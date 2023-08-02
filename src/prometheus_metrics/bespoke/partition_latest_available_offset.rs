use const_format::formatcp;

use super::super::{LABEL_CLUSTER_ID, LABEL_PARTITION, LABEL_TOPIC, NAMESPACE};
use super::{HEADER_HELP, HEADER_TYPE, TYPE_GAUGE};

const NAME: &str = formatcp!("{NAMESPACE}_kafka_partition_latest_available_offset");
const HELP: &str =
    formatcp!("{HEADER_HELP} {NAME} Latest offset available to consumers of the topic partition.");
const TYPE: &str = formatcp!("{HEADER_TYPE} {NAME} {TYPE_GAUGE}");

pub(crate) fn append_headers(res: &mut Vec<String>) {
    res.push(HELP.into());
    res.push(TYPE.into());
}

pub(crate) fn append_metric(
    cluster_id: &str,
    topic: &str,
    partition: u32,
    offset: u64,
    res: &mut Vec<String>,
) {
    res.push(format!(
        "{NAME}\
        {{\
            {LABEL_CLUSTER_ID}=\"{cluster_id}\",\
            {LABEL_TOPIC}=\"{topic}\",\
            {LABEL_PARTITION}=\"{partition}\"\
        }} \
        {offset}"
    ));
}
