/// Kafka internal topic that keeps track of Consumer's committed Offsets.
/// This is consumed inside the `konsumer_offsets_data` module.
pub(crate) const KONSUMER_OFFSETS_DATA_TOPIC: &str = "__consumer_offsets";

/// This is the Consumer Group (`group.id`) value used by
/// the Consumer inside the `konsumer_offsets_data` module.
pub(crate) const KONSUMER_OFFSETS_KCL_CONSUMER: &str = "__kcl__consumer_offsets_consumer";
