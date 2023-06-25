/// Represents a single Topic-Partition pair
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

impl TopicPartition {
    pub(crate) fn new(topic: String, partition: u32) -> Self {
        Self {
            topic,
            partition,
        }
    }
}
