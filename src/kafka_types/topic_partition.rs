use konsumer_offsets::TopicPartitions;
use std::fmt::{Display, Formatter};

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

    pub(crate) fn vec_from(topic_partitions: TopicPartitions) -> Vec<Self> {
        topic_partitions
            .partitions
            .into_iter()
            .map(|p| TopicPartition::new(topic_partitions.topic.clone(), p as u32))
            .collect()
    }
}

impl Display for TopicPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.topic, self.partition)
    }
}
