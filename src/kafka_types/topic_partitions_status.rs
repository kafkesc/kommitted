use rdkafka::metadata::{MetadataPartition, MetadataTopic};


/// For a given Topic, it describes its status as reported by the Kafka cluster.
///
/// In details, it describes where each partition is, which broker leads each partition,
/// and which follower broker is in sync with each partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct TopicPartitionsStatus {
    pub name: String,
    pub partitions: Vec<PartitionStatus>,
}

impl From<&MetadataTopic> for TopicPartitionsStatus {
    fn from(t: &MetadataTopic) -> Self {
        TopicPartitionsStatus {
            name: t.name().to_owned(),
            partitions: t.partitions().iter().map(PartitionStatus::from).collect(),
        }
    }
}

/// For a given Partition, it describes its status as reported by the Kafka cluster.
///
/// The details make sense only in the context of the containing Topic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PartitionStatus {
    pub id: u32,
    pub leader_broker: u32,
    pub replica_brokers: Vec<u32>,
    pub in_sync_replica_brokers: Vec<u32>,
}

impl From<&MetadataPartition> for PartitionStatus {
    fn from(p: &MetadataPartition) -> Self {
        PartitionStatus {
            id: p.id() as u32,
            leader_broker: p.leader() as u32,
            replica_brokers: p.replicas().iter().map(|r| r.to_owned() as u32).collect(),
            in_sync_replica_brokers: p.isr().iter().map(|isr| isr.to_owned() as u32).collect(),
        }
    }
}
