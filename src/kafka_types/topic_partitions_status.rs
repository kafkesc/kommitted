use rdkafka::metadata::{MetadataPartition, MetadataTopic};

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PartitionStatus {
    pub id: u32,
    pub leader_broker: u32,
    pub replica_brokers: Vec<u32>,
    pub in_sync_replica_brokers: Vec<u32>,
    pub begin_offset: u64,
    pub end_offset: u64,
}

impl From<&MetadataPartition> for PartitionStatus {
    fn from(p: &MetadataPartition) -> Self {
        PartitionStatus {
            id: p.id() as u32,
            leader_broker: p.leader() as u32,
            replica_brokers: p.replicas().iter().map(|r| r.to_owned() as u32).collect(),
            in_sync_replica_brokers: p.isr().iter().map(|isr| isr.to_owned() as u32).collect(),
            ..Default::default()
        }
    }
}
