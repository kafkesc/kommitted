use rdkafka::metadata::MetadataPartition;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Partition {
    pub id: u32,
    pub leader_broker: u32,
    pub replica_brokers: Vec<u32>,
    pub in_sync_replica_brokers: Vec<u32>,
}

impl From<&MetadataPartition> for Partition {
    fn from(p: &MetadataPartition) -> Self {
        Partition {
            id: p.id() as u32,
            leader_broker: p.leader() as u32,
            replica_brokers: p.replicas().iter().map(|r| r.to_owned() as u32).collect(),
            in_sync_replica_brokers: p.isr().iter().map(|isr| isr.to_owned() as u32).collect(),
        }
    }
}
