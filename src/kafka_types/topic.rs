use rdkafka::metadata::MetadataTopic;

use super::partition::Partition;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

impl From<&MetadataTopic> for Topic {
    fn from(t: &MetadataTopic) -> Self {
        Topic {
            name: t.name().to_owned(),
            partitions: t.partitions().iter().map(|p| Partition::from(p)).collect(),
        }
    }
}
