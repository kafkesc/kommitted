use rdkafka::metadata::MetadataBroker;

/// A Brokers that is part of a Kafka cluster.
///
/// It is identified by a unique identifier for the given Cluster,
/// and the host and port to connect to it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Broker {
    pub id: u32,
    pub host: String,
    pub port: u16,
}

impl From<&MetadataBroker> for Broker {
    fn from(b: &MetadataBroker) -> Self {
        Broker {
            id: b.id() as u32,
            host: b.host().to_owned(),
            port: b.port() as u16,
        }
    }
}
