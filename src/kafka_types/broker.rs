use rdkafka::metadata::MetadataBroker;

/// A Brokers that is part of a Kafka cluster.
///
/// It is identified by a unique identifier for the given Cluster,
/// and the host and port to connect to it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Broker {
    /// Broker unique identifier, as configured at the Kafka Cluster level.
    /// Note that uniqueness is "expected" by Brokers,
    /// that refuse to work if they detect a collision.
    pub id: u32,

    /// Host of the Broker
    pub host: String,

    /// Port the Broker listens on, from the perspective of the Admin Client
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
