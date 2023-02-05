use rdkafka::metadata::MetadataBroker;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
