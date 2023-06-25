use std::collections::HashMap;

/// Holds Consumer Group Member information, like `client.id`, host and other client specifics.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Member {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
}

/// Hold Consumer Group information, like memberships.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Group {
    pub name: String,
    pub members: HashMap<String, Member>,
    pub protocol: String,
    pub protocol_type: String,
    pub state: String,
}