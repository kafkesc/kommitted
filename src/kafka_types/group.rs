use std::collections::{HashMap, HashSet};

use crate::kafka_types::TopicPartition;

/// Consumer Group Member
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Member {
    /// Identifier
    pub id: String,

    /// Value of `client.id` set by the Consumer
    pub client_id: String,

    /// Host where the Consumer is running
    pub client_host: String,
}

/// Consumer Group Member, paired with the set of [`TopicPartition`] assigned to it
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemberWithAssignment {
    /// The [`Member`] itself
    pub member: Member,

    /// The [`HashSet`] of [`TopicPartition`] assigned to the [`Member`]
    pub assignment: HashSet<TopicPartition>,
}

/// Consumer Group
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Group {
    /// Group name
    pub name: String,

    /// Type of Protocol used by this Group
    pub protocol_type: String,

    /// Group Protocol of `protocol_type` used by this Group
    pub protocol: String,

    /// Group state
    pub state: String,
}

/// Consumer Group, paired with a map of [`MemberWithAssignment`] indexed by [`Member::id`]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupWithMembers {
    pub group: Group,
    pub members: HashMap<String, MemberWithAssignment>,
}
