use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
};

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

impl Hash for MemberWithAssignment {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.member.hash(state);
        for tp in &self.assignment {
            tp.hash(state);
        }
    }
}

/// Consumer Group
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
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

impl Hash for GroupWithMembers {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group.hash(state);
        for (g, gm) in &self.members {
            g.hash(state);
            gm.hash(state);
        }
    }
}
