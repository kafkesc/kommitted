use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use konsumer_offsets::ConsumerProtocolAssignment;
use rdkafka::{admin::AdminClient, client::DefaultClientContext, groups::GroupList, ClientConfig};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::constants::KONSUMER_OFFSETS_KCL_CONSUMER;
use crate::internals::Emitter;
use crate::kafka_types::{Group, GroupWithMembers, Member, MemberWithAssignment, TopicPartition};

const CHANNEL_SIZE: usize = 5;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const FETCH_INTERVAL: Duration = Duration::from_secs(60);

/// A map of all the known Consumer Groups, at a given point in time.
///
/// This reflects the internal state of Kafka and it's active Consumer Groups.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerGroups {
    pub(crate) groups: HashMap<String, GroupWithMembers>,
}

impl From<GroupList> for ConsumerGroups {
    fn from(gl: GroupList) -> Self {
        let mut res = Self {
            groups: HashMap::with_capacity(gl.groups().len()),
        };

        for g in gl.groups() {
            // Ignore own consumer of `__consumer_offsets` topic
            if g.name() == KONSUMER_OFFSETS_KCL_CONSUMER {
                continue;
            }

            let mut res_members = HashMap::with_capacity(g.members().len());

            for m in g.members() {
                res_members.insert(
                    m.id().to_string(),
                    MemberWithAssignment {
                        member: Member {
                            id: m.id().to_string(),
                            client_id: m.client_id().to_string(),
                            client_host: m.client_host().to_string(),
                        },
                        assignment: if let Some(assignment_bytes) = m.assignment() {
                            match ConsumerProtocolAssignment::try_from(assignment_bytes) {
                                Ok(cpa) => cpa
                                    .assigned_topic_partitions
                                    .into_iter()
                                    .flat_map(TopicPartition::vec_from)
                                    .collect::<HashSet<TopicPartition>>(),
                                Err(e) => {
                                    warn!("Unable to parse 'assignment' bytes when listing Consumer Groups: {}", e);
                                    HashSet::new()
                                },
                            }
                        } else {
                            HashSet::new()
                        },
                    },
                );
            }

            res.groups.insert(
                g.name().to_string(),
                GroupWithMembers {
                    group: Group {
                        name: g.name().to_string(),
                        protocol: g.protocol().to_string(),
                        protocol_type: g.protocol_type().to_string(),
                        state: g.state().to_string(),
                    },
                    members: res_members,
                },
            );
        }

        res
    }
}

/// Emits [`ConsumerGroups`] via a provided [`mpsc::channel`].
///
/// It wraps an Admin Kafka Client, regularly requests it for the cluster consumer groups list,
/// and then emits it as [`ConsumerGroups`].
///
/// It shuts down when the provided [`CancellationToken`] is cancelled.
pub struct ConsumerGroupsEmitter {
    admin_client_config: ClientConfig,
}

impl ConsumerGroupsEmitter {
    /// Create a new [`ConsumerGroupsEmitter`]
    ///
    /// # Arguments
    ///
    /// * `admin_client_config` - Kafka admin client configuration, used to fetch Consumer Groups
    pub fn new(admin_client_config: ClientConfig) -> Self {
        Self {
            admin_client_config,
        }
    }
}

#[async_trait]
impl Emitter for ConsumerGroupsEmitter {
    type Emitted = ConsumerGroups;

    /// Spawn a new async task to run the business logic of this struct.
    ///
    /// When this emitter gets spawned, it returns a [`mpsc::Receiver`] for [`ConsumerGroups`],
    /// and a [`JoinHandle`] to help join on the task spawned internally.
    /// The task concludes (joins) only ones the inner task of the emitter terminates.
    ///
    /// # Arguments
    ///
    /// * `shutdown_token`: A [`CancellationToken`] that, when cancelled, will make the internal loop terminate.
    ///
    fn spawn(
        &self,
        shutdown_token: CancellationToken,
    ) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>) {
        let admin_client: AdminClient<DefaultClientContext> =
            self.admin_client_config.create().expect("Failed to allocate Admin Client");

        let (sx, rx) = mpsc::channel::<Self::Emitted>(CHANNEL_SIZE);

        let join_handle = tokio::spawn(async move {
            let mut interval = interval(FETCH_INTERVAL);

            loop {
                let res_groups = admin_client
                    .inner()
                    .fetch_group_list(None, FETCH_TIMEOUT)
                    .map(Self::Emitted::from);

                match res_groups {
                    Ok(groups) => {
                        tokio::select! {
                            res = Self::emit_with_interval(&sx, groups, &mut interval) => {
                                if let Err(e) = res {
                                    error!("Failed to emit {}: {e}", std::any::type_name::<ConsumerGroups>());
                                }
                            },
                            _ = shutdown_token.cancelled() => {
                                info!("Shutting down");
                                break;
                            },
                        }
                    },
                    Err(e) => {
                        error!("Failed to fetch consumer groups: {e}");
                    },
                }
            }
        });

        (rx, join_handle)
    }
}
