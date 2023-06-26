use std::collections::HashMap;

use async_trait::async_trait;
use rdkafka::{admin::AdminClient, client::DefaultClientContext, groups::GroupList, ClientConfig};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{interval, Duration},
};

use crate::internals::Emitter;
use crate::kafka_types::{Group, Member};

const CHANNEL_SIZE: usize = 5;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const FETCH_INTERVAL: Duration = Duration::from_secs(60);

/// A map of all the known Consumer Groups, at a given point in time.
///
/// This reflects the internal state of Kafka and it's active Consumer Groups.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerGroups {
    pub(crate) groups: HashMap<String, Group>,
}

impl From<GroupList> for ConsumerGroups {
    fn from(gl: GroupList) -> Self {
        let mut res = Self {
            groups: HashMap::with_capacity(gl.groups().len()),
        };

        for g in gl.groups() {
            let mut res_members = HashMap::with_capacity(g.members().len());

            for m in g.members() {
                res_members.insert(
                    m.id().to_string(),
                    Member {
                        id: m.id().to_string(),
                        client_id: m.client_id().to_string(),
                        client_host: m.client_host().to_string(),
                    },
                );
            }

            res.groups.insert(
                g.name().to_string(),
                Group {
                    name: g.name().to_string(),
                    members: res_members,
                    protocol: g.protocol().to_string(),
                    protocol_type: g.protocol_type().to_string(),
                    state: g.state().to_string(),
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
/// It shuts down by sending a unit via a provided [`broadcast`].
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

    fn spawn(&self, mut shutdown_rx: broadcast::Receiver<()>) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>) {
        let admin_client: AdminClient<DefaultClientContext> =
            self.admin_client_config.create().expect("Failed to allocate Admin Client");

        let (sx, rx) = mpsc::channel::<Self::Emitted>(CHANNEL_SIZE);

        let join_handle = tokio::spawn(async move {
            let mut interval = interval(FETCH_INTERVAL);

            'outer: loop {
                let res_groups = admin_client.inner().fetch_group_list(None, FETCH_TIMEOUT).map(Self::Emitted::from);

                match res_groups {
                    Ok(groups) => {
                        tokio::select! {
                            res = Self::emit_with_interval(&sx, groups, &mut interval) => {
                                if let Err(e) = res {
                                    error!("Failed to emit {}: {e}", std::any::type_name::<ConsumerGroups>());
                                }
                            },
                            _ = shutdown_rx.recv() => {
                                info!("Received shutdown signal");
                                break 'outer;
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
