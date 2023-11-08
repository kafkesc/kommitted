use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use const_format::formatcp;
use konsumer_offsets::ConsumerProtocolAssignment;
use prometheus::{
    register_histogram_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry, Histogram, IntGauge, IntGaugeVec, Registry,
};
use rdkafka::{admin::AdminClient, client::DefaultClientContext, groups::GroupList, ClientConfig};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::constants::KOMMITTED_CONSUMER_OFFSETS_CONSUMER;
use crate::internals::Emitter;
use crate::kafka_types::{Group, GroupWithMembers, Member, MemberWithAssignment, TopicPartition};
use crate::prometheus_metrics::{LABEL_GROUP, NAMESPACE};

const CHANNEL_SIZE: usize = 5;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const FETCH_INTERVAL: Duration = Duration::from_secs(60);

const MET_TOT_NAME: &str = formatcp!("{NAMESPACE}_consumer_groups_total");
const MET_TOT_HELP: &str = "Consumer groups currently in the cluster";
const MET_MEMBERS_TOT_NAME: &str = formatcp!("{NAMESPACE}_consumer_groups_members_total");
const MET_MEMBERS_TOT_HELP: &str = "Members of consumer groups currently in the cluster";
const MET_FETCH_NAME: &str =
    formatcp!("{NAMESPACE}_consumer_groups_emitter_fetch_time_milliseconds");
const MET_FETCH_HELP: &str =
    "Time (ms) taken to fetch information about all consumer groups in cluster";
const MET_CH_CAP_NAME: &str = formatcp!("{NAMESPACE}_consumer_groups_emitter_channel_capacity");
const MET_CH_CAP_HELP: &str =
    "Capacity of internal channel used to send consumer groups metadata to rest of the service";

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
            if g.name() == KOMMITTED_CONSUMER_OFFSETS_CONSUMER {
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

    // Prometheus Metrics
    metric_tot: IntGauge,
    metric_members_tot: IntGaugeVec,
    metric_fetch: Histogram,
    metric_ch_cap: IntGauge,
}

impl ConsumerGroupsEmitter {
    /// Create a new [`ConsumerGroupsEmitter`]
    ///
    /// # Arguments
    ///
    /// * `admin_client_config` - Kafka admin client configuration, used to fetch Consumer Groups
    pub fn new(admin_client_config: ClientConfig, metrics: Arc<Registry>) -> Self {
        Self {
            admin_client_config,
            metric_tot: register_int_gauge_with_registry!(MET_TOT_NAME, MET_TOT_HELP, metrics)
                .unwrap_or_else(|_| panic!("Failed to create metric: {MET_TOT_NAME}")),
            metric_members_tot: register_int_gauge_vec_with_registry!(
                MET_MEMBERS_TOT_NAME,
                MET_MEMBERS_TOT_HELP,
                &[LABEL_GROUP],
                metrics
            )
            .unwrap_or_else(|_| panic!("Failed to create metric: {MET_MEMBERS_TOT_NAME}")),
            metric_fetch: register_histogram_with_registry!(
                MET_FETCH_NAME,
                MET_FETCH_HELP,
                metrics
            )
            .unwrap_or_else(|_| panic!("Failed to create metric: {MET_FETCH_NAME}")),
            metric_ch_cap: register_int_gauge_with_registry!(
                MET_CH_CAP_NAME,
                MET_CH_CAP_HELP,
                metrics
            )
            .unwrap_or_else(|_| panic!("Failed to create metric: {MET_CH_CAP_NAME}")),
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

        // Clone metrics so they can be used in the spawned future
        let metric_cg = self.metric_tot.clone();
        let metric_cg_members = self.metric_members_tot.clone();
        let metric_cg_fetch = self.metric_fetch.clone();
        let metric_cg_ch_cap = self.metric_ch_cap.clone();

        let join_handle = tokio::spawn(async move {
            let mut interval = interval(FETCH_INTERVAL);

            loop {
                let timer = metric_cg_fetch.start_timer();
                let res_groups = admin_client
                    .inner()
                    .fetch_group_list(None, FETCH_TIMEOUT)
                    .map(Self::Emitted::from);

                // Update fetching time metric
                timer.observe_duration();

                match res_groups {
                    Ok(cg) => {
                        // Update group and group member metrics
                        metric_cg.set(cg.groups.len() as i64);
                        for (g, gm) in cg.groups.iter() {
                            metric_cg_members.with_label_values(&[&g]).set(gm.members.len() as i64);
                        }
                        // Update channel capacity metric
                        metric_cg_ch_cap.set(sx.capacity() as i64);

                        tokio::select! {
                            res = Self::emit_with_interval(&sx, cg, &mut interval) => {
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
