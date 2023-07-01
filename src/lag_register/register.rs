use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use konsumer_offsets::{GroupMetadata, KonsumerOffsetsData, OffsetCommit};
use log::Level::{Debug, Trace};
use tokio::sync::{mpsc, RwLock};

use crate::consumer_groups::ConsumerGroups;
use crate::kafka_types::{Group, Member, TopicPartition};
use crate::partition_offsets::{KnownOffset, PartitionOffsetsRegister};

/// Describes the "lag" (or "latency"), and it's usually paired with a Consumer [`GroupWithMembers`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Lag {
    /// Offset that a given Consumer [`GroupWithMembers`] is at when consuming a specific [`TopicPartition`].
    pub(crate) offset: u64,

    /// [`DateTime<Utc>`] that the `offset` was consumed by the Consumer Group.
    pub(crate) offset_timestamp: DateTime<Utc>,

    /// Lag in consuming a specific [`TopicPartition`] as reported by the the Consumer (and in the `__consumer_offsets` internal topic).
    pub(crate) offset_lag: u64,

    /// Estimated time latency between the Consumer [`GroupWithMembers`] consuming a specific [`TopicPartition`], and the [`DateTime<Utc>`] when the high watermark (end offset) was produced.
    pub(crate) time_lag: Duration,

    pub(crate) earliest: KnownOffset,

    pub(crate) latest: KnownOffset,
}

impl Default for Lag {
    fn default() -> Self {
        Lag {
            offset: 0,
            offset_timestamp: DateTime::<Utc>::default(),
            offset_lag: 0,
            time_lag: Duration::zero(),
            earliest: KnownOffset::default(),
            latest: KnownOffset::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub(crate) struct LagWithOwner {
    lag: Option<Lag>,
    owner: Option<Member>,
}

/// Describes the "lag" (or "latency") of a specific Consumer [`GroupWithMembers`] in respect to a collection of [`TopicPartition`] that it consumes.
#[derive(Debug, Clone, Default)]
pub struct GroupWithLag {
    group: Group,
    // TODO Wrap in a `RwLock` so we can modify a specific group lag,
    //   without holding a w-lock on the whole register
    lag_by_topic_partition: HashMap<TopicPartition, LagWithOwner>,
}

#[derive(Debug)]
pub struct LagRegister {
    lag_by_group: Arc<RwLock<HashMap<String, GroupWithLag>>>,
}

impl LagRegister {
    pub fn new(
        mut cg_rx: mpsc::Receiver<ConsumerGroups>,
        mut kod_rx: mpsc::Receiver<KonsumerOffsetsData>,
        po_reg: Arc<PartitionOffsetsRegister>,
    ) -> Self {
        let lr = LagRegister {
            lag_by_group: Arc::new(RwLock::new(HashMap::default())),
        };

        let lag_by_group_clone = lr.lag_by_group.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(cg) = cg_rx.recv() => {
                        trace!("Processing {} reporting {} Groups", std::any::type_name::<ConsumerGroups>(), cg.groups.len());
                        process_consumer_groups(cg, lag_by_group_clone.clone()).await;
                    },
                    Some(kod) = kod_rx.recv() => {
                        match kod {
                            KonsumerOffsetsData::OffsetCommit(oc) => {
                                trace!("Processing {} of Group '{}' for Topic Partition '{}:{}'", std::any::type_name::<OffsetCommit>(), oc.group, oc.topic, oc.partition);
                                process_offset_commit(oc, lag_by_group_clone.clone(), po_reg.clone()).await;
                            },
                            KonsumerOffsetsData::GroupMetadata(gm) => {
                                debug!("Processing {} of Group '{}' with {} Members", std::any::type_name::<GroupMetadata>(), gm.group, gm.members.len());
                                process_group_metadata(gm, lag_by_group_clone.clone()).await;
                            }
                        }
                    },
                    else => {
                        info!("Emitters stopping: breaking (internal) loop");
                        break;
                    }
                }

                if log_enabled!(Debug) {
                    let r_guard = lag_by_group_clone.read().await;
                    for (name, gwl) in r_guard.iter() {
                        if log_enabled!(Trace) {
                            trace!("{:#?}", gwl);
                        } else {
                            debug!(
                                "Group {} has Lag info for {} partitions: {} Lags, {} Owners",
                                name,
                                gwl.lag_by_topic_partition.len(),
                                gwl.lag_by_topic_partition.iter().filter(|x| x.1.lag.is_some()).count(),
                                gwl.lag_by_topic_partition.iter().filter(|x| x.1.owner.is_some()).count(),
                            );
                        }
                    }
                }
            }
        });

        lr
    }
}

async fn process_consumer_groups(cg: ConsumerGroups, lag_register_groups: Arc<RwLock<HashMap<String, GroupWithLag>>>) {
    for (group_name, group_with_members) in cg.groups.into_iter() {
        let mut w_guard = lag_register_groups.write().await;

        // Organise all the Group Members by the TopicPartition they own
        let members_by_topic_partition = group_with_members
            .members
            .into_iter()
            .flat_map(|(_, mwa)| {
                mwa.assignment
                    .into_iter()
                    .map(|tp| (tp, mwa.member.clone()))
                    .collect::<HashMap<TopicPartition, Member>>()
            })
            .collect::<HashMap<TopicPartition, Member>>();

        // Insert or update "group name -> group with lag" map entries
        if let Entry::Vacant(e) = w_guard.entry(group_name.clone()) {
            e.insert(GroupWithLag {
                group: group_with_members.group,
                // Given this is a new Group,
                lag_by_topic_partition: members_by_topic_partition
                    .into_iter()
                    .map(|(tp, m)| {
                        (
                            tp,
                            LagWithOwner {
                                owner: Some(m),
                                ..Default::default()
                            },
                        )
                    })
                    .collect(),
            });
        } else {
            let gwl = w_guard.get_mut(&group_name).unwrap_or_else(|| {
                panic!(
                    "{} for {:#?} could not be found: this should never happen!",
                    std::any::type_name::<GroupWithLag>(),
                    group_name
                )
            });

            // Set the Group (probably unchanged)
            gwl.group = group_with_members.group;

            // Remove from map of LagWithOwner the entries with key TopicPartition not owner by any member of this group
            gwl.lag_by_topic_partition.retain(|tp, _| members_by_topic_partition.contains_key(tp));

            // Create or Update a entries `TopicPartition -> LagWithOwner`:
            // either update the owner Member of an existing one,
            // or create a new entry with no Lag set.
            for (tp, m) in members_by_topic_partition.into_iter() {
                gwl.lag_by_topic_partition.entry(tp).and_modify(|lwo| lwo.owner = Some(m.clone())).or_insert_with(
                    || LagWithOwner {
                        owner: Some(m),
                        ..Default::default()
                    },
                );
            }
        };
    }
}

async fn process_offset_commit(
    oc: OffsetCommit,
    lag_register_groups: Arc<RwLock<HashMap<String, GroupWithLag>>>,
    po_reg: Arc<PartitionOffsetsRegister>,
) {
    let mut w_guard = lag_register_groups.write().await;

    match w_guard.get_mut(&oc.group) {
        Some(gwl) => {
            let tp = TopicPartition::new(oc.topic, oc.partition as u32);

            // Prepare all the Lag fields
            let l = Lag {
                offset: oc.offset as u64,
                offset_timestamp: oc.commit_timestamp,
                offset_lag: match po_reg.estimate_offset_lag(&tp, oc.offset as u64).await {
                    Ok(ol) => ol,
                    Err(e) => {
                        error!(
                            "Failed to estimate Offset Lag of Group '{}' for Topic Partition '{}': {}",
                            oc.group, tp, e
                        );
                        0
                    },
                },
                time_lag: match po_reg.estimate_time_lag(&tp, oc.offset as u64, oc.commit_timestamp).await {
                    Ok(tl) => tl,
                    Err(e) => {
                        error!(
                            "Failed to estimate Time Lag of Group '{}' for Topic Partition '{}': {}",
                            oc.group, tp, e
                        );
                        Duration::zero()
                    },
                },
                earliest: match po_reg.get_earliest_offset(&tp).await {
                    Ok(eo) => eo,
                    Err(e) => {
                        error!("Unable to determine Earliest Offset for Topic Partition '{}': {}", tp, e);
                        Default::default()
                    },
                },
                latest: match po_reg.get_latest_offset(&tp).await {
                    Ok(eo) => eo,
                    Err(e) => {
                        error!(
                            "Failed to estimate Time Lag of Group '{}' for Topic Partition '{}': {}",
                            oc.group, tp, e
                        );
                        Default::default()
                    },
                },
            };

            // Create or update entry `TopicPartition -> LagWithOwner`:
            // either update the Lag of an existing one,
            // or create a new entry with no owner set.
            gwl.lag_by_topic_partition.entry(tp).and_modify(|lwo| lwo.lag = Some(l.clone())).or_insert_with(|| {
                LagWithOwner {
                    lag: Some(l),
                    owner: None,
                }
            });
        },
        None => {
            warn!("Received {} about unknown Group '{}': ignoring", std::any::type_name::<OffsetCommit>(), oc.group);
        },
    }
}

async fn process_group_metadata(gm: GroupMetadata, lag_register_groups: Arc<RwLock<HashMap<String, GroupWithLag>>>) {
    let mut w_guard = lag_register_groups.write().await;

    match w_guard.get_mut(&gm.group) {
        Some(gwl) => {
            // New map of Topic Partition->Member (owner), that the Group is consuming
            let new_tp_to_owner = gm
                .members
                .into_iter()
                .flat_map(|m| {
                    let owner = Member {
                        id: m.id,
                        client_id: m.client_id,
                        client_host: m.client_host,
                    };

                    // Collect all Group Coordinator Assigned Topic Partitions
                    let assignment_tps = m
                        .assignment
                        .assigned_topic_partitions
                        .into_iter()
                        .flat_map(TopicPartition::vec_from)
                        .map(|tp| (tp, owner.clone()))
                        .collect::<HashMap<TopicPartition, Member>>();

                    // Collect all Group Subscribed Topic Partitions
                    let subscription_tps = m
                        .subscription
                        .owned_topic_partitions
                        .into_iter()
                        .flat_map(TopicPartition::vec_from)
                        .map(|tp| (tp, owner.clone()))
                        .collect::<HashMap<TopicPartition, Member>>();

                    assignment_tps.into_iter().chain(subscription_tps).collect::<HashMap<TopicPartition, Member>>()
                })
                .collect::<HashMap<TopicPartition, Member>>();

            // Keep a Topic-Partition Lag for this Group, only if it was in the GroupMetadata.
            //
            // NOTE: The new ones that are NOT YET in the map, will be added when an
            // OffsetCommit for this Group and this Topic-Partition is received and Lag calculated.
            gwl.lag_by_topic_partition.retain(|tp, _| new_tp_to_owner.contains_key(tp));

            // For all the Topic-Partition in the GroupMetadata, set the Member that owns it
            for (tp, owner) in new_tp_to_owner.into_iter() {
                if let Some(lwo) = gwl.lag_by_topic_partition.get_mut(&tp) {
                    lwo.owner = Some(owner)
                }
            }
        },
        None => {
            warn!("Received {} about unknown Group '{}': ignoring", std::any::type_name::<GroupMetadata>(), gm.group);
        },
    }
}
