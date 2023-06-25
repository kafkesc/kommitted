use std::collections::{HashMap, HashSet};
use std::ops::Sub;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use konsumer_offsets::{GroupMetadata, KonsumerOffsetsData, OffsetCommit};
use log::Level::Debug;
use rdkafka::bindings::rd_kafka_destroy_flags;
use tokio::sync::{broadcast, mpsc, RwLock};

use crate::consumer_groups::ConsumerGroups;
use crate::kafka_types::{Group, TopicPartition};
use crate::partition_offsets::PartitionOffsetsRegister;

/// Describes the "lag" (or "latency"), and it's usually paired with a Consumer [`Group`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lag {
    /// Offset that a given Consumer [`Group`] is at when consuming a specific [`TopicPartition`].
    offset: u64,

    /// [`DateTime<Utc>`] that the `offset` was consumed by the Consumer Group.
    offset_timestamp: DateTime<Utc>,

    /// Lag in consuming a specific [`TopicPartition`] as reported by the the Consumer (and in the `__consumer_offsets` internal topic).
    offset_lag: u64,

    /// Estimated time latency between the Consumer [`Group`] consuming a specific [`TopicPartition`], and the [`DateTime<Utc>`] when the high watermark (end offset) was produced.
    time_lag: Duration,
}

impl Default for Lag {
    fn default() -> Self {
        Lag {
            time_lag: Duration::zero(),
            ..Default::default()
        }
    }
}

impl Lag {
    pub(crate) fn new(
        offset: u64,
        offset_timestamp: DateTime<Utc>,
        offset_lag: u64,
        time_lag: Duration,
    ) -> Self {
        Self {
            offset,
            offset_timestamp,
            offset_lag,
            time_lag,
        }
    }
}

/// Describes the "lag" (or "latency") of a specific Consumer [`Group`] in respect to a collection of [`TopicPartition`] that it consumes.
#[derive(Debug, Clone, Default)]
pub struct GroupWithLag {
    group: Group,
    lag_by_topic_partition: HashMap<TopicPartition, Lag>,
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
                                trace!("Processing {} of Group '{}' with {} Members", std::any::type_name::<GroupMetadata>(), gm.group, gm.members.len());
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
                    debug!("Known Groups: {}", r_guard.len());
                    for (name, gwl) in r_guard.iter() {
                        debug!(
                            "Group {} has Lag info for {} partitions",
                            name,
                            gwl.lag_by_topic_partition.len()
                        );
                    }
                }
            }
        });

        lr
    }
}

async fn process_consumer_groups(
    cg: ConsumerGroups,
    lag_register_groups: Arc<RwLock<HashMap<String, GroupWithLag>>>,
) {
    for (group_name, group) in cg.groups.into_iter() {
        let mut w_guard = lag_register_groups.write().await;

        // Insert or update "group name -> group with lag" map entries
        if !w_guard.contains_key(&group_name) {
            w_guard.insert(
                group_name,
                GroupWithLag {
                    group,
                    ..Default::default()
                },
            );
        } else {
            w_guard
                .get_mut(&group_name)
                .expect(format!("{} for {:#?} could not be found: this should never happen!", std::any::type_name::<GroupWithLag>(), group_name).as_str())
                .group = group;
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
            let l = Lag::new(
                oc.offset as u64,
                oc.commit_timestamp,
                match po_reg
                    .estimate_offset_lag(
                        &tp.topic,
                        tp.partition,
                        oc.offset as u64,
                    )
                    .await
                {
                    Ok(ol) => ol,
                    Err(e) => {
                        error!("Failed to estimate Offset Lag of Group '{}' for Topic Partition '{}:{}': {}", oc.group, tp.topic, tp.partition, e);
                        0
                    },
                },
                match po_reg
                    .estimate_time_lag(
                        &tp.topic,
                        tp.partition,
                        oc.offset as u64,
                        oc.commit_timestamp,
                    )
                    .await
                {
                    Ok(tl) => tl,
                    Err(e) => {
                        error!("Failed to estimate Time Lag of Group '{}' for Topic Partition '{}:{}': {}", oc.group, tp.topic, tp.partition, e);
                        Duration::zero()
                    },
                },
            );

            gwl.lag_by_topic_partition.insert(tp, l);
        },
        None => {
            warn!(
                "Received {} about unknown Group '{}': ignoring",
                std::any::type_name::<OffsetCommit>(),
                oc.group
            );
        },
    }
}

async fn process_group_metadata(
    gm: GroupMetadata,
    lag_register_groups: Arc<RwLock<HashMap<String, GroupWithLag>>>,
) {
    let mut w_guard = lag_register_groups.write().await;

    match w_guard.get_mut(&gm.group) {
        Some(gwl) => {
            // New set of Topic Partitions that Group is consuming
            let new_gtp_set = gm
                .members
                .into_iter()
                .map(|m| {
                    let mut tp_set = HashSet::with_capacity(
                        m.assignment.assigned_topic_partitions.len()
                            + m.subscription.owned_topic_partitions.len(),
                    );

                    // Collect all Group Coordinator Assigned Topic Partitions
                    for assigned_tps in m.assignment.assigned_topic_partitions {
                        for tps in assigned_tps.partitions {
                            tp_set.insert(TopicPartition::new(
                                assigned_tps.topic.to_owned(),
                                tps as u32,
                            ));
                        }
                    }

                    // Collect all Group Subscribed Topic Partitions
                    for owned_tps in m.subscription.owned_topic_partitions {
                        for tps in owned_tps.partitions {
                            tp_set.insert(TopicPartition::new(
                                owned_tps.topic.to_owned(),
                                tps as u32,
                            ));
                        }
                    }

                    tp_set
                })
                .flatten()
                .collect::<HashSet<TopicPartition>>();

            // Keep a Topic-Partition Lag for this Group, only if it was in the GroupMetadata.
            //
            // NOTE: The new ones that are NOT YET in the map, will be added when an
            // OffsetCommit for this Group and this Topic-Partition is received and Lag calculated.
            gwl.lag_by_topic_partition.retain(|tp, l| new_gtp_set.contains(tp));
        },
        None => {
            warn!(
                "Received {} about unknown Group '{}': ignoring",
                std::any::type_name::<GroupMetadata>(),
                gm.group
            );
        },
    }
}
