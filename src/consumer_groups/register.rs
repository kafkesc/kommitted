use std::hash::{Hash, Hasher};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use prometheus::{
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, IntGauge, IntGaugeVec,
    Registry,
};
use tokio::sync::{mpsc::Receiver, RwLock};

use super::emitter::ConsumerGroups;

use crate::internals::Awaitable;
use crate::kafka_types::GroupWithMembers;
use crate::prometheus_metrics::LABEL_GROUP;

const MET_TOT_NAME: &str = "consumer_groups_total";
const MET_TOT_HELP: &str = "Consumer groups currently in the cluster";
const MET_MEMBERS_TOT_NAME: &str = "consumer_groups_members_total";
const MET_MEMBERS_TOT_HELP: &str = "Members of consumer groups currently in the cluster";

/// Contains a [`GroupWithMembers`] as well as when was the last time it was "seen".
///
/// In this case "seen" means that the Kafka Cluster knew of its existence.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupWithMembersLastSeen {
    pub group_with_members: GroupWithMembers,
    pub last_seen: DateTime<Utc>,
}

impl From<GroupWithMembers> for GroupWithMembersLastSeen {
    /// Creates a [`Self`] and marks it as _last seen_ now (i.e. when method is invoked).
    fn from(group_with_members: GroupWithMembers) -> Self {
        GroupWithMembersLastSeen {
            group_with_members,
            last_seen: Utc::now(),
        }
    }
}

impl Hash for GroupWithMembersLastSeen {
    /// [`Self`] implements [`Hash`] in a bespoke way,
    /// as we need it to return the same value as the hashing of the contained [`GroupWithMembers`].
    ///
    /// This is desirable because as long as the contained [`GroupWithMembers`] doesn't change,
    /// but it's "seen" during an update cycle, we don't want to alter the hash-value because
    /// the internal `last_seen` gets updated every time.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group_with_members.hash(state);
    }
}

impl GroupWithMembersLastSeen {
    /// Returns `true` if this group is "expired".
    /// If expired, this group should be "forgotten".
    ///
    /// # Arguments
    /// * `expire_after` - Amount of time after which this group should be considered "expired".
    pub fn is_expired(&self, expire_after: &Duration) -> bool {
        Utc::now() - self.last_seen > *expire_after
    }
}

/// Registers and exposes the latest, still known [`GroupWithMembersLastSeen`].
///
/// When the register is created, it's provided a `forget_after` [`Duration`].
/// A Group that stops being reported/known by the Kafka Cluster, will be automatically
/// removed ("forgotten") by this register, after `forget_after` time.
///
/// It exposes the accessor methods via an async interface,
/// while dealing internally with concurrency and synchronization.
#[derive(Debug)]
pub struct ConsumerGroupsRegister {
    /// Map ([`HashMap`]) of [`GroupWithMembersLastSeen`], paired with its own _hash_ value.
    ///
    /// The hash is calculated every time the map is updated, using [`DefaultHasher`].
    known_groups: Arc<RwLock<(u64, HashMap<String, GroupWithMembersLastSeen>)>>,

    /// Every time a new [`ConsumerGroups`] is received by this register,
    /// [`GroupWithMembersLastSeen`] that were not "seen" for longer than this value,
    /// are removed from the `known_groups` map.
    forget_after: Duration,

    // Prometheus Metrics
    metric_tot: IntGauge,
    metric_members_tot: IntGaugeVec,
}

impl ConsumerGroupsRegister {
    pub fn new(
        mut rx: Receiver<ConsumerGroups>,
        forget_after: Duration,
        metrics: Arc<Registry>,
    ) -> Self {
        let cgr = Self {
            known_groups: Arc::new(RwLock::new((0u64, HashMap::new()))),
            forget_after,
            metric_tot: register_int_gauge_with_registry!(MET_TOT_NAME, MET_TOT_HELP, metrics)
                .unwrap_or_else(|e| panic!("Failed to create metric '{MET_TOT_NAME}': {e}")),
            metric_members_tot: register_int_gauge_vec_with_registry!(
                MET_MEMBERS_TOT_NAME,
                MET_MEMBERS_TOT_HELP,
                &[LABEL_GROUP],
                metrics
            )
            .unwrap_or_else(|e| panic!("Failed to create metric '{MET_MEMBERS_TOT_NAME}': {e}")),
        };

        // A clone of the `cgr.known_groups` will be moved into the async task
        // that updates the register.
        let known_groups_arc_clone = cgr.known_groups.clone();

        // Clone metrics so they can be used in the spawned future
        let metric_tot = cgr.metric_tot.clone();
        let metric_members_tot = cgr.metric_members_tot.clone();

        // The Register is essentially "self updating" its data, by listening
        // on a channel for updates.
        //
        // The internal async task will terminate when the internal loop breaks:
        // that will happen when the `Receiver` `rx` receives `None`.
        // And, in turn, that will happen when the `Sender` part of the channel is dropped.
        tokio::spawn(async move {
            debug!("Begin receiving ConsumerGroups updates");

            loop {
                tokio::select! {
                    Some(cg) = rx.recv() => {
                        trace!("Received:\n{:#?}", cg);

                        // Update total metric
                        metric_tot.set(cg.groups.len() as i64);

                        // Update the `known_groups` and it's hash
                        let mut w_guard = known_groups_arc_clone.write().await;
                        for (g_name, g_members) in cg.groups.into_iter() {
                            // Update group-specific metric
                            metric_members_tot.with_label_values(&[&g_name]).set(g_members.members.len() as i64);

                            // Upserting the map entry
                            w_guard.1.insert(g_name, g_members.into());

                            // Remove all the Groups that have expired
                            // (i.e. excited the expected "forget after" duration)
                            w_guard.1.retain(|_, g| !g.is_expired(&cgr.forget_after));

                            // Calculate and update the hash
                            let mut h = DefaultHasher::new();
                            for (g, gm) in &w_guard.1 {
                                g.hash(&mut h);
                                gm.hash(&mut h);
                            }
                            w_guard.0 = h.finish();
                        }
                    },
                    else => {
                        info!("Emitters stopping: breaking (internal) loop");
                        break;
                    },
                }

                info!(
                    "Updated (Known) Consumer Groups: {}",
                    known_groups_arc_clone.read().await.1.len()
                );
            }
        });

        cgr
    }

    /// Returns [`Vec<String>`] with all the known Consumer Group identifiers.
    pub async fn get_groups(&self) -> Vec<String> {
        self.known_groups.read().await.1.keys().cloned().collect()
    }

    /// Returns a specific [`GroupWithMembers`], if found.
    ///
    /// # Arguments
    /// * `group` - Consumer Group identifier (name)
    pub async fn get_group(&self, group: &str) -> Option<GroupWithMembers> {
        self.known_groups.read().await.1.get(group).map(|g| g.group_with_members.clone())
    }

    /// Returns a `u64` value representing the [`Hash`] of the current [`GroupWithMembers`] held in this register.
    pub async fn get_hash(&self) -> u64 {
        self.known_groups.read().await.0
    }
}

impl Awaitable for ConsumerGroupsRegister {
    /// [`Self`] ready when it's not empty.
    async fn is_ready(&self) -> bool {
        !self.known_groups.read().await.1.is_empty()
    }
}

#[cfg(test)]
mod test {
    // TODO...
}
