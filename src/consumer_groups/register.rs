use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use prometheus::{
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, IntGauge, IntGaugeVec,
    Registry,
};
use tokio::{
    sync::{mpsc::Receiver, RwLock, RwLockWriteGuard},
    time::interval,
};

use super::emitter::ConsumerGroups;

use crate::internals::Awaitable;
use crate::kafka_types::GroupWithMembers;
use crate::prometheus_metrics::LABEL_GROUP;

const REMOVE_EXPIRED_INTERVAL: Duration = Duration::from_secs(1);

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
        match chrono::Duration::from_std(*expire_after) {
            Ok(expire_after_chrono) => {
                let elapsed_ms = (Utc::now() - self.last_seen).num_milliseconds();
                let expire_after_ms = expire_after_chrono.num_milliseconds();
                elapsed_ms > expire_after_ms
            },
            Err(e) => {
                warn!("Unable to convert Duration {:?} from std to chrono: {}", expire_after, e);
                false
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConsumerGroupsMap {
    hash: u64,
    map: HashMap<String, GroupWithMembersLastSeen>,
}

impl Default for ConsumerGroupsMap {
    fn default() -> Self {
        Self {
            hash: DefaultHasher::new().finish(),
            map: HashMap::new(),
        }
    }
}

impl Hash for ConsumerGroupsMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
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
    known_groups: Arc<RwLock<ConsumerGroupsMap>>,

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
            known_groups: Arc::new(RwLock::new(ConsumerGroupsMap::default())),
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

            let mut remove_expired_timeout = interval(REMOVE_EXPIRED_INTERVAL);

            loop {
                tokio::select! {
                    Some(cg) = rx.recv() => {
                        trace!("Received:\n{:#?}", cg);

                        // Update total metric
                        metric_tot.set(cg.groups.len() as i64);

                        // Upsert map entries
                        let mut w_guard = known_groups_arc_clone.write().await;
                        for (g_name, g_members) in cg.groups.into_iter() {
                            // Update group-specific metric
                            metric_members_tot.with_label_values(&[&g_name]).set(g_members.members.len() as i64);

                            w_guard.map.insert(g_name, g_members.into());
                        }

                        remove_expired_and_update_hash(w_guard, &cgr.forget_after);
                    },
                    _ = remove_expired_timeout.tick() => {
                        remove_expired_and_update_hash(known_groups_arc_clone.write().await, &cgr.forget_after);
                    },
                    else => {
                        info!("Emitters stopping: breaking (internal) loop");
                        break;
                    },
                }

                let r_guard = known_groups_arc_clone.read().await;
                trace!(
                    "Updated (Known) Consumer Groups: {} (hash: {})",
                    r_guard.map.len(),
                    r_guard.hash,
                );
            }
        });

        cgr
    }

    /// Returns [`Vec<String>`] with all the known Consumer Group identifiers.
    pub async fn get_groups(&self) -> Vec<String> {
        self.known_groups.read().await.map.keys().cloned().collect()
    }

    /// Returns count of known Consumer Groups.
    #[allow(dead_code)]
    pub async fn get_groups_count(&self) -> usize {
        self.known_groups.read().await.map.len()
    }

    /// Returns a specific [`GroupWithMembers`], if found.
    ///
    /// # Arguments
    /// * `group` - Consumer Group identifier (name)
    pub async fn get_group(&self, group: &str) -> Option<GroupWithMembers> {
        self.known_groups.read().await.map.get(group).map(|g| g.group_with_members.clone())
    }

    /// Returns `true` if [`Self`] contains a specific Consumer Group.
    ///
    /// # Arguments
    /// * `group` - Consumer Group identifier (name)
    #[allow(dead_code)]
    pub async fn contains_group(&self, group: &str) -> bool {
        self.known_groups.read().await.map.contains_key(group)
    }

    /// Returns a `u64` value representing the [`Hash`] of the current [`GroupWithMembers`] held in this register.
    pub async fn get_hash(&self) -> u64 {
        self.known_groups.read().await.hash
    }
}

fn remove_expired_and_update_hash(
    mut w_guard: RwLockWriteGuard<ConsumerGroupsMap>,
    forget_after: &Duration,
) {
    // Remove all the Groups that have expired
    // (i.e. exceeded the expected "forget after" duration)
    w_guard.map.retain(|_, g| !g.is_expired(forget_after));

    // Get group names sorted
    let mut sorted_groups = w_guard.map.keys().by_ref().collect::<Vec<&String>>();
    sorted_groups.sort();

    // Calculate the new hash, minding to keep the order of the groups stable
    let mut hasher = DefaultHasher::new();
    for g in sorted_groups {
        match w_guard.map.get_key_value(g) {
            Some((g, gm)) => {
                g.hash(&mut hasher);
                gm.hash(&mut hasher);
            },
            None => {
                panic!("Group {} is missing: this should never happen", g);
            },
        }
    }
    w_guard.hash = hasher.finish();
}

impl Awaitable for ConsumerGroupsRegister {
    /// [`Self`] ready when it's not empty.
    async fn is_ready(&self) -> bool {
        !self.known_groups.read().await.map.is_empty()
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        future::Future,
        sync::Arc,
    };

    use prometheus::Registry;
    use tokio::{
        sync::mpsc,
        time::{sleep, Duration, Instant},
    };

    use crate::consumer_groups::{emitter::ConsumerGroups, ConsumerGroupsRegister};
    use crate::kafka_types::{Group, GroupWithMembers, Member, MemberWithAssignment};

    fn make_group_with_members(
        g_name: &str,
        m_prefix: &str,
        count: usize,
    ) -> (String, GroupWithMembers) {
        let mut gwm = GroupWithMembers {
            group: Group {
                name: format!("{}", g_name),
                state: format!("{}-state", g_name),
                protocol: format!("{}-protocol", g_name),
                protocol_type: format!("{}-protocol_type", g_name),
            },
            members: HashMap::with_capacity(count),
        };

        for i in 0..count {
            let m_id = format!("{}{}{}", g_name, m_prefix, i);
            gwm.members.insert(
                m_id.clone(),
                MemberWithAssignment {
                    member: Member {
                        client_id: format!("{}-client_id", m_id),
                        client_host: format!("{}-client_host", m_id),
                        id: m_id,
                    },
                    assignment: HashSet::new(),
                },
            );
        }

        (g_name.to_string(), gwm)
    }

    const BLOCK_ON_CONDITION_CHECK_FREQ: Duration = Duration::from_millis(10);

    async fn block_on<F, Fut>(f: F, timeout: Duration)
    where
        F: Fn() -> Fut,
        Fut: Future<Output = bool>,
    {
        let start = Instant::now();
        loop {
            if f().await {
                break;
            }

            assert!(
                Instant::now().duration_since(start) < timeout,
                "Timed out waiting on desired condition"
            );
            sleep(BLOCK_ON_CONDITION_CHECK_FREQ).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_forget_after() {
        let (sx, rx) = mpsc::channel::<ConsumerGroups>(10);
        let forget_after = Duration::from_secs(3);
        let metrics_reg_arc = Arc::new(Registry::new());

        let reg_under_test = ConsumerGroupsRegister::new(rx, forget_after, metrics_reg_arc);

        // Start with 3 groups
        sx.send(ConsumerGroups {
            groups: HashMap::from([
                make_group_with_members("g1", "m", 10),
                make_group_with_members("g2", "m", 5),
                make_group_with_members("g3", "m", 2),
            ]),
        })
        .await
        .unwrap();

        // Wait for 3 groups to be visible when querying Register
        block_on(|| async { reg_under_test.get_groups_count().await == 3 }, Duration::from_secs(5))
            .await;
        assert!(reg_under_test.contains_group("g1").await);
        assert!(reg_under_test.contains_group("g2").await);
        assert!(reg_under_test.contains_group("g3").await);

        // A new group is detected
        sx.send(ConsumerGroups {
            groups: HashMap::from([
                make_group_with_members("g1", "m", 10),
                make_group_with_members("g2", "m", 5),
                make_group_with_members("g3", "m", 2),
                make_group_with_members("g4", "m", 15),
            ]),
        })
        .await
        .unwrap();

        // Confirm new group is visible
        block_on(|| async { reg_under_test.get_groups_count().await == 4 }, Duration::from_secs(5))
            .await;
        assert!(reg_under_test.contains_group("g4").await);

        // Wait 2 seconds, then remove 2 groups
        sleep(Duration::from_secs(2)).await;
        sx.send(ConsumerGroups {
            groups: HashMap::from([
                make_group_with_members("g1", "m", 10),
                make_group_with_members("g3", "m", 2),
            ]),
        })
        .await
        .unwrap();

        // Confirm all 4 groups are still visible, for a little while longer
        block_on(|| async { reg_under_test.get_groups_count().await == 4 }, Duration::from_secs(5))
            .await;
        assert!(reg_under_test.contains_group("g1").await);
        assert!(reg_under_test.contains_group("g2").await);
        assert!(reg_under_test.contains_group("g3").await);
        assert!(reg_under_test.contains_group("g4").await);

        // Confirm that, 2 groups removed earlier are now finally gone
        block_on(
            || async { reg_under_test.get_groups_count().await == 2 },
            Duration::from_secs(20),
        )
        .await;
        assert!(reg_under_test.contains_group("g1").await);
        assert!(reg_under_test.contains_group("g3").await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_track_hash() {
        let (sx, rx) = mpsc::channel::<ConsumerGroups>(10);
        let forget_after = Duration::from_secs(3);
        let metrics_reg_arc = Arc::new(Registry::new());

        let reg = ConsumerGroupsRegister::new(rx, forget_after, metrics_reg_arc);

        // Start with no groups and grab the "empty" hash
        sx.send(ConsumerGroups {
            groups: HashMap::new(),
        })
        .await
        .unwrap();
        let initial_hash = reg.get_hash().await;
        let mut curr_hash = initial_hash;

        // Start with 1 group
        sx.send(ConsumerGroups {
            groups: HashMap::from([make_group_with_members("g1", "m", 10)]),
        })
        .await
        .unwrap();
        block_on(|| async { reg.get_groups_count().await == 1 }, Duration::from_secs(5)).await;
        let mut new_hash = reg.get_hash().await;
        assert_ne!(curr_hash, new_hash);
        curr_hash = new_hash;

        // Unchanged
        sx.send(ConsumerGroups {
            groups: HashMap::from([make_group_with_members("g1", "m", 10)]),
        })
        .await
        .unwrap();
        block_on(|| async { reg.get_groups_count().await == 1 }, Duration::from_secs(5)).await;
        new_hash = reg.get_hash().await;
        assert_eq!(curr_hash, new_hash);

        // Add a group
        sx.send(ConsumerGroups {
            groups: HashMap::from([
                make_group_with_members("g1", "m", 10),
                make_group_with_members("g2", "m", 20),
            ]),
        })
        .await
        .unwrap();
        block_on(|| async { reg.get_groups_count().await == 2 }, Duration::from_secs(5)).await;
        new_hash = reg.get_hash().await;
        assert_ne!(curr_hash, new_hash);
        curr_hash = new_hash;

        // Wait 2 seconds, then remove 1 groups
        sleep(Duration::from_secs(2)).await;
        sx.send(ConsumerGroups {
            groups: HashMap::from([make_group_with_members("g2", "m", 20)]),
        })
        .await
        .unwrap();

        // Confirm 2 groups are still visible, for a little while longer, and hash hasn't yet changed
        block_on(|| async { reg.get_groups_count().await == 2 }, Duration::from_secs(5)).await;
        new_hash = reg.get_hash().await;
        assert_eq!(curr_hash, new_hash);

        // Confirm that, eventually, only 1 group is left and hash gets updated
        block_on(|| async { reg.get_groups_count().await == 1 }, Duration::from_secs(20)).await;
        new_hash = reg.get_hash().await;
        assert_ne!(curr_hash, new_hash);
        curr_hash = new_hash;

        // Confirm that, eventually, all groups are removed and the hash goes back to the "empty" one
        block_on(|| async { reg.get_groups_count().await == 0 }, Duration::from_secs(20)).await;
        new_hash = reg.get_hash().await;
        assert_ne!(curr_hash, new_hash);
        assert_eq!(new_hash, initial_hash);
    }
}
