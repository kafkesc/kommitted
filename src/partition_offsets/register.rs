use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

use super::emitter::PartitionOffset;
use super::errors::{PartitionOffsetsError, PartitionOffsetsResult};
use super::lag_estimator::PartitionLagEstimator;

use crate::kafka_types::TopicPartition;

/// A [`PartitionLagEstimator`], wrapped in a [`RwLock`] for cross thread concurrency.
type PLEVal = RwLock<PartitionLagEstimator>;

/// Holds the offset of all Topic Partitions in the Kafka Cluster, and can estimate lag of Consumers.
///
/// This is where a known Consumer Group, at a known offset in time, can get it's lag estimated.
pub struct PartitionOffsetsRegister {
    estimators: Arc<RwLock<HashMap<TopicPartition, PLEVal>>>,
}

impl PartitionOffsetsRegister {
    /// Create a new [`Self`], able to hold the given offset history.
    ///
    /// # Arguments
    ///
    /// * `rx` - Channel [`Receiver`] for [`PartitionOffset`]
    /// * `offsets_history` - For each Topic Partition, how much offset history to hold.
    ///   History for each (`Topic, Partition`) pair is kept in a queue-like structure of this
    ///   size. Each entry in the structure is the pair (`Offset, UTC TS`): each pair represents
    ///   at what moment in time that particular offset was valid.
    pub fn new(mut rx: Receiver<PartitionOffset>, offsets_history: usize) -> Self {
        let por = Self {
            estimators: Arc::new(RwLock::new(HashMap::new())),
        };

        // A clone of the `por.estimator` will be moved into the async task
        // that updates the register.
        let estimators_clone = por.estimators.clone();

        // The Register is essentially "self updating" its data, by listening
        // on a channel for updates.
        //
        // The internal async task will terminate when the internal loop breaks:
        // that will happen when the `Receiver` `rx` receives `None`.
        // And, in turn, that will happen when the `Sender` part of the channel is dropped.
        tokio::spawn(async move {
            debug!("Begin receiving PartitionOffset updates");

            loop {
                tokio::select! {
                    Some(po) = rx.recv() => {
                        let k = TopicPartition{
                            topic: po.topic,
                            partition: po.partition,
                        };

                        // First, check if we need to create the estimator for this Key
                        let mut w_guard = estimators_clone.write().await;
                        if !w_guard.contains_key(&k) {
                            w_guard.insert(
                                k.clone(),
                                RwLock::new(PartitionLagEstimator::new(
                                    offsets_history,
                                )),
                            );
                        }

                        trace!("Updating Partition: {:?}", k);
                        // Second, update the PartitionLagEstimator for this Key
                        w_guard
                            .downgrade() //< Here the exclusive write lock, becomes a read lock
                            .get(&k)
                            .unwrap_or_else(|| panic!("{} for {:#?} could not be found: this should never happen!", std::any::type_name::<PartitionLagEstimator>(), k))
                            .write()
                            .await
                            .update(po.latest_offset, po.read_datetime);
                    },
                    else => {
                        info!("Emitters stopping: breaking (internal) loop");
                        break;
                    }
                }
            }
        });

        por
    }
}

impl PartitionOffsetsRegister {
    /// Estimate offset lag for consumer of specific `topic`, `partition`, given it's current `consumed_offset`.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic consumed by the Consumer
    /// * `partition` - Partition of the Topic consumed by the Consumer
    /// * `consumed_offset` - Offset up to which the Consumer has consumed
    pub async fn estimate_offset_lag(
        &self,
        topic: &str,
        partition: u32,
        consumed_offset: u64,
    ) -> PartitionOffsetsResult<u64> {
        let k = TopicPartition {
            topic: topic.to_string(),
            partition,
        };

        self.estimators
            .read()
            .await
            .get(&k)
            .ok_or(PartitionOffsetsError::LagEstimatorNotFound(topic.to_string(), partition))?
            .read()
            .await
            .estimate_offset_lag(consumed_offset)
    }

    /// Estimate time lag for consumer of specific `topic`, `partition`, given it's current `consumed_offset` and `consumed_offset_datetime`.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic consumed by the Consumer
    /// * `partition` - Partition of the Topic consumed by the Consumer
    /// * `consumed_offset` - Offset up to which the Consumer has consumed
    /// * `consumed_offset_datetime` - [`Datetime<Utc>`] when the `consumed_offset` was committed
    pub async fn estimate_time_lag(
        &self,
        topic: &str,
        partition: u32,
        consumed_offset: u64,
        consumed_offset_datetime: DateTime<Utc>,
    ) -> PartitionOffsetsResult<Duration> {
        let k = TopicPartition {
            topic: topic.to_string(),
            partition,
        };

        self.estimators
            .read()
            .await
            .get(&k)
            .ok_or(PartitionOffsetsError::LagEstimatorNotFound(topic.to_string(), partition))?
            .read()
            .await
            .estimate_time_lag(consumed_offset, consumed_offset_datetime)
    }

    /// Checks if a `topic` + `partition` pair is present.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic
    /// * `partition` - Partition pf the Topic
    pub async fn contains_topic_partition(&self, topic: &str, partition: u32) -> bool {
        let k = TopicPartition {
            topic: topic.to_string(),
            partition,
        };

        self.estimators.read().await.contains_key(&k)
    }

    /// Get all the `(topic, partition)` tuples it contains.
    pub async fn get_topic_partitions(&self) -> HashSet<TopicPartition> {
        HashSet::from_iter(self.estimators.read().await.keys().cloned())
    }
}
