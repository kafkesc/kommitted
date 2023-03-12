use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

use super::emitter::PartitionOffset;
use super::errors::{PartitionOffsetsError, PartitionOffsetsResult};
use super::lag_estimator::PartitionLagEstimator;
use crate::internals::Register;

/// (Topic, Partition)
type Key = (String, u32);

/// [`PartitionLagEstimator`], wrapped in a [`RwLock`] for cross thread concurrency.
type Val = RwLock<PartitionLagEstimator>;

/// Holds the offset of all Topic Partitions in the Kafka Cluster, and can estimate lag of Consumers.
pub struct PartitionOffsetsRegister {
    estimators: Arc<RwLock<HashMap<Key, Val>>>,
}

impl Register for PartitionOffsetsRegister {
    type Registered = PartitionOffset;

    fn new(mut rx: Receiver<Self::Registered>) -> Self {
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

            while let Some(po) = rx.recv().await {
                let k: Key = (po.topic, po.partition);

                // First, check if we need to create the estimator for this Key
                let mut guard = estimators_clone.write().await;
                if !guard.contains_key(&k) {
                    guard.insert(
                        k.clone(),
                        RwLock::new(PartitionLagEstimator::new(1000)),
                    );
                }

                trace!("Updating Partition: {:?}", k);
                // Second, update the PartitionLagEstimator for this Key
                guard
                    .downgrade() //< Here the exclusive write lock, becomes a read lock
                    .get(&k)
                    .unwrap_or_else(|| panic!("PartitionLagEstimator for {:#?} could not be found: this should never happen!", k))
                    .write()
                    .await
                    .update(po.latest_offset, po.read_datetime);
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
        let k: Key = (topic.to_string(), partition);

        self.estimators
            .read()
            .await
            .get(&k)
            .ok_or(PartitionOffsetsError::LagEstimatorNotFound(
                topic.to_string(),
                partition,
            ))?
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
        let k: Key = (topic.to_string(), partition);

        self.estimators
            .read()
            .await
            .get(&k)
            .ok_or(PartitionOffsetsError::LagEstimatorNotFound(
                topic.to_string(),
                partition,
            ))?
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
    pub async fn contains_topic_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> bool {
        let k: Key = (topic.to_string(), partition);

        self.estimators.read().await.contains_key(&k)
    }

    /// Get all the `(topic, partition)` tuples it contains.
    pub async fn get_topic_partitions(&self) -> HashSet<Key> {
        HashSet::from_iter(self.estimators.read().await.keys().cloned())
    }
}
