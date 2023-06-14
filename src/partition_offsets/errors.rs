use chrono::{DateTime, Utc};
use thiserror::Error;

/// Possible errors from the [`super::lag_estimator`] module.
#[derive(Error, Debug, Eq, PartialEq)]
pub enum PartitionOffsetsError {
    /// [`super::lag_estimator::PartitionLagEstimator`] has not received enough data points yet.
    /// It's not ready to estimate lag.
    #[error("Lag Estimator has not received enough data points yet")]
    LagEstimatorNotReady,

    /// Can't find the [`super::lag_estimator::PartitionLagEstimator`] that corresponds to the given Topic and Partition.
    #[error("Lag Estimator for '{0}:{1}' not found")]
    LagEstimatorNotFound(String, u32),

    /// Commit [`Datetime`] for a Consumed Offset is ahead of its Produced Offset
    #[error("Commit Datetime Consumed Offset '{0}' is ahead of its Produced Offset '{1}'")]
    ConsumedAheadOfProducedOffsetDatetime(DateTime<Utc>, DateTime<Utc>),

    /// UTC Timestamp milliseconds is not a valid amount
    #[error("UTC Timestamp milliseconds is not valid: {0}")]
    UtcTimestampMillisInvalid(i64),
}

pub type PartitionOffsetsResult<T> = Result<T, PartitionOffsetsError>;
