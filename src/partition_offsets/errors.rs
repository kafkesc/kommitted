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

    /// UTC Timestamp milliseconds is not a valid amount
    #[error("UTC Timestamp milliseconds is not valid: {0}")]
    UtcTimestampMillisInvalid(i64),
}

pub type PartitionOffsetsResult<T> = Result<T, PartitionOffsetsError>;
