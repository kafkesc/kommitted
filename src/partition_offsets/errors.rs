use thiserror::Error;

use chrono::{DateTime, Utc};

/// Possible errors from the [`super::lag_estimator`] module.
#[derive(Error, Debug, Eq, PartialEq)]
pub enum PartitionOffsetsError {
    /// [`super::lag_estimator::PartitionLagEstimator`] has not received enough data points yet.
    /// It's not ready to estimate lag.
    #[error("Lag Estimator has not received enough data points yet")]
    LagEstimatorNotReady,

    /// [`Datetime`] for a Consumed Offset is ahead of the one of a Produced Offset
    #[error("Datetime Consumed Offset '{0}' is ahead of Produced Offset '{1}'")]
    ConsumedAheadOfProducedOffsetDatetime(DateTime<Utc>, DateTime<Utc>),

    /// UTC Timestamp milliseconds is not a valid amount
    #[error("UTC Timestamp milliseconds is not valid: {0}")]
    UtcTimestampMillisInvalid(i64),
}
