use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use std::collections::VecDeque;

use super::errors::{PartitionOffsetsError, PartitionOffsetsResult};
use super::known_offset::KnownOffset;
use super::known_offset::{search, KnownOffsetSearchRes};

/// Estimates lag for a given Topic Partition.
///
/// Bare in mind: this only contains the offset data of the partition,
/// but no reference to a topic partition.
/// The idea is to pair this with a specific Topic Partition.
///
/// It has to be [`Self::update`]-ed regularly with the latest begin/end offset and date-time
/// of when that information was read.
///
/// It can then be queried to estimate _lag_ and _time lag_, by providing information about
/// the committed offset of a specific consumer group of this Topic Partition.
pub struct PartitionLagEstimator {
    known: VecDeque<KnownOffset>,
}

impl PartitionLagEstimator {
    /// Create new [`PartitionLagEstimator`] of given capacity for [`KnownOffset`]s.
    ///
    /// As a rule of thumb, we allocate enough to fit 1 call to [`update`] per second:
    /// every second a new `latest_offset` would be added to the estimator.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The amount of data points (i.e. history of known offsets) we want to keep
    pub fn new(capacity: usize) -> PartitionLagEstimator {
        PartitionLagEstimator {
            known: VecDeque::with_capacity(capacity),
        }
    }

    /// Update estimator with a new data point: `new_latest` offset and related `new_latest_datetime`.
    ///
    /// It will automatically remove the oldest known offset, if the internal collection
    /// has reached capacity (decided at creation time).
    ///
    /// NOTE: This will ignore any `new_latest` offset data point,
    /// that is in the past or already known.
    ///
    /// # Arguments
    ///
    /// * `new_latest` - A new offset that should become the new latest known
    /// * `new_latest_datetime` - The [`DateTime<Utc>`] of the new offset
    pub fn update(
        &mut self,
        new_latest: u64,
        new_latest_datetime: DateTime<Utc>,
    ) {
        // Validate the input, comparing to the latest known offset
        if let Some(curr_latest) = self.known.back() {
            if curr_latest.offset == new_latest {
                // Ignore update if we already know this offset
                trace!(
                    "Update with offset {} already known: ignoring",
                    curr_latest.offset
                );
                return;
            } else if curr_latest.offset > new_latest {
                // Unlikely scenario: ignore update if the offset precedes latest known
                warn!("Update with offset {} that precedes current latest {}: ignoring",
                    new_latest,
                    curr_latest.offset
                );
                return;
            } else if curr_latest.offset < new_latest
                && curr_latest.at > new_latest_datetime
            {
                // Very unlikely scenario: ignore update if offset date-time precedes latest known
                warn!("Update with offset {} of date-time '{}' that precedes current latest {} of '{}': ignoring",
                    new_latest,
                    new_latest_datetime,
                    curr_latest.offset,
                    curr_latest.at
                );
                return;
            }
        }

        // If we have no more spare capacity, drop the front instead of letting capacity grow
        if self.spare_capacity() == 0 {
            self.known.pop_front();
        }

        // Append to the back
        self.known.push_back(KnownOffset {
            offset: new_latest,
            at: new_latest_datetime,
        });

        // Ensure it's a contiguous slice, so that we can do binary search on it
        // when estimating.
        self.known.make_contiguous();
    }

    /// Estimate offset lag.
    ///
    /// Compares the given consumer group offset for this partition, with the last produced offset.
    ///
    /// Note, as this is an estimation, if the given `consumed_offset` is greater than the latest
    /// end offset the estimator knows about, it will return `0`: it means that the consumer is well
    /// up to speed, and so it's a matter of eventual consistency that the estimator has currently
    /// got stale data.
    ///
    /// # Arguments
    ///
    /// * `offset` - Given offset we want to compare against the latest known offset
    pub fn estimate_offset_lag(
        &self,
        offset: u64,
    ) -> PartitionOffsetsResult<u64> {
        let known_latest_offset = self
            .known
            .back()
            .ok_or(PartitionOffsetsError::LagEstimatorNotReady)?
            .offset;

        // It's rare, but if we happen to receive a consumed offset that is ahead of the last
        // known end offset, we can just return `0` for lag.
        if offset > known_latest_offset {
            Ok(0)
        } else {
            Ok(known_latest_offset - offset)
        }
    }

    /// Estimate time lag.
    ///
    /// Extrapolates the given consumer group offset and related read date time for this partition,
    /// a returns a [`Duration`] estimation of the time lag accumulated by the consumer group.
    ///
    /// This estimation is done by a linear interpolation/extrapolation, where the fixed points
    /// are the [`KnownOffset`]s contained in the [`PartitionLagEstimator`] at the time of call.
    ///
    /// # Arguments
    ///
    /// * `offset` - Given offset we want to compare against the latest known offset
    /// * `offset_datetime` - The [`DateTime<Utc>`] this offset was committed
    pub fn estimate_time_lag(
        &self,
        offset: u64,
        offset_datetime: DateTime<Utc>,
    ) -> PartitionOffsetsResult<Duration> {
        // NOTE: Please look up `VecDequeue::make_contiguous()` that we call every time we update
        // the internal collection, for this to make sense.
        //
        // At this stage we need slice-type access to the content for search, and because internally
        // a `VecDequeue` is a ring-buffer, calls to `make_contiguous()` ensure that we get all
        // the content in a single borrowed slice when we get here.
        let (slice, _) = self.known.as_slices();

        let search_res = search(offset, slice);

        let estimated_produced_offset_datetime = match search_res {
            KnownOffsetSearchRes::Exact(found) => found.at,
            KnownOffsetSearchRes::Range(known_before, known_after) => {
                interpolate_offset_to_datetime(
                    &known_before,
                    &known_after,
                    offset,
                )?
            },
            KnownOffsetSearchRes::None => {
                let earliest_known = self
                    .known
                    .front()
                    .ok_or(PartitionOffsetsError::LagEstimatorNotReady)?;
                let latest_known = self
                    .known
                    .back()
                    .ok_or(PartitionOffsetsError::LagEstimatorNotReady)?;
                interpolate_offset_to_datetime(
                    earliest_known,
                    latest_known,
                    offset,
                )?
            },
        };

        // It's rare, but if we happen to receive a consumed offset datetime that is behind
        // of the estimated end offset datetime, we produce an error: it's simply not possible
        // for an offset to be consumed before it's produced.
        if offset_datetime < estimated_produced_offset_datetime {
            Err(PartitionOffsetsError::ConsumedAheadOfProducedOffsetDatetime(
                offset_datetime,
                estimated_produced_offset_datetime,
            ))
        } else {
            Ok(offset_datetime - estimated_produced_offset_datetime)
        }
    }

    /// Given the constructor-time `capacity`, how much capacity is left spare, before
    /// a new [`PartitionLagEstimator::update()`] call will need to drop the earliest known?
    pub fn spare_capacity(&self) -> usize {
        self.known.capacity() - self.known.len()
    }
}

/// Interpolate [`KnownOffset`]s and Kafka Topic Partition offset, to get a [`DateTime<Utc>`].
///
/// The "cartesian plan" can imagined with _time_ on the _x-axis_, and _offset_ on the _y-axis_.
///
/// # Arguments
///
/// * `p1` - First point for the linear interpolation
/// * `p2` - Second point for the linear interpolation
/// * `y_offset` - The _y_ offset coordinate we want to find the _x_ [`DateTime<Utc>`] coordinate of.
fn interpolate_offset_to_datetime(
    p1: &KnownOffset,
    p2: &KnownOffset,
    y_offset: u64,
) -> PartitionOffsetsResult<DateTime<Utc>> {
    // Formula:
    //   y = m * x + c

    let x1 = p1.at.timestamp_millis() as f64;
    let y1 = p1.offset as f64;
    let x2 = p2.at.timestamp_millis() as f64;
    let y2 = p2.offset as f64;
    let y_offset = y_offset as f64;

    // Find slope `m`:
    //   m = (y2 - y1) / (x2 - x1)
    let m = (y2 - y1) / (x2 - x1);

    // Find y-intercept `c` using `p1` (could use `p2` as well):
    //   c = y1 - (m * x1)
    let c = y1 - (m * x1);

    // Find `x_timestamp` (milliseconds) for `y_offset`
    //   x = (y - c) / m
    let x_timestamp = (y_offset - c) / m;

    utc_from_ms(x_timestamp.round() as i64)
}

/// Create a [`DateTime<Utc>`] from an amount of milliseconds since UTC Epoch.
///
/// # Arguments
///
/// * `utc_timestamp_ms` - Amount of milliseconds since UTC Epoch.
fn utc_from_ms(utc_timestamp_ms: i64) -> PartitionOffsetsResult<DateTime<Utc>> {
    Ok(DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp_millis(utc_timestamp_ms).ok_or(
            PartitionOffsetsError::UtcTimestampMillisInvalid(utc_timestamp_ms),
        )?,
        Utc,
    ))
}

#[cfg(test)]
mod test {
    use crate::partition_offsets::known_offset::KnownOffset;
    use crate::partition_offsets::lag_estimator::{
        interpolate_offset_to_datetime, utc_from_ms, PartitionLagEstimator,
    };
    use chrono::Duration;

    fn example_known_offsets() -> (Vec<u64>, Vec<i64>) {
        (
            vec![1346, 1500, 1700, 1893, 2001, 2091, 2341, 2559],
            vec![
                1677706286068,
                1677706438418,
                1677706636274,
                1677706827206,
                1677706934048,
                1677707023084,
                1677707270404,
                1677707486068,
            ],
        )
    }

    #[test]
    fn test_interpolate_offset_to_datetime() {
        let (off, ts) = example_known_offsets();

        let p1 = KnownOffset {
            offset: off[0],
            at: utc_from_ms(ts[0]).unwrap(),
        };

        let p2 = KnownOffset {
            offset: off[7],
            at: utc_from_ms(ts[7]).unwrap(),
        };

        let mut prev = off[0];
        let mut curr;
        for (idx, offset) in off[1..].iter().enumerate() {
            curr = interpolate_offset_to_datetime(&p1, &p2, *offset)
                .unwrap()
                .timestamp_millis();

            assert!(prev < curr as u64);
            assert_eq!(ts[idx + 1], curr);

            prev = curr as u64;
        }
    }

    #[test]
    fn estimate_offset_lag() {
        let (off, ts) = example_known_offsets();

        // Setup estimator with example input
        let mut estimator = PartitionLagEstimator::new(100);
        for (idx, offset) in off.iter().enumerate() {
            estimator.update(*offset, utc_from_ms(ts[idx]).unwrap());
        }

        let last_off = off.last().unwrap();
        assert_eq!(estimator.estimate_offset_lag(800), Ok(last_off - 800));
        assert_eq!(estimator.estimate_offset_lag(1200), Ok(last_off - 1200));
        assert_eq!(estimator.estimate_offset_lag(1346), Ok(last_off - 1346));
        assert_eq!(estimator.estimate_offset_lag(1400), Ok(last_off - 1400));
        assert_eq!(estimator.estimate_offset_lag(1600), Ok(last_off - 1600));
        assert_eq!(estimator.estimate_offset_lag(1689), Ok(last_off - 1689));
        assert_eq!(estimator.estimate_offset_lag(2019), Ok(last_off - 2019));
        assert_eq!(estimator.estimate_offset_lag(2680), Ok(0));
        assert_eq!(estimator.estimate_offset_lag(3000), Ok(0));
    }

    #[test]
    fn should_ignore_updates_that_known_datapoints() {
        let (off, ts) = example_known_offsets();

        // Setup estimator with example input
        let mut estimator = PartitionLagEstimator::new(10);
        for (idx, offset) in off.iter().enumerate() {
            estimator.update(*offset, utc_from_ms(ts[idx]).unwrap());
        }

        assert_eq!(estimator.spare_capacity(), 2);
        estimator.update(off[7], utc_from_ms(ts[7]).unwrap());
        estimator.update(off[7], utc_from_ms(ts[7]).unwrap());
        estimator.update(off[7], utc_from_ms(ts[7]).unwrap());
        assert_eq!(estimator.spare_capacity(), 2);
    }

    #[test]
    fn estimate_time_lag() {
        let (off, ts) = example_known_offsets();

        // Setup estimator with example input
        let mut estimator = PartitionLagEstimator::new(100);
        for (idx, offset) in off.iter().enumerate() {
            estimator.update(*offset, utc_from_ms(ts[idx]).unwrap());
        }

        assert_eq!(
            estimator
                .estimate_time_lag(800, utc_from_ms(1677706399068).unwrap()),
            Ok(Duration::nanoseconds(653148000000))
        );
        assert_eq!(
            estimator
                .estimate_time_lag(1346, utc_from_ms(1677706286068).unwrap()),
            Ok(Duration::milliseconds(0))
        );
        assert_eq!(
            estimator
                .estimate_time_lag(1446, utc_from_ms(1677706399068).unwrap()),
            Ok(Duration::nanoseconds(14071000000))
        );
        assert_eq!(
            estimator
                .estimate_time_lag(1500, utc_from_ms(1677706438418).unwrap()),
            Ok(Duration::nanoseconds(0))
        );
    }

    #[test]
    fn discard_old_known_offsets() {
        let mut estimator = PartitionLagEstimator::new(5);

        // Add first 5 points
        estimator.update(5, utc_from_ms(10).unwrap()); //< empty
        estimator.update(10, utc_from_ms(20).unwrap());
        estimator.update(13, utc_from_ms(22).unwrap());
        estimator.update(21, utc_from_ms(31).unwrap());
        estimator.update(33, utc_from_ms(59).unwrap()); //< at capacity

        // Estimate on and inside the first 2: the lag is predictable,
        // by looking at the data we just entered above for offset `5` and `10`
        assert_eq!(
            estimator.estimate_time_lag(5, utc_from_ms(11).unwrap()),
            Ok(Duration::nanoseconds(1000000))
        );
        assert_eq!(
            estimator.estimate_time_lag(7, utc_from_ms(16).unwrap()),
            Ok(Duration::nanoseconds(2000000))
        );
        assert_eq!(
            estimator.estimate_time_lag(10, utc_from_ms(23).unwrap()),
            Ok(Duration::nanoseconds(3000000))
        );

        // Add 2 more: this should push the first 2 (offsets `5` and `10` off the internal queue)
        estimator.update(39, utc_from_ms(73).unwrap());
        estimator.update(41, utc_from_ms(81).unwrap());

        // Estimation increases for the same point we evaluated above:
        // this happens because the remaining points lead to a interpolation that moves the estimated
        // line for discarded points, a bit back on the x-axis of time.
        assert_eq!(
            estimator.estimate_time_lag(5, utc_from_ms(11).unwrap()),
            Ok(Duration::nanoseconds(6000000))
        );
        assert_eq!(
            estimator.estimate_time_lag(7, utc_from_ms(16).unwrap()),
            Ok(Duration::nanoseconds(7000000))
        );
        assert_eq!(
            estimator.estimate_time_lag(10, utc_from_ms(23).unwrap()),
            Ok(Duration::nanoseconds(7000000))
        );
    }
}
