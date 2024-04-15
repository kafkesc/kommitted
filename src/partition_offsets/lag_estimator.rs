use std::collections::VecDeque;

use chrono::{DateTime, Duration, Utc};

use super::errors::{PartitionOffsetsError, PartitionOffsetsResult};
use super::tracked_offset::TrackedOffset;
use super::tracked_offset::{search, TrackedOffsetSearchRes};

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
    /// Earliest offset that the Cluster is storing about a given Topic Partition, still available to consume.
    earliest_available_offset: Option<u64>,

    /// Latest offsets tracked by the estimator for a given Topic Partition.
    ///
    /// The `front` of the [`VecQueue`] is the first "latest offset" we collected of this
    /// topic partition, before new ones were collected: for lack of a better name, it is
    /// the "earliest latest tracked offset".
    ///
    /// The `back` of the [`VecQueue`] is of course the last "latest offset" we collected
    /// of this topic partition.
    ///
    /// Based on the `capacity` provided when calling [`Self::new(usize)`], the `front` and
    /// `back` move like a sliding window, as we don't want the system to keep track of every
    /// offset ever collected. Instead we keep a specific amount (`capacity`) that progresses
    /// towards newer offset information over time.
    latest_tracked_offsets: VecDeque<TrackedOffset>,
}

impl PartitionLagEstimator {
    /// Create new [`PartitionLagEstimator`] of given capacity for [`TrackedOffset`]s.
    ///
    /// As a rule of thumb, we allocate enough to fit 1 call to [`update`] per second:
    /// every second a new `latest_offset` would be added to the estimator.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The amount of data points (i.e. history of tracked offsets) we want to keep
    pub fn new(capacity: usize) -> PartitionLagEstimator {
        PartitionLagEstimator {
            earliest_available_offset: None,
            latest_tracked_offsets: VecDeque::with_capacity(capacity),
        }
    }

    /// Update estimator with a new data points.
    ///
    /// It will automatically remove the oldest tracked offset, if the internal collection
    /// has reached capacity (decided at creation time).
    ///
    /// NOTE: This will ignore any `new_latest` offset data point,
    /// that is in the past or already tracked.
    ///
    /// # Arguments
    ///
    /// * `new_earliest_available` - An old offset that is now the earliest still available into the cluster;
    ///   this is NOT the the same as the "earliest latest tracked", but what actually is the
    ///   earliest offset that the cluster still holds; it means that any previous offset
    ///   has been discarded by Kafka
    /// * `new_latest_tracked` - A new offset that should become the new latest tracked
    /// * `new_latest_tracked_datetime` - The [`DateTime<Utc>`] of the new offset
    pub fn update(
        &mut self,
        new_earliest_available: u64,
        new_latest_tracked: u64,
        new_latest_tracked_datetime: DateTime<Utc>,
    ) {
        // Update the earliest offset available in the cluster
        if let Some(eso) = self.earliest_available_offset {
            if eso > new_earliest_available {
                warn!(
                    "Update with earliest available offset {} precedes current {}: should never happen",
                    new_earliest_available, eso
                )
            }
        }
        self.earliest_available_offset = Some(new_earliest_available);

        // Validate the input, comparing to the latest tracked offset
        if let Some(curr_latest) = self.latest_tracked_offsets.back() {
            if curr_latest.offset == new_latest_tracked {
                // Ignore update if we already know this offset
                trace!("Update with offset {} already tracked: ignoring", curr_latest.offset);
                return;
            } else if curr_latest.offset > new_latest_tracked {
                // Unlikely scenario: ignore update if the offset precedes latest tracked
                warn!(
                    "Update with offset {} that precedes current latest {}: ignoring",
                    new_latest_tracked, curr_latest.offset
                );
                return;
            } else if curr_latest.offset < new_latest_tracked
                && curr_latest.at > new_latest_tracked_datetime
            {
                // Very unlikely scenario: ignore update if offset date-time precedes latest tracked
                warn!(
                    "Update with offset {} of date-time '{}' that precedes current latest {} of '{}': ignoring",
                    new_latest_tracked, new_latest_tracked_datetime, curr_latest.offset, curr_latest.at
                );
                return;
            }
        }

        // If we have no more spare capacity, drop the front instead of letting capacity grow
        if self.spare_capacity() == 0 {
            self.latest_tracked_offsets.pop_front();
        }

        // Append to the back
        self.latest_tracked_offsets.push_back(TrackedOffset {
            offset: new_latest_tracked,
            at: new_latest_tracked_datetime,
        });

        // Ensure it's a contiguous slice, so that we can do binary search on it
        // when estimating.
        self.latest_tracked_offsets.make_contiguous();
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
    /// * `offset` - Given offset we want to compare against the latest tracked offset
    pub fn estimate_offset_lag(&self, offset: u64) -> PartitionOffsetsResult<u64> {
        let lto = self.latest_tracked_offset()?.offset;

        // It's rare, but if we happen to receive a consumed offset that is ahead of the last
        // tracked end offset, we can just return `0` for lag.
        if offset > lto {
            Ok(0)
        } else {
            Ok(lto - offset)
        }
    }

    /// Estimate time lag.
    ///
    /// Extrapolates the given consumer group offset and related read date time for this partition,
    /// a returns a [`Duration`] estimation of the time lag accumulated by the consumer group.
    ///
    /// This estimation is done by a linear interpolation/extrapolation, where the fixed points
    /// are the [`TrackedOffset`]s contained in the [`PartitionLagEstimator`] at the time of call.
    ///
    /// # Arguments
    ///
    /// * `offset` - Given offset we want to compare against the latest tracked offset
    /// * `offset_datetime` - The [`DateTime<Utc>`] this offset was committed
    pub fn estimate_time_lag(
        &self,
        offset: u64,
        offset_datetime: DateTime<Utc>,
    ) -> PartitionOffsetsResult<Duration> {
        // It's rare, but if we happen to receive a consumed offset that is ahead of the last
        // tracked end offset, we can just return a Duration of `0` for time lag.
        let lto = self.latest_tracked_offset()?.offset;
        if offset > lto {
            return Ok(Duration::zero());
        }

        // NOTE: Please look up `VecDequeue::make_contiguous()` that we call every time we update
        // the internal collection, for this to make sense.
        //
        // At this stage we need slice-type access to the content for search, and because internally
        // a `VecDequeue` is a ring-buffer, calls to `make_contiguous()` ensure that we get all
        // the content in a single borrowed slice when we get here.
        let (slice, _) = self.latest_tracked_offsets.as_slices();

        let search_res = search(offset, slice);

        let estimated_produced_offset_datetime = match search_res {
            TrackedOffsetSearchRes::Exact(found) => found.at,
            TrackedOffsetSearchRes::Range(tracked_before, tracked_after) => {
                interpolate_offset_to_datetime(&tracked_before, &tracked_after, offset)?
            },
            TrackedOffsetSearchRes::None => {
                let earliest_tracked = self.earliest_tracked_offset()?;
                let latest_tracked = self.latest_tracked_offset()?;
                let second_latest_tracked = self.nth_latest_tracked_offset(2)?;

                // Estimate production time, considering widest range possible: earliest and latest tracked
                let widest_estimate =
                    interpolate_offset_to_datetime(earliest_tracked, latest_tracked, offset)?;

                // Estimate production time, considering narrowest range possible: 2nd-latest and latest tracked
                let narrowest_estimate =
                    interpolate_offset_to_datetime(second_latest_tracked, latest_tracked, offset)?;

                // Return the average of the 2 estimates
                if widest_estimate < narrowest_estimate {
                    widest_estimate + (narrowest_estimate - widest_estimate)
                } else {
                    narrowest_estimate + (widest_estimate - narrowest_estimate)
                }
            },
        };

        // It's infrequent, but when we receive a consumed offset datetime that is AHEAD
        // of the estimated production datetime, we return zero.
        //
        // While it's not possible for an offset to be consumed before it's produced (obviously),
        // it can happen that the linear interpolation done above, estimates the production time
        // to be later then it ACTUALLY was.
        //
        // When that happen, is perfectly ok to consider the time lag to be EFFECTIVELY zero.
        if offset_datetime < estimated_produced_offset_datetime {
            Ok(Duration::zero())
        } else {
            Ok(offset_datetime - estimated_produced_offset_datetime)
        }
    }

    /// How many [`TrackedOffset`] are stored.
    pub fn usage(&self) -> usize {
        self.latest_tracked_offsets.len()
    }

    /// Given the constructor-time `capacity`, how much capacity is left spare, before
    /// a new [`PartitionLagEstimator::update()`] call will need to drop the earliest tracked?
    pub fn spare_capacity(&self) -> usize {
        self.latest_tracked_offsets.capacity() - self.latest_tracked_offsets.len()
    }

    /// Given the constructor-time `capacity`, at how much usage percent is it, before
    /// a new [`PartitionLagEstimator::update()`] call will need to drop the earliest tracked?
    ///
    /// This is useful to assess how "full" is the `PartitionLagEstimator`.
    pub fn usage_percent(&self) -> f64 {
        self.latest_tracked_offsets.len() as f64 / self.latest_tracked_offsets.capacity() as f64
            * 100_f64
    }

    /// Get the earliest offset available in the cluster
    pub fn earliest_available_offset(&self) -> PartitionOffsetsResult<u64> {
        self.earliest_available_offset.ok_or(PartitionOffsetsError::LagEstimatorNotReady)
    }

    /// Get the latest offset available in the cluster
    pub fn latest_available_offset(&self) -> PartitionOffsetsResult<u64> {
        self.latest_tracked_offset().map(|ko| ko.offset)
    }

    /// Get a reference to the earliest [`TrackedOffset`].
    pub fn earliest_tracked_offset(&self) -> PartitionOffsetsResult<&TrackedOffset> {
        self.latest_tracked_offsets.front().ok_or(PartitionOffsetsError::LagEstimatorNotReady)
    }

    /// Get a reference to the latest [`TrackedOffset`]
    pub fn latest_tracked_offset(&self) -> PartitionOffsetsResult<&TrackedOffset> {
        self.latest_tracked_offsets.back().ok_or(PartitionOffsetsError::LagEstimatorNotReady)
    }

    /// Get a reference to the Nth latest [`TrackedOffset`]
    pub fn nth_latest_tracked_offset(&self, pos: usize) -> PartitionOffsetsResult<&TrackedOffset> {
        self.latest_tracked_offsets
            .get(self.latest_tracked_offsets.len().wrapping_sub(pos))
            .ok_or(PartitionOffsetsError::LagEstimatorNotReady)
    }
}

/// Interpolate [`TrackedOffset`]s and Kafka Topic Partition offset, to get a [`DateTime<Utc>`].
///
/// The "cartesian plan" can imagined with _time_ on the _x-axis_, and _offset_ on the _y-axis_.
///
/// # Arguments
///
/// * `p1` - First point for the linear interpolation
/// * `p2` - Second point for the linear interpolation
/// * `y_offset` - The _y_ offset coordinate we want to find the _x_ [`DateTime<Utc>`] coordinate of.
fn interpolate_offset_to_datetime(
    p1: &TrackedOffset,
    p2: &TrackedOffset,
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
    DateTime::<Utc>::from_timestamp_millis(utc_timestamp_ms)
        .ok_or(PartitionOffsetsError::UtcTimestampMillisInvalid(utc_timestamp_ms))
}

#[cfg(test)]
mod test {
    use chrono::Duration;

    use crate::partition_offsets::lag_estimator::{
        interpolate_offset_to_datetime, utc_from_ms, PartitionLagEstimator,
    };
    use crate::partition_offsets::tracked_offset::TrackedOffset;

    fn example_tracked_offsets() -> (Vec<u64>, Vec<i64>) {
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
        let (off, ts) = example_tracked_offsets();

        let p1 = TrackedOffset {
            offset: off[0],
            at: utc_from_ms(ts[0]).unwrap(),
        };

        let p2 = TrackedOffset {
            offset: off[7],
            at: utc_from_ms(ts[7]).unwrap(),
        };

        let mut prev = off[0];
        let mut curr;
        for (idx, offset) in off[1..].iter().enumerate() {
            curr = interpolate_offset_to_datetime(&p1, &p2, *offset).unwrap().timestamp_millis();

            assert!(prev < curr as u64);
            assert_eq!(ts[idx + 1], curr);

            prev = curr as u64;
        }
    }

    #[test]
    fn estimate_offset_lag() {
        let (off, ts) = example_tracked_offsets();

        // Setup estimator with example input
        let mut estimator = PartitionLagEstimator::new(10);
        for (idx, offset) in off.iter().enumerate() {
            estimator.update(10, *offset, utc_from_ms(ts[idx]).unwrap());
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
    fn should_ignore_updates_that_tracked_datapoints() {
        let (off, ts) = example_tracked_offsets();

        // Setup estimator with example input
        let mut estimator = PartitionLagEstimator::new(10);
        for (idx, offset) in off.iter().enumerate() {
            estimator.update(1, *offset, utc_from_ms(ts[idx]).unwrap());
        }

        assert_eq!(estimator.spare_capacity(), 2);
        estimator.update(10, off[7], utc_from_ms(ts[7]).unwrap());
        estimator.update(11, off[7], utc_from_ms(ts[7]).unwrap());
        estimator.update(12, off[7], utc_from_ms(ts[7]).unwrap());
        assert_eq!(estimator.spare_capacity(), 2);
    }

    #[test]
    fn estimate_time_lag() {
        let (off, ts) = example_tracked_offsets();

        // Setup estimator with example input
        let mut estimator = PartitionLagEstimator::new(10);
        for (idx, offset) in off.iter().enumerate() {
            estimator.update(10, *offset, utc_from_ms(ts[idx]).unwrap());
        }

        assert_eq!(
            estimator.estimate_time_lag(800, utc_from_ms(1677706399068).unwrap()),
            Ok(Duration::nanoseconds(653148000000))
        );
        assert_eq!(
            estimator.estimate_time_lag(1346, utc_from_ms(1677706286068).unwrap()),
            Ok(Duration::milliseconds(0))
        );
        assert_eq!(
            estimator.estimate_time_lag(1446, utc_from_ms(1677706399068).unwrap()),
            Ok(Duration::nanoseconds(14071000000))
        );
        assert_eq!(
            estimator.estimate_time_lag(1500, utc_from_ms(1677706438418).unwrap()),
            Ok(Duration::zero())
        );
    }

    #[test]
    fn discard_old_tracked_offsets() {
        let mut estimator = PartitionLagEstimator::new(5);

        // Add first 5 points
        estimator.update(1, 5, utc_from_ms(10).unwrap()); //< empty
        estimator.update(1, 10, utc_from_ms(20).unwrap());
        estimator.update(1, 13, utc_from_ms(22).unwrap());
        estimator.update(1, 21, utc_from_ms(31).unwrap());
        estimator.update(1, 33, utc_from_ms(59).unwrap()); //< at capacity

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
        estimator.update(2, 39, utc_from_ms(73).unwrap());
        estimator.update(2, 41, utc_from_ms(81).unwrap());

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

    #[test]
    fn use_percent() {
        let (off, ts) = example_tracked_offsets();

        let mut estimator = PartitionLagEstimator::new(10);

        // Check how usage percent grows along the way, but remains below 100% (extra capacity available)
        assert_eq!(estimator.usage_percent(), 0_f64);
        for (idx, offset) in off.iter().enumerate() {
            assert_eq!(estimator.usage_percent(), idx as f64 * 10_f64);
            estimator.update(1, *offset, utc_from_ms(ts[idx]).unwrap());
            assert_eq!(estimator.usage_percent(), (idx + 1) as f64 * 10_f64);
        }
        assert_eq!(estimator.usage_percent(), 80_f64);

        let mut estimator = PartitionLagEstimator::new(5);

        // Check how usage percent grows along the way, but reaches and stays at 100% (no extra capacity available)
        assert_eq!(estimator.usage_percent(), 0_f64);
        for (idx, offset) in off.iter().enumerate() {
            assert_eq!(estimator.usage_percent(), ((idx) as f64 * 20_f64).min(100_f64));
            estimator.update(2, *offset, utc_from_ms(ts[idx]).unwrap());
            assert_eq!(estimator.usage_percent(), ((idx + 1) as f64 * 20_f64).min(100_f64));
        }
        assert_eq!(estimator.usage_percent(), 100_f64);
    }
}
