use std::cmp::Ordering;

use chrono::{DateTime, Utc};

/// An Offset in a Topic Partition, and the date-time at which it is tracked.
///
/// This is used to represent concepts like
/// "the timestamp at which a Topic Partition offset was produced".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct TrackedOffset {
    pub offset: u64,
    pub at: DateTime<Utc>,
}

/// Result of a call to [`search`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TrackedOffsetSearchRes {
    /// Offset found, it was part of the search input.
    Exact(TrackedOffset),

    /// Offset not found, but 2 [`TrackedOffset`] that contain it were found.
    Range(TrackedOffset, TrackedOffset),

    /// Offset not found.
    None,
}

/// Search an offset (`needle`) inside an slice of [`TrackedOffset`] (`haystack`).
///
/// If found, returns a [`TrackedOffsetSearchRes::Exact`]. Alternatively, if the offset is
/// _not_ found _but_ the offsets in the haystack contain it, return the 2 closest
/// [`TrackedOffset`] in a [`TrackedOffsetSearchRes::Range`].
/// Otherwise, it returns [`TrackedOffsetSearchRes::None`].
///
/// # Argument
///
/// * `needle` - Offset we are searching for
/// * `haystack` - Slice of [`TrackedOffset`] to search
pub fn search(needle: u64, haystack: &[TrackedOffset]) -> TrackedOffsetSearchRes {
    // Base case: empty
    if haystack.is_empty() {
        return TrackedOffsetSearchRes::None;
    }

    // Base case: single element
    if haystack.len() == 1 {
        return if haystack[0].offset == needle {
            // `haystack` contains 1 element, and it is `needle`
            TrackedOffsetSearchRes::Exact(haystack[0].clone())
        } else {
            TrackedOffsetSearchRes::None
        };
    }

    // Base case: 2 elements
    if haystack.len() == 2 {
        return if haystack[0].offset == needle {
            TrackedOffsetSearchRes::Exact(haystack[0].clone())
        } else if haystack[1].offset == needle {
            TrackedOffsetSearchRes::Exact(haystack[1].clone())
        } else if haystack[0].offset < needle && needle < haystack[1].offset {
            TrackedOffsetSearchRes::Range(haystack[0].clone(), haystack[1].clone())
        } else {
            TrackedOffsetSearchRes::None
        };
    }

    // Select left `l`, right `r` and pivot `p`: boundaries of the search
    let l: usize = 0;
    let r: usize = haystack.len() - 1;
    let p = (r - l) / 2;

    match needle.cmp(&haystack[p].offset) {
        Ordering::Equal => {
            // Found `needle`
            TrackedOffsetSearchRes::Exact(haystack[p].clone())
        },
        Ordering::Less => {
            if haystack[p - 1].offset < needle {
                // `needle` not found: return the approximate range `[p-1, p]`
                TrackedOffsetSearchRes::Range(haystack[p - 1].clone(), haystack[p].clone())
            } else {
                // Keep searching to the left of pivot
                search(needle, &haystack[0..=p - 1])
            }
        },
        Ordering::Greater => {
            if needle < haystack[p + 1].offset {
                // `needle` not found: return the approximate range `[p, p+1]`
                TrackedOffsetSearchRes::Range(haystack[p].clone(), haystack[p + 1].clone())
            } else {
                // Keep searching to the right of pivot
                search(needle, &haystack[p + 1..=r])
            }
        },
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{FixedOffset, Utc};
    use std::ops::Add;

    fn build_even_input() -> Vec<TrackedOffset> {
        vec![
            TrackedOffset {
                offset: 1,
                at: Utc::now().add(FixedOffset::east_opt(0).unwrap()),
            },
            TrackedOffset {
                offset: 3,
                at: Utc::now().add(FixedOffset::east_opt(100).unwrap()),
            },
            TrackedOffset {
                offset: 7,
                at: Utc::now().add(FixedOffset::east_opt(500).unwrap()),
            },
            TrackedOffset {
                offset: 10,
                at: Utc::now().add(FixedOffset::east_opt(700).unwrap()),
            },
            TrackedOffset {
                offset: 17,
                at: Utc::now().add(FixedOffset::east_opt(1200).unwrap()),
            },
            TrackedOffset {
                offset: 27,
                at: Utc::now().add(FixedOffset::east_opt(2100).unwrap()),
            },
            TrackedOffset {
                offset: 44,
                at: Utc::now().add(FixedOffset::east_opt(3900).unwrap()),
            },
            TrackedOffset {
                offset: 45,
                at: Utc::now().add(FixedOffset::east_opt(3950).unwrap()),
            },
            TrackedOffset {
                offset: 89,
                at: Utc::now().add(FixedOffset::east_opt(6000).unwrap()),
            },
            TrackedOffset {
                offset: 123,
                at: Utc::now().add(FixedOffset::east_opt(11111).unwrap()),
            },
        ]
    }

    fn build_odd_input() -> Vec<TrackedOffset> {
        let res = build_even_input();
        res[0..res.len() - 1].to_vec()
    }

    #[test]
    fn should_find_within_even_input() {
        let input = build_even_input();

        assert!(matches!(search(0, &input), TrackedOffsetSearchRes::None));

        assert!(matches!(
            search(1, &input),
            TrackedOffsetSearchRes::Exact(TrackedOffset {
                offset: 1,
                ..
            })
        ));

        assert!(matches!(
            search(11, &input),
            TrackedOffsetSearchRes::Range(
                TrackedOffset {
                    offset: 10,
                    ..
                },
                TrackedOffset {
                    offset: 17,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(45, &input),
            TrackedOffsetSearchRes::Exact(TrackedOffset {
                offset: 45,
                ..
            })
        ));

        assert!(matches!(
            search(70, &input),
            TrackedOffsetSearchRes::Range(
                TrackedOffset {
                    offset: 45,
                    ..
                },
                TrackedOffset {
                    offset: 89,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(123, &input),
            TrackedOffsetSearchRes::Exact(TrackedOffset {
                offset: 123,
                ..
            })
        ));

        assert!(matches!(search(124, &input), TrackedOffsetSearchRes::None));
        assert!(matches!(search(200, &input), TrackedOffsetSearchRes::None));
        assert!(matches!(search(400, &input), TrackedOffsetSearchRes::None));
    }

    #[test]
    fn should_find_within_odd_input() {
        let input = build_odd_input();

        assert!(matches!(search(0, &input), TrackedOffsetSearchRes::None));

        assert!(matches!(
            search(1, &input),
            TrackedOffsetSearchRes::Exact(TrackedOffset {
                offset: 1,
                ..
            })
        ));

        assert!(matches!(
            search(11, &input),
            TrackedOffsetSearchRes::Range(
                TrackedOffset {
                    offset: 10,
                    ..
                },
                TrackedOffset {
                    offset: 17,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(45, &input),
            TrackedOffsetSearchRes::Exact(TrackedOffset {
                offset: 45,
                ..
            })
        ));

        assert!(matches!(
            search(70, &input),
            TrackedOffsetSearchRes::Range(
                TrackedOffset {
                    offset: 45,
                    ..
                },
                TrackedOffset {
                    offset: 89,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(89, &input),
            TrackedOffsetSearchRes::Exact(TrackedOffset {
                offset: 89,
                ..
            })
        ));

        assert!(matches!(search(123, &input), TrackedOffsetSearchRes::None));
        assert!(matches!(search(200, &input), TrackedOffsetSearchRes::None));
        assert!(matches!(search(400, &input), TrackedOffsetSearchRes::None));
    }
}
